use clap::Parser;
use std::io::{Read, Write};
use std::sync::Arc;
use std::time::Instant;

use ssp_core::Pubkey;
use ssp_core::filters::ResolvedFilters;

mod bench;
mod db;
mod pipeline;
mod rpc;
mod tui;

#[derive(clap::Args, Debug, Clone)]
pub struct Filters {
    #[arg(long)]
    pub owner: Option<String>,

    #[arg(long)]
    pub hash: Option<String>,

    #[arg(long)]
    pub pubkey: Option<String>,

    #[arg(long, default_value = "false")]
    pub include_dead: bool,

    #[arg(long, default_value = "false")]
    pub include_spam: bool,
}

impl Filters {
    pub fn resolve(&self) -> Result<ResolvedFilters, anyhow::Error> {
        Ok(ResolvedFilters {
            owner: Pubkey::try_from_b58(self.owner.as_deref())?,
            hash: decode_b58_32(&self.hash)?,
            pubkey: Pubkey::try_from_b58(self.pubkey.as_deref())?,
            include_dead: self.include_dead,
            include_spam: self.include_spam,
        })
    }
}

fn decode_b58_32(input: &Option<String>) -> Result<Option<[u8; 32]>, anyhow::Error> {
    input
        .as_deref()
        .map(|s| {
            let mut buf = [0u8; 32];
            bs58::decode(s).onto(&mut buf)?;
            Ok(buf)
        })
        .transpose()
}

#[derive(Parser, Debug)]
#[command(version, about)]
pub struct CliArgs {
    #[arg(short, long)]
    path: Option<String>,

    #[arg(long)]
    bench: bool,

    #[arg(long)]
    discover: bool,

    #[arg(long)]
    incremental: bool,

    #[arg(long, default_value = "false", conflicts_with = "download_incremental")]
    download_full: bool,

    #[arg(long, default_value = "false", conflicts_with = "download_full")]
    download_incremental: bool,

    #[arg(long, help = "Output directory for download mode")]
    output: Option<String>,

    #[command(flatten)]
    filters: Filters,
}

fn main() -> anyhow::Result<()> {
    let args = CliArgs::parse();

    if args.download_full || args.download_incremental {
        let incremental = args.download_incremental;
        download_snapshot(incremental, args.output.as_deref())?;
        return Ok(());
    }

    if args.bench {
        let path = args.path.as_deref().expect("--bench requires --path");
        eprintln!("=== Stage 1: zstd only ===");
        bench::run(std::fs::File::open(path)?);
        eprintln!("\n=== Stage 2: zstd + tar ===");
        bench::run_tar(std::fs::File::open(path)?);
        eprintln!("\n=== Stage 3: zstd + tar + parse ===");
        bench::run_full(std::fs::File::open(path)?);
        return Ok(());
    }

    if !args.bench && args.path.is_none() && !args.discover {
        return tui::run_interactive();
    }

    let filters = args.filters.resolve()?;

    let reader: Box<dyn Read + Send> = if let Some(path) = &args.path {
        Box::new(std::fs::File::open(path)?)
    } else if args.discover {
        let rt = tokio::runtime::Runtime::new()?;
        let source = rt.block_on(rpc::find_fastest_snapshot(None, args.incremental))?;
        eprintln!(
            "streaming from {} ({:.1} MB/s, {:.1} GB)",
            source.url,
            source.speed_mbps,
            source.size.unwrap_or(0) as f64 / 1_073_741_824.0
        );
        let resp = reqwest::blocking::Client::builder()
            .timeout(None)
            .build()?
            .get(&source.url)
            .send()?;
        Box::new(resp)
    } else {
        unreachable!()
    };

    let stats = Arc::new(pipeline::PipelineStats::new());
    let stats_clone = stats.clone();

    let start = std::time::Instant::now();
    pipeline::run(reader, filters, stats_clone)?;
    let elapsed = start.elapsed();

    let rows = stats.rows_parsed.load(std::sync::atomic::Ordering::Relaxed);
    let bytes = stats.bytes_read.load(std::sync::atomic::Ordering::Relaxed);
    eprintln!(
        "done: {} rows, {:.1} GB read in {:.1}s",
        rows,
        bytes as f64 / 1_073_741_824.0,
        elapsed.as_secs_f64(),
    );

    Ok(())
}

fn download_snapshot(incremental: bool, output: Option<&str>) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    let source = rt.block_on(rpc::find_fastest_snapshot(None, incremental))?;

    let source_name = infer_snapshot_filename(&source.url).ok_or_else(|| {
        anyhow::anyhow!(
            "failed to infer snapshot filename from source url: {} (pass --output <dir> to choose a directory, filename always follows upstream)",
            source.url
        )
    })?;

    let output_dir = output.unwrap_or("/mnt/snapshot");
    let filename = std::path::Path::new(output_dir)
        .join(&source_name)
        .to_string_lossy()
        .to_string();

    eprintln!(
        "downloading {} snapshot from fastest source:\n  {}\n  speed probe: {:.1} MB/s\n  size: {:.1} GB\n  output: {}",
        if incremental { "incremental" } else { "full" },
        source.url,
        source.speed_mbps,
        source.size.unwrap_or(0) as f64 / 1_073_741_824.0,
        filename,
    );

    let mut resp = reqwest::blocking::Client::builder()
        .timeout(None)
        .build()?
        .get(&source.url)
        .send()?
        .error_for_status()?;

    if let Some(parent) = std::path::Path::new(&filename).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut file = std::fs::File::create(&filename)?;
    let mut buf = vec![0u8; 1024 * 1024];
    let mut downloaded: u64 = 0;
    let total = source
        .size
        .or_else(|| resp.content_length())
        .filter(|v| *v > 0);
    let start = Instant::now();
    let mut last_report = Instant::now();

    loop {
        let n = resp.read(&mut buf)?;
        if n == 0 {
            break;
        }
        file.write_all(&buf[..n])?;
        downloaded += n as u64;

        if last_report.elapsed().as_secs_f64() >= 1.0 {
            let secs = start.elapsed().as_secs_f64();
            let speed = if secs > 0.0 {
                downloaded as f64 / 1_048_576.0 / secs
            } else {
                0.0
            };

            if let Some(total) = total {
                let pct = (downloaded as f64 / total as f64 * 100.0).min(100.0);
                eprintln!(
                    "downloaded {:.2}/{:.2} GB ({:.1}%) at {:.1} MB/s",
                    downloaded as f64 / 1_073_741_824.0,
                    total as f64 / 1_073_741_824.0,
                    pct,
                    speed
                );
            } else {
                eprintln!(
                    "downloaded {:.2} GB at {:.1} MB/s",
                    downloaded as f64 / 1_073_741_824.0,
                    speed
                );
            }

            last_report = Instant::now();
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    eprintln!(
        "download complete: {:.2} GB in {:.1}s ({:.1} MB/s)",
        downloaded as f64 / 1_073_741_824.0,
        elapsed,
        if elapsed > 0.0 {
            downloaded as f64 / 1_048_576.0 / elapsed
        } else {
            0.0
        }
    );

    Ok(())
}

fn infer_snapshot_filename(url: &str) -> Option<String> {
    let parsed = reqwest::Url::parse(url).ok()?;
    parsed
        .path_segments()?
        .filter(|segment| !segment.is_empty())
        .next_back()
        .map(ToOwned::to_owned)
}
