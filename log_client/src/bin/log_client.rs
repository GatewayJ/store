use std::time::Duration;

use common_proto::{LogStreamId, LogStreamConfig};

use msc_log_client::{MetaNode, LogClientBuilder};
use tracing_subscriber::EnvFilter;
use clap::Parser;

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    #[clap(long)]
    pub rpc_addr: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup the logger
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let opt = Opt::parse();
    let mut log_client = LogClientBuilder::new(vec![MetaNode::new(opt.rpc_addr)])
        .build()
        .await?;
    println!("log_clent created");

    let create = true;
    let log_stream = if create {
        log_client.create_stream(LogStreamConfig { 
            extent_size: 1024 * 1024 * 128, 
            num_replicas: 3 
        }).await
    } else {
        log_client.open_stream(LogStreamId::new(2)).await
    }?;
    println!("log_stream {}", if create { "created" } else { "opened" });

    let last_extent = log_stream.last_extent().await;
    println!("{:?}", last_extent);

    tokio::time::sleep(Duration::from_secs(60)).await;

    log_stream.close().await?;

    Ok(())
}

