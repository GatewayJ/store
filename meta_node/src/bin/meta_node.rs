    
use std::str::FromStr;
use clap::Parser;
use msc_meta_node::server::ServerBuilder;
use tracing_subscriber::EnvFilter;
use tikv_jemallocator::Jemalloc;
use uuid::Uuid;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
struct Opt {
    #[clap(long)]
    pub id: u64,
    #[clap(long)]
    pub rpc_addr: String,
    #[arg(long, group="create")]
    pub new_raft: Option<String>,
    #[arg(long, group="create")]
    pub new_learner: Option<String>,
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
    let db_path = format!("{}", opt.rpc_addr);

    let mut builder = ServerBuilder::new(opt.id, opt.rpc_addr, db_path);
    if let Some(uuid) = opt.new_raft.as_ref().or(opt.new_learner.as_ref()) {
        builder.create(Uuid::from_str(uuid)?);
    }

    let mut server = builder.build().await?;

    if let Some(uuid) = opt.new_raft {
        server.init_raft().await?;
        eprintln!("succeed in creating new raft cluster {}", uuid);
    } else if let Some(uuid) = opt.new_learner {
        eprintln!("succeed in creating learner for raft cluster {}", uuid);
    }

    server.serve().await?;
    Ok(())
}
