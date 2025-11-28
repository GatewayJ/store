use msc_data_node::server::{ServerBuilder, DataNodeConfig};
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    #[clap(long)]
    pub conf: String,
    #[arg(long)]
    pub new_node: bool,
    #[arg(long)]
    pub new_volume: Vec<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup the logger
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let opt = Opt::parse();
    let config = DataNodeConfig::load(&opt.conf)?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    let mut builder = ServerBuilder::new(config);

    if opt.new_node {
        builder = runtime.block_on(async move {
            match builder.create(&opt.conf).await {
                Ok(id) => {
                    eprintln!("succeed in creating new node {:?}", id);
                    Ok(builder)
                }
                Err(err) => Err(err),
            }
        })?;
    }

    let runtime1 = runtime.handle().clone();
    runtime.block_on(async move {
        let mut server = builder.build(runtime1).await?;

        if let Err(err) = server.load_volumes().await {
            eprintln!("failed to load any volume, {:?}", err);
        }

        for bdev in opt.new_volume.iter() {
            match server.create_volume(bdev.to_owned()).await {
                Ok(id) => {
                    eprintln!("succeed in creating volume on {}, {:?}", bdev, id);
                }
                Err(err) => {
                    eprintln!("failed to create volume on {}, {:?}", bdev, err);
                    return Err(err);
                }
            }
        }

        server.serve().await?;
        Ok::<_, anyhow::Error>(())
    })?;
    Ok(())
}