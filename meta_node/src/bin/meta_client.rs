use msc_meta_node::{
    MetaNode, RaftChangeMembers, client::MetaClient 
};
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
    let mut client = MetaClient::new(vec![(0, MetaNode::new(opt.rpc_addr))]);

    //client.add_raft_leaner((2, Node::new("127.0.0.1:5002".to_string())), true).await.unwrap();
    //client.add_raft_leaner((3, Node::new("127.0.0.1:5003".to_string())), true).await.unwrap();

    let members = RaftChangeMembers::ReplaceAllVoters([1, 2, 3].into());
    client.change_raft_membership(members, true).await.unwrap();

    let metrics = client.get_raft_metrics().await.unwrap();
    println!("{:?}", metrics);

    Ok(())
}
