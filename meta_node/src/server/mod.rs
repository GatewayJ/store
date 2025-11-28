mod data_node;
mod meta_cache;
mod extent;
mod log_stream;
mod client_session;
mod meta_service;
mod stream_service;
mod recovery;
pub(crate) mod merge;
pub(crate) mod utils;

use std::{ sync::Arc, collections::BTreeMap };
use crate::{
    network::RaftNetworkImpl,
    store::{ create_storage, open_storage },
    types::{
        Node, NodeId, TypeConfig, SnapshotPolicy, 
        common_proto::ClientId,
        dnode_service::DataNodeServiceServer,
        mngmt_service::MngmtServiceServer,
        raft_service::RaftServiceServer,
        stream_service::StreamServiceServer,
    }
};
use uuid::Uuid;

pub(crate) use meta_service::MetaNodeService;

pub struct ClientRequestContext {
    pub client_id: Option<ClientId>,
}

pub struct Server {
    pub node_id: NodeId,
    pub rpc_addr: String,
    pub(crate) db: Arc<rocksdb::DB>,
    pub(crate) raft: Arc<openraft::Raft<TypeConfig>>,
    pub(crate) raft_cfg: Arc<openraft::Config>,
}

impl Server {
    pub async fn serve(self) -> anyhow::Result<()> {
        let rpc_addr = self.rpc_addr.parse()?;
        let stream_svc = MetaNodeService::new(self.raft, self.db);

        tonic::transport::Server::builder()
            .add_service(RaftServiceServer::from_arc(stream_svc.clone()))
            .add_service(DataNodeServiceServer::from_arc(stream_svc.clone()))
            .add_service(MngmtServiceServer::from_arc(stream_svc.clone()))
            .add_service(StreamServiceServer::from_arc(stream_svc.clone()))
            .serve(rpc_addr)
            .await
            .map_err(|err| {
                eprintln!("failed to start rpc service, {:?}", err);
                err
            })?;

        Ok(())
    }

    pub async fn init_raft(&mut self) -> anyhow::Result<()> {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            self.node_id, Node::new(self.rpc_addr.clone())
        );
        self.raft.initialize(nodes).await?;
        Ok(())
    }
}

pub struct ServerBuilder {
    id: NodeId,
    rpc_addr: String,
    db_path: String,
    raft_cfg: openraft::Config,
    create_with_uuid: Option<Uuid>,
}

impl ServerBuilder {
    pub fn new(id: NodeId, rpc_addr: String, db_path: String) -> Self {
        let raft_cfg = openraft::Config {
            heartbeat_interval: 500,
            election_timeout_min: 1000,
            election_timeout_max: 2000,
            install_snapshot_timeout: 1000 * 3600,
            max_payload_entries: 128,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(1000000),
            ..Default::default()
        };

        Self {
            id, rpc_addr, db_path, raft_cfg,
            create_with_uuid: None,
        }
    }

    pub fn create(&mut self, uuid: Uuid) -> &mut Self {
        self.create_with_uuid.replace(uuid);
        self
    }

    pub async fn build(mut self) -> anyhow::Result<Server> {
        if let Some(uuid) = self.create_with_uuid {
            create_storage(self.id, uuid, &self.db_path).await?;
        }
        let (db, uuid, log_store, sm_store) =
            open_storage(self.id, &self.db_path).await?;

        self.raft_cfg.cluster_name = uuid.to_string();
        let raft_cfg = Arc::new(self.raft_cfg.validate()?);

        let sm_data= sm_store.shared_data.clone();
        let raft = Arc::new(openraft::Raft::new(
            self.id, raft_cfg.clone(), RaftNetworkImpl::new(sm_data),
            log_store, sm_store
        ).await?);

        Ok(Server { 
            node_id: self.id,
            rpc_addr: self.rpc_addr,
            db, raft, raft_cfg,
        })
    }
}

