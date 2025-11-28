use std::fmt::Debug;

pub mod raft_service;
pub mod mngmt_service;
pub mod stream_service;
pub mod dnode_service;
pub mod common_proto;
use raft_service::PushSnapshot;
use crate::store::{ StoreRequest, StoreResponse };

pub type NodeId = u64;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
pub struct Node {
    pub rpc_addr: String,
}

impl Node {
    pub fn new(rpc_addr: String) -> Self {
        Self { rpc_addr }
    }
}

#[derive(Debug)]
pub enum SnapshotData {
    None,
    Receiver(tonic::Streaming<PushSnapshot>)
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = StoreRequest,
        R = StoreResponse,
        Node = Node,
        SnapshotData = SnapshotData,
);

pub use openraft::SnapshotPolicy;

pub type RaftVote = openraft::Vote<NodeId>;
pub type RaftMetrics = openraft::metrics::RaftMetrics<NodeId, Node>;
pub type RaftChangeMembers = openraft::ChangeMembers<NodeId, Node>;
pub type RaftSnapshot = openraft::Snapshot<TypeConfig>;
pub type RaftSnapshotMeta = openraft::SnapshotMeta<NodeId, Node>;
pub type RaftStorageError = openraft::StorageError<NodeId>;
pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;
pub type RaftServerState = openraft::ServerState;
pub type ForwardToLeader = openraft::error::ForwardToLeader<NodeId, Node>;
pub type ClientWriteError = openraft::error::ClientWriteError<NodeId, Node>;

