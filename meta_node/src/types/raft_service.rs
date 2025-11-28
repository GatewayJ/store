tonic::include_proto!("raft_service");

pub use raft_service_server::RaftServiceServer;
pub use raft_service_client::RaftServiceClient;
use serde::{Deserialize, Serialize};
use super::{RaftVote, RaftSnapshotMeta};

impl RaftRequest {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data, }
    }
}

impl RaftResponse {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data, }
    }
}

impl PushSnapshot {
    pub fn with_request(request: Vec<u8>) -> Self {
        Self {
            request,
            ..Default::default()
        }
    }
    pub fn with_cf(cf_name: &str) -> Self {
        Self {
            cf_name: cf_name.to_string(),
            ..Default::default()
        }
    }
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.key_vals.push(KeyValue { key, value });
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FullSnapshotRequest {
    pub vote: RaftVote,
    pub snap_meta: RaftSnapshotMeta,
}
