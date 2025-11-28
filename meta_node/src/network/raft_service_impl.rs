use std::fmt::{Debug, Display};
use futures::StreamExt;
use serde::Serialize;
use crate::{
    server::MetaNodeService,
    types::{
        RaftSnapshot, SnapshotData, RaftError,
        raft_service::{
            raft_service_server::RaftService,
            FullSnapshotRequest, PushSnapshot, RaftRequest, RaftResponse
        }
    }
};

fn map_raft_error<E: Serialize + Debug + Display>(e: RaftError<E>) -> tonic::Status {
    bincode::serialize(&e)
        .map_or_else(
            |_| tonic::Status::internal(e.to_string()),
            |v| tonic::Status::with_details(tonic::Code::Internal, e.to_string(), v.into())
        )
}

#[tonic::async_trait]
impl RaftService for MetaNodeService {
    async fn append_entries(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftResponse>, tonic::Status> {
        let req = request.into_inner();
        let ae_req = bincode::deserialize(&req.data)
            .map_err(|x| tonic::Status::internal(x.to_string()))?;
        let resp = self.raft.append_entries(ae_req)
            .await
            .map_err(map_raft_error)?;
        let data = bincode::serialize(&resp).expect("fail to serialize resp");
        Ok(tonic::Response::new(RaftResponse::new(data)))
    }

    async fn vote(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftResponse>, tonic::Status> {
        let req = request.into_inner();
        let v_req = bincode::deserialize(&req.data)
            .map_err(|x| tonic::Status::internal(x.to_string()))?;
        let resp = self.raft.vote(v_req)
            .await
            .map_err(map_raft_error)?;
        let data = bincode::serialize(&resp).expect("fail to serialize resp");
        Ok(tonic::Response::new(RaftResponse::new(data)))
    }

    async fn full_snapshot(
        &self,
        request: tonic::Request<tonic::Streaming<PushSnapshot>>,
    ) -> std::result::Result<tonic::Response<RaftResponse>, tonic::Status> {
        let mut in_stream = request.into_inner();
        let req : PushSnapshot = in_stream.next().await
            .ok_or(tonic::Status::invalid_argument("no request"))??;
        
        let s_req : FullSnapshotRequest = bincode::deserialize(&req.request)
            .map_err(|x| tonic::Status::internal(x.to_string()))?;

        let snapshot = RaftSnapshot {
            meta: s_req.snap_meta,
            snapshot: Box::new(SnapshotData::Receiver(in_stream))
        };

        let resp = self.raft.install_full_snapshot(s_req.vote, snapshot)
            .await
            .map_err(|e| {
                bincode::serialize(&e)
                    .map_or_else(
                        |_| tonic::Status::internal(e.to_string()),
                        |v| tonic::Status::with_details(tonic::Code::Internal, e.to_string(), v.into())
                    )
            })?;

        let data = bincode::serialize(&resp).expect("fail to serialize resp");
        Ok(tonic::Response::new(RaftResponse::new(data)))
    }
}
