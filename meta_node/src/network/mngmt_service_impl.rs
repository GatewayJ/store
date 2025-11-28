use crate::{
    server::{MetaNodeService, utils::map_raft_error},
    types::{
        Node, RaftChangeMembers,
        mngmt_service::{
            RaftMngmtRequest, RaftMngmtResponse, RaftMngmtRequestFlags,
            VolumeMngmtRequest, VolumeMngmtResponse,
            mngmt_service_server::MngmtService,
        }
    },
};

#[tonic::async_trait]
impl MngmtService for MetaNodeService {
    async fn add_raft_leaner(
        &self,
        request: tonic::Request<RaftMngmtRequest>,
    ) -> Result<tonic::Response<RaftMngmtResponse>, tonic::Status> {
        let mut req = request.into_inner();
        let node = req.nodes.pop()
            .ok_or(tonic::Status::invalid_argument("no node specified"))?;
        let blocking = req.flags & RaftMngmtRequestFlags::Blocking as u32 != 0;
        let resp = self.raft
            .add_learner(node.id, Node::new(node.rpc_addr), blocking)
            .await
            .map_err(map_raft_error)?;
        let data = serde_json::to_string(&resp).expect("fail to serialize resp");
        Ok(tonic::Response::new(RaftMngmtResponse::new(data)))
    }

    async fn change_raft_membership(
        &self,
        request: tonic::Request<RaftMngmtRequest>,
    ) -> Result<tonic::Response<RaftMngmtResponse>, tonic::Status> {
        let req = request.into_inner();
        let retain = req.flags & RaftMngmtRequestFlags::Retain as u32 != 0;
        let members: RaftChangeMembers = req.try_into()
            .map_err(|_| tonic::Status::invalid_argument("invalid opcode"))?;
        let resp = self.raft.change_membership(members, retain)
            .await
            .map_err(map_raft_error)?;
        let data = serde_json::to_string(&resp).expect("fail to serialize resp");
        Ok(tonic::Response::new(RaftMngmtResponse::new(data)))
    }

    async fn get_raft_metrics(
        &self,
        _req: tonic::Request<RaftMngmtRequest>,
    ) -> Result<tonic::Response<RaftMngmtResponse>, tonic::Status> {
        let metrics = self.raft.metrics().borrow().clone();
        let data = serde_json::to_string(&metrics).expect("fail to serialize metrics");
        Ok(tonic::Response::new(RaftMngmtResponse::new(data)))
    }

    async fn create_data_node(
        &self,
        request: tonic::Request<VolumeMngmtRequest>,
    ) -> Result<tonic::Response<VolumeMngmtResponse>, tonic::Status> {
        let req = request.into_inner();
        let resp = self.handle_create_data_node(req).await?;
        Ok(tonic::Response::new(resp))
    }

    async fn create_volume(
        &self,
        request: tonic::Request<VolumeMngmtRequest>,
    ) -> Result<tonic::Response<VolumeMngmtResponse>, tonic::Status> {
        let req = request.into_inner();
        let resp = self.handle_create_volume(req).await?;
        Ok(tonic::Response::new(resp))
    }
}
