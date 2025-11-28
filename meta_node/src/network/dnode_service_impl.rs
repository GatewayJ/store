use crate::{
    server::MetaNodeService,
    types::dnode_service::{
        DataNodeBeacon, DataNodeBeaconAck, DataNodeService
    },
};

#[tonic::async_trait]
impl DataNodeService for MetaNodeService {
    async fn beacon(
        &self, request: tonic::Request<DataNodeBeacon>,
    ) -> std::result::Result<tonic::Response<DataNodeBeaconAck>, tonic::Status> {
        let beacon = request.into_inner();
        let ack = self.handle_data_node_beacon(beacon).await?;
        Ok(tonic::Response::new(ack))
    }
}