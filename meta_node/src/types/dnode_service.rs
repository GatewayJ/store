tonic::include_proto!("dnode_service");

pub use data_node_service_client::DataNodeServiceClient;
pub use data_node_service_server::{DataNodeService, DataNodeServiceServer};
pub use data_volume_stats::Flags as DataVolumeFlags;

impl DataVolumeStats {
    pub fn is_up(&self) -> bool {
        self.flags & DataVolumeFlags::Up as u32 != 0
    }
}