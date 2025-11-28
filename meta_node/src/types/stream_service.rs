
tonic::include_proto!("stream_service");

pub use stream_service_client::StreamServiceClient;
pub use stream_service_server::{
    StreamServiceServer, StreamService,
};

use common_proto::{ClientId, ExtentVersion, LogStreamConfig};

impl OpenResponse {
    pub fn new(version: ExtentVersion, config: LogStreamConfig) -> Self {
        Self {
            extent_version: Some(version),
            config: Some(config)
        }
    }
}

impl CloseResponse {
    pub fn new(version: ExtentVersion) -> Self {
        Self {
            extent_version: Some(version),
        }
    }
}

impl SessionResponse {
    pub fn new(client_id: Option<ClientId>) -> Self {
        Self {
            client_id,
            lease: None,
        }
    }
    pub fn with_lease(client_id: ClientId, lease: ClientLease) -> Self {
        Self {
            client_id: Some(client_id),
            lease: Some(lease),
        }
    }
}

impl ClientLease {
    pub fn new(duration_ms: u32) -> Self {
        Self { duration_ms }
    }
}
