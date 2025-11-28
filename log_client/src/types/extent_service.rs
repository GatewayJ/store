use nix::errno::Errno;
tonic::include_proto!("extent_service");

pub use extent_request::Kind as ExtentRequestKind;
pub use extent_response::Kind as ExtentResponseKind;
pub use extent_service_client::ExtentServiceClient;

impl ExtentResponseKind {
    pub fn errno(errno: Errno) -> Self {
        ExtentResponseKind::Errcode(-(errno as i32))
    }
}

impl From<(ExtentResponseKind, &ExtentRequest)> for ExtentResponse {
    fn from((kind, req): (ExtentResponseKind, &ExtentRequest)) -> Self {
        Self {
            request_seq: req.request_seq,
            extent_id: req.extent_id,
            volume_id: req.volume_id,
            kind: Some(kind),
        }
    }
}

impl ExtentResponse {
    pub fn errno(req: &ExtentRequest, errno: Errno) -> Self {
        (ExtentResponseKind::Errcode(-(errno as i32)), req).into()
    }

    pub fn ok(&self) -> Result<(), Errno> {
        if let Some(ExtentResponseKind::Errcode(errcode)) = self.kind {
            Err(Errno::from_raw(-errcode))
        } else {
            Ok(())
        }
    }

    pub fn is_valid(&self) -> bool {
        self.extent_id.is_some() &&
        self.volume_id.is_some() &&
        self.kind.is_some()
    }
}