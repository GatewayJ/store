use std::convert::TryFrom;
use nix::errno::Errno;

tonic::include_proto!("extent_service");

pub use extent_request::Kind as ExtentRequestKind;
pub use extent_response::Kind as ExtentResponseKind;
pub use read_params::Flags as ExtentReadFlags;
pub use extent_service_server::{ ExtentService, ExtentServiceServer };
 
impl ExtentRequest {
    pub fn is_valid(&self) -> bool {
        self.extent_id.is_some() &&
        self.extent_version.is_some() &&
        self.volume_id.is_some()
    }
    pub fn is_create(&self) -> bool {
        matches!(self.kind, Some(ExtentRequestKind::Create(_)))
    }
}

impl<'a> TryFrom<Option<&'a ExtentRequestKind>> for &'a StatParams {
    type Error = ();
    fn try_from(kind: Option<&'a ExtentRequestKind>) -> Result<Self, Self::Error> {
        if let Some(ExtentRequestKind::Stat(stat)) = kind {
            Ok(stat)
        } else {
            Err(())
        }
    }
}

impl<'a> TryFrom<Option<&'a ExtentRequestKind>> for &'a ReadParams {
    type Error = ();
    fn try_from(kind: Option<&'a ExtentRequestKind>) -> Result<Self, Self::Error> {
        if let Some(ExtentRequestKind::Read(read)) = kind {
            Ok(read)
        } else {
            Err(())
        }
    }
}

impl<'a> TryFrom<Option<&'a ExtentRequestKind>> for &'a AppendParams {
    type Error = ();
    fn try_from(kind: Option<&'a ExtentRequestKind>) -> Result<Self, Self::Error> {
        if let Some(ExtentRequestKind::Append(append)) = kind {
            Ok(append)
        } else {
            Err(())
        }
    }
}

impl<'a> TryFrom<Option<&'a ExtentRequestKind>> for &'a SealParams {
    type Error = ();
    fn try_from(kind: Option<&'a ExtentRequestKind>) -> Result<Self, Self::Error> {
        if let Some(ExtentRequestKind::Seal(seal)) = kind {
            Ok(seal)
        } else {
            Err(())
        }
    }
}

impl<'a> TryFrom<Option<&'a ExtentRequestKind>> for &'a CreateParams {
    type Error = ();
    fn try_from(kind: Option<&'a ExtentRequestKind>) -> Result<Self, Self::Error> {
        if let Some(ExtentRequestKind::Create(create)) = kind {
            Ok(create)
        } else {
            Err(())
        }
    }
}

impl<'a> TryFrom<Option<&'a ExtentRequestKind>> for &'a DeleteParams {
    type Error = ();
    fn try_from(kind: Option<&'a ExtentRequestKind>) -> Result<Self, Self::Error> {
        if let Some(ExtentRequestKind::Delete(delete)) = kind {
            Ok(delete)
        } else {
            Err(())
        }
    }
}

impl ExtentResponse {
    pub fn errno(req: &ExtentRequest, errno: Errno) -> Self {
        (ExtentResponseKind::Errcode(-(errno as i32)), req).into()
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

impl From<(StatResponse, &ExtentRequest)> for ExtentResponse {
    fn from((resp, req) :(StatResponse, &ExtentRequest)) -> Self {
        (ExtentResponseKind::Stat(resp), req).into()
    }
}

impl From<(CreateResponse, &ExtentRequest)> for ExtentResponse {
    fn from((resp, req): (CreateResponse, &ExtentRequest)) -> Self {
        (ExtentResponseKind::Create(resp), req).into()
    }
}

impl From<(DeleteResponse, &ExtentRequest)> for ExtentResponse {
    fn from((resp, req): (DeleteResponse, &ExtentRequest)) -> Self {
        (ExtentResponseKind::Delete(resp), req).into()
    }
}

impl From<(AppendResponse, &ExtentRequest)> for ExtentResponse {
    fn from((resp, req): (AppendResponse, &ExtentRequest)) -> Self {
        (ExtentResponseKind::Append(resp), req).into()
    }
}

impl From<(ReadResponse, &ExtentRequest)> for ExtentResponse {
    fn from((resp, req): (ReadResponse, &ExtentRequest)) -> Self {
        (ExtentResponseKind::Read(resp), req).into()
    }
}

impl From<(SealResponse, &ExtentRequest)> for ExtentResponse {
    fn from((resp, req): (SealResponse, &ExtentRequest)) -> Self {
        (ExtentResponseKind::Seal(resp), req).into()
    }
}