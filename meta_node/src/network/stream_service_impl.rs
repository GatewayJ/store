use std::str::FromStr;
use tonic::metadata::MetadataMap;

use crate::{
    server::{ MetaNodeService, ClientRequestContext },
    types::{
        common_proto::ClientId,
        stream_service::{
            CreateRequest, CreateResponse, OpenRequest, OpenResponse,
            CloseRequest, CloseResponse, AllocExtentRequest, AllocExtentResponse,
            StatExtentRequest, StatExtentResponse, SessionRequest, SessionResponse,
            StreamService,
        }
    },
};

fn parse_metadata<T: FromStr>(
    meta_map: &MetadataMap, key: &str
) -> Result<Option<T>, tonic::Status>  {
    meta_map.get("client-id")
        .map(|v| {
            v.to_str()
            .map_err(|_| tonic::Status::invalid_argument(format!("non-ascii {}", key)))?
            .parse::<T>()
            .map_err(|_| tonic::Status::invalid_argument(format!("failed to parse {}", key)))
    })
    .transpose()
}

fn get_request_context<R>(
    req: &tonic::Request<R>
) -> Result<ClientRequestContext, tonic::Status> {
    let client_id = parse_metadata(req.metadata(), "client-id")?;
    Ok(ClientRequestContext {
        client_id: client_id.map(ClientId::new),
    })
}

#[tonic::async_trait]
impl StreamService for MetaNodeService {
    async fn open_session(
        &self,
        request: tonic::Request<SessionRequest>,
    ) -> Result<tonic::Response<SessionResponse>, tonic::Status> {
        self.handle_open_session(request.into_inner())
            .await
            .map(tonic::Response::new)
    }

    async fn close_session(
        &self,
        request: tonic::Request<SessionRequest>,
    ) -> Result<tonic::Response<SessionResponse>, tonic::Status> {
        self.handle_close_session(request.into_inner())
            .await
            .map(tonic::Response::new)
    }
    async fn renew_session(
        &self,
        request: tonic::Request<SessionRequest>,
    ) -> Result<tonic::Response<SessionResponse>, tonic::Status> {
        self.handle_renew_session(request.into_inner())
            .await
            .map(tonic::Response::new)
    }

    async fn create_stream(
        &self,
        request: tonic::Request<CreateRequest>,
    ) -> Result<tonic::Response<CreateResponse>, tonic::Status> {
        let req_ctx = get_request_context(&request)?;
        self.handle_create_stream(req_ctx, request.into_inner())
            .await
            .map(tonic::Response::new)
    }

    async fn open_stream(
        &self,
        request: tonic::Request<OpenRequest>,
    ) -> Result<tonic::Response<OpenResponse>, tonic::Status> {
        let req_ctx = get_request_context(&request)?;
        self.handle_open_stream(req_ctx, request.into_inner())
            .await
            .map(tonic::Response::new)
    }

    async fn close_stream(
        &self,
        request: tonic::Request<CloseRequest>,
    ) -> Result<tonic::Response<CloseResponse>, tonic::Status> {
        let req_ctx = get_request_context(&request)?;
        self.handle_close_stream(req_ctx, request.into_inner())
            .await
            .map(tonic::Response::new)
    }

    async fn alloc_extent(
        &self,
        request: tonic::Request<AllocExtentRequest>,
    ) -> Result<tonic::Response<AllocExtentResponse>, tonic::Status> {
        let req_ctx = get_request_context(&request)?;
        self.handle_alloc_extent(req_ctx, request.into_inner())
            .await
            .map(tonic::Response::new)
    }

    async fn stat_extent(
        &self,
        request: tonic::Request<StatExtentRequest>,
    ) -> Result<tonic::Response<StatExtentResponse>, tonic::Status> {
        let req_ctx = get_request_context(&request)?;
        self.handle_stat_extent(req_ctx, request.into_inner())
            .await
            .map(tonic::Response::new)
    }
}