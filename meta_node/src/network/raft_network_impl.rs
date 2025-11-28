use std::{ sync::{Arc, Mutex}, future::Future };
use openraft::{
    error::{
        Fatal, NetworkError, RPCError, RemoteError, Unreachable,
        ReplicationClosed, StreamingError
    },
    network::{ RPCOption, RaftNetwork, RaftNetworkFactory },
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse,
        VoteRequest, VoteResponse
    },
    OptionalSend, StorageIOError, AnyError,
};
use tonic::{
    transport::{Channel, Endpoint},
    client::GrpcService,
};
use serde::de::DeserializeOwned;
use crate::{
    store::StateMachineSharedData,
    types::{
        Node, NodeId, TypeConfig, RaftError, RaftVote, RaftSnapshot, RaftStorageError,
        raft_service::{ 
            RaftRequest, PushSnapshot, FullSnapshotRequest, RaftServiceClient
        },
    }
};

type RaftRPCError<E> = openraft::error::RPCError<NodeId, Node, E>;

pub struct RaftNetworkImpl {
    sm_data: Arc<Mutex<StateMachineSharedData>>,
}

impl RaftNetworkImpl {
    pub fn new(
        sm_data: Arc<Mutex<StateMachineSharedData>>
    ) -> Self {
        Self { sm_data }
    }
}

impl RaftNetworkFactory<TypeConfig> for RaftNetworkImpl {
    type Network = RaftServiceEndpoint;
    
    #[tracing::instrument(level = "debug", skip_all)]
    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        let addr = format!("https://{}", node.rpc_addr);
        tracing::debug!("new_client: {}", addr);
        let endpoint = Endpoint::from_shared(addr)
            .expect("failed to parse rpc addr");

        RaftServiceEndpoint {
            endpoint, target,
            rpc_chan: None,
            sm_data: self.sm_data.clone()
         }
    }
}

pub struct RaftServiceEndpoint {
    target: NodeId,
    endpoint: Endpoint,
    rpc_chan: Option<Channel>,
    sm_data: Arc<Mutex<StateMachineSharedData>>,
}

impl RaftServiceEndpoint {
    async fn get_client<E: std::error::Error + DeserializeOwned>(
        &mut self
    ) -> Result<RaftServiceClient<Channel>, RaftRPCError<E>> {
        if let Some(chan) = &mut self.rpc_chan {
            if futures::future::poll_fn(|cx| chan.poll_ready(cx)).await.is_ok() {
                return Ok(RaftServiceClient::new(chan.clone()))
            }
        }

        let chan = self.endpoint.connect()
            .await
            .map_err(|e| RaftRPCError::Unreachable(Unreachable::new(&e)))?;
                
        self.rpc_chan.replace(chan.clone());
        Ok(RaftServiceClient::new(chan))
    }

    fn handle_rpc_error<E: std::error::Error + DeserializeOwned>(
        &mut self, status: tonic::Status
    ) -> RaftRPCError<E> {
        if status.code() == tonic::Code::Internal {
            let err = bincode::deserialize(status.details()).unwrap();
            RaftRPCError::RemoteError(RemoteError::new(self.target, err))
        } else {
            self.rpc_chan.take();
            RaftRPCError::Network(NetworkError::new(&status))
        }
    }
}

impl tonic::IntoRequest<RaftRequest> for AppendEntriesRequest<TypeConfig> {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = RaftRequest {
            data: bincode::serialize(&self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}

impl tonic::IntoRequest<RaftRequest> for VoteRequest<NodeId> {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = RaftRequest {
            data: bincode::serialize(&self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}

fn to_stream_error<E: std::error::Error + DeserializeOwned>(
    e: RaftRPCError<E>
) -> StreamingError<TypeConfig, E> {
    match e {
        RPCError::Timeout(e) => StreamingError::Timeout(e),
        RPCError::Unreachable(e) => StreamingError::Unreachable(e),
        RPCError::Network(e) => StreamingError::Network(e),
        RPCError::RemoteError(e) => StreamingError::RemoteError(e),
        RPCError::PayloadTooLarge(_) => panic!()
    }
}

impl RaftNetwork<TypeConfig> for RaftServiceEndpoint {
    #[tracing::instrument(level = "debug", skip_all, err(Debug))]
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RaftRPCError<RaftError>> {
        tracing::debug!(req = debug(&req), "append_entries");
        let mut client = self.get_client().await?;
        let resp = client.append_entries(req)
            .await
            .map_err(|e| self.handle_rpc_error(e))?;
        tracing::debug!("append_entries resp from: id={}: {:?}", self.target, resp);

        let resp = resp.into_inner();
        let data = bincode::deserialize(&resp.data)
            .map_err(|e| RaftRPCError::Network(NetworkError::new(&e)))?;
        Ok(data)
    }

    #[tracing::instrument(level = "debug", skip_all, err(Debug))]
    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RaftRPCError<RaftError>> {
        tracing::debug!(req = debug(&req), "vote");
        let mut client = self.get_client().await?;
        let resp = client.vote(req)
            .await
            .map_err(|e| self.handle_rpc_error(e))?;
        tracing::debug!("vote resp from: id={}: {:?}", self.target, resp);

        let resp = resp.into_inner();
        let data = bincode::deserialize(&resp.data)
            .map_err(|e| RaftRPCError::Network(NetworkError::new(&e)))?;
        Ok(data)
    }

    #[tracing::instrument(level = "debug", skip_all, err(Debug))]
    async fn full_snapshot(
        &mut self,
        vote: RaftVote,
        snapshot: RaftSnapshot,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<NodeId>, StreamingError<TypeConfig, Fatal<NodeId>>> {
        tracing::debug!(vote = debug(&vote), "full_snapshot");
        let sm_snap = self.sm_data.lock().unwrap().snapshot.clone()
            .ok_or("not found")
            .and_then(|sm_snap| {
                if sm_snap.get_snapshot_meta() == snapshot.meta {
                    Ok(sm_snap)
                } else {
                    Err("metadata mismatch")
                }
            })
            .map_err(|msg| 
                StreamingError::StorageError(RaftStorageError::IO { 
                source: StorageIOError::read_snapshot(
                    Some(snapshot.meta.signature()),
                    AnyError::error(msg)
                )
            }))?;

        let mut client = self.get_client()
            .await
            .map_err(to_stream_error)?;

        let request = bincode::serialize(&FullSnapshotRequest{ vote, snap_meta: snapshot.meta })
            .expect("fail to serialize (vote, snapshot)");

        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let send_task = tokio::spawn(async move {
            tx.send(PushSnapshot::with_request(request)).await
                .map_err(|e| StreamingError::Closed(ReplicationClosed::new(e.to_string())))?;
            sm_snap.send(tx, cancel).await
        });

        let resp = client.full_snapshot(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .map_err(|e| to_stream_error(self.handle_rpc_error(e)))?;

        send_task.await.expect("failed to spawn send task")?;

        let data = bincode::deserialize(&resp.into_inner().data)
            .map_err(|e| StreamingError::Network(NetworkError::new(&e)))?;
        Ok(data)
    }
}
