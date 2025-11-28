use std::{
    future::Future, time::Duration, vec::Vec, time::Instant,
    sync::{ atomic::{ AtomicU64, Ordering }, Arc },
};
use crate::types::{
    dnode_service::{
        DataNodeBeacon, DataNodeBeaconAck, DataNodeServiceClient,
    },
    mngmt_service::{
        MngmtServiceClient, RaftMngmtRequest, RaftMngmtRequestFlags, VolumeMngmtRequest, 
    },
    stream_service::{
        CreateRequest, OpenRequest, SessionRequest, SessionResponse, StatExtentRequest, StreamServiceClient
    },
    ClientWriteError, ForwardToLeader, Node, NodeId, RaftChangeMembers, RaftError, RaftMetrics
};
use common_proto::{
    ClientId, DataNodeId, ExtentId, ExtentStats, LogStreamConfig, LogStreamId, VolumeId
};
use super::LogStream;
use tower::timeout::Timeout;
use tonic::{
    service::interceptor::{InterceptedService, Interceptor},
    transport::{Channel, Endpoint},
};
use tokio::sync::Mutex;
use arc_swap::ArcSwap;
use tracing::{error, warn};

pub(super) trait ServiceClient<'a> : Sized {
    fn with_interceptor(_: Channel, _: ClientInterceptor<'a>) -> Self;
}

trait RaftServiceClient : Sized {
    async fn get_raft_nodes(&mut self) -> anyhow::Result<Vec<(NodeId, Node)>>;
}

type ClientInterceptedService<'a, T> = InterceptedService<T, ClientInterceptor<'a>>;

impl<'a> ServiceClient<'a> for MngmtServiceClient<ClientInterceptedService<'a, Channel>> {
    fn with_interceptor(svc_chan: Channel, interceptor: ClientInterceptor<'a>) -> Self {
        MngmtServiceClient::with_interceptor(svc_chan, interceptor)
    }
}

impl RaftServiceClient for MngmtServiceClient<Channel> {
    async fn get_raft_nodes(&mut self) -> anyhow::Result<Vec<(NodeId, Node)>> {
        let resp = self.get_raft_metrics(RaftMngmtRequest::new()).await?;
        let metrics: RaftMetrics = serde_json::from_str(&resp.into_inner().data)?;
        let mut nodes: Vec<_> = metrics.membership_config.nodes()
            .map(|(id, node)| (*id, node.clone()))
            .collect();
        if let Some(leader) = metrics.current_leader {
            let mut leader_idx = 0;
            for (idx, node) in nodes.iter().enumerate() {
                if node.0 == leader {
                    leader_idx = idx;
                    break;
                }
            }
            if leader_idx != 0 {
                nodes.swap(0, leader_idx);
            }
        }
        Ok(nodes)
    }
}

impl<'a> ServiceClient<'a> for StreamServiceClient<ClientInterceptedService<'a, Channel>> {
    fn with_interceptor(svc_chan: Channel, interceptor: ClientInterceptor<'a>) -> Self {
        StreamServiceClient::with_interceptor(svc_chan, interceptor)
    }
}

impl<'a> ServiceClient<'a> for DataNodeServiceClient<ClientInterceptedService<'a, Timeout<Channel>>> {
    fn with_interceptor(svc_chan: Channel, interceptor: ClientInterceptor<'a>) -> Self {
        let timeout_chan = Timeout::new(svc_chan, interceptor.timeout.map_or(Duration::MAX, |d| d));
        DataNodeServiceClient::with_interceptor(timeout_chan, interceptor)
    }
}

struct ClientMetaSessionMut {
    nodes: Vec<(NodeId, Node)>,
    opened_stamp: Option<Instant>,
}

impl ClientMetaSessionMut {
    const URL_SCHEME: &'static str = "http://";

    fn new(nodes: Vec<(NodeId, Node)>) -> Self {
        Self { nodes, opened_stamp: None }
    }

    async fn connect(&mut self, dst: &str) -> anyhow::Result<Channel> {
        let mut svc_chan = Endpoint::from_shared(String::from(Self::URL_SCHEME) + dst)?
            .connect()
            .await?;

        let mut client = MngmtServiceClient::new(svc_chan.clone());
        let nodes = client.get_raft_nodes().await?;
        if !nodes.is_empty() {
            if let Some(leader) = nodes.first() {
                if leader.1.rpc_addr != dst {
                    svc_chan = Endpoint::from_shared(String::from(Self::URL_SCHEME) + &leader.1.rpc_addr)?
                        .connect()
                        .await?;
                }
            }
            self.nodes = nodes;
        }
        Ok(svc_chan)
    }

    fn update_leader(&mut self, mut forward: ForwardToLeader) {
        let mut leader_idx = self.nodes.len();
        for (idx, node) in self.nodes.iter_mut().enumerate() {
            if let Some(id) = forward.leader_id {
                if node.0 != id {
                    continue;
                }
                leader_idx = idx;
                if let Some(leader) = forward.leader_node.take() {
                    node.1 = leader;
                }
                break;
            }
        }

        if leader_idx == self.nodes.len() {
            if let Some(item) = forward.leader_id.zip(forward.leader_node) {
                self.nodes.push(item);
                self.nodes.swap(0, leader_idx);
            }
        } else if leader_idx > 0 {
            self.nodes.swap(0, leader_idx);
        }
    }
}

pub(super) struct ClientMetaSession {
    client_id: AtomicU64,
    request_seq: AtomicU64,
    lease_ttl: AtomicU64,

    svc_chan: ArcSwap<(Option<Channel>, u64)>,
    mutable: Mutex<ClientMetaSessionMut>
}

impl ClientMetaSession {
    fn new(nodes: Vec<(NodeId, Node)>) -> Self {
        Self {
            client_id: AtomicU64::new(0),
            request_seq: AtomicU64::new(0),
            lease_ttl: AtomicU64::new(0),
            svc_chan: ArcSwap::from_pointee((None, 1)),
            mutable: Mutex::new(ClientMetaSessionMut::new(nodes)),
        }
    }

    async fn is_opened(&self) -> bool {
        self.mutable.lock().await.opened_stamp.is_some()
    }

    async fn notify_opened(
        self: &Arc<Self>, resp: SessionResponse, rpc_stamp: Instant
    ) {
        let client_id = resp.client_id.unwrap();
        let lease = resp.lease.unwrap();

        assert!(client_id.id > 0);
        self.client_id.store(client_id.id, Ordering::Relaxed);
        
        let opened_stamp = Instant::now();
        let lease_ttl = rpc_stamp
            .checked_add(Duration::from_millis(lease.duration_ms as u64))
            .map(|stamp| stamp.duration_since(opened_stamp));
        self.lease_ttl.store(
            lease_ttl.map_or(0, |ttl| ttl.as_millis() as u64),
            Ordering::Relaxed
        );

        self.mutable.lock().await.opened_stamp.replace(opened_stamp);

        let this = self.clone();
        tokio::spawn(async move {
            this.renew_task(client_id, opened_stamp).await;
        });
    }

    async fn set_closed(&self) {
        self.mutable.lock().await.opened_stamp.take();
    }

    fn clear_client_id(&self)  {
        self.client_id.store(0, Ordering::Relaxed);
        self.lease_ttl.store(0, Ordering::Relaxed);
    }

    pub fn get_client_id(&self) -> Option<u64> {
        let id = self.client_id.load(Ordering::Relaxed);
        (id > 0).then_some(id)
    }

    pub fn next_request_seq(&self) -> u64 {
        self.request_seq.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn handle_rpc_error(status: tonic::Status) -> anyhow::Result<ForwardToLeader> {
        if status.code() == tonic::Code::Internal {
            if let Ok(err) = serde_json::from_slice(status.details()) {
                if let RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) = err {
                    return Ok(forward)
                } else {
                    return Err(err.into())
                }
            }
        }
        Err(status.into())
    }

    pub async fn send_rpc<'a, C, R, T, F, Fut>(
        &'a self, req: R, timeout: Option<Duration>, rpc: F
    ) -> anyhow::Result<T>
    where
        C: ServiceClient<'a>,
        R: Clone,
        F: Fn(C, R) -> Fut,
        Fut: Future<Output = Result<T, tonic::Status>>
    {
        loop {
            let (svc_chan, connect_seq) = {
                let res = self.svc_chan.load();
                (res.0.clone(), res.1)
            };
            let mut forward = None;
            if let Some(svc_chan) = svc_chan {
                let svc_client = C::with_interceptor(
                    svc_chan, ClientInterceptor::new(self, timeout)
                );

                match rpc(svc_client, req.clone()).await {
                    Ok(resp) => {
                        return Ok(resp);
                    }
                    Err(status) => {
                        let res = Self::handle_rpc_error(status)?;
                        if res.leader_id.is_none() {
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                        forward.replace(res);
                    }
                }
            }

            let mut inner = self.mutable.lock().await;
            if connect_seq != self.svc_chan.load().1 {
                continue;
            }

            if let Some(forward) = forward  {
                inner.update_leader(forward);
            }
            
            let mut last_err = None;
            let mut new_chan = (None, connect_seq + 1);

            for node in inner.nodes.clone().into_iter() {
                match inner.connect(&node.1.rpc_addr).await {
                    Ok(svc_chan) => {
                        new_chan.0.replace(svc_chan);
                        break;
                    }
                    Err(err) => {
                        last_err.replace(err);
                    }
                }
            }

            self.svc_chan.store(Arc::new(new_chan.clone()));

            if new_chan.0.is_none() {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    async fn renew_task(&self, client_id: ClientId, opened_stamp: Instant) {
        while let Some(id) = self.get_client_id() {
            if id != client_id.id {
                break;
            }
            let req = SessionRequest {
                request_seq: 0,
                client_id: Some(client_id),
            };
            let (lease, rpc_stamp) = match self.send_rpc(
                req, None,
                |mut client: StreamServiceClient<_>, req| async move {
                    let stamp = Instant::now();
                    let resp = client.renew_session(req).await.map(|resp| resp.into_inner())?;
                    Ok((resp, stamp))
                },
            ).await {
                Err(err) => {
                    warn!("error renew meta session {:?}", err);
                    break;
                }
                Ok((resp, rpc_stamp)) => {
                    if resp.client_id.is_none() {
                        error!("session({}) has been evicted", client_id.id);
                        break;
                    }
                    (resp.lease.unwrap(), rpc_stamp)
                }
            };
            
            let lease_ttl = rpc_stamp.checked_add(Duration::from_millis(lease.duration_ms as u64))
                .map(|stamp| stamp.duration_since(opened_stamp));
            self.lease_ttl.store(
                lease_ttl.map_or(0, |ttl| ttl.as_millis() as u64),
                Ordering::Relaxed
            );
            if let Some(next_renew) = rpc_stamp.checked_add(
                Duration::from_millis(lease.duration_ms as u64 / 3)
            ) {
                tokio::time::sleep_until(next_renew.into()).await;
            } else {
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct ClientInterceptor<'a> {
    session: &'a ClientMetaSession,
    timeout: Option<Duration>,
}

impl<'a> ClientInterceptor<'a> {
    fn new(
        session: &'a ClientMetaSession, timeout: Option<Duration>
    ) -> Self {
        Self { session, timeout }
    }
}

impl<'a> Interceptor for ClientInterceptor<'a> {
    fn call(
        &mut self, mut req: tonic::Request<()>
    ) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(timeout) = &self.timeout {
            req.set_timeout(*timeout);
        }
        let meta_mut = req.metadata_mut();
        if let Some(client_id) = self.session.get_client_id() {
            let val = client_id.to_string().parse().unwrap();
            meta_mut.insert("client-id", val);
        }
        Ok(req)
    }
}

pub struct Client {
    session: Arc<ClientMetaSession>,
}

impl Client {
    pub fn new(nodes: Vec<(NodeId, Node)>) -> Self {
        let session = Arc::new(ClientMetaSession::new(nodes));
        Self { session }
    }

    pub async fn add_raft_leaner(
        &mut self, node: (NodeId, Node), blocking: bool
    ) -> anyhow::Result<()> {
        let mut req = RaftMngmtRequest::new();
        req.nodes.push(node.clone().into());
        if blocking {
            req.set_flags(RaftMngmtRequestFlags::Blocking as u32);
        }
        self.session.send_rpc(
            req, None,
            |mut client: MngmtServiceClient<_>, req| async move {
                client.add_raft_leaner(req).await.map(|resp| resp.into_inner())
            }
        ).await.map(|_| ())
    }

    pub async fn change_raft_membership(
        &mut self, members: RaftChangeMembers, retain: bool
    ) -> anyhow::Result<()> {
        let mut req: RaftMngmtRequest = members.into();
        if retain {
            req.set_flags(RaftMngmtRequestFlags::Retain as u32);
        }
        self.session.send_rpc(
            req, None,
            |mut client: MngmtServiceClient<_>, req| async move {
                client.change_raft_membership(req).await.map(|resp| resp.into_inner())
            }
        ).await.map(|_| ())
    }

    pub async fn get_raft_metrics(&self) -> anyhow::Result<RaftMetrics> {
        let req = RaftMngmtRequest::new();
        let resp = self.session.send_rpc(
            req, None,
            |mut client: MngmtServiceClient<_>, req| async move {
                client.get_raft_metrics(req).await.map(|resp| resp.into_inner())
            }
        ).await?;
        let metrics: RaftMetrics = serde_json::from_str(&resp.data)?;
        Ok(metrics)
    }

    pub async fn open_session(&mut self) -> anyhow::Result<()> {
        if self.session.is_opened().await {
            return Err(anyhow::Error::msg(
                format!("already opened with {:?}", self.session.get_client_id())
            ));
        }

        let req = SessionRequest {
            request_seq: 0,
            client_id: self.session.get_client_id().map(ClientId::new),
        };

        let (resp, rpc_stamp) = self.session.send_rpc(
            req, None,
            |mut client: StreamServiceClient<_>, req| async move {
                let stamp = Instant::now();
                let res = client.open_session(req).await.map(|resp| resp.into_inner())?;
                Ok((res, stamp))
            }
        ).await?;

        self.session.notify_opened(resp, rpc_stamp).await;
        Ok(())
    }

    pub async fn close_session(&mut self) -> anyhow::Result<()> {
        if let Some(id) = self.session.get_client_id() {
            let req = SessionRequest {
                request_seq: 0,
                client_id: Some(ClientId::new(id)),
            };
            self.session.clear_client_id();
            self.session.send_rpc(
                req, None,
                |mut client: StreamServiceClient<_>, req| async move {
                    client.close_session(req).await.map(|resp| resp.into_inner())
                },
            ).await?;
            self.session.set_closed().await;
        }
        Ok(())
    }

    pub async fn create_date_node(
        &mut self, uuid: String, host: String,
    ) -> anyhow::Result<DataNodeId> {
        let req = VolumeMngmtRequest {
            uuid, host,
            ..Default::default()
        };
        let resp = self.session.send_rpc(
            req, None,
            |mut client: MngmtServiceClient<_>, req| async move {
                client.create_data_node(req).await.map(|resp| resp.into_inner())
            },
        ).await?;
        Ok(resp.volume_id.unwrap().data_node())
    }

    pub async fn create_volume(
        &self, uuid: String, node_id: DataNodeId, capacity: u64
    ) -> anyhow::Result<VolumeId> {
        let req = VolumeMngmtRequest {
            volume_id: Some(VolumeId::new(node_id.seq(), 0)),
            uuid, capacity,
            ..Default::default()
        };
        let resp = self.session.send_rpc(
            req, None,
            |mut client: MngmtServiceClient<_>, req| async move {
                client.create_volume(req).await.map(|resp| resp.into_inner())
            },
        ).await?;
        Ok(resp.volume_id.unwrap())
    }

    pub async fn create_stream(
        &mut self, config: LogStreamConfig,
    ) -> anyhow::Result<LogStream> {
        let req = CreateRequest {
            request_seq: self.session.next_request_seq(),
            config: Some(config),
        };
        let resp = self.session.send_rpc(
            req, None,
            |mut client: StreamServiceClient<_>, req| async move {
                client.create_stream(req).await.map(|resp| resp.into_inner())
            },
        ).await?;

        let id = resp.stream_id.unwrap();
        let version = resp.extent_version.unwrap();
        Ok(LogStream::new(self.session.clone(), id, config, version))
    }

    pub async fn open_stream(
        &mut self, id: LogStreamId,
    ) -> anyhow::Result<LogStream> {
        let req = OpenRequest {
            request_seq: self.session.next_request_seq(),
            stream_id: Some(id),
        };
        let resp = self.session.send_rpc(
            req, None,
            |mut client: StreamServiceClient<_>, req| async move {
                client.open_stream(req).await.map(|resp| resp.into_inner())
            },
        ).await?;
        let version = resp.extent_version.unwrap();
        let config = resp.config.unwrap();
        Ok(LogStream::new(self.session.clone(), id, config, version))
    }

    pub async fn stat_exent(
        &self, id: ExtentId,
    ) -> anyhow::Result<Option<ExtentStats>> {
        let req = StatExtentRequest {
            extent_id: Some(id),
        };
        let resp = self.session.send_rpc(
            req, None,
            |mut client: StreamServiceClient<_>, req| async move {
                client.stat_extent(req).await.map(|resp| resp.into_inner())
            },
        ).await?;
        Ok(resp.stats)
    }

    pub async fn data_node_beacon(
        &self, beacon: DataNodeBeacon, timeout: Duration
    ) -> anyhow::Result<DataNodeBeaconAck> {
        let resp = self.session.send_rpc(
            beacon, Some(timeout),
            |mut client: DataNodeServiceClient<_>, beacon| async move {
                client.beacon(beacon).await.map(|resp| resp.into_inner())
            },
        ).await?;
        Ok(resp)
    }
}
