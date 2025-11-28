use std::{
    cell::OnceCell, collections::VecDeque, convert::From, future::Future,
    sync::{ atomic::{AtomicBool, AtomicIsize, AtomicU64, Ordering}, Arc, Weak },
    time::{Duration, Instant, SystemTime}
};
use tokio::sync::{oneshot, mpsc, Notify};
use arc_swap::ArcSwap;
use rocksdb::DB as RocksDB;
use tracing::{error, warn, info};

use super::{
    client_session::{ClientSession, ClientSessionMap},
    data_node::{DataNodeManager, VolumeStats},
    extent::Extent, log_stream::LogStream, meta_cache::CacheEntry,
    recovery::RecoveryManager, stream_service::LogStreamService,
    utils::map_raft_error,
    ClientRequestContext
};
use crate::{
    store::StoreRequest,
    types::{
        mngmt_service::{ VolumeMngmtRequest, VolumeMngmtResponse },
        stream_service::{
            AllocExtentRequest, AllocExtentResponse, CloseRequest, CloseResponse,
            CreateRequest, CreateResponse, OpenRequest, OpenResponse, SessionRequest,
            SessionResponse, StatExtentRequest, StatExtentResponse
        },
        ClientWriteError, ForwardToLeader, NodeId, RaftError, RaftMetrics, RaftServerState, TypeConfig
    },
    DataNodeBeacon, DataNodeBeaconAck
};
use common_proto::{
    ClientId, ExtentPlacement, LogStreamConfig, VolumeId
};

struct StateCondVar(Notify, AtomicBool);

impl StateCondVar {
    fn new() -> Self {
        Self(Notify::new(), AtomicBool::new(false))
    }

    async fn finish(&self) {
        self.1.store(true, Ordering::SeqCst);
        self.0.notify_waiters();
    }

    async fn wait(&self) {
        let notified = self.0.notified();
        if !self.1.load(Ordering::SeqCst) {
            notified.await;
        }
    }
}
pub struct StateDown {}

struct StateLeader {
    term: u64,
    loading: Option<StateCondVar>,
}

impl StateLeader {
    fn new(term: u64) -> Self {
        Self {
            term,
            loading: Some(StateCondVar::new()),
        }
    }

    async fn notify_waiters(&self) {
        if let Some(loading) = &self.loading {
            loading.finish().await;
        }
    }
}

enum StateFollower {
    Follower(NodeId),
    Electing(StateCondVar),
}

impl StateFollower {
    fn new(leader: Option<NodeId>) -> Self {
        if let Some(leader) = leader {
            Self::Follower(leader)
        } else {
            Self::Electing(StateCondVar::new())
        }
    }

    async fn notify_waiters(&self) {
        if let Self::Electing(electing) = self {
            electing.finish().await;
        }
    }
}

enum StateLeaner {
    Leaner(NodeId),
    Electing(StateCondVar),
}

impl StateLeaner {
    fn new(leader: Option<NodeId>) -> Self {
        if let Some(leader) = leader {
            Self::Leaner(leader)
        } else {
            Self::Electing(StateCondVar::new())
        }
    }

    async fn notify_waiters(&self) {
        if let Self::Electing(electing) = self {
            electing.finish().await;
        }
    }
}

enum ServiceStatus {
    Down(StateDown),
    Leader(StateLeader),
    Follower(StateFollower),
    Leaner(StateLeaner),
}

impl From<&RaftMetrics> for ServiceStatus {
    fn from(metrics: &RaftMetrics) -> Self {
        match metrics.state {
            RaftServerState::Learner => {
                ServiceStatus::Leaner(StateLeaner::new(metrics.current_leader))
            }
            RaftServerState::Follower => {
                ServiceStatus::Follower(StateFollower::new(metrics.current_leader))
            },
            RaftServerState::Candidate => {
                ServiceStatus::Follower(StateFollower::new(metrics.current_leader))
            },
            RaftServerState::Leader => {
                ServiceStatus::Leader(StateLeader::new(metrics.current_term))
            },
            RaftServerState::Shutdown => {
                ServiceStatus::Down(StateDown{})
            },
        }
    }
}

impl ServiceStatus {
    fn new() -> Self {
        ServiceStatus::Down(StateDown{})
    }

    fn is_leader(&self) -> bool {
        matches!(self, ServiceStatus::Leader(..))
    }

    fn ready(&self) -> Option<u64> {
        if let ServiceStatus::Leader(leader) = &self {
            leader.loading.is_none().then_some(leader.term)
        } else {
            None
        }
    }

    fn is_down(&self) -> bool {
        matches!(self, ServiceStatus::Down(..))
    }

    async fn notify_waiters(&self) {
        match self {
            ServiceStatus::Leader(leader) => {
                leader.notify_waiters().await;
            }
            ServiceStatus::Follower(follower) => {
                follower.notify_waiters().await;
            }
            ServiceStatus::Leaner(leaner) => {
                leaner.notify_waiters().await;
            }
            _ => {}
        }
    }

    async fn watch_raft(meta_svc_weak: Weak<MetaNodeService>) {
        let mut old_state = RaftServerState::Shutdown;
        let mut old_leader = None;
        loop {
            let meta_service = match meta_svc_weak.upgrade() {
                None => { break; }
                Some(svc) => svc,
            };

            let metrics = match meta_service.raft.wait(None).metrics(
                |metric| { 
                    old_state != metric.state ||
                    old_leader != metric.current_leader
                },
                "service_status_watcher"
            ).await {
                Err(_) => {
                    meta_service.shutdown().await;
                    break;
                }
                Ok(metrics) => metrics,
            };

            let new_status = Arc::new(ServiceStatus::from(&metrics));

            if old_state == metrics.state {
                meta_service.status.swap(new_status.clone())
                    .notify_waiters().await;
                old_leader = metrics.current_leader;
                continue;
            }

            if new_status.is_down() {
                break;
            }
            
            if let ServiceStatus::Leader(state) = &*new_status {
                meta_service.raft.runtime_config().elect(false);
                meta_service.update_raft_term(state.term)
            }

            //println!("raft state {:?} -> {:?}", old_state, new_state);
            meta_service.status.swap(new_status.clone())
                .notify_waiters().await;

            if old_state == metrics.state {

            } else if new_status.is_leader() {
                tokio::spawn(async move {
                    if let Err(err) = meta_service.reload_states(new_status).await {
                        eprintln!("reload states error: {:?}", err);
                        meta_service.shutdown().await;
                    }
                });
            } else if matches!(old_state, RaftServerState::Leader) {
                while meta_service.inflight_ops.load(Ordering::Relaxed) > 0 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                meta_service.invalidate_all().await;
                meta_service.raft.runtime_config().elect(true);
            }

            old_state = metrics.state;
            old_leader = metrics.current_leader;
        }
        info!("raft watcher exited");
}
}

pub(crate) struct MetaNodeService {
    pub this: Weak<Self>,
    pub raft: Arc<openraft::Raft<TypeConfig>>,
    data_nodes: DataNodeManager,
    client_sessions: ClientSessionMap,
    stream_service: LogStreamService,
    recovery_mgr: Arc<RecoveryManager>,
    batch_command_tx: mpsc::UnboundedSender<MetaBatchCommand>,
    batch_flushed_upto: AtomicU64,
    status: ArcSwap<ServiceStatus>,
    inflight_ops: AtomicIsize,
}

impl MetaNodeService {
    pub fn new(
        raft: Arc<openraft::Raft<TypeConfig>>, db: Arc<RocksDB>
    ) -> Arc<Self> {
        let (batch_command_tx, batch_write_rx) = mpsc::unbounded_channel();

        let meta_svc = Arc::new_cyclic(|me| {
            let recovery_mgr = Arc::new(RecoveryManager::new(me.clone()));
            let data_nodes = DataNodeManager::new(db.clone(), recovery_mgr.clone());
            let client_sessions = ClientSessionMap::new(db.clone(), recovery_mgr.clone());
            let stream_service = LogStreamService::new(db.clone(), recovery_mgr.clone());

            let this = me.clone();
            Self {
                this, raft, data_nodes, client_sessions,
                stream_service, recovery_mgr, batch_command_tx,
                batch_flushed_upto: AtomicU64::new(0),
                status: ArcSwap::from_pointee(ServiceStatus::new()),
                inflight_ops: AtomicIsize::new(0),
            }
        });

        let this = meta_svc.this.clone();
        tokio::spawn(async move {
            ServiceStatus::watch_raft(this).await;
        });
        let this = meta_svc.this.clone();
        tokio::spawn(async move {
            MetaBatchWriter::flush_work(this, batch_write_rx).await;
        });
        let this = meta_svc.this.clone();
        tokio::spawn(async move {
            Self::tick_work(this).await;
        });
        meta_svc
    }

    fn get_batch_writer(&self) -> MetaBatchWriter {
        let secs = self.batch_flushed_upto.load(Ordering::Relaxed);
        MetaBatchWriter {
            command_tx: self.batch_command_tx.clone(),
            flushed_upto: SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
        }
    }

    fn flush_batch_writer(&self) -> impl Future<Output=Result<(), ClientWriteError>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.batch_command_tx.send(MetaBatchCommand::Flush { resp_tx }).unwrap();
        async move {
            resp_rx.await.unwrap()
        }
    }

    fn update_batch_flushed(&self, stamp: SystemTime) {
        let secs = stamp.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        self.batch_flushed_upto.store(secs, Ordering::Relaxed);
    }

    async fn tick_work(this_weak: Weak<Self>) {
        let interval = Duration::from_secs(1);
        loop {
            let next_beacon = Instant::now() + interval;
            let this = match this_weak.upgrade() {
                None => {
                    info!("meta service tick task exited");
                    break;
                }
                Some(this) => this,
            };

            if let Some(term) = this.status.load().ready() {
                let now = SystemTime::now();
                let _guard = MetaRequestGuard::new(&this, term);
                let (res1, res2) = tokio::join!(
                    this.data_nodes.tick(this.get_batch_writer(), now),
                    this.client_sessions.tick(this.get_batch_writer(), now),
                );
                if let Err(err) = res1 {
                    warn!("error DataNodeManager tick {:?}", err);
                }
                if let Err(err) = res2 {
                    warn!("error SessionMap tick {:?}", err);
                }
                match this.flush_batch_writer().await {
                    Ok(_) => {
                        this.update_batch_flushed(now);
                    }
                    Err(err) => {
                        warn!("error batch writer flush {:?}", err);
                    }
                }
            }

            tokio::time::sleep_until(next_beacon.into()).await;
        }
    }

    fn update_raft_term(&self, term: u64) {
        self.stream_service.update_raft_term(term);
    }

    async fn invalidate_all(&self) {
        let _ = self.flush_batch_writer().await;
        self.stream_service.invalidate_all().await;
    }

    async fn reload_states(
        &self, leader_state: Arc<ServiceStatus>
    ) -> anyhow::Result<()> { 
        let term = if let ServiceStatus::Leader(state) = &*leader_state {
            state.term
        } else {
            panic!("not in leader state");
        };

        self.recovery_mgr.begin_reload();
        let res = match tokio::try_join!(
            self.stream_service.reload(),
            self.data_nodes.reload(),
            self.client_sessions.reload(),
        ) {
            Ok(_) => {
                let next_state = Arc::new(ServiceStatus::Leader(
                    StateLeader{
                        term, loading: None,
                    }
                ));
                self.update_batch_flushed(SystemTime::now());
                let _ = self.status.compare_and_swap(&leader_state, next_state);
                leader_state.notify_waiters().await;
                
                Ok(())
            }
            Err(err) => {
                if Arc::ptr_eq(&*self.status.load(), &leader_state) {
                    Err(err)
                } else {
                    Ok(())
                }
            }
        };
        self.recovery_mgr.finish_reload(res.is_ok());
        res
    }

    async fn shutdown(&self) {
        let _ = self.raft.shutdown().await;
    }

    async fn check_status(&self) -> Result<MetaRequestGuard<'_>, tonic::Status> {
        let forward = |leader: NodeId| {
            let err_fwd = RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader{
                leader_id: Some(leader),
                leader_node: None,
            }));
            map_raft_error(err_fwd)
        };

        loop {
            match &**self.status.load() {
                ServiceStatus::Down(_) => {
                    break Err(tonic::Status::unavailable("raft service down"));
                }
                ServiceStatus::Follower(state) => {
                    match state {
                        StateFollower::Follower(leader) => {
                            break Err(forward(*leader));
                        }
                        StateFollower::Electing(electing) => {
                            electing.wait().await;
                        }
                    }
                }
                ServiceStatus::Leaner(state) => {
                    match state {
                        StateLeaner::Leaner(leader) => {
                            break Err(forward(*leader));
                        }
                        StateLeaner::Electing(electing) => {
                            electing.wait().await;
                        }
                    }
                }
                ServiceStatus::Leader(leader) => {
                    match &leader.loading {
                        None => {
                            break Ok(MetaRequestGuard::new(self, leader.term));
                        }
                        Some(loading) => {
                            loading.wait().await;
                        }
                    }
                }
            }
        }
    }

    fn try_get_session(
        &self, client_id: &Option<ClientId>, 
    ) -> Result<Option<Arc<ClientSession>>, tonic::Status> {
        client_id.map_or_else(
            || Err(tonic::Status::invalid_argument("no client id")),
            |client_id| Ok(self.client_sessions.get(&client_id))
        )
    }

    fn get_session(
        &self, client_id: &Option<ClientId>, 
    ) -> Result<Arc<ClientSession>, tonic::Status> {
        self.try_get_session(client_id)?
            .ok_or_else(|| {
                tonic::Status::unauthenticated(
                    format!("no session for {:?}", client_id.unwrap())
                )
            })
    }

    pub async fn handle_create_data_node(
        &self, req: VolumeMngmtRequest
    ) -> Result<VolumeMngmtResponse, tonic::Status> {
        let mut guard = self.check_status().await?;
        self.data_nodes.create_data_node(req, &mut guard).await
    }
    pub async fn handle_create_volume(
        &self, req: VolumeMngmtRequest
    ) -> Result<VolumeMngmtResponse, tonic::Status> {
        let mut guard = self.check_status().await?;
        self.data_nodes.create_volume(req, &mut guard).await
    }

    pub async fn handle_data_node_beacon(
        &self, beacon: DataNodeBeacon
    ) -> Result<DataNodeBeaconAck, tonic::Status> {
        let _guard = self.check_status().await?;
        self.data_nodes.handle_beacon(self.get_batch_writer(), beacon).await
    }

    pub async fn handle_open_session(
        &self, req: SessionRequest
    ) -> Result<SessionResponse, tonic::Status> {
        let mut guard = self.check_status().await?;
        self.client_sessions.handle_open(req, &mut guard).await
    }

    pub async fn handle_close_session(
        &self, req: SessionRequest
    ) -> Result<SessionResponse, tonic::Status> {
        let _guard = self.check_status().await?;
        self.client_sessions.handle_close(self.get_batch_writer(), req).await
    }

    pub async fn handle_renew_session(
        &self, req: SessionRequest
    ) -> Result<SessionResponse, tonic::Status> {
        let _guard = self.check_status().await?;
        self.client_sessions.handle_renew(self.get_batch_writer(), req).await
    }

    pub async fn handle_create_stream(
        &self, ctx: ClientRequestContext, req: CreateRequest
    ) -> Result<CreateResponse, tonic::Status> {
        let mut guard = self.check_status().await?;
        let session = self.get_session(&ctx.client_id)?;
        self.stream_service.handle_create_stream(req, &session, &mut guard).await
    }

    pub async fn handle_open_stream(
        &self, ctx: ClientRequestContext, req: OpenRequest
    ) -> Result<OpenResponse, tonic::Status> {
        let mut guard = self.check_status().await?;
        let session = self.get_session(&ctx.client_id)?;
        self.stream_service.handle_open_stream(req, &session, &mut guard).await
    }

    pub async fn handle_close_stream(
        &self, ctx: ClientRequestContext, req: CloseRequest
    ) -> Result<CloseResponse, tonic::Status> {
        let mut guard = self.check_status().await?;
        let session = self.get_session(&ctx.client_id)?;
        self.stream_service.handle_close_stream(req, &session, &mut guard).await
    }

    pub async fn handle_alloc_extent(
        &self, ctx: ClientRequestContext, req: AllocExtentRequest
    ) -> Result<AllocExtentResponse, tonic::Status> {
        let mut guard = self.check_status().await?;
        let session = self.get_session(&ctx.client_id)?;
        self.stream_service.handle_alloc_extent(
            req, &session, &mut guard, &self.data_nodes,
        ).await
    }

    pub async fn handle_stat_extent(
        &self, _ctx: ClientRequestContext, req: StatExtentRequest
    ) -> Result<StatExtentResponse, tonic::Status> {
        let _guard = self.check_status().await?;
        self.stream_service.handle_stat_extent(req).await
    }
}

enum MetaBatchCommand {
    Write {
        store_req: StoreRequest,
        resp_tx: Option<oneshot::Sender<Result<(), ClientWriteError>>>,
    },
    Flush {
        resp_tx: oneshot::Sender<Result<(), ClientWriteError>>,
    }
}


pub(super) struct MetaBatchWriter {
    command_tx: mpsc::UnboundedSender<MetaBatchCommand>,
    flushed_upto: SystemTime,
}

impl MetaBatchWriter {
    const BATCH_SIZE: usize = 1024 * 1024;

    async fn flush_work(
        meta_svc_weak: Weak<MetaNodeService>,
        mut cmd_rx: mpsc::UnboundedReceiver<MetaBatchCommand>
    ) {
        let mut store_batch = None;
        let mut store_batch_size = 0;
        let mut written_txs = VecDeque::new();
        
        while let Some(cmd) = cmd_rx.recv().await {
            let store_req = match cmd {
                MetaBatchCommand::Write { store_req, resp_tx } => {
                    store_batch_size += store_batch
                        .get_or_insert_with(StoreRequest::default)
                        .append(store_req);

                    if let Some(resp_tx) = resp_tx {
                        written_txs.push_back(resp_tx);

                        while store_batch_size < Self::BATCH_SIZE {
                            match cmd_rx.try_recv() {
                                Ok(MetaBatchCommand::Write { store_req, resp_tx }) => {
                                    store_batch_size += store_batch
                                        .as_mut()
                                        .unwrap()
                                        .append(store_req);

                                    if let Some(resp_tx) = resp_tx {
                                        written_txs.push_back(resp_tx);
                                    }
                                }
                                Ok(MetaBatchCommand::Flush { resp_tx }) => {
                                    written_txs.push_back(resp_tx);
                                    break;
                                }
                                Err(_) => break,
                            }
                        }
                    }

                    if !written_txs.is_empty() ||
                       store_batch_size >= Self::BATCH_SIZE {
                        store_batch_size = 0;
                        store_batch.take().unwrap()
                    } else {
                        continue;
                    }
                }
                MetaBatchCommand::Flush { resp_tx } => {
                    if let Some(store_req) = store_batch.take() {
                        written_txs.push_back(resp_tx);
                        store_batch_size = 0;
                        store_req
                    } else {
                        let _ = resp_tx.send(Ok(()));
                        continue;
                    }
                }
            };

            let meta_service = match meta_svc_weak.upgrade() {
                None => {
                    info!("meta batch flusher exited");
                    break;
                }
                Some(svc) => svc,
            };
            let res = match meta_service.raft.client_write(store_req).await {
                Ok(_) => {
                    Ok(())
                }
                Err(RaftError::APIError(err@ClientWriteError::ForwardToLeader(_))) => {
                    Err(err)
                }
                Err(err@RaftError::APIError(ClientWriteError::ChangeMembershipError(_))) => {
                    panic!("unexpected raft err {:?}", err);
                }
                Err(RaftError::Fatal(err)) => {
                    panic!("fatal raft err {:?}", err);
                }
            };

            while let Some(resp_tx) = written_txs.pop_front() {
                let _ = resp_tx.send(res.clone());
            }
        }
    }

    pub fn get_flushed_upto(&self) -> SystemTime {
        self.flushed_upto
    }

    pub fn queue_write(
        &mut self, store_req: StoreRequest
    ) {
        self.command_tx.send(
            MetaBatchCommand::Write { store_req, resp_tx: None }
        ).unwrap();
    }

    pub fn raft_write(&mut self, store_req: StoreRequest) -> impl Future<Output=Result<(), ClientWriteError>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(
            MetaBatchCommand::Write { store_req, resp_tx: Some(resp_tx) }
        ).unwrap();
        async move {
            resp_rx.await.unwrap()
        }
    }

    pub fn client_write(&mut self, store_req: StoreRequest) -> impl Future<Output=Result<(), tonic::Status>> {
        let res = self.raft_write(store_req);
        async move {
            res.await.map_err(|err| map_raft_error(RaftError::APIError(err)))
        }
    }
}
 
pub(super) struct MetaRequestGuard<'a> {
    meta_service: &'a MetaNodeService,
    term: u64,
    log_stream: OnceCell<Arc<LogStream>>,
    extent: OnceCell<Arc<Extent>>,
    extent2: OnceCell<Arc<Extent>>,
    freed_spaces: OnceCell<(Vec<VolumeId>, VolumeStats)>
}

impl <'a> MetaRequestGuard<'a> {
    pub fn new (meta_service: &'a MetaNodeService, term: u64) -> Self {
        meta_service.inflight_ops.fetch_add(1, Ordering::Relaxed);
        Self {
            meta_service, term,
            log_stream: OnceCell::new(),
            extent: OnceCell::new(),
            extent2: OnceCell::new(),
            freed_spaces: OnceCell::new(),
        }
    }
    pub fn add_locked_stream(&self, log_stream: Arc<LogStream>) -> &Arc<LogStream> {
        let res = self.log_stream.set(log_stream);
        assert!(res.is_ok());
        self.log_stream.get().unwrap()
    }
    pub fn add_locked_extent(&self, extent: Arc<Extent>) -> &Arc<Extent> {
        let ext_cell = if let Some(old) = self.extent.get() {
            assert!(!Arc::ptr_eq(old, &extent));
            &self.extent2
        } else {
            &self.extent
        };
        let res = ext_cell.set(extent);
        assert!(res.is_ok());
        ext_cell.get().unwrap()
    }

    pub fn free_spaces(
        &self, store_req: &mut StoreRequest,
        places: Vec<ExtentPlacement>, config: &LogStreamConfig
    ) {
        let stats = VolumeStats::with_extent(config.extent_size);
        let mut vols = Vec::new();
        for place in places.into_iter() {
            stats.negtive().merge(&place.volume(), store_req);
            vols.push(place.volume())
        }
        let res = self.freed_spaces.set((vols, stats));
        assert!(res.is_ok());    
    }

    pub async fn client_write(&mut self, store_req: StoreRequest) -> Result<(), tonic::Status> {
        match self.meta_service.raft.client_write(store_req).await {
            Ok(_) => {
                Ok(())
            }
            Err(err@RaftError::APIError(ClientWriteError::ForwardToLeader(_))) => {
                Err(map_raft_error(err))
            }
            Err(err@RaftError::APIError(ClientWriteError::ChangeMembershipError(_))) => {
                panic!("unexpected raft err {:?}", err);
            }
            Err(RaftError::Fatal(err)) => {
                panic!("fatal raft err {:?}", err);
            }
        }
    }
}

impl Drop for MetaRequestGuard<'_> {
    fn drop(&mut self) {
        if let Some(extent) = self.extent2.take() {
            extent.finish_update(Ok(()));
        }
        if let Some(extent) = self.extent.take() {
            extent.finish_update(Ok(()));
        }
        if let Some(log_stream) = self.log_stream.take() {
            log_stream.finish_update(Ok(()));
        }
        if let Some((vols, stats)) = self.freed_spaces.take() {
            self.meta_service.data_nodes.free_spaces(vols, stats);
        }
        self.meta_service.inflight_ops.fetch_sub(1, Ordering::Relaxed);
    }
}


