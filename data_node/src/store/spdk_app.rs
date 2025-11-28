use std::{
    rc::Rc, sync::{Arc, RwLock},  cell::RefCell, thread_local,
    collections::{ btree_map::Entry, BTreeMap }, 
};
use async_spdk::{
    env as spdk_env, blob_bdev,
    event::{self as spdk_event, AppOpts as SpdkAppOpts},
    thread::SpdkPoller, SpdkError,
};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use tokio::{
    runtime, sync::{mpsc, oneshot, Semaphore}, task::JoinHandle
};
use uuid::Uuid;
use prost::Message;
use arc_swap::ArcSwap;
use nix::errno::Errno;
use super::spdk_store::{ExtentOp, ExtentResponder, ExtentStore};
use crate::types::{
    common_proto::{DataNodeId, VolumeId},
    extent_service::{ ExtentRequest, ExtentResponse },
};
use msc_meta_node::{DataVolumeStats, DataVolumeFlags };
use tracing::{error};

#[derive(Clone, Message)]
pub(super) struct VolumeMeta {
    #[prost(message, optional, tag = "1")]
    pub volume_id: Option<VolumeId>,
    #[prost(string, tag = "2")]
    pub volume_uuid: String,
    #[prost(string, tag = "3")]
    pub node_uuid: String,
}

impl VolumeMeta {
    pub const XATTR_NAME: &'static str = "_volume_meta";
}

struct VolumeOp {
    bdev: String,
    kind: VolumeOpKind,
}
enum VolumeOpKind {
    PrepCreate {
        meta: VolumeMeta,
        resp_tx: oneshot::Sender<Result<u64, SpdkError>>,
    },
    FinishCreate {
        volume_id: Option<VolumeId>,
        resp_tx: oneshot::Sender<Result<(), SpdkError>>,
    },
    Load {
        node_uuid: Uuid,
        resp_tx: oneshot::Sender<Result<VolumeId, SpdkError>>,
    },
    GatherStats {
        volume_id: VolumeId,
        resp_tx: mpsc::Sender<DataVolumeStats>,
    },
}

enum SpdkStoreOp {
    Extent(ExtentOp),
    Volume(VolumeOp),
}

struct SpdkStoreShard {
    stores: BTreeMap<VolumeId, Rc<ExtentStore>>,
    creating_stores: BTreeMap<String, ExtentStore>,
    poller: Option<SpdkPoller>,
}

impl SpdkStoreShard {
    fn new() -> Self {
        Self {
            stores: BTreeMap::new(),
            creating_stores: BTreeMap::new(),
            poller: None,
        }
    }

    fn handle_volume_op(op: VolumeOp) {
        spdk_event::spawn_local(async move {
            Self::do_volume_op(op).await;
        }).detach();
    }

    async fn do_volume_op(op: VolumeOp) {
        let bdev = op.bdev;
        match op.kind {
            VolumeOpKind::PrepCreate { meta, resp_tx } => {
                let res = match ExtentStore::create(&bdev, meta).await {
                    Ok(store) => {
                        let capacity = store.capacity();
                        SpdkStoreApp::SHARD.with(|shard| {
                            shard.borrow_mut().creating_stores.insert(bdev, store);
                        });
                        Ok(capacity)
                    }
                    Err(err) => Err(err),
                };
                let _ = resp_tx.send(res);
            }
            VolumeOpKind::FinishCreate { volume_id, resp_tx } => {
                if let Some(mut store) = SpdkStoreApp::SHARD.with(|shard| {
                    shard.borrow_mut().creating_stores.remove(&bdev)
                }) {
                    let res = match volume_id {
                        Some(volume_id) => {
                            match store.set_volume_id(volume_id).await {
                                Ok(_) => {
                                    SpdkStoreApp::shard_add_store(volume_id, store)
                                }
                                Err(err) => Err(err),
                            }
                        }
                        None => Ok(()),
                    };
                    let _ = resp_tx.send(res);
                } else {
                    let _ = resp_tx.send(Err(Errno::ENODEV.into()));
                }
            }
            VolumeOpKind::Load { node_uuid, resp_tx } => {
                let res = match ExtentStore::load(&bdev, node_uuid).await {
                    Ok(store) => {
                        if let Some(volume_id) = store.get_volume_id() {
                            SpdkStoreApp::shard_add_store(volume_id, store)
                                .map(|_| volume_id)
                        } else {
                            Err(Errno::ENOKEY.into())
                        }
                    }
                    Err(err) => Err(err),
                };
                let _ = resp_tx.send(res);
            }
            VolumeOpKind::GatherStats { volume_id, resp_tx } => {
                SpdkStoreApp::SHARD.with(|shard| {
                    let shard = shard.borrow();
                    if let Some(store) = shard.stores.get(&volume_id) {
                        let capacity = store.capacity();
                        let used_bytes = capacity.saturating_sub(store.free_bytes());
                        let permit = resp_tx.try_reserve().unwrap();
                        permit.send(DataVolumeStats {
                            volume_id: Some(volume_id),
                            volume_uuid: store.get_volume_uuid(),
                            flags: DataVolumeFlags::Up as u32,
                            used_extents: store.get_num_extents() as u64,
                            capacity, used_bytes,
                        });
                    }
                });
            }
        }
    }
}

pub struct SpdkStoreApp {
    node_uuid: Uuid,
    spdk_task: JoinHandle<Result<(), SpdkError>>,
    op_queues: Vec<flume::Sender<SpdkStoreOp>>,
    volume_shards: ArcSwap<BTreeMap<VolumeId, usize>>,
    bdev_volumes: RwLock<BTreeMap<String, Option<VolumeId>>>,
    volume_sem: Semaphore,
}

impl SpdkStoreApp {    
    thread_local! {
        static SHARD: RefCell<SpdkStoreShard> = RefCell::new(SpdkStoreShard::new());
    }

    fn create_local_poller(
        op_queue: flume::Receiver<SpdkStoreOp>
    ) -> Result<(), SpdkError> {
        let poller = SpdkPoller::new(move || {
            let mut count = 0;
            while let Ok(op) = op_queue.try_recv() {
                match op {
                    SpdkStoreOp::Extent(op) => {
                        Self::SHARD.with(|shard| {
                            let shard = shard.borrow();
                            if let Some(store) = shard.stores.get(op.get_volume_id()) {
                                store.handle_extent_op(op)
                            }
                        });
                    }
                    SpdkStoreOp::Volume(op) => {
                        SpdkStoreShard::handle_volume_op(op)
                    }
                }
                count += 1;
                if count > 16 {
                    break;
                }
            }
            count > 0
        })?;
        Self::SHARD.with(|shard| {
            let mut shard = shard.borrow_mut();
            shard.poller.replace(poller);
        });
        Ok(())
    }

    fn stop_all_pollers() {
        for core in spdk_env::get_reactor_cores().into_iter() {
            spdk_event::SpdkEvent::call_on(core, || {
                Self::SHARD.with(|shard| {
                    let mut shard = shard.borrow_mut();
                    let _ = shard.poller.take();
                });
            })
            .ok();
        }
    }

    pub fn shutdown() {
        Self::stop_all_pollers();
        spdk_event::send_shutdown();
    }

    pub async fn wait_until_exit(self) -> Result<(), SpdkError> {
        self.spdk_task.await.unwrap()
    }

    fn shard_add_store(volume_id: VolumeId, store: ExtentStore) -> Result<(), SpdkError> {
        SpdkStoreApp::SHARD.with(|shard| {
            match shard.borrow_mut().stores.entry(volume_id) {
                Entry::Vacant(v) => {
                    v.insert(Rc::new(store));
                    Ok(())
                }
                Entry::Occupied(_) => {
                    Err(Errno::EEXIST.into())
                }
            }
        })   
    }

    async fn queue_op(&self, shard_idx: usize, op: SpdkStoreOp) {
        self.op_queues[shard_idx].send_async(op).await.unwrap()
    }

    fn set_volume_shard(&self, volume_id: VolumeId, shard_idx: usize) {
        let mut volume_shards: BTreeMap<_, _> =
            self.volume_shards.load().iter().map(|(k, v)| (*k, *v)).collect();
        volume_shards.insert(volume_id, shard_idx);
        self.volume_shards.store(Arc::new(volume_shards));
    }

    pub async fn handle_extent_op(
        &self, req: ExtentRequest, resp_tx: flume::Sender<ExtentResponse>
    ) {
        if !req.is_valid() {
            let _ = resp_tx.send(ExtentResponse::errno(&req, Errno::EINVAL));
            return;
        }
        
        if let Some(shard_idx) = self.volume_shards.load().get(req.volume_id.as_ref().unwrap()) {
            self.queue_op(*shard_idx, SpdkStoreOp::Extent(
                ExtentOp::new(req, ExtentResponder::flume(resp_tx))
            )).await;
        } else {
            let _ = resp_tx.send(ExtentResponse::errno(&req, Errno::ENODEV));
        }
    }

    fn choose_volume_shard(&self) -> usize {
        let num_shards = self.op_queues.len();
        let mut shard_vols = Vec::with_capacity(num_shards);
        for i in 0..num_shards {
            shard_vols.push((i, 0));
        }
        for (_, shard_idx) in self.volume_shards.load().iter() {
            shard_vols[*shard_idx].1 += 1;
        }
        shard_vols.iter().min_by(|x, y| x.1.cmp(&y.1)).unwrap().0
    }

    pub async fn prepare_create_volume(
        &self, bdev: String, shard_seq: Option<usize>, 
    ) -> Result<CreateVolumeGuard, SpdkError> {
        match self.bdev_volumes.write().unwrap().entry(bdev.clone()) {
            Entry::Occupied(_) => {
                return Err(Errno::EBUSY.into());
            }
            Entry::Vacant(v) => {
                v.insert(None);
            }
        }

        let shard_idx = match shard_seq {
            Some(seq) => seq & self.op_queues.len(),
            None => self.choose_volume_shard(),
        };

        let volume_uuid = Uuid::new_v4();
        let meta = VolumeMeta {
            volume_id: None,
            volume_uuid: volume_uuid.to_string(),
            node_uuid: self.node_uuid.to_string(),
        };

        let (resp_tx, resp_rx) = oneshot::channel();
        self.queue_op(shard_idx, SpdkStoreOp::Volume(VolumeOp{
            bdev: bdev.clone(),
            kind: VolumeOpKind::PrepCreate { meta, resp_tx }
        })).await;
        let capacity = resp_rx.await.unwrap()?;
        
        Ok(CreateVolumeGuard {
            store_app: self,
            bdev_name: Some(bdev),
            volume_uuid,
            capacity,
            shard_idx,
        })
    }

    async fn finish_create_volume(
        &self, shard_idx: usize, bdev: String, volume_id: Option<VolumeId>,
    ) -> Result<(), SpdkError> {

        match self.bdev_volumes.write().unwrap().entry(bdev.clone()) {
            Entry::Occupied(mut o) => {
                if let Some(id) = volume_id {
                    o.get_mut().replace(id);
                } else {
                    o.remove();
                }
            }
            Entry::Vacant(_) => {
                error!("bdev_volumes does not contain bdev {}", bdev);
            }
        }

        let (resp_tx, resp_rx) = oneshot::channel();
        self.queue_op(shard_idx, SpdkStoreOp::Volume(VolumeOp{
            bdev,
            kind: VolumeOpKind::FinishCreate { volume_id, resp_tx }
        })).await;
        resp_rx.await.unwrap()?;

        if let Some(volume_id) = volume_id {
            let _guard = self.volume_sem.acquire().await.unwrap();
            self.set_volume_shard(volume_id, shard_idx);
        }
        Ok(())
    }

    async fn load_volume(&self, shard_idx: usize, bdev: String) -> Result<VolumeId, SpdkError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.queue_op(shard_idx, SpdkStoreOp::Volume(VolumeOp{
            bdev: bdev.clone(),
            kind: VolumeOpKind::Load { node_uuid: self.node_uuid, resp_tx }
        })).await;
        let volume_id = resp_rx.await.unwrap()?;
        let _guard = self.volume_sem.acquire().await.unwrap();
        
        self.bdev_volumes.write().unwrap().insert(bdev, Some(volume_id));
        self.set_volume_shard(volume_id, shard_idx);

        Ok(volume_id)
    }

    pub async fn load_volumes(&self) -> Vec<(String, Result<VolumeId, SpdkError>)> {
        let mut shard_idx = 0;
        let results = FuturesUnordered::new();
        for bdev in blob_bdev::BlobStoreBDev::list_all().into_iter() {
            results.push(async move {
                let res = self.load_volume(shard_idx, bdev.clone()).await;
                (bdev, res)
            });
            shard_idx += 1;
            shard_idx %= self.op_queues.len();
        }
        results.collect::<Vec<_>>().await
    }

    pub async fn gather_volumes_stats(&self) -> Vec<DataVolumeStats> {
        let count = self.volume_shards.load().len();
        let mut out_stats = Vec::with_capacity(count);
        if count > 0 {
            let (stats_tx, mut stats_rx) = mpsc::channel(count);
            for (&volume_id, &shard_idx) in self.volume_shards.load().iter() {
                self.queue_op(shard_idx, SpdkStoreOp::Volume(VolumeOp {
                    bdev: String::new(),
                    kind: VolumeOpKind::GatherStats { volume_id, resp_tx: stats_tx.clone() }
                })).await;
            }
            drop(stats_tx);
            while let Some(stats) = stats_rx.recv().await {
                out_stats.push(stats);
            }
            out_stats.sort_by(|a, b| a.volume_id.partial_cmp(&b.volume_id).unwrap())
        }
        out_stats
    }
}

pub struct CreateVolumeGuard<'a> {
    store_app: &'a SpdkStoreApp,
    bdev_name: Option<String>,
    volume_uuid: Uuid,
    capacity: u64,
    shard_idx: usize,
}

impl<'a> CreateVolumeGuard<'a> {
    pub async fn created(mut self, volume_id: VolumeId) -> Result<(), SpdkError> {
        let bdev_name = self.bdev_name.take().unwrap();
        self.store_app.finish_create_volume(
            self.shard_idx, bdev_name, Some(volume_id)
        ).await
    }

    pub fn get_uuid(&self) -> &Uuid {
        &self.volume_uuid
    }

    pub fn get_capacity(&self) -> u64 {
        self.capacity
    }
}

impl<'a> Drop for CreateVolumeGuard<'a> {
    fn drop(&mut self) {
        if let Some(bdev_name) = self.bdev_name.take() {
            self.store_app.finish_create_volume(
                self.shard_idx, bdev_name, None
            ).now_or_never();
        }
    }
}

pub struct SpdkStoreBuilder {
    node_id: DataNodeId,
    node_uuid: Uuid,
    spdk_opts: SpdkAppOpts,
}

impl SpdkStoreBuilder {
    pub fn new(node_id: DataNodeId, node_uuid: Uuid) -> Self {
        Self {
            node_id, node_uuid,
            spdk_opts: SpdkAppOpts::new(),
        }
    }

    pub fn app_name(mut self, name: &str) -> Self {
        self.spdk_opts = self.spdk_opts.name(name);
        self
    }

    pub fn reactor_mask(mut self, mask: &str) -> Self {
        self.spdk_opts = self.spdk_opts.reactor_mask(mask);
        self
    }

    pub fn config_file(mut self, config_file: &str) -> Self {
        self.spdk_opts = self.spdk_opts.config_file(config_file);
        self
    }

    pub async fn build(self, runtime: runtime::Handle) -> Result<SpdkStoreApp, SpdkError> {
        let spdk_opts = self.spdk_opts;
        let (init_tx, init_rx) = oneshot::channel();
        let spdk_task = runtime.spawn_blocking(move || {
            spdk_opts.app_start(async move {
                let main_core = spdk_env::get_current_core().unwrap();
                let mut op_queues = Vec::new();
                for core in spdk_env::get_reactor_cores() {
                    let (op_tx, op_rx) = flume::bounded(512);
                    let poller_res = if core == main_core {
                        SpdkStoreApp::create_local_poller(op_rx)
                    } else {
                        let (resp_tx, resp_rx) = oneshot::channel();
                        let called = spdk_event::SpdkEvent::call_on(core, move || {
                            resp_tx.send(
                                SpdkStoreApp::create_local_poller(op_rx)
                            ).unwrap();
                        });
                        match called {
                            Ok(_) => {
                                resp_rx.await.unwrap()
                            }
                            Err(err) => Err(err)
                        }
                    };
                    match poller_res {
                        Ok(_) => {
                            op_queues.push(op_tx);
                        }
                        Err(err) => {
                            SpdkStoreApp::shutdown();
                            init_tx.send(Err(err)).unwrap();
                            return;
                        }
                    }
                }
                init_tx.send(Ok(op_queues)).unwrap();
            })
        });

        match init_rx.await.unwrap() {
            Ok(op_queues) => {
                let node_uuid = self.node_uuid;
                Ok(SpdkStoreApp {
                    node_uuid, spdk_task, op_queues,
                    volume_shards: ArcSwap::from_pointee(BTreeMap::new()),
                    bdev_volumes: RwLock::new(BTreeMap::new()),
                    volume_sem: Semaphore::new(1),
                })
            }
            Err(err) => {
                let _ = spdk_task.await;
                Err(err)
            }
        }
    }
}

