use super::{checksum::ChecksumKind, spdk_log::LogHeaderWriter};
use super::{spdk_app::VolumeMeta, spdk_log::LogHeaderReader};
use crate::types::{
    common_proto::{ExtentId, ExtentVersion, VolumeId},
    extent_service::{
        AppendParams, AppendResponse, CreateParams, CreateResponse, DeleteParams, DeleteResponse,
        ExtentReadFlags, ExtentRequest, ExtentRequestKind, ExtentResponse, ExtentResponseKind,
        ReadParams, ReadResponse, SealResponse, StatResponse,
    },
};
use async_spdk::{
    blob::{Blob, BlobId, BlobStore},
    blob_bdev, env as spdk_env, event as spdk_event, SpdkError,
};
use bytes::{Buf, BufMut};
use futures::StreamExt;
use nix::errno::Errno;
use prost::Message;
use rand::{thread_rng, Rng};
use std::{
    rc::Rc, cell::RefCell, ops::{Deref, DerefMut}, mem::transmute,
    collections::{hash_map::Entry, HashMap, VecDeque},
};
use tokio::sync::Semaphore;
use tracing::{error, trace, warn, info};
use uuid::Uuid;

pub enum ExtentResponder {
    Flume(flume::Sender<ExtentResponse>),
}

impl ExtentResponder {
    pub fn flume(resp: flume::Sender<ExtentResponse>) -> Self {
        Self::Flume(resp)
    }

    async fn send(self, resp: ExtentResponse) {
        match self {
            ExtentResponder::Flume(tx) => {
                let _ = tx.send(resp);
            }
        }
    }
}

pub struct ExtentOp {
    request: ExtentRequest,
    responder: Option<ExtentResponder>,
}

impl ExtentOp {
    pub fn new(request: ExtentRequest, resp: ExtentResponder) -> Self {
        Self {
            request,
            responder: Some(resp),
        }
    }

    pub fn get_volume_id(&self) -> &VolumeId {
        self.request.volume_id.as_ref().unwrap()
    }

    async fn respond_errno(&mut self, errno: Errno) {
        self.respond_errcode(-(errno as i32)).await
    }

    async fn respond_errcode(&mut self, errcode: i32) {
        let resp = ExtentResponseKind::Errcode(errcode);
        self.responder.take().unwrap().send(
            (resp, &self.request).into()
        ).await;
    }

    async fn respond_ok<'a, R>(&'a mut self, resp: R) 
    where
        (R, &'a ExtentRequest): Into<ExtentResponse>
    {
        self.responder.take().unwrap().send(
            (resp, &self.request).into()
        ).await;
    }
}

#[derive(Clone, Message)]
struct ExtentMeta {
    #[prost(message, optional, tag = "1")]
    pub extent_id: Option<ExtentId>,
    #[prost(message, optional, tag = "2")]
    pub version: Option<ExtentVersion>,
    #[prost(uint32, tag = "3")]
    pub flags: u32,
    #[prost(uint64, tag = "4")]
    pub sentinel: u64,
    #[prost(uint32, tag = "5")]
    pub csum_kind: u32,
    #[prost(uint32, tag = "6")]
    pub first_log_offset: u32,
}

impl ExtentMeta {
    pub const XATTR_NAME: &'static str = "_extent_meta";

    fn new(extent_id: ExtentId) -> Self {
        Self {
            extent_id: Some(extent_id),
            ..Default::default()
        }
    }
    fn is_valid(&self) -> bool {
        self.extent_id.is_some() && self.version.is_some()
    }

    fn is_sealed(&self) -> bool {
        self.flags & ExtentMetaFlags::Sealed as u32 != 0
    }
    fn mark_sealed(&mut self) {
        self.flags |= ExtentMetaFlags::Sealed as u32;
    }
}

#[repr(u32)]
pub enum ExtentMetaFlags {
    Sealed = 1,
}

pub struct ExtentHandle {
    meta: ExtentMeta,
    blob: Option<Blob>,
    log_offsets: Vec<u32>,
    op_queue: VecDeque<ExtentOp>,
    flags: u32,
}

#[repr(u32)]
pub enum ExtentHandleFlags {
    Executing = 1,
    NeedsLoadLogOffs = 2,
    NeedsReplayLogs = 4,
}

impl ExtentHandle {
    fn with_meta(meta: ExtentMeta, blob: Option<Blob>) -> Self {
        let mut flags = 0;
        if blob.is_some() {
            if meta.is_sealed() {
                flags |= ExtentHandleFlags::NeedsLoadLogOffs as u32;
            } else {
                flags |= ExtentHandleFlags::NeedsReplayLogs as u32;
            }
        }
        Self {
            meta,
            blob,
            flags,
            log_offsets: Vec::new(),
            op_queue: VecDeque::new(),
        }
    }
    fn new(extent_id: ExtentId) -> Self {
        Self::with_meta(ExtentMeta::new(extent_id), None)
    }
    fn blob_id(&self) -> Option<BlobId> {
        self.blob.as_ref().map(|blob| blob.blob_id())
    }
    fn extent_id(&self) -> &ExtentId {
        self.meta.extent_id.as_ref().unwrap()
    }
    fn prepare_blob<F, R>(
        this: &RefCell<ExtentHandle>,
        func: F,
    ) -> Result<(BlobGuard<'_>, R), Errno>
    where
        F: FnOnce(&mut ExtentMeta, &Blob) -> Result<R, Errno>,
    {
        let extent = &mut *this.borrow_mut();
        if let Some(blob) = &extent.blob {
            let ret = func(&mut extent.meta, blob)?;
            let guard = BlobGuard {
                blob: Some(extent.blob.take().unwrap()),
                extent: this,
            };
            Ok((guard, ret))
        } else {
            Err(Errno::ENOENT)
        }
    }

    fn get_write_offset(&self) -> usize {
        if let Some(offset) = self.log_offsets.last() {
            *offset as usize
        } else {
            self.meta.first_log_offset as usize
        }
    }

    fn load_log_offsets(&mut self, buf: &[u8], io_unit_size: usize) -> ExtentVersion {
        let extent_id = self.meta.extent_id.unwrap();
        let sentinel = self.meta.sentinel;
        let csum_kind = ChecksumKind::try_from(self.meta.csum_kind).unwrap();

        let num_io_units = buf.len() / io_unit_size;
        let mut buf_off = 0;
        let mut next_version = ExtentVersion {
            log_seq: extent_id.log_seq,
            ..self.meta.version.unwrap()
        };
        for i in 0..num_io_units {
            let mut log_reader = LogHeaderReader::new(
                &buf[buf_off..],
                io_unit_size,
                csum_kind,
                sentinel,
                extent_id.stream_id,
                next_version,
            );
            if let Some(log_hdr) = log_reader.get() {
                if log_hdr.data_offset as usize == i && log_hdr.check_header_csum(csum_kind) {
                    if let Some(mut off_buf) = log_hdr.user_header {
                        while off_buf.remaining() >= 4 {
                            self.log_offsets.push(off_buf.get_u32_le());
                            next_version.log_seq += 1;
                        }
                    };
                } else {
                    break;
                }
            } else {
                break;
            }
            buf_off += io_unit_size;
        }
        self.flags &= !(ExtentHandleFlags::NeedsLoadLogOffs as u32);
        info!(?next_version, "load log offsets");
        next_version
    }

    fn test_and_mark_executing(&mut self) -> bool {
        if self.flags & ExtentHandleFlags::Executing as u32 == 0 {
            self.flags |= ExtentHandleFlags::Executing as u32;
            false
        } else {
            true
        }
    }

    fn clear_executing(&mut self) {
        self.flags &= !(ExtentHandleFlags::Executing as u32);
    }

    fn needs_replay_logs(&self) -> bool {
        self.flags & ExtentHandleFlags::NeedsReplayLogs as u32 != 0
    }
    fn needs_load_log_offs(&self) -> bool {
        self.flags & ExtentHandleFlags::NeedsLoadLogOffs as u32 != 0
    }

    fn put_spdk_blob(&mut self, blob: Blob) {
        self.blob.replace(blob);
    }
    fn has_spdk_blob(&self) -> bool {
        self.blob.is_some()
    }
    fn queue_op(&mut self, op: ExtentOp) {
        self.op_queue.push_back(op);
    }
    fn dequeue_op(&mut self) -> Option<ExtentOp> {
        self.op_queue.pop_front()
    }
}

struct BlobGuard<'a> {
    blob: Option<Blob>,
    extent: &'a RefCell<ExtentHandle>,
}

impl<'a> BlobGuard<'a> {
    fn new(blob: Blob, extent: &'a RefCell<ExtentHandle>) -> Self {
        Self {
            blob: Some(blob),
            extent,
        }
    }
    fn into_inner(mut self) -> Blob {
        self.blob.take().unwrap()
    }
}

impl Deref for BlobGuard<'_> {
    type Target = Blob;
    fn deref(&self) -> &Self::Target {
        self.blob.as_ref().unwrap()
    }
}

impl DerefMut for BlobGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.blob.as_mut().unwrap()
    }
}

impl Drop for BlobGuard<'_> {
    fn drop(&mut self) {
        if let Some(blob) = self.blob.take() {
            self.extent.borrow_mut().put_spdk_blob(blob);
        }
    }
}

enum PreparedUpdate<T> {
    Ready(T),
    Duplicated,
}

struct ExtentStoreInner {
    volume_meta: VolumeMeta,
    extent_map: HashMap<ExtentId, Rc<RefCell<ExtentHandle>>>,
    replay_queue: VecDeque<ExtentId>,
}

impl ExtentStoreInner {
    fn new(volume_meta: VolumeMeta) -> Self {
        Self {
            volume_meta,
            extent_map: HashMap::new(),
            replay_queue: VecDeque::new(),
        }
    }
}

pub struct ExtentStore {
    pub name: String,
    io_unit_size: usize,
    cluster_size: usize,
    inner: RefCell<ExtentStoreInner>,
    super_blob: Blob,
    blob_store: BlobStore,
    replay_sem: Semaphore,
}

impl ExtentStore {
    fn new(name: String, blob_store: BlobStore, super_blob: Blob, inner: ExtentStoreInner) -> Self {
        Self {
            name,
            io_unit_size: blob_store.io_unit_size() as _,
            cluster_size: blob_store.cluster_size() as _,
            inner: RefCell::new(inner),
            super_blob, blob_store,
            replay_sem: Semaphore::new(16),
        }
    }

    fn align_cluster_size(&self, val: usize) -> usize {
        val.next_multiple_of(self.cluster_size)
    }

    fn align_io_unit_size(&self, val: usize) -> usize {
        val.next_multiple_of(self.io_unit_size)
    }

    pub fn capacity(&self) -> u64 {
        self.blob_store.total_data_cluster_count() * self.cluster_size as u64
    }

    pub fn free_bytes(&self) -> u64 {
        self.blob_store.free_cluster_count() * self.cluster_size as u64
    }

    pub async fn create(blob_bdev: &str, vol_meta: VolumeMeta) -> Result<ExtentStore, SpdkError> {
        let mut bs_dev = blob_bdev::BlobStoreBDev::create(blob_bdev)?;
        let blob_store = BlobStore::init(&mut bs_dev).await?;

        let blob_id = blob_store.create_blob().await?;
        let (blob, blob_op_res) = match blob_store.open_blob(blob_id).await {
            Err(err) => (None, Err(err)),
            Ok(mut blob) => {
                let meta_buf = vol_meta.encode_to_vec();
                let blob_op_res = async {
                    blob.resize(1).await?;
                    let io_channel = blob_store.get_io_channel();
                    blob.write_zero(&io_channel, 0, blob_store.cluster_size()).await?;
                    blob.set_xattr(VolumeMeta::XATTR_NAME, meta_buf.as_slice())?;
                    blob.sync_metadata().await?;
                    blob_store.set_super(blob_id).await?;
                    Ok(())
                }
                .await;
                (Some(blob), blob_op_res)
            }
        };

        match blob_op_res {
            Err(err) => {
                if let Some(blob) = blob {
                    let _ = blob.close().await.map_err(|err1| {
                        warn!("close blob {} err {:?}", blob_id, err1);
                    });
                }
                let _ = blob_store.delete_blob(blob_id).await.map_err(|err1| {
                    warn!("delete blob {} err {:?}", blob_id, err1);
                });
                Err(err)
            }
            Ok(_) => Ok(Self::new(
                blob_bdev.to_owned(),
                blob_store,
                blob.unwrap(),
                ExtentStoreInner::new(vol_meta),
            )),
        }
    }

    pub async fn load(blob_bdev: &str, node_uuid: Uuid) -> Result<ExtentStore, SpdkError> {
        let mut bs_dev = blob_bdev::BlobStoreBDev::create(blob_bdev)?;
        let mut blob_store = BlobStore::load(&mut bs_dev).await?;

        let super_blob = blob_store.open_blob(blob_store.get_super().await?).await?;

        let vol_meta = match super_blob.get_xattr_value(VolumeMeta::XATTR_NAME, |value| {
            VolumeMeta::decode(value).ok()
        }) {
            Ok(Some(meta)) => {
                if node_uuid.to_string() != meta.node_uuid {
                    error!(
                        "volume {} has invalid node uuid {}, expected {}",
                        meta.volume_uuid,
                        meta.node_uuid,
                        node_uuid.to_string()
                    );
                    return Err(Errno::ENXIO.into());
                }
                meta
            }
            Ok(None) => {
                error!(
                    "failed to decode volume meta, blob {}",
                    super_blob.blob_id()
                );
                return Err(Errno::EIO.into());
            }
            Err(err) => {
                error!(
                    "failed to get extent meta xattr, blob {} err {:?}",
                    super_blob.blob_id(),
                    err
                );
                return Err(err);
            }
        };

        let mut store_inner = ExtentStoreInner::new(vol_meta);
        let mut blobs = blob_store.list_blobs();
        while let Some(blob) = blobs.next().await {
            let blob = blob?;
            if blob.blob_id() == super_blob.blob_id() {
                continue;
            }
            let meta = match blob.get_xattr_value(ExtentMeta::XATTR_NAME, |value| {
                ExtentMeta::decode(value).ok()
            }) {
                Ok(Some(meta)) => meta,
                Ok(None) => {
                    error!("failed to decode extent meta, blob {}", blob.blob_id());
                    continue;
                }
                Err(err) => {
                    error!(
                        "failed to get extent meta xattr, blob {} err {:?}",
                        blob.blob_id(),
                        err
                    );
                    continue;
                }
            };
            let extent_id = match &meta.extent_id {
                None => {
                    error!(
                        "no extent_id in decoded extent meta, blob {} ",
                        blob.blob_id()
                    );
                    continue;
                }
                Some(id) => *id,
            };
            match store_inner.extent_map.entry(extent_id) {
                Entry::Occupied(existing) => {
                    let existing = existing.get().borrow();
                    error!(
                        "got duplicated chunk, meta {:?}/{:?}, blob {:?}/{:?}",
                        existing.meta,
                        meta,
                        existing.blob_id(),
                        blob.blob_id()
                    );
                    continue;
                }
                Entry::Vacant(entry) => {
                    trace!("loaded chunk {:?} blob {}", entry.key(), blob.blob_id());
                    let extent = ExtentHandle::with_meta(meta, Some(blob));
                    let needs_replay = extent.needs_replay_logs();
                    entry.insert(Rc::new(RefCell::new(extent)));
                    if needs_replay {
                        store_inner.replay_queue.push_back(extent_id);
                    }
                }
            }
        }
        drop(blobs);

        Ok(Self::new(blob_bdev.to_owned(), blob_store, super_blob, store_inner))
    }

    pub async fn set_volume_id(&mut self, volume_id: VolumeId) -> Result<(), SpdkError> {
        let vol_meta = &mut self.inner.get_mut().volume_meta;
        vol_meta.volume_id.replace(volume_id);

        let meta_buf = vol_meta.encode_to_vec();
        self.super_blob
            .set_xattr(VolumeMeta::XATTR_NAME, meta_buf.as_slice())?;
        self.super_blob.sync_metadata().await?;
        Ok(())
    }

    pub fn get_volume_id(&self) -> Option<VolumeId> {
        self.inner.borrow().volume_meta.volume_id
    }

    pub fn get_volume_uuid(&self) -> String {
        self.inner.borrow().volume_meta.volume_uuid.clone()
    }

    pub fn get_num_extents(&self) -> usize {
        self.inner.borrow().extent_map.len()
    }

    fn get_or_create_extent_handle(
        &self,
        extent_id: &ExtentId,
        create: bool,
    ) -> Option<Rc<RefCell<ExtentHandle>>> {
        if create {
            let mut inner_mut = self.inner.borrow_mut();
            let extent = inner_mut
                .extent_map
                .entry(*extent_id)
                .or_insert_with(|| Rc::new(RefCell::new(ExtentHandle::new(*extent_id))));
            Some(extent.clone())
        } else {
            self.inner.borrow().extent_map.get(extent_id).cloned()
        }
    }

    fn remove_extent_handle(&self, chunk: Rc<RefCell<ExtentHandle>>) {
        let mut inner_mut = self.inner.borrow_mut();
        let removed = inner_mut
            .extent_map
            .remove(chunk.borrow().extent_id())
            .unwrap();
        assert!(Rc::ptr_eq(&chunk, &removed));
    }

    pub fn handle_extent_op(self: &Rc<Self>, op: ExtentOp) {
        let extent = self.get_or_create_extent_handle(
            op.request.extent_id.as_ref().unwrap(),
            op.request.is_create(),
        );

        if let Some(extent) = &extent {
            let mut extent_mut = extent.borrow_mut();
            if extent_mut.test_and_mark_executing() {
                extent_mut.queue_op(op);
                return;
            }
        }

        let this = self.clone();
        let mut curr_op = op;
        spdk_event::spawn_local(async move {
            let extent = match extent {
                None => {
                    curr_op.respond_errno(Errno::ENOENT).await;
                    return;
                }
                Some(extent) => extent,
            };

            if extent.borrow().needs_replay_logs() {
                let _guard = this.replay_sem.acquire().await;
                if let Err(err) = this.replay_extent_logs(&extent).await {
                    error!("error replay extent logs {:?}", err);
                }
            } else if extent.borrow().needs_load_log_offs() {
                if let Err(err) = this.load_extent_log_offsets(&extent).await {
                    error!("error loading extent log offsets {:?}", err);
                }
            }

            let deleted = loop {
                this.do_extent_op(&extent, curr_op).await;
                let mut extent_mut = extent.borrow_mut();
                match extent_mut.dequeue_op() {
                    Some(op) => {
                        curr_op = op;
                    }
                    None => {
                        extent_mut.clear_executing();
                        break !extent_mut.has_spdk_blob();
                    }
                }
            };
            if deleted {
                this.remove_extent_handle(extent);
            }
        })
        .detach();
    }

    async fn do_extent_op(&self, extent: &RefCell<ExtentHandle>, mut op: ExtentOp) {
        let res = match op.request.kind.as_ref().unwrap() {
            ExtentRequestKind::Stat(_) => self.do_extent_stat(extent, &mut op).await,
            ExtentRequestKind::Read(_) => self.do_extent_read(extent, &mut op).await,
            ExtentRequestKind::Append(_) => self.do_extent_append(extent, &mut op).await,
            ExtentRequestKind::Seal(_) => self.do_extent_seal(extent, &mut op).await,
            ExtentRequestKind::Create(_) => self.do_extent_create(extent, &mut op).await,
            ExtentRequestKind::Delete(_) => self.do_extent_delete(extent, &mut op).await,
        };
        if let Err(err) = res {
            op.respond_errcode(err.errcode()).await
        }
    }

    async fn do_extent_stat(
        &self,
        extent: &RefCell<ExtentHandle>,
        op: &mut ExtentOp,
    ) -> Result<(), SpdkError> {
        let extent_version = extent.borrow().meta.version;
        op.respond_ok(StatResponse { extent_version }).await;
        Ok(())
    }

    async fn do_extent_seal(
        &self,
        extent: &RefCell<ExtentHandle>,
        op: &mut ExtentOp,
    ) -> Result<(), SpdkError> {
        let extent_id = *extent.borrow().extent_id();
        let req_version = op.request.extent_version.unwrap();
            
        let (mut blob, prep_res) = ExtentHandle::prepare_blob(extent, |meta, _blob| {
            let meta_version = meta.version.as_ref().unwrap();
            if req_version.epoch != meta_version.epoch {
                return Err(Errno::ESTALE);
            }
            if req_version.log_seq > meta_version.log_seq {
                return Err(Errno::ERANGE);
            }
            if req_version.log_seq > 0 && req_version.log_seq < extent_id.log_seq {
                return Err(Errno::EINVAL);
            }
            if meta.is_sealed() && req_version.log_seq == meta_version.log_seq {
                return Ok(PreparedUpdate::Duplicated);
            }
            let csum_kind = ChecksumKind::try_from(meta.csum_kind).map_err(|_| Errno::EIO)?;
            Ok(PreparedUpdate::Ready((meta.sentinel, csum_kind)))
        })?;
  
        let (sentinel, csum_kind) = match prep_res {
            PreparedUpdate::Duplicated => {
                let used_bytes = blob.num_clusters() * self.cluster_size as u64;
                op.respond_ok(SealResponse { used_bytes }).await;
                return Ok(());
            }
            PreparedUpdate::Ready(res) => res,
        };

        let (num_logs, blob_end, dma_buf) = {
            let extent_ref = extent.borrow();
            let num_logs = if req_version.log_seq > 0 {
                (req_version.log_seq + 1 - extent_id.log_seq) as usize
            } else {
                0
            };

            let user_hdr_max = LogHeaderWriter::user_header_max_size(
                0, self.io_unit_size, csum_kind
            ).unwrap();

            let num_wr_units = (extent_ref.log_offsets.len() * 4).div_ceil(user_hdr_max);
            let mut dma_buf =
                spdk_env::DmaBuf::alloc(self.io_unit_size * num_wr_units, self.io_unit_size);

            let mut log_idx = 0;
            for i in 0..num_wr_units {
                let mut log_header = LogHeaderWriter::new(
                    &mut dma_buf.as_mut()[self.io_unit_size * i..],
                    0,
                    self.io_unit_size,
                    ChecksumKind::CRC32,
                );
                let user_hdr_len = std::cmp::min(user_hdr_max, (num_logs - log_idx) * 4);
                let mut buf = log_header.alloc_user_header(user_hdr_len).unwrap();
                let version = ExtentVersion {
                    log_seq: extent_id.log_seq + log_idx as u64,
                    ..req_version
                };
                while buf.remaining_mut() > 0 {
                    buf.put_u32_le(extent_ref.log_offsets[log_idx]);
                    log_idx += 1;
                }
                log_header.put_metadata(sentinel, extent_id.stream_id, version, i as u32);
                log_header.finalize();
            }
            let blob_end = if num_logs > 0 {
                extent_ref.log_offsets[num_logs - 1] as usize
            } else {
                0  
            };
            (num_logs, blob_end, dma_buf)
        };

        let dma_buf = unsafe { dma_buf.assume_init() };
        let io_channel = self.blob_store.get_io_channel();
        blob.write(&io_channel, 0, &dma_buf).await?;

        let num_clusters =
            self.align_cluster_size(blob_end * self.io_unit_size) / self.cluster_size;
        if blob.num_clusters() as usize > num_clusters {
            let _ = blob
                .resize(num_clusters as u64)
                .await
                .map_err(|err| warn!("resize blob {} err {:?}", blob.blob_id(), err));
        }

        let meta_buf = {
            let mut extent_mut = extent.borrow_mut();
            if let Some(version) = &mut extent_mut.meta.version {
                version.log_seq = req_version.log_seq;
            }
            extent_mut.log_offsets.truncate(num_logs);
            extent_mut.meta.mark_sealed();
            extent_mut.meta.encode_to_vec()
        };

        match blob.set_xattr(ExtentMeta::XATTR_NAME, meta_buf.as_slice()) {
            Ok(_) => blob.sync_metadata().await,
            Err(err) => {
                warn!("blob {} set_xattr err {:?}", blob.blob_id(), err);
                Err(err)
            }
        }?;
        let used_bytes = (num_clusters * self.cluster_size) as u64;
        op.respond_ok(SealResponse { used_bytes }).await;
        Ok(())
    }

    async fn do_extent_read(
        &self,
        extent: &RefCell<ExtentHandle>,
        op: &mut ExtentOp,
    ) -> Result<(), SpdkError> {
        let req_version = op.request.extent_version.unwrap();
        let params: &ReadParams = op.request.kind.as_ref().try_into().unwrap();

        let (blob, prep_res) = ExtentHandle::prepare_blob(extent, |meta, _blob| {
            let meta_version = meta.version.as_ref().unwrap();
            if req_version.log_seq > meta_version.log_seq {
                return Err(Errno::ERANGE);
            }
            let csum_kind = ChecksumKind::try_from(meta.csum_kind).map_err(|_| Errno::EIO)?;
            Ok((meta_version.epoch, meta.sentinel, csum_kind))
        })?;

        let (epoch, sentinel, csum_kind) = prep_res;

        let (extent_id, log_offset, log_end) = {
            let extent_ref = extent.borrow();
            let extent_id = *extent_ref.extent_id();
            if req_version.log_seq == extent_id.log_seq {
                (
                    extent_id,
                    extent_ref.meta.first_log_offset as usize,
                    extent_ref.log_offsets[0] as usize,
                )
            } else {
                let log_idx = (req_version.log_seq - extent_id.log_seq) as usize;
                (
                    extent_id,
                    extent_ref.log_offsets[log_idx - 1] as usize,
                    extent_ref.log_offsets[log_idx] as usize,
                )
            }
        };

        let io_channel = self.blob_store.get_io_channel();
        let mut dma_buf = spdk_env::DmaBuf::alloc(
            (log_end - log_offset) * self.io_unit_size,
            self.io_unit_size,
        );

        blob.read(
            &io_channel,
            (log_offset * self.io_unit_size) as u64,
            &mut dma_buf,
        )
        .await?;
        let dma_buf = unsafe { dma_buf.assume_init() };

        let mut log_reader = LogHeaderReader::new(
            dma_buf.as_ref(),
            self.io_unit_size,
            csum_kind,
            sentinel,
            extent_id.stream_id,
            ExtentVersion {
                epoch,
                log_seq: req_version.log_seq,
            },
        );

        let log_hdr = match log_reader.get() {
            None => return Err(Errno::EIO.into()),
            Some(log_hdr) => log_hdr,
        };

        let data_buf = &dma_buf.as_ref()[
            self.io_unit_size..self.io_unit_size + log_hdr.data_len as usize
        ];

        if params.flags & ExtentReadFlags::IgnoreDataCsum as u32 == 0 &&
           log_hdr.check_data_csums(
                csum_kind,
                data_buf,
                log_hdr.data_offset,
                self.io_unit_size
            ).is_err() {
            return Err(Errno::EIO.into());
        }
        op.respond_ok(ReadResponse {
            user_header: log_hdr.user_header.map_or(Vec::new(), ToOwned::to_owned),
            payload: data_buf.to_vec(),
        }).await;
        Ok(())
    }

    async fn do_extent_create(
        &self,
        extent: &RefCell<ExtentHandle>,
        op: &mut ExtentOp,
    ) -> Result<(), SpdkError> {
        if extent.borrow().has_spdk_blob() {
            return Err(Errno::EEXIST.into());
        }
        let req_version = op.request.extent_version.unwrap();

        let params: &CreateParams = op.request.kind.as_ref().try_into().unwrap();

        let blob_size = self.align_cluster_size(params.size as usize);
        let num_clusters = blob_size / self.cluster_size;
        let num_io_units = blob_size / self.io_unit_size;

        let csum_kind = ChecksumKind::CRC32;
        let user_hdr_max = LogHeaderWriter::user_header_max_size(
            0, self.io_unit_size, csum_kind
        ).unwrap();

        let first_log_offset =(num_io_units * 4)
            .div_ceil(user_hdr_max)
            .next_power_of_two() as u32;

        let version = Some(ExtentVersion {
            log_seq: 0,
            epoch: req_version.epoch,
        });
        let sentinel = thread_rng().gen();
        let extent_meta = ExtentMeta {
            extent_id: op.request.extent_id,
            version,
            flags: 0,
            sentinel,
            csum_kind: csum_kind as u32,
            first_log_offset,
        };
        if !extent_meta.is_valid() {
            return Err(Errno::EINVAL.into());
        }

        let blob_id = self.blob_store.create_blob().await?;
        let (blob, blob_op_res) = match self.blob_store.open_blob(blob_id).await {
            Err(err) => (None, Err(err)),
            Ok(mut blob) => {
                let op_res = async {
                    blob.resize(num_clusters as u64).await?;
                    let meta_buf = {
                        let mut extent_mut = extent.borrow_mut();
                        extent_mut.meta = extent_meta;
                        extent_mut.meta.encode_to_vec()
                    };
                    blob.set_xattr(ExtentMeta::XATTR_NAME, meta_buf.as_slice())?;
                    blob.sync_metadata().await?;
                    Ok(())
                }
                .await;
                (Some(blob), op_res)
            }
        };

        match blob_op_res {
            ret @ Err(_) => {
                if let Some(blob) = blob {
                    let _ = blob.close().await.map_err(|err| {
                        warn!("close blob {} err {:?}", blob_id, err);
                    });
                }
                let _ = self.blob_store.delete_blob(blob_id).await.map_err(|err| {
                    warn!("delete blob {} err {:?}", blob_id, err);
                });
                ret
            }
            Ok(_) => {
                let _guard = BlobGuard::new(blob.unwrap(), extent);
                let avail_bytes = blob_size - (self.io_unit_size * first_log_offset as usize);
                op.respond_ok(CreateResponse{ avail_bytes: avail_bytes as u64 }).await;
                Ok(())
            }
        }
    }

    async fn do_extent_delete(
        &self,
        extent: &RefCell<ExtentHandle>,
        op: &mut ExtentOp,
    ) -> Result<(), SpdkError> {
        let _params: &DeleteParams = op.request.kind.as_ref().try_into().unwrap();
        let req_version = op.request.extent_version.unwrap();
        let (blob, _) = ExtentHandle::prepare_blob(extent, |meta, _| {
            let meta_version = meta.version.as_ref().unwrap();
            if req_version.epoch != meta_version.epoch {
                return Err(Errno::ESTALE);
            }
            *meta = Default::default();
            Ok(())
        })?;
        let blob_id = blob.blob_id();
        let _ = blob.into_inner().close().await.map_err(|err| {
            warn!("close blob {} err {:?}", blob_id, err);
        });
        let _ = self.blob_store.delete_blob(blob_id).await.map_err(|err| {
            warn!("delete blob {} err {:?}", blob_id, err);
        });
        op.respond_ok(DeleteResponse::default()).await;
        Ok(())
    }

    async fn do_extent_append(
        &self,
        extent: &RefCell<ExtentHandle>,
        op: &mut ExtentOp,
    ) -> Result<(), SpdkError> {
        let params: &AppendParams = op.request.kind.as_ref().try_into().unwrap();
        let req_version = op.request.extent_version.unwrap();

        let extent_id = *extent.borrow().extent_id();
        let user_header = &params.user_header;
        let payload = &params.payload;

        let (blob, prep_res) = ExtentHandle::prepare_blob(extent, |meta, _blob| {
            let meta_version = meta.version.as_ref().unwrap();
            if req_version.epoch != meta_version.epoch {
                return Err(Errno::ESTALE);
            }
            if req_version.log_seq < extent_id.log_seq {
                return Err(Errno::EINVAL);
            }
            if req_version.log_seq <= meta_version.log_seq {
                return Ok(PreparedUpdate::Duplicated);
            }
            if req_version.log_seq != meta_version.log_seq + 1 &&
               !(meta_version.log_seq == 0 && req_version.log_seq == extent_id.log_seq) {
                return Err(Errno::ERANGE);
            }
            let csum_kind = ChecksumKind::try_from(meta.csum_kind).map_err(|_| Errno::EIO)?;
            let user_hdr_max = LogHeaderWriter::user_header_max_size(
                self.align_io_unit_size(payload.len()),
                self.io_unit_size,
                csum_kind,
            )?;
            if user_header.len() > user_hdr_max {
                return Err(Errno::ENOSPC);
            }
            Ok(PreparedUpdate::Ready((meta.sentinel, csum_kind)))
        })?;

        let (sentinel, csum_kind) = match  prep_res {
            PreparedUpdate::Duplicated => {
                op.respond_ok(AppendResponse{}).await;
                return Ok(());
            }
            PreparedUpdate::Ready(res) => res,
        };

        let io_channel = self.blob_store.get_io_channel();
        let mut dma_buf = spdk_env::DmaBuf::alloc(
            self.align_io_unit_size(payload.len()) + self.io_unit_size,
            self.io_unit_size,
        );

        let (header_buf, payload_buf) = dma_buf.as_mut().split_at_mut(self.io_unit_size);

        {
            let mut buf = &mut payload_buf[..];
            buf.put_slice(payload.as_slice());
            if buf.remaining_mut() > 0 {
                buf.put_bytes(0, buf.remaining_mut());
            }
        }

        let mut log_header = LogHeaderWriter::new(
            header_buf,
            payload.len(),
            self.io_unit_size,
            csum_kind
        );

        log_header
            .calc_data_csums(unsafe { transmute(payload_buf) })
            .unwrap();
        log_header.put_user_header(user_header.as_slice()).unwrap();
        let write_offset = extent.borrow().get_write_offset();
        log_header.put_metadata(
            sentinel,
            extent_id.stream_id,
            req_version,
            write_offset as u32 + 1,
        );
        log_header.finalize();

        let dma_buf = unsafe { dma_buf.assume_init() };

        blob.write(
            &io_channel,
            (write_offset * self.io_unit_size) as u64,
            &dma_buf,
        ).await?;

        {
            let mut extent_mut = extent.borrow_mut();
            extent_mut.meta.version = Some(req_version);
            extent_mut
                .log_offsets
                .push((write_offset + (dma_buf.len() / self.io_unit_size)) as u32);
        }

        op.respond_ok(AppendResponse{}).await;
        Ok(())
    }

    async fn load_extent_log_offsets(
        &self,
        extent: &RefCell<ExtentHandle>,
    ) -> Result<(), SpdkError> {
        let extent_id = *extent.borrow().extent_id();
        let (blob, info) = ExtentHandle::prepare_blob(extent, |meta, _| {
            ChecksumKind::try_from(meta.csum_kind).map_err(|_| Errno::EIO)?;
            assert!(meta.is_sealed());
            Ok((meta.version.unwrap(), meta.first_log_offset as usize))
        })?;

        let (extent_version, first_log_offset) = info;
        let mut dma_buf =
            spdk_env::DmaBuf::alloc(self.io_unit_size * first_log_offset, self.io_unit_size);
        let io_channel = self.blob_store.get_io_channel();
        blob.read(&io_channel, 0, &mut dma_buf).await?;
        let dma_buf = unsafe { dma_buf.assume_init() };

        let next_version = extent
            .borrow_mut()
            .load_log_offsets(dma_buf.as_ref(), self.io_unit_size);
        if (extent_version.log_seq == 0 && extent_id.log_seq != next_version.log_seq)
            || (extent_version.log_seq > 0 && extent_version.log_seq + 1 != next_version.log_seq)
        {
            error!(
                ?extent_id,
                "inconsistent version and logs {:?}, {:?}", extent_version, next_version
            );
        }

        Ok(())
    }

    async fn replay_extent_logs(&self, extent: &RefCell<ExtentHandle>) -> Result<(), SpdkError> {
        let extent_id = *extent.borrow().extent_id();
        info!(?extent_id, "replay extent_logs");

        let (blob, info) = ExtentHandle::prepare_blob(extent, |meta, _| {
            let csum_kind = ChecksumKind::try_from(meta.csum_kind).map_err(|_| Errno::EIO)?;
            let first_log_offset = meta.first_log_offset as usize;
            Ok((
                meta.version.unwrap(),
                meta.sentinel,
                first_log_offset,
                csum_kind,
            ))
        })?;

        let (version, sentinel, first_log_offset, csum_kind) = info;
        let mut next_version = ExtentVersion {
            log_seq: extent_id.log_seq,
            ..version
        };

        let mut dma_buf = spdk_env::DmaBuf::alloc(self.cluster_size, self.io_unit_size);

        let units_per_cluster = self.cluster_size / self.io_unit_size;
        let mut io_unit_idx = 0;
        let mut buf_cluster_idx = usize::MAX;
        let mut log_reader_buf = Vec::with_capacity(self.io_unit_size);

        let io_channel = self.blob_store.get_io_channel();
        while io_unit_idx < blob.num_clusters() as usize * units_per_cluster {
            let cluster_idx = io_unit_idx / units_per_cluster;
            if buf_cluster_idx != cluster_idx {
                blob.read(
                    &io_channel,
                    (self.cluster_size * cluster_idx) as u64,
                    &mut dma_buf,
                ).await?;
                buf_cluster_idx = cluster_idx;
            }

            let buf: &[u8] = unsafe { transmute(dma_buf.as_mut()) };
            let mut buf_off = (io_unit_idx % units_per_cluster) * self.io_unit_size;

            if io_unit_idx < first_log_offset {
                assert!(io_unit_idx == 0);
                next_version = extent.borrow_mut().load_log_offsets(
                    &buf[..self.io_unit_size * first_log_offset],
                    self.io_unit_size,
                );
                io_unit_idx = first_log_offset;
                if let Some(idx) = extent.borrow().log_offsets.last() {
                    io_unit_idx = *idx as usize
                }
            } else {
                let log_hdr = if !log_reader_buf.is_empty() {
                    let mut log_reader = LogHeaderReader::new(
                        log_reader_buf.as_slice(),
                        self.io_unit_size,
                        csum_kind,
                        sentinel,
                        extent_id.stream_id,
                        next_version,
                    );
                    log_reader.get().unwrap()
                } else {
                    let mut log_reader = LogHeaderReader::new(
                        &buf[buf_off..],
                        self.io_unit_size,
                        csum_kind,
                        sentinel,
                        extent_id.stream_id,
                        next_version,
                    );
                    buf_off += self.io_unit_size;
                    io_unit_idx += 1;

                    let log_hdr = match log_reader.get() {
                        None => break,
                        Some(log_hdr) => log_hdr,
                    };

                    if !log_hdr.check_header_csum(csum_kind) {
                        break;
                    }
                    log_hdr
                };

                let data_remaining = self.align_io_unit_size(log_hdr.data_len as usize)
                    - (io_unit_idx - log_hdr.data_offset as usize) * self.io_unit_size;

                let csum_data_len = std::cmp::min(self.cluster_size - buf_off, data_remaining);

                if csum_data_len > 0 {
                    if let Err(_err) = log_hdr.check_data_csums(
                        csum_kind,
                        &buf[buf_off..(buf_off + csum_data_len)],
                        io_unit_idx as u32,
                        self.io_unit_size,
                    ) {
                        break;
                    }
                }

                io_unit_idx += csum_data_len / self.io_unit_size;

                if csum_data_len < data_remaining {
                    if log_reader_buf.is_empty() {
                        log_reader_buf.put_slice(&buf[(buf_off - self.io_unit_size)..buf_off]);
                    }
                } else {
                    log_reader_buf.clear();
                    next_version.log_seq += 1;
                    extent.borrow_mut().log_offsets.push(io_unit_idx as u32);
                }
            }
        }

        let mut extent_mut = extent.borrow_mut();
        if next_version.log_seq == extent_id.log_seq {
            if let Some(version) = &mut extent_mut.meta.version {
                version.log_seq = 0;
            }
            extent_mut.log_offsets.clear();
        } else if let Some(version) = &mut extent_mut.meta.version {
            version.log_seq = next_version.log_seq - 1
        }

        extent_mut.flags &= !(ExtentHandleFlags::NeedsReplayLogs as u32);

        Ok(())
    }
}

impl Drop for ExtentStore {
    fn drop(&mut self) {
        eprintln!("droping extent store {}", self.name);
    }
}
