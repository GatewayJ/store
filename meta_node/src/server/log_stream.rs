use std::sync::{
    atomic::{ AtomicU32, Ordering },
    Arc, Mutex, MutexGuard
};
use crate::{
    store::{StoreColumnFamily, StoreRequest},
    types::{
        common_proto::{
            MetaStoreKey, ClientId, ExtentId, ExtentPlacement, ExtentVersion,
            LogStreamConfig, LogStreamId, VolumeIdStoreKey
        },
        stream_service::SealExtentRequest
    }
};
use super::{
    extent::Extent,
    meta_cache::{CacheEntry, CacheEntryInner, CacheError, CacheState, MetaCache },
};
use anyhow::Result;
use prost::Message;
use rocksdb::{Direction, IteratorMode, ReadOptions, DB as RocksDB };

#[derive(Clone, PartialEq, Message)]
struct LogStreamMeta {
    #[prost(uint32, tag = "1")]
    pub flags: u32,
    #[prost(uint64, tag = "2")]
    pub extent_size: u64,
    #[prost(uint32, tag = "3")]
    pub num_replicas: u32,
    #[prost(uint64, tag = "4")]
    pub last_extent_start: u64,
    #[prost(uint32, tag = "5")]
    pub last_extent_epoch: u32,
    #[prost(message, repeated, tag = "6")]
    pub prealloc_places: Vec<ExtentPlacement>,
    #[prost(message, optional, tag = "7")]
    pub writer: Option<ClientId>,
    #[prost(uint64, tag = "8")]
    pub last_write_req: u64,
}

impl LogStreamMeta {
    fn init_new(
        &mut self, config: &LogStreamConfig,
        version: &ExtentVersion, writer: ClientId, req_seq: u64
    ) {
        self.flags = 0;
        self.extent_size = config.extent_size;
        self.num_replicas = config.num_replicas;
        self.last_extent_start = version.log_seq;
        self.last_extent_epoch = version.epoch;
        self.prealloc_places = Vec::new();
        self.writer.replace(writer);
        self.last_write_req = req_seq;
    }

    pub fn update_writable(
        &mut self, version: &ExtentVersion, prealloc_places: &[ExtentPlacement]
    ) {
        self.flags |= LogStreamFlags::LastWritable as u32;
        self.last_extent_start = version.log_seq;
        self.last_extent_epoch = version.epoch;
        prealloc_places.clone_into(&mut self.prealloc_places);
    }

    pub fn open(&mut self, writer: ClientId, req_seq: u64) {
        self.writer.replace(writer);
        self.last_write_req = req_seq;
    }

    pub fn close(&mut self, req_seq: u64, version: &ExtentVersion) {
        self.flags &= !(LogStreamFlags::LastWritable as u32);
        self.last_extent_start = version.log_seq;
        self.last_extent_epoch = version.epoch;
        self.prealloc_places.clear();
        self.writer.take();
        self.last_write_req = req_seq;
    }

    fn version(&self) -> ExtentVersion {
        ExtentVersion::new(self.last_extent_epoch, self.last_extent_start)
    }

    fn get_config(&self) -> LogStreamConfig {
        LogStreamConfig {
            extent_size: self.extent_size,
            num_replicas: self.num_replicas
        }
    }

    fn is_last_writable(&self) -> bool {
        self.flags & LogStreamFlags::LastWritable as u32 != 0
    }
}

#[repr(u32)]
pub enum LogStreamFlags {
    LastWritable = 1,
}

pub struct LogStream {
    pub id: LogStreamId,
    mutable: Mutex<LogStreamMut>,
}

pub(super) enum PreparedOpen<T> {
    Ready(T),
    Duplicated(T),
}

pub(super) enum PreparedUpdate<D> {
    Ready((LogStreamConfig, Vec<ExtentPlacement>, ExtentId)),
    Duplicated(D),
}

impl LogStream {
    fn finish_load(
        &self, ext_meta: Result<LogStreamMeta, CacheError>
    ) -> Result<(), CacheError> {
        let mut lst_inner = self.mutable.lock().unwrap();
        assert!(lst_inner.cache_state.is_locked());
        let (res, _old_state) = match ext_meta {
            Ok(lst_meta) => {
                lst_inner.stored_meta = lst_meta;
                (Ok(()), lst_inner.cache_state.mark_uptodate())
            }
            Err(err) => {
                (Err(err.clone()), lst_inner.cache_state.mark_error(err))
            }
        };
        drop(lst_inner);
        res
    }

    pub fn update<F, R>(&self, req: &mut StoreRequest, func: F) -> R
    where
        F: FnOnce(&mut LogStreamMut) -> R
    {
        let mut lst_inner = self.lock_inner();
        assert!(lst_inner.cache_state.is_locked());

        let mut old_prealloc = lst_inner.stored_meta.prealloc_places.clone();
        
        let res = func(&mut lst_inner);
        req.set(
            StoreColumnFamily::LogStream,
            self.id.store_key().to_vec(),
            lst_inner.stored_meta.encode_to_vec(),
        );

        let prealloc_id = ExtentId::prealloc(self.id);
        for place in lst_inner.stored_meta.prealloc_places.iter() {
            let old_count = old_prealloc.len();
            old_prealloc.retain(|p| p.volume_id != place.volume_id);
            if old_count > old_prealloc.len() {
                continue;
            }
            req.set(
                StoreColumnFamily::DataNode,
                place.volume().extent_store_key(&prealloc_id).to_vec(), Vec::new(),
            );
        }

        for place in old_prealloc.iter() {
            req.delete(
                StoreColumnFamily::DataNode,
                place.volume().extent_store_key(&prealloc_id).to_vec(),
            );
        }
        res
    }

    pub(super) async fn prepare_open(
        &self, client_id: ClientId, req_seq: u64,
    ) -> Result<PreparedOpen<(LogStreamConfig, ExtentVersion)>, tonic::Status> {
        self.prepare_update(|lst_inner| {
            let stored_meta = &lst_inner.stored_meta;
            if let Some(writer) = &stored_meta.writer {
                if *writer != client_id {
                    Err(tonic::Status::failed_precondition("opened by other client"))
                } else if req_seq != stored_meta.last_write_req {
                    Err(tonic::Status::failed_precondition("already opened"))
                } else {
                    Ok(PreparedOpen::Duplicated((stored_meta.get_config(), stored_meta.version()))) 
                }
            } else {
                Ok(PreparedOpen::Ready((stored_meta.get_config(), stored_meta.version())))
            }
        }).await
    }

    pub(super) async fn prepare_close(
        &self, client_id: ClientId, req_seq: u64,
        seal_request: &SealExtentRequest,
    ) -> Result<PreparedUpdate<ExtentVersion>, tonic::Status> {
        self.prepare_update(|lst_inner| {
            let stored_meta = &lst_inner.stored_meta;
            if let Some(writer) = &stored_meta.writer {
                if *writer != client_id {
                    return Err(tonic::Status::failed_precondition("client is not writer"));
                }
            } else if req_seq == stored_meta.last_write_req {
                return Ok(PreparedUpdate::Duplicated(stored_meta.version()))
            }
            seal_request.extent_version.map_or(
                Err(tonic::Status::invalid_argument("no seal version")),
                |version| {
                    if version.epoch != stored_meta.last_extent_epoch {
                        let msg = format!(
                            "seal epoch mismatch, {} != expected {}",
                            version.epoch, stored_meta.last_extent_epoch
                        );
                        Err(tonic::Status::failed_precondition(msg))
                    } else if version.log_seq > 0 {
                        if version.log_seq < stored_meta.last_extent_start {
                            Err(tonic::Status::failed_precondition("stale seal seq"))
                        } else {
                            Ok(())
                        }
                    } else {
                        Ok(())
                    }
                }
            )?;

            let config = stored_meta.get_config();
            let prev_extent_id = ExtentId::new(self.id, stored_meta.last_extent_start);
            let prealloc_places = stored_meta.prealloc_places.clone();
            Ok(PreparedUpdate::Ready((config, prealloc_places, prev_extent_id)))
        }).await
    }

    pub(super) async fn prepare_new_extent(
        &self, client_id: ClientId, req_seq: u64,
        extent_id: &ExtentId, seal_request: &SealExtentRequest,
    ) -> Result<PreparedUpdate<(Vec<ExtentPlacement>, ExtentId)>, tonic::Status> {
        self.prepare_update(|lst_inner| {
            let stored_meta = &lst_inner.stored_meta;
            if stored_meta.writer.as_ref().map_or(true, |writer| *writer != client_id) {
                return Err(tonic::Status::failed_precondition("client is not writer"));
            }
            if req_seq == stored_meta.last_write_req {
                let last_extent_id = ExtentId::new(self.id, stored_meta.last_extent_start);
                let prealloc_places = stored_meta.prealloc_places.clone();
                return Ok(PreparedUpdate::Duplicated((prealloc_places, last_extent_id)));
            }
            seal_request.extent_version.map_or(
                Err(tonic::Status::invalid_argument("no seal version")),
                |version| {
                    if version.epoch != stored_meta.last_extent_epoch {
                        Err(tonic::Status::failed_precondition("seal epoch mismatch"))
                    } else if extent_id.log_seq < stored_meta.last_extent_start {
                        Err(tonic::Status::failed_precondition("stale extent seq"))
                    } else if version.log_seq > 0 {
                        if !stored_meta.is_last_writable() {
                            Err(tonic::Status::failed_precondition("nothing to seal"))
                        } else if extent_id.log_seq != version.log_seq + 1 {
                            Err(tonic::Status::failed_precondition("bad extent seq"))
                        } else if extent_id.log_seq == stored_meta.last_extent_start {
                            Err(tonic::Status::failed_precondition("same extent seq"))
                        } else {
                            Ok(())
                        }
                    } else if extent_id.log_seq != stored_meta.last_extent_start {
                        Err(tonic::Status::failed_precondition("bad extent seq"))
                    } else {
                        Ok(())
                    }
                }
            )?;
            
            let prev_extent_id = ExtentId::new(self.id, stored_meta.last_extent_start);
            let config = stored_meta.get_config();
            let prealloc_places = stored_meta.prealloc_places.clone();
            Ok(PreparedUpdate::Ready((config, prealloc_places, prev_extent_id)))
        }).await
    }
}

impl CacheEntry for LogStream {
    type Key = LogStreamId;
    type Inner = LogStreamMut;

    fn alloc(id: LogStreamId, term: u64) -> Self {
        let mutable = Mutex::new(LogStreamMut::locked(term));
        Self { id, mutable }
    }

    async fn load(&self, db: &Arc<RocksDB>) -> Result<(), CacheError> {
        let id = self.id;
        let db = db.clone();
        let lst_meta = tokio::task::spawn_blocking(move || {
            db.get_cf(
                StoreColumnFamily::LogStream.cf_handle(&db),
                id.store_key())
                .map_err(|err| err.into())
                .and_then(|buf| {
                    if let Some(buf) = buf {
                        LogStreamMeta::decode(buf.as_slice()).map_err(|err| err.into())
                    } else {
                        Err(CacheError::MetaNotFound)
                    }
                })
        }).await.unwrap();

        self.finish_load(lst_meta)
    }

    fn lock_inner(&self) -> MutexGuard<LogStreamMut> {
        self.mutable.lock().unwrap()
    }

    async fn revalidate(&self, term: u64) -> CacheState {
        let mut lst_inner = self.lock_state().await;
        if lst_inner.cache_state.get_term() < term {
            let _ = lst_inner.writable_extent.take();
            lst_inner.cache_state.mark_locked(Some(term));
        }
        lst_inner.cache_state.clone()
    }
}

pub struct LogStreamMut {
    cache_state: CacheState,
    stored_meta: LogStreamMeta,
    writable_extent: Option<Arc<Extent>>,
}

impl LogStreamMut {
    fn locked(term: u64) -> Self {
        Self {
            cache_state: CacheState::locked(term),
            stored_meta: Default::default(),
            writable_extent: None,
        }
    }

    pub fn init_new(
        &mut self, config: &LogStreamConfig,
        version: &ExtentVersion, writer: ClientId, req_seq: u64,
    ) {
        assert!(self.cache_state.is_locked());
        self.stored_meta.init_new(config, version, writer, req_seq);
    }

    pub fn update(
        &mut self, version: &ExtentVersion, 
        prealloc_places: &[ExtentPlacement],
        writable_extent: &Arc<Extent>, 
    ) {
        self.stored_meta.update_writable(version, prealloc_places);
        self.writable_extent = Some(writable_extent.clone());
    }

    pub fn open(&mut self, writer: ClientId, req_seq: u64) {
        self.stored_meta.open(writer, req_seq);
    }

    pub fn close(&mut self, req_seq: u64, version: &ExtentVersion) {
        self.stored_meta.close(req_seq, version);
        self.writable_extent.take();
    }
}

impl CacheEntryInner for LogStreamMut {
    fn cache_state(&mut self) -> &mut CacheState {
        &mut self.cache_state
    }

   fn invalidate(&mut self) {
        self.writable_extent.take();
    }
}

pub struct LogStreamCache {
    db: Arc<RocksDB>,
    cache: MetaCache<LogStream>,
    last_id: AtomicU32,
}

impl LogStreamCache {
    pub fn new(db: Arc<RocksDB>, cache_size: usize) -> Self {
        let cache = MetaCache::new(cache_size);
        Self {
            db, cache,
            last_id: AtomicU32::new(0),
        }
    }

    pub fn set_raft_term(&self, term: u64) {
        self.cache.set_raft_term(term)
    }

    pub async fn reload(&self) -> anyhow::Result<()> {
        let db = self.db.clone();
        let last_id = tokio::task::spawn_blocking(move || {
            let max_id = LogStreamId::new(u32::MAX);
            let mut iter = db.iterator_cf_opt(
                StoreColumnFamily::LogStream.cf_handle(&db),
                ReadOptions::default(),
                IteratorMode::From(&max_id.store_key(), Direction::Reverse)
            );

            let last_id = if let Some(item) = iter.next() {
                let (key, _) = item?;
                LogStreamId::parse(&key).map_or(1, |id| id.id)
            } else {
                1
            };
            Ok(last_id) 
        })
            .await
            .map_err(anyhow::Error::new)?
            .map_err(|err: rocksdb::Error| anyhow::Error::new(err))?;

        self.last_id.store(last_id, Ordering::Relaxed);
        Ok(())
    }

    pub async fn invalidate_all(&self) {
        self.cache.invalidate_range(LogStreamId::MIN..LogStreamId::MAX).await;
    }

    pub async fn get(&self, key: LogStreamId) -> Result<Arc<LogStream>, CacheError> {
        self.cache.get_or_create(key, Some(&self.db)).await
    }
    
    pub async fn create(&self) -> Result<Arc<LogStream>, CacheError> {
        let key = LogStreamId::new(
            self.last_id.fetch_add(1, Ordering::Relaxed) + 1
        );
        self.cache.get_or_create(key, None).await
    }
}

