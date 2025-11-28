use std::sync::{ Arc, Mutex, MutexGuard };
use crate::{
    store::{StoreColumnFamily, StoreRequest},
    types::common_proto::{
        ExtentId, ExtentVersion, ExtentStats, ExtentPlacement, ExtentPlacements,
        MetaStoreKey, VolumeIdStoreKey,
    }
};
use super::meta_cache::{CacheEntry, CacheEntryInner, CacheError, CacheState, MetaCache };
use prost::Message;
use rocksdb::DB as RocksDB;

#[derive(Clone, PartialEq, Message)]
struct ExtentMeta {
    #[prost(uint32, tag = "1")]
    pub flags: u32,
    #[prost(message, optional, tag = "2")]
    pub version: Option<ExtentVersion>,
    #[prost(message, repeated, tag = "3")]
    pub places: Vec<ExtentPlacement>,
}

impl ExtentMeta {
    fn reset(
        &mut self, places: Vec<ExtentPlacement>
    ) -> (u32, Vec<ExtentPlacement>) {
        let version = ExtentVersion::new(
            self.version.map_or(1, |v| v.epoch + 1), 0
        );
        let epoch = version.epoch;
        self.flags = 0;
        self.version = Some(version);
        let old_spaces = std::mem::replace(&mut self.places, places);
        (epoch, old_spaces)
    }

    fn mark_sealed(
        &mut self, version: ExtentVersion, sealed: &Vec<ExtentPlacement>
    ) -> Vec<ExtentPlacement>{
        self.version = Some(version);
        self.flags |= ExtentFlags::Sealed as u32;

        if sealed != &self.places {
            let mut unsealed = self.places.clone();
            unsealed.subtract(sealed);
            unsealed
        } else {
            Vec::new()
        }
    }

    fn next_epoch(&self) -> u32 {
        self.version.map_or(1, |v| v.epoch + 1)
    }

    fn stat(&self) -> ExtentStats {
        let version = self.version.map_or(ExtentVersion::new(0, 0), |v| v);
        let places = self.places.clone();
        ExtentStats { 
            version: Some(version),
            places,
            nodes_info: Vec::new(),
        }
    }
}

#[repr(u32)]
pub enum ExtentFlags {
    Sealed = 1,
}

pub struct Extent {
    pub id: ExtentId,
    mutable: Mutex<ExtentMut>,
}

impl Extent {
    fn finish_load(
        &self, ext_meta: Result<ExtentMeta, CacheError>
    ) -> Result<(), CacheError> {
        let mut ext_inner = self.mutable.lock().unwrap();
        assert!(ext_inner.cache_state.is_locked());
        let (res, _old_state) = match ext_meta {
            Ok(ext_meta) => {
                ext_inner.stored_meta = ext_meta;
                (Ok(()), ext_inner.cache_state.mark_uptodate())
            }
            Err(err) => {
                (Err(err.clone()), ext_inner.cache_state.mark_error(err))
            }
        };
        drop(ext_inner);
        res
    }

    pub fn stat(&self) -> ExtentStats {
        let ext_inner = self.lock_inner();
        ext_inner.stored_meta.stat()
    }

    pub fn update<F, R>(&self, req: &mut StoreRequest, func: F) -> R
    where
        F: FnOnce(&mut ExtentMut) -> R
    {
        let mut ext_inner = self.lock_inner();
        assert!(ext_inner.cache_state.is_locked());

        let mut old_places = ext_inner.stored_meta.places.clone();
        
        let res = func(&mut ext_inner);
        req.set(
            StoreColumnFamily::LogStream,
            self.id.store_key().to_vec(),
            ext_inner.stored_meta.encode_to_vec(),
        );
        
        for place in ext_inner.stored_meta.places.iter() {
            let old_count = old_places.len();
            old_places.retain(|p| p.volume_id != place.volume_id);
            if old_count > old_places.len() {
                continue;
            }

            req.set(
                StoreColumnFamily::DataNode,
                place.volume().extent_store_key(&self.id).to_vec(),
                Vec::new(),
            );
        }
        for place in old_places.iter() {
            req.delete(
                StoreColumnFamily::DataNode,
                place.volume().extent_store_key(&self.id).to_vec()
            );
        }
        res
    }
}

impl CacheEntry for Extent {
    type Key = ExtentId;
    type Inner = ExtentMut;

    fn alloc(id: ExtentId, term: u64) -> Self {
        let mutable = Mutex::new(ExtentMut::locked(term));
        Self { id, mutable }
    }

    async fn load(&self, db: &Arc<RocksDB>) -> Result<(), CacheError> {
        let id = self.id;
        let db = db.clone();
        let ext_meta = tokio::task::spawn_blocking(move || {
            db.get_cf(
                StoreColumnFamily::LogStream.cf_handle(&db),
                id.store_key())
                .map_err(|err| err.into())
                .and_then(|buf| {
                    if let Some(buf) = buf {
                        ExtentMeta::decode(buf.as_slice()).map_err(|err| err.into())
                    } else {
                        Err(CacheError::MetaNotFound)
                    }
                })
        }).await.unwrap();

        self.finish_load(ext_meta)
    }

    fn lock_inner(&self) -> MutexGuard<ExtentMut> {
        self.mutable.lock().unwrap()
    }

    async fn revalidate(&self, term: u64) -> CacheState {
        let mut ext_inner = self.lock_state().await;
        if ext_inner.cache_state.get_term() < term {
            ext_inner.cache_state.mark_locked(Some(term));
        }
        ext_inner.cache_state.clone()
    }
}

pub struct ExtentMut {
    cache_state: CacheState,
    stored_meta: ExtentMeta,
}

impl ExtentMut {
    fn locked(term: u64) -> Self {
        let cache_state = CacheState::locked(term);
        Self {
            cache_state,
            stored_meta: Default::default(),
        }
    }

    pub fn next_epoch(&self) -> u32 {
        self.stored_meta.next_epoch()
    }

    pub fn reset(
        &mut self, places: Vec<ExtentPlacement>
    ) -> (u32, Vec<ExtentPlacement>) {
        assert!(self.cache_state.is_locked());
        self.stored_meta.reset(places)
    }

    pub fn mark_sealed(
        &mut self, verion: ExtentVersion, sealed: &Vec<ExtentPlacement>
    ) -> Vec<ExtentPlacement> {
        assert!(self.cache_state.is_locked());
        self.stored_meta.mark_sealed(verion, sealed)
    }
}

impl CacheEntryInner for ExtentMut {
    fn cache_state(&mut self) -> &mut CacheState {
        &mut self.cache_state
    }
}

pub struct ExtentCache {
    db: Arc<RocksDB>,
    cache: MetaCache<Extent>,
}

impl ExtentCache {
    pub fn new(db: Arc<RocksDB>, cache_size: usize) -> Self {
        let cache = MetaCache::new(cache_size);
        Self { db, cache }
    }

    pub fn set_raft_term(&self, term: u64) {
        self.cache.set_raft_term(term)
    }

    pub async fn get(&self, key: ExtentId) -> Result<Arc<Extent>, CacheError> {
        self.cache.get_or_create(key, Some(&self.db)).await
    }

    pub async fn create(&self, key: ExtentId) -> Result<Arc<Extent>, CacheError> {
        self.cache.get_or_create(key, None).await
    }

    pub async fn invalidate_all(&self) {
        self.cache.invalidate_range(ExtentId::MIN..ExtentId::MAX).await;
    }
}


