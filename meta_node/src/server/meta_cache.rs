use std::{
    collections::BTreeMap,
    hash::Hash,
    ops::Range,
    future::Future,
    ops::Drop,
    sync::{
        Arc, Weak, MutexGuard,
        atomic::{ AtomicU64, Ordering },
    }
};
use rocksdb::{DB as RocksDB, Error as RocksError};
use tokio::sync::{ mpsc, oneshot, Notify };
use prost::DecodeError;
use quick_cache::{
    sync::Cache,
    Weighter, Lifecycle, DefaultHashBuilder
};

#[derive(thiserror::Error, Debug, Clone)]
pub(super) enum CacheError {
    #[error("rocksdb error")]
    DBError(RocksError),
    #[error("decode error")]
    DecodeError(DecodeError),
    #[error("metadata not found")]
    MetaNotFound,
    #[error("invalidated")]
    Invalidated,
}

impl From<RocksError> for CacheError {
    fn from(err: RocksError) -> Self {
        Self::DBError(err)
    }
}

impl From<DecodeError> for CacheError {
    fn from(err: DecodeError) -> Self {
        Self::DecodeError(err)
    }
}

#[derive(Clone)]
pub(super) enum CacheState {
    Locked((Arc<Notify>, u64)),
    Uptodate(u64),
    Error((CacheError, u64)),
}

impl CacheState {
    pub fn locked(term: u64) -> Self {
        CacheState::Locked((Arc::new(Notify::new()), term))
    }

    pub fn is_locked(&self) -> bool {
        matches!(self, CacheState::Locked(_))
    }

    pub fn get_term(&self) -> u64 {
        match self {
            CacheState::Locked((_, term)) => *term,
            CacheState::Uptodate(term) => *term,
            CacheState::Error((_, term)) => *term,
        }
    }

    fn replace(&mut self, new_state: Self) -> OldCacheState {
        let old_state = std::mem::replace(self, new_state);
        OldCacheState(old_state)
    }

    pub fn mark_uptodate(&mut self) -> OldCacheState {
        let term = self.get_term();
        self.replace(CacheState::Uptodate(term))
    }

    pub fn mark_error(&mut self, err: CacheError) -> OldCacheState {
        let term = self.get_term();
        self.replace(CacheState::Error((err, term)))
    }

    pub fn mark_locked(&mut self, term: Option<u64>) -> OldCacheState {
        assert!(!self.is_locked());
        let term = term.map_or_else(|| self.get_term(), |v| v);
        self.replace(CacheState::Locked((Arc::new(Notify::new()), term)))
    }
}

pub struct OldCacheState (CacheState);

impl Drop for OldCacheState {
    fn drop(&mut self) {
        if let CacheState::Locked((notify, _)) = &self.0 {
            notify.notify_waiters();
        }
    }
}

pub(super) trait CacheEntryInner: 'static + Sync + Send {
    fn cache_state(&mut self) -> &mut CacheState;
    fn invalidate(&mut self) { }
}

pub(super) trait CacheEntry : 'static + Sync + Send {
    type Key: 'static + Sync + Send + Eq + Ord + Hash + Clone;
    type Inner: CacheEntryInner;

    fn alloc(key: Self::Key, term: u64) -> Self;
    async fn load(&self, db: &Arc<RocksDB>) -> Result<(), CacheError>;
    fn lock_inner(&self) -> MutexGuard<Self::Inner>;

    fn lock_state(&self) -> impl Future<Output = MutexGuard<Self::Inner>> + Send {
        async {
            loop {
                let notify1;
                let notified;
                {
                    let mut inner = self.lock_inner();
                    if let CacheState::Locked((notify, _)) = inner.cache_state() {
                        notify1 = notify.clone();
                        notified = notify1.notified();
                    } else {
                        break inner;
                    }
                };
                notified.await;
            }
        }
    }

    async fn revalidate(&self, term: u64) -> CacheState {
        let mut inner = self.lock_state().await;
        if inner.cache_state().get_term() < term {
            inner.invalidate();
            inner.cache_state().mark_locked(Some(term));
        }
        return inner.cache_state().clone();
    }

    fn invalidate(&self) -> impl Future<Output = ()> + Send {
        async {
            let mut inner = self.lock_state().await;
            inner.invalidate();
            inner.cache_state().mark_error(CacheError::Invalidated);
        }
    }
    
    async fn prepare_update<F, R, E>(&self, func: F) -> Result<R, E>
    where
        F: FnOnce(&mut Self::Inner) -> Result<R, E>
    {
        let mut inner = self.lock_state().await;
        let res = func(&mut *inner);
        if res.is_ok() {
            inner.cache_state().mark_locked(None);
        }
        res
    }
    
    fn finish_update(&self, res: Result<(), CacheError>) {
        let mut inner = self.lock_inner();
        assert!(inner.cache_state().is_locked());
        let _old_state = match res {
            Err(err) => {
                inner.cache_state().mark_error(err)
            }
            Ok(_) => {
                inner.cache_state().mark_uptodate()
            }
        };
        drop(inner);
    }
}

struct CacheEntryRef<C: CacheEntry> {
    entry: Arc<C>,
    flags: u32,
}

impl<C: CacheEntry> Clone for CacheEntryRef<C> {
    fn clone(&self) -> Self {
        Self {
            entry: self.entry.clone(),
            flags: self.flags,
        }
    }
}

impl<C: CacheEntry> CacheEntryRef<C> {
    const FLAG_PINNED: u32 = 1;

    fn new(entry: Arc<C>) -> Self {
        Self { entry, flags: 0 }
    }
    fn is_pinned(&self) -> bool {
        self.flags & Self::FLAG_PINNED != 0
    }
    fn set_pinned(&mut self) {
        self.flags |= Self::FLAG_PINNED;
    }
}
struct CacheLifecycle<C: CacheEntry> {
    command_tx: mpsc::UnboundedSender<CacheCommand<C>>,
}

impl<C: CacheEntry> CacheLifecycle<C> {
    fn new(command_tx: mpsc::UnboundedSender<CacheCommand<C>>) -> Self {
        Self { command_tx }
    }
}

impl<C: CacheEntry> Clone for CacheLifecycle<C> {
    fn clone(&self) -> Self {
        Self { command_tx: self.command_tx.clone() }
    }
}

impl<C: CacheEntry> Lifecycle<C::Key, CacheEntryRef<C>> for CacheLifecycle<C> {
    type RequestState = ();
    fn begin_request(&self) -> Self::RequestState { }
    fn on_evict(&self, _: &mut Self::RequestState, key: C::Key, val: CacheEntryRef<C>) {
        assert!(!val.is_pinned());
        self.command_tx.send(CacheCommand::Evict(key)).unwrap();
    }
    fn before_evict(&self, _: &mut Self::RequestState, key: &C::Key, val: &mut CacheEntryRef<C>) {
        if Arc::strong_count(&val.entry) > 1 {
            val.set_pinned();
            self.command_tx.send(CacheCommand::Unpin((key.clone(), val.entry.clone()))).unwrap();
        }
    }
}

#[derive(Clone)]
struct CacheWeighter {}

impl<C: CacheEntry> Weighter<C::Key, CacheEntryRef<C>> for CacheWeighter {
    fn weight(&self, _: &C::Key, val: &CacheEntryRef<C>) -> u64 {
        if val.is_pinned() { 0 } else { 1 }
    }
}

enum CacheCommand<C: CacheEntry> {
    Insert((C::Key, Weak<C>)),
    Evict(C::Key),
    Unpin((C::Key, Arc<C>)),
    Invalidate((Range<C::Key>, oneshot::Sender<()>))
}

type QuickCache<C> = Cache<<C as CacheEntry>::Key, CacheEntryRef<C>, CacheWeighter, DefaultHashBuilder, CacheLifecycle<C>>;

pub(super) struct MetaCache<C: CacheEntry> {
    cache: Arc<QuickCache<C>>,
    command_tx: mpsc::UnboundedSender<CacheCommand<C>>,
    raft_term: AtomicU64,
}

impl<C: CacheEntry> MetaCache<C> {
    pub fn new(cache_size: usize) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let cache = Arc::new(QuickCache::with(
            cache_size * 2, cache_size as u64,
            CacheWeighter{},
            DefaultHashBuilder::default(),
            CacheLifecycle::new(command_tx.clone())
        ));
        let cache1 = cache.clone();
        tokio::spawn(async move {
            Self::handle_command(cache1, command_rx).await;
        });
        Self {
            cache, command_tx,
            raft_term: AtomicU64::new(0),
        }
    }

    pub fn set_raft_term(&self, term: u64) {
        self.raft_term.store(term, Ordering::Relaxed);
    }

    fn get_raft_term(&self) -> u64 {
        self.raft_term.load(Ordering::Relaxed)
    }
 
    pub async fn get_or_create(
        &self, key: C::Key, db: Option<&Arc<RocksDB>>
    ) -> Result<Arc<C>, CacheError> {
        let entry = loop {
            match self.cache.get_value_or_guard_async(&key).await {
                Ok(val) => {
                    let entry = val.entry;
                    match entry.revalidate(self.get_raft_term()).await {
                        CacheState::Uptodate(_) => {
                            assert!(db.is_some());
                            return Ok(entry);
                        }
                        CacheState::Error((err, _)) => {
                            return Err(err);
                        }
                        CacheState::Locked(_) => {
                            break entry;
                        }
                    }
                }
                Err(guard) => {
                    let entry = Arc::new(C::alloc(
                        key.clone(), self.get_raft_term()
                    ));
                    if guard.insert_with_lifecycle(
                        CacheEntryRef::new(entry.clone())
                    ).is_ok() {
                        self.command_tx
                            .send(CacheCommand::Insert((key, Arc::downgrade(&entry))))
                            .unwrap();
                        break entry;
                    }
                }
            };
        };
        if let Some(db) = db {
            entry.load(db).await?;
        }
        Ok(entry)
    }

    pub async fn invalidate_range(&self, range: Range<C::Key>) {
        let (done_tx, done_rx) = oneshot::channel();
        self.command_tx.send(CacheCommand::Invalidate((range, done_tx))).unwrap();
        done_rx.await.unwrap();
    }

    async fn handle_command(
        cache: Arc<QuickCache<C>>,
        mut event_rx: mpsc::UnboundedReceiver<CacheCommand<C>>
    ) {
        let mut entries = BTreeMap::new();
        while let Some(cmd) = event_rx.recv().await {
            match cmd {
                CacheCommand::Insert((key, entry)) => {
                    entries.insert(key, entry);
                }
                CacheCommand::Evict(key) => {
                    entries.remove(&key);
                }
                CacheCommand::Unpin((key, entry)) => {
                    let _ = cache.replace_with_lifecycle(
                        key, CacheEntryRef::new(entry), true
                    );
                }
                CacheCommand::Invalidate((range, done)) => {
                    for (_, entry) in entries.range(range) {
                        if let Some(entry) = entry.upgrade() {
                            entry.invalidate().await;
                        }
                    }
                    let _ = done.send(());
                }
            }
        }
    }
}
