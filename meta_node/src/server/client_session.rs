use std::{
    sync::{ atomic::{AtomicU64, Ordering }, Arc, Mutex, Weak },
    collections::BTreeSet, time::{Duration, SystemTime}
};
use crate::{
    store::{StoreColumnFamily, StoreRequest},
    types::{
        common_proto::{ClientId, ClientIdStoreKey, LogStreamId, MetaStoreKey},
        stream_service::{ClientLease, SessionRequest, SessionResponse}, ClientWriteError,
    }
};
use super::{
    meta_service::{MetaBatchWriter, MetaRequestGuard},
    recovery::RecoveryManager,
    utils::{systime_from_proto, systime_to_proto}
};
use tokio::sync::{mpsc, oneshot};
use prost::Message;
use rocksdb::{DB as RocksDB, MergeOperands, ErrorKind};
use dashmap::{DashMap, Entry};
use intrusive_collections::{
    intrusive_adapter, LinkedList, LinkedListLink
};
use tracing::info;

#[derive(Clone, PartialEq, Message)]
pub struct CreatedStream {
    #[prost(uint64, tag = "1")]
    pub request_seq: u64,
    #[prost(message, optional, tag = "2")]
    pub stream_id: Option<LogStreamId>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ClientSessionMeta {
    #[prost(message, optional, tag = "1")]
    pub last_seem: Option<prost_types::Timestamp>,
    #[prost(message, optional, tag = "2")]
    pub last_created_stream: Option<CreatedStream>,
}

impl From<&ClientSessionMut> for ClientSessionMeta {
    fn from(client: &ClientSessionMut) -> Self {
        Self {
            last_seem: systime_to_proto(&client.last_seem),
            last_created_stream: client.last_create_stream.clone(),
        }
    }
}

struct SessionListLink {
    session: Weak<ClientSession>,
    link: LinkedListLink,
}

impl SessionListLink  {
    fn new(session: Weak<ClientSession>) -> Arc<Self> {
        Arc::new(Self {
            session,
            link: LinkedListLink::new(),
        })
    }
    fn is_linked(&self) -> bool {
        self.link.is_linked()
    }
}

unsafe impl Sync for SessionListLink {}

intrusive_adapter!(SessionListAdapter = Arc<SessionListLink>: SessionListLink { link: LinkedListLink });

struct SessionList {
    list: LinkedList<SessionListAdapter>,
}

impl SessionList {
    fn new() -> Self {
        Self {
            list: LinkedList::new(SessionListAdapter::new()),
        }
    }

    fn handle_command(&mut self, cmd: SessionListCommand) {
        match cmd {
            SessionListCommand::Touch { link } => {
                if link.is_linked() {
                    unsafe {
                        self.list
                            .cursor_mut_from_ptr(Arc::as_ptr(&link))
                            .remove();
                    }
                }
                self.list.push_back(link);
            }
            SessionListCommand::Expire { stamp, resp_tx } => {
                let mut expired = Vec::new();
                let mut cursor = self.list.cursor_mut();
                cursor.move_next();
                while let Some(link) = cursor.get() {
                    let session = match link.session.upgrade() {
                        None => {
                            cursor.remove();
                            continue;
                        }
                        Some(session) => session,
                    };
                    if session.last_seem() > stamp {
                        break;
                    }
                    expired.push(session);
                    cursor.remove();
                }
                let _ = resp_tx.send(expired);
            }
            SessionListCommand::Reset { links } => {
                self.list.clear();
                for link in links.into_iter() {
                    self.list.push_back(link)
                }
            }
        }
    }
}

enum SessionListCommand {
    Touch {
        link: Arc<SessionListLink>
    },
    Expire {
        stamp: SystemTime,
        resp_tx: oneshot::Sender<Vec<Arc<ClientSession>>>,
    },
    Reset {
        links: Vec<Arc<SessionListLink>>,
    },
}

pub struct ClientSession {
    pub id: ClientId,
    link: Arc<SessionListLink>,
    mutable: Mutex<ClientSessionMut>,
}

impl ClientSession {
    fn with_inner(id: ClientId, inner: ClientSessionMut) -> Arc<Self> {
        Arc::new_cyclic(|me| {
            Self {
                id,
                link: SessionListLink::new(me.clone()),
                mutable: Mutex::new(inner),
            }
        })
    }

    fn with_meta(id: ClientId, meta: ClientSessionMeta) -> Arc<Self> {
        Self::with_inner(id, ClientSessionMut::new(meta))
    }

    fn last_seem(&self) -> SystemTime {
        let inner = self.mutable.lock().unwrap();
        inner.last_seem.map_or(SystemTime::UNIX_EPOCH, |time| time)
    }
    
    pub fn check_created_stream(
        &self, request_seq: u64,
    ) -> Option<LogStreamId> {
        let inner = self.mutable.lock().unwrap();
        if let Some(created) = &inner.last_create_stream {
            if created.request_seq == request_seq {
                return created.stream_id
            }
        }
        None
    }

    pub fn close_stream(
        &self, stream_id: LogStreamId, store_req: &mut StoreRequest
    ) -> bool {
        let mut inner = self.mutable.lock().unwrap();
        if inner.opened_streams.remove(&stream_id) {
            store_req.delete(
                StoreColumnFamily::LogStream,
                self.id.stream_store_key(&stream_id).to_vec(),
            );
            true
        } else {
            false
        }
    }

    pub fn open_stream(
        &self, stream_id: LogStreamId,
        create_req: Option<u64>, store_req: &mut StoreRequest
    ) -> bool {
        let mut inner = self.mutable.lock().unwrap();
        if inner.opened_streams.insert(stream_id) {
            if let Some(request_seq) = create_req {
                inner.last_create_stream.replace(CreatedStream {
                    request_seq,
                    stream_id: Some(stream_id),
                });
            }
            store_req.set(
                StoreColumnFamily::LogStream,
                self.id.store_key().to_vec(),
                Vec::new(),
            );
            store_req.set(
                StoreColumnFamily::LogStream,
                self.id.stream_store_key(&stream_id).to_vec(),
                Vec::new(),
            );
            true
        } else {
            false
        }
    }

    fn update_opened_streams(&self, streams: BTreeSet<LogStreamId>) {
        let mut inner = self.mutable.lock().unwrap();
        inner.opened_streams = streams;
    }
}

#[derive(Default)]
pub struct ClientSessionMut {
    last_seem: Option<SystemTime>,
    last_create_stream: Option<CreatedStream>,
    opened_streams: BTreeSet<LogStreamId>,
}

impl ClientSessionMut {
    fn new(meta: ClientSessionMeta) -> Self {
        Self {
            last_seem: systime_from_proto(&meta.last_seem),
            last_create_stream: meta.last_created_stream.clone(),
            opened_streams: BTreeSet::new(),
        }
    }
}

pub struct ClientSessionMap {
    db: Arc<RocksDB>,
    sessions: Arc<DashMap<ClientId, Arc<ClientSession>>>,
    command_tx: mpsc::UnboundedSender<SessionListCommand>,
    recovery_mgr: Arc<RecoveryManager>,
    last_id: AtomicU64,
}

impl ClientSessionMap {
    const CLIENT_LEASE_MS: u32 = 1000 * 30;

    pub fn new(db: Arc<RocksDB>, recovery_mgr: Arc<RecoveryManager>) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            Self::command_task(command_rx).await;
        });
        Self {
            db, command_tx, recovery_mgr,
            sessions: Arc::new(DashMap::new()),
            last_id: AtomicU64::new(1),
        }
    }

    async fn command_task(mut cmd_rx: mpsc::UnboundedReceiver<SessionListCommand>) {
        let mut session_list = SessionList::new();
        while let Some(cmd) = cmd_rx.recv().await {
            session_list.handle_command(cmd);
        }
    }

    pub fn get(&self, id: &ClientId) -> Option<Arc<ClientSession>> {
        self.sessions.get(id).map(|v| v.clone())
    }

    pub async fn tick(
        &self, mut writer: MetaBatchWriter, now: SystemTime,
    ) -> Result<(), ClientWriteError> {
        let (resp_tx, resp_rx) = oneshot::channel();

        let expire_stamp = now.checked_sub(
            Duration::from_millis(Self::CLIENT_LEASE_MS as _)
        ).unwrap();
        self.command_tx.send(
            SessionListCommand::Expire { stamp: expire_stamp, resp_tx }
        ).unwrap();

        let mut store_req = StoreRequest::default();
        
        let expired = resp_rx.await.unwrap();
        for session in expired.into_iter() {
            match self.sessions.entry(session.id) {
                Entry::Vacant(_) => {
                    continue;
                }
                Entry::Occupied(o) => {
                    if session.last_seem() <= expire_stamp {
                        info!("evicting session {:?}", session.id);
                        o.remove();
                        store_req.delete(
                            StoreColumnFamily::LogStream,
                            session.id.store_key().to_vec()
                        );

                        let mut inner = session.mutable.lock().unwrap();
                        if !inner.opened_streams.is_empty() {
                            let stream_ids = std::mem::replace(
                                &mut inner.opened_streams, BTreeSet::new()
                            );
                            self.recovery_mgr.add_unclosed_streams(stream_ids, session.id);
                        }
                        inner.opened_streams.clear();
                    }
                }
            }
        }

        if !store_req.is_empty() {
            writer.raft_write(store_req).await?;
        }
        Ok(())
    }

    pub async fn handle_open(
        &self, _req: SessionRequest, guard: &mut MetaRequestGuard<'_>
    ) -> Result<SessionResponse, tonic::Status> {
        let inner = ClientSessionMut {
            last_seem: Some(SystemTime::now()),
            ..Default::default()
        };

        let last_id = self.last_id.fetch_add(1, Ordering::Relaxed) + 1;
        let client_id = ClientId::new(last_id);

        let mut store_req = StoreRequest::default();
        let meta: ClientSessionMeta = (&inner).into();
        store_req.set(
            StoreColumnFamily::LogStream,
            client_id.store_key().to_vec(), meta.encode_to_vec()
        );
        store_req.merge(
            StoreColumnFamily::LogStream,
            ClientId::last_id_key().to_vec(),
            last_id.to_be_bytes().to_vec()
        );

        let session = ClientSession::with_inner(client_id, inner);

        guard.client_write(store_req).await?;

        match self.sessions.entry(session.id) {
            Entry::Occupied(_) => {
                panic!("duplicated session {:?}", session.id);
            }
            Entry::Vacant(v) => {
                self.command_tx.send(
                    SessionListCommand::Touch { link: session.link.clone() }
                ).unwrap();
                v.insert(session);
            }
        }

        Ok(SessionResponse::with_lease(
            client_id, ClientLease::new(Self::CLIENT_LEASE_MS)
        ))
    }

    pub async fn handle_close(
        &self, mut writer: MetaBatchWriter, req: SessionRequest,
    ) -> Result<SessionResponse, tonic::Status> {
        let client_id = match req.client_id {
            None => {
                return Err(tonic::Status::invalid_argument("no client id"));
            }
            Some(client_id) => client_id,
        };

        if let Some(session) = self.sessions.remove(&client_id).map(|(_, s)| s) {
            let mut store_req = StoreRequest::default();
            store_req.delete(
                StoreColumnFamily::LogStream,
                client_id.store_key().to_vec()
            );
            writer.client_write(store_req).await?;
            
            let mut inner = session.mutable.lock().unwrap();
            if !inner.opened_streams.is_empty() {
                let stream_ids = std::mem::take(&mut inner.opened_streams);
                self.recovery_mgr.add_unclosed_streams(stream_ids, client_id);
            }
            inner.opened_streams.clear();

            Ok(SessionResponse::new(Some(client_id)))
        } else {
            Ok(SessionResponse::new(None))
        }
    }

    pub async fn handle_renew(
        &self, mut writer: MetaBatchWriter, req: SessionRequest,
    ) -> Result<SessionResponse, tonic::Status> {
        let client_id = match req.client_id {
            None => {
                return Err(tonic::Status::invalid_argument("no client id"));
            }
            Some(client_id) => client_id,
        };

        let now = SystemTime::now();
        let (sess_meta, last_seem) = match self.sessions.entry(client_id) {
            Entry::Vacant(_) => {
                return Err(tonic::Status::not_found(format!("{:?}", client_id)));
            }
            Entry::Occupied(o) => {
                let session = o.get();
                let mut inner = session.mutable.lock().unwrap();
                let last_seem = inner.last_seem.replace(now);
                let sess_meta: ClientSessionMeta = (&*inner).into();
                drop(inner);
                self.command_tx.send(
                    SessionListCommand::Touch { link: session.link.clone() }
                ).unwrap();
                (sess_meta, last_seem)
            }
        };

        let mut store_req = StoreRequest::default();
        store_req.set(
            StoreColumnFamily::LogStream,
            client_id.store_key().to_vec(), sess_meta.encode_to_vec()
        );

        if last_seem.map_or(false, |last_seem| {
            last_seem < writer.get_flushed_upto() &&
            now.duration_since(last_seem).map_or(false, |elapsed| {
                (elapsed.as_millis() as u64) < Self::CLIENT_LEASE_MS as u64 * 2 / 3
            })
        }) {
            writer.queue_write(store_req);
        } else {
            writer.client_write(store_req).await?;
        }

        Ok(SessionResponse::with_lease(
            client_id, ClientLease::new(Self::CLIENT_LEASE_MS)
        ))
    }

    pub async fn reload(&self) -> anyhow::Result<()> {
        let db = self.db.clone();
        let sessions = self.sessions.clone();
        sessions.clear();
        let recovery_mgr = self.recovery_mgr.clone();
        
        let last_id = tokio::task::spawn_blocking(move || {
            let last_id = match Self::load_sessions(&db, &sessions) {
                Err(err) => {
                    return Err(err);
                }
                Ok(res) => res,
            };

            for (client_id, stream_ids) in Self::load_opened_streams(&db) {
                match sessions.get(&client_id) {
                    Some(client) => {
                        client.update_opened_streams(stream_ids);
                    }
                    None => {
                        recovery_mgr.add_unclosed_streams(stream_ids, client_id);
                    }
                }
            }
            Ok(last_id)
        })
            .await
            .map_err(anyhow::Error::new)??;

        let mut session_list = Vec::new();
        for session in self.sessions.iter() {
            info!("loaded session {:?}", session.id);
            session_list.push((session.last_seem(), session.link.clone()))
        }
        session_list.sort_by(|a, b| a.0.cmp(&b.0));

        self.command_tx.send(
            SessionListCommand::Reset {
                links: session_list.into_iter().map(|item| item.1).collect()
            }
        ).unwrap();

        self.last_id.store(last_id, Ordering::Relaxed);
        Ok(())
    }

    fn load_sessions(
        db: &RocksDB, sessions: &DashMap<ClientId, Arc<ClientSession>>
    ) -> anyhow::Result<u64> {
        let mut last_id = 1;
        for item in db.prefix_iterator_cf(
            StoreColumnFamily::LogStream.cf_handle(db),
            [ClientId::KEY_PREFIX]
        ) {
            let (key, val) = match item {
                Err(err) => {
                    if let ErrorKind::Corruption = err.kind() {
                        continue;
                    }
                    println!("load session err {:?}", err);
                    return Err(anyhow::Error::new(err));
                }
                Ok(item) => item,
            };

            if key[0] != ClientId::KEY_PREFIX {
                break;
            }

            if key.as_ref() == ClientId::last_id_key() {
                if let Ok(id) = val.as_ref().try_into().map(u64::from_be_bytes) {
                    last_id = std::cmp::max(last_id, id);
                }
                continue;
            }

            let id = match ClientId::parse(key.as_ref()) {
                Some(id) => id,
                None => {
                    eprintln!("failed to parse ClientId {:?}", key);
                    continue;
                }
            };

            last_id = std::cmp::max(last_id, id.id);

            let meta = match ClientSessionMeta::decode(val.as_ref()) {
                Ok(meta) => meta,
                Err(err) => {
                    eprintln!("failed to decode {:?}, {:?}", id, err);
                    continue;
                }
            };
            let session = ClientSession::with_meta(id, meta);
            sessions.insert(id, session);
        }
        
        Ok(last_id)
    }

    fn load_opened_streams(
        db: &RocksDB
    ) -> Vec<(ClientId, BTreeSet<LogStreamId>)> {
        let mut opened_streams = Vec::<(ClientId, BTreeSet<_>)>::new();
        for item in db.prefix_iterator_cf(
            StoreColumnFamily::LogStream.cf_handle(db),
            [ClientId::STREAM_KEY_PREFIX]
        ) {
            let (key, _) = match item {
                Err(err) => {
                    println!("load open stream err {:?}", err);
                    continue;
                }
                Ok(item) => item,
            };

            if key[0] != ClientId::STREAM_KEY_PREFIX {
                break;
            }

            match ClientId::parse_stream(key.as_ref()) {
                Some((client_id, stream_id)) => {
                    if opened_streams.last().map_or(true, |(id, _)| *id != client_id) {
                        opened_streams.push((client_id, BTreeSet::new()));
                    }
                    opened_streams.last_mut().unwrap().1.insert(stream_id);
                }
                None => {
                    eprintln!("failed to parse (ClientId, LogSteamId) {:?}", key);
                }
            }
        }
        opened_streams
    }
}

pub fn merge_last_session(
    key: &[u8], existing: Option<&[u8]>, operands: &MergeOperands
) -> Option<Vec<u8>> {
    if key.cmp(&ClientId::last_id_key()).is_ne() {
        return None
    }

    let mut last_id = existing;
    for val in operands {
        if last_id.map_or(true, |id| id.cmp(val).is_le()) {
            last_id.replace(val);
        }
    }
    Some(last_id.unwrap().to_vec())
}
