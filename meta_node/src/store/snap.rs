use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    future::Future,
};
use crate::types::{
    NodeId, SnapshotData, TypeConfig,
    RaftSnapshot, RaftSnapshotMeta, RaftStorageError,
    raft_service::PushSnapshot,
};
use super::{
    id_to_bin,
    StateMachineStore, StateMachineData, StoreColumnFamily
};
use futures::StreamExt;
use openraft::{
    error::{
        Fatal, ReplicationClosed, StreamingError,
    },
    AnyError, OptionalSend, RaftSnapshotBuilder, StorageIOError
};
use rocksdb::{
    DB as RocksDB, Snapshot as DBSnapshot,
    IteratorMode,  WriteBatch, WriteOptions, FlushOptions,
};

pub struct StateMachineSnapshot {
    data: StateMachineData,
    db_snap: DBSnapshot<'static>,
    db: Arc<RocksDB>,
}

impl StateMachineSnapshot {
    const PUSH_FLAG_COMPLETED: u32 = 1;
    const PUSH_FLAG_CF_START: u32 = 2;

    const COLUMN_FAMILIES: [StoreColumnFamily; 2] = [
        StoreColumnFamily::LogStream,
        StoreColumnFamily::DataNode,
    ];

    pub fn new(data: StateMachineData, db: Arc<RocksDB>) -> Self {
        let db_snap = unsafe {
            std::mem::transmute::<&RocksDB, &'static RocksDB>(db.as_ref())
        }.snapshot();
        Self { data, db_snap, db }
    }

    pub fn get_snapshot_meta(&self) -> RaftSnapshotMeta {
        RaftSnapshotMeta {
            last_log_id: self.data.last_applied_log_id,
            last_membership: self.data.last_membership.clone(),
            snapshot_id: self.data.last_snapshot_id.to_string(),
        }
    }

    fn flush_snapshot_cfs(db: &Arc<RocksDB>) -> Result<(), AnyError> {
        let mut cfs: Vec<_> = Self::COLUMN_FAMILIES.iter()
            .map(|cf| db.cf_handle(cf.name()).unwrap())
            .collect();
        cfs.push(db.cf_handle("default").unwrap());
        let mut flush_opts = FlushOptions::default();
        flush_opts.set_wait(true);
        db.flush_cfs_opt(cfs.as_slice(), &flush_opts)
            .map_err(|e| AnyError::new(&e))
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn build_snapshot(
        self, sm_shared: &Arc<Mutex<super::StateMachineSharedData>>
    ) -> Result<RaftSnapshot, RaftStorageError> {
        let this = tokio::task::spawn_blocking(move || {
            Self::flush_snapshot_cfs(&self.db)
                .map_err(|e| {
                    let sign = Some(self.get_snapshot_meta().signature());
                    StorageIOError::write_snapshot(sign, e)
                }
            )?;
            StateMachineStore::set_last_snapshot_id(&self.db, &self.data)?;
            Ok::<_, RaftStorageError>(self)
        })
            .await
            .expect("failed to spawn blocking")?;

        let meta = this.get_snapshot_meta();
        sm_shared.lock().unwrap().snapshot.replace(Arc::new(this));

        let snapshot = Box::new(SnapshotData::None);
        Ok(RaftSnapshot { meta, snapshot })
    }

    pub async fn send(
        self: Arc<Self>, tx: tokio::sync::mpsc::Sender<PushSnapshot>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
    ) -> Result<(), StreamingError<TypeConfig, Fatal<NodeId>>> {

        let cancel1 = Arc::new(AtomicBool::new(false));
        let cancel2 = cancel1.clone();
        
        let mut send_task = tokio::task::spawn_blocking(move || {
            for (idx, cf) in Self::COLUMN_FAMILIES.iter().enumerate() {
                if let Some(cfh) = self.db.cf_handle(cf.name()) {
                    let mut push_size = 0;
                    let mut push_req = PushSnapshot::with_cf(cf.name());
                    push_req.flags |= Self::PUSH_FLAG_CF_START;
                    let iter = self.db_snap.iterator_cf(cfh, IteratorMode::Start);
                    for item in iter {
                        if cancel2.load(Ordering::Relaxed) {
                            return Err(StreamingError::Closed(ReplicationClosed::new("cancelled")));
                        }
                        let (key, value) = item.unwrap();
                        if push_size > 2 * 1024 * 1024 {
                            tx.blocking_send(push_req)
                                .map_err(|e| StreamingError::Closed(ReplicationClosed::new(e.to_string())))?;
                            push_size = 0;
                            push_req = PushSnapshot::with_cf(cf.name());
                        }
                        
                        push_size += key.len() + value.len();
                        push_req.put(key.to_vec(), value.to_vec());
                    }
                    if idx + 1 == Self::COLUMN_FAMILIES.len() {
                        push_req.flags |= Self::PUSH_FLAG_COMPLETED;
                    }
                    tx.blocking_send(push_req)
                        .map_err(|e| StreamingError::Closed(ReplicationClosed::new(e.to_string())))?;
                } else {
                    return Err(StreamingError::StorageError(RaftStorageError::IO {
                        source: StorageIOError::read_snapshot(
                            Some(self.get_snapshot_meta().signature()),
                            AnyError::error(format!("cf '{}' not found", cf.name()))
                        )
                    }));
                }
            }
            Ok(())
        });

        tokio::select! {
            _ = cancel => {
                cancel1.store(true, Ordering::Relaxed);
            }
            res = &mut send_task => {
                return res.expect("failed to spawn blocking");
            }
        };
        send_task.await.expect("failed to spawn blocking")
    }

    pub async fn import_snapshot(
        db: Arc<RocksDB>, meta: &RaftSnapshotMeta,
        mut in_stream: tonic::Streaming<PushSnapshot>,
    ) -> Result<(), RaftStorageError> {
        println!("importing snapshot");
        let (tx, mut rx) = tokio::sync::mpsc::channel::<PushSnapshot>(32);

        let complete = Arc::new(AtomicBool::new(false));
        let complete2 = complete.clone();

        let db_task = tokio::task::spawn_blocking(move || {
            let mut write_opts = WriteOptions::default();
            write_opts.disable_wal(true);
            write_opts.set_sync(false);

            while let Some(push_req) = rx.blocking_recv() {
                if let Some(cf) = db.cf_handle(&push_req.cf_name) {
                    if push_req.flags & Self::PUSH_FLAG_CF_START != 0 {
                        db.delete_range_cf_opt(
                            cf, id_to_bin(u64::MIN), id_to_bin(u64::MAX), &write_opts
                        ).map_err(|e| AnyError::new(&e))?;
                    }

                    let mut batch = WriteBatch::default();
                    for item in push_req.key_vals.into_iter() {
                        batch.put_cf(cf, item.key, item.value);
                    }
                    db.write_opt(batch, &write_opts).map_err(|e| AnyError::new(&e))?;

                    if push_req.flags & Self::PUSH_FLAG_COMPLETED != 0 {
                        complete2.store(true, Ordering::Relaxed);
                    }
                } else {
                    let msg = format!("column family '{}' not found", push_req.cf_name);
                    return Err(AnyError::error(msg))
                }
            }
            Self::flush_snapshot_cfs(&db)?;
            Ok(())
        });

        let mut stream_err = Ok(());
        while let Some(push_snap) = in_stream.next().await {
            match push_snap {
                Ok(push_snap) => {
                    if tx.send(push_snap).await.is_err() {
                        break;
                    }
                }
                Err(err) => {
                    stream_err = Err(AnyError::new(&err));
                    break;
                }
            }
        }
        drop(tx);

        let db_err = db_task.await.expect("failed to spawn db task");
        stream_err
            .and(db_err)
            .and_then(|_| {
                if complete.load(Ordering::Relaxed) {
                    Ok(())
                } else {
                    Err(AnyError::error("incomplete"))
                }
            })
            .map_err(|err| RaftStorageError::IO {
                source: StorageIOError::write_snapshot(
                    Some(meta.signature()), err
                )
            })?;
        println!("import snapshot done");
        
        Ok(())
    }
}

pub struct StateMachineSnapshotBuilder {
    snapshot: Option<StateMachineSnapshot>,
    sm_data: Arc<Mutex<super::StateMachineSharedData>>,
}

impl StateMachineSnapshotBuilder {
    pub fn new(store: &StateMachineStore) -> Self {
        Self {
            snapshot: Some(StateMachineSnapshot::new(store.data.clone(), store.db.clone())),
            sm_data: store.shared_data.clone(),
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineSnapshotBuilder {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn build_snapshot(&mut self) -> Result<RaftSnapshot, RaftStorageError> {
        self.snapshot.take().unwrap().build_snapshot(&self.sm_data).await
    }
}
