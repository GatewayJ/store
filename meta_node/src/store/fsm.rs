use std::{
    sync::{Arc, Mutex},
    fmt::Debug,
};
use crate::types::{
    Node, NodeId, TypeConfig, SnapshotData,
    RaftSnapshot, RaftSnapshotMeta, RaftStorageError
};
use super::{
    map_io_err, snap::{StateMachineSnapshot, StateMachineSnapshotBuilder}, RaftEntry, RaftLogId, StorageResult, StoreColumnFamily, StoreRequest, StoreResponse
};
use openraft::{
    storage::RaftStateMachine,
    AnyError, EntryPayload, OptionalSend, StorageIOError, StoredMembership 
};
use rocksdb::{
    DB as RocksDB, ColumnFamily, WriteOptions, WriteBatch,
};
use serde::de::DeserializeOwned;

pub struct StateMachineSharedData {
    pub snapshot: Option<Arc<StateMachineSnapshot>>
}

pub struct StateMachineStore {
    pub(super) data: StateMachineData,

    pub shared_data: Arc<Mutex<StateMachineSharedData>>,

    /// State machine stores snapshot in db.
    pub(super) db: Arc<RocksDB>,
}

#[derive(Debug, Clone)]
pub struct StateMachineData {
    pub _whoami: NodeId,

    pub last_applied_log_id: Option<RaftLogId>,

    pub last_membership: StoredMembership<NodeId, Node>,

    pub last_snapshot_id: u64,
}

impl StateMachineStore {
    const APPLIED_LOG_ID_KEY: &'static [u8] = b"applied_log_id";
    const MEMBERSHIP_KEY: &'static [u8] = b"membership";
    const SNAPSHOT_ID_KEY: &'static [u8] = b"last_snapshot_id";

    pub async fn new(whoami: NodeId, db: Arc<RocksDB>) -> Result<StateMachineStore, RaftStorageError> {
        let mut sm = Self {
            data: StateMachineData {
                _whoami: whoami,
                last_applied_log_id: None,
                last_membership: Default::default(),
                last_snapshot_id: 0,
            },
            shared_data: Arc::new(Mutex::new(StateMachineSharedData {
                snapshot: None,
            })),
            db,
        };

        if let Some(snap_meta) = sm.get_current_snapshot_()? {
            sm.update_state_machine_(&snap_meta)?;
        }

        Ok(sm)
    }

    fn get_cf_handle(&self, cf: StoreColumnFamily) -> &ColumnFamily {
        self.db.cf_handle(cf.name()).unwrap()
    }

    fn update_state_machine_(
        &mut self,
        snap_meta: &RaftSnapshotMeta,
    ) -> Result<(), RaftStorageError> {
        self.data.last_applied_log_id = snap_meta.last_log_id;
        self.data.last_membership = snap_meta.last_membership.clone();
        if let Ok(snapshot_id) = snap_meta.snapshot_id.parse() {
            if snapshot_id > self.data.last_snapshot_id {
                self.data.last_snapshot_id = snapshot_id;
            }
        } else {
            eprintln!("failed to parse snapshot_id");
        }
        Ok(())
    }

    fn get_current_snapshot_(&self) -> StorageResult<Option<RaftSnapshotMeta>> {
        fn decode_value<R: DeserializeOwned>(
            val: Result<Option<Vec<u8>>, rocksdb::Error>
        ) -> StorageResult<Option<R>> {
            match val {
                Ok(Some(val)) => {
                    let res = bincode::deserialize(&val)
                        .map_err(|e| map_io_err(StorageIOError::read_state_machine(&e)))?;
                    Ok(Some(res))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(map_io_err(StorageIOError::read_state_machine(&e)))
            }
        }

        let mut vals = self.db.multi_get([
            Self::SNAPSHOT_ID_KEY,
            Self::MEMBERSHIP_KEY,
            Self::APPLIED_LOG_ID_KEY,
        ]);

        let last_log_id = match decode_value(vals.pop().unwrap())? {
            Some(log_id) => { log_id }
            None => { return Ok(None); }
        };
        let last_membership = decode_value(vals.pop().unwrap())?
            .unwrap_or(Default::default());
        let snapshot_id: u64 = decode_value(vals.pop().unwrap())?
            .unwrap_or(0);

        Ok(Some(RaftSnapshotMeta {
            last_log_id: Some(last_log_id),
            last_membership, 
            snapshot_id: snapshot_id.to_string(),
        }))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn set_current_snapshot_(&self) -> StorageResult<()> {
        let mut batch = WriteBatch::default();

        let last_log_id = self.data.last_applied_log_id.as_ref().unwrap();
        let encoded = bincode::serialize(last_log_id).unwrap();
        batch.put(Self::APPLIED_LOG_ID_KEY, encoded);

        let encoded = bincode::serialize(&self.data.last_membership).unwrap();
        batch.put(Self::MEMBERSHIP_KEY, encoded);

        let encoded = bincode::serialize(&self.data.last_snapshot_id).unwrap();
        batch.put(Self::SNAPSHOT_ID_KEY, encoded);

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(true);
        self.db.write_opt(batch, &write_opts)
            .map_err(|e| map_io_err(StorageIOError::write(&e)))?;

        Ok(())
    }

    pub fn set_last_snapshot_id(db: &Arc<RocksDB>, data: &StateMachineData) -> StorageResult<()> {
        let encoded = bincode::serialize(&data.last_snapshot_id).unwrap();
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(true);
        db.put_opt(Self::SNAPSHOT_ID_KEY, encoded, &write_opts)
            .map_err(|e| map_io_err(StorageIOError::write(&e)))?;
        Ok(())
    }

    fn apply_request(&self, batch: &mut WriteBatch, req: StoreRequest) {
        for (cf, key) in req.keys_to_del.into_iter() {
            batch.delete_cf(self.get_cf_handle(cf), key);
        }
        for ((cf, key), value) in req.kvs_to_set.into_iter() {
            batch.put_cf(self.get_cf_handle(cf), key, value);
        }
        for ((cf, key), value) in req.kvs_to_merge.into_iter() {
            batch.merge_cf(self.get_cf_handle(cf), key, value);
        }
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = StateMachineSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<RaftLogId>, StoredMembership<NodeId, Node>), RaftStorageError> {
        Ok((
            self.data.last_applied_log_id,
            self.data.last_membership.clone(),
        ))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<StoreResponse>, RaftStorageError>
    where
        I: IntoIterator<Item = RaftEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter();
        let mut replies = Vec::with_capacity(entries.size_hint().0);

        let mut batch = WriteBatch::default();
        for ent in entries {
            let encoded = bincode::serialize(&ent.log_id).unwrap();
            batch.put(Self::APPLIED_LOG_ID_KEY, encoded);
            self.data.last_applied_log_id = Some(ent.log_id);

            match ent.payload {
                EntryPayload::Blank => {}
                EntryPayload::Membership(mem) => {
                    let last_membership = StoredMembership::new(Some(ent.log_id), mem);
                    let encoded = bincode::serialize(&last_membership).unwrap();
                    batch.put(Self::MEMBERSHIP_KEY, encoded);
                    self.data.last_membership = last_membership;
                }
                EntryPayload::Normal(req) => {
                    self.apply_request(&mut batch, req);
                },
            }
            replies.push(StoreResponse {});
        }

        let mut write_opts = WriteOptions::default();
        write_opts.disable_wal(true);
        write_opts.set_sync(false);
        self.db.write_opt(batch, &write_opts)
            .map_err(|e| RaftStorageError::IO { source: StorageIOError::write(&e) })?;

        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.data.last_snapshot_id += 1;
        StateMachineSnapshotBuilder::new(self)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<SnapshotData>, RaftStorageError> {
        Ok(Box::new(SnapshotData::None))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &RaftSnapshotMeta,
        snapshot: Box<SnapshotData>,
    ) -> Result<(), RaftStorageError> {
        if let SnapshotData::Receiver(in_stream) = *snapshot {
            StateMachineSnapshot::import_snapshot(self.db.clone(), meta, in_stream).await?;
            self.update_state_machine_(meta)?;
            self.set_current_snapshot_()?;
            Ok(())
        } else {
            Err(map_io_err(StorageIOError::write_snapshot(
                Some(meta.signature()), AnyError::error("no input stream")
            )))
        }
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<RaftSnapshot>, RaftStorageError> {
        let sm_snap = self.shared_data.lock().unwrap().snapshot.clone();
        let snapshot = sm_snap.map(|snap| RaftSnapshot {
                meta: snap.get_snapshot_meta(),
                snapshot: Box::new(SnapshotData::None),
            });
        Ok(snapshot)
    }
}
