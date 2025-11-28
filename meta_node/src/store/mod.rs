mod snap;
mod log;
mod fsm;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug, path::Path, sync::Arc
};
use crate::{
    types::{ NodeId, TypeConfig, RaftStorageError },
    server::merge::{merge_cf_log_stream, merge_cf_data_node},
};
use openraft::StorageIOError;
use rocksdb::{
    DB as RocksDB, Options, SliceTransform,
    ColumnFamily, ColumnFamilyDescriptor,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use fsm::{StateMachineStore, StateMachineData};
use log::LogStore;
pub use fsm::StateMachineSharedData;

type RaftLogId = openraft::LogId<NodeId>;
type RaftEntry = openraft::Entry<TypeConfig>;

type StorageResult<T> = Result<T, RaftStorageError>;

fn id_to_bin(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

fn bin_to_id(buf: &[u8]) -> Option<u64> {
    buf.try_into().ok().map(u64::from_be_bytes)
}

fn map_io_err(source: StorageIOError<NodeId>) -> RaftStorageError {
    RaftStorageError::IO { source,}
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(usize)]
pub enum StoreColumnFamily {
    RaftLog = 0,
    LogStream = 1,
    DataNode = 2,
}

impl StoreColumnFamily {
    pub fn name(&self) -> &'static str {
        match self {
            StoreColumnFamily::RaftLog => "raftlog",
            StoreColumnFamily::LogStream => "logstream",
            StoreColumnFamily::DataNode => "datanode",
        }
    }

    pub fn cf_handle<'a>(&self, db: &'a RocksDB) -> &'a ColumnFamily {
        db.cf_handle(self.name()).unwrap()
    }

    fn all_descriptors() -> Vec<ColumnFamilyDescriptor> {
        let mut ls_opts = Options::default();
        ls_opts.set_prefix_extractor(
            SliceTransform::create_fixed_prefix(1)
        );
        ls_opts.set_merge_operator(
            "cf_log_stream", merge_cf_log_stream, merge_cf_log_stream
        );
        let mut dn_opts = Options::default();
        dn_opts.set_prefix_extractor(
            SliceTransform::create_fixed_prefix(1)
        );
        dn_opts.set_merge_operator(
            "cf_data_node", merge_cf_data_node, merge_cf_data_node
        );
        vec![
            ColumnFamilyDescriptor::new(Self::RaftLog.name(), Options::default()),
            ColumnFamilyDescriptor::new(Self::LogStream.name(), ls_opts.clone()),
            ColumnFamilyDescriptor::new(Self::DataNode.name(), dn_opts.clone()),
        ]
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct StoreRequest {
    kvs_to_set: BTreeMap<(StoreColumnFamily, Vec<u8>), Vec<u8>>,
    kvs_to_merge: BTreeMap<(StoreColumnFamily, Vec<u8>), Vec<u8>>,
    keys_to_del: BTreeSet<(StoreColumnFamily, Vec<u8>)>,
}

impl StoreRequest {
    pub fn is_empty(&self) -> bool {
        self.kvs_to_set.is_empty() &&
        self.kvs_to_merge.is_empty() &&
        self.keys_to_del.is_empty()
    }
    pub fn append(&mut self, other: Self) -> usize {
        let mut added_size = 0;
        for cf_key in other.keys_to_del.into_iter() {
            added_size += cf_key.1.len() + 16;
            self.kvs_to_set.remove(&cf_key);
            self.kvs_to_merge.remove(&cf_key);
            self.keys_to_del.insert(cf_key);
        }
        for (cf_key, value) in other.kvs_to_set.into_iter() {
            added_size += cf_key.1.len() + value.len() + 24;
            self.keys_to_del.remove(&cf_key);
            self.kvs_to_set.insert(cf_key, value);
        }
        for (cf_key, value) in other.kvs_to_merge.into_iter() {
            added_size += cf_key.1.len() + value.len() + 24;
            self.keys_to_del.remove(&cf_key);
            self.kvs_to_merge.insert(cf_key, value);
        }
        added_size
    }
    pub fn set(&mut self, cf: StoreColumnFamily, key: Vec<u8>, val: Vec<u8>) {
        self.kvs_to_set.insert((cf, key), val);
    }
    pub fn merge(&mut self, cf: StoreColumnFamily, key: Vec<u8>, val: Vec<u8>) {
        self.kvs_to_merge.insert((cf, key), val);
    }
    pub fn delete(&mut self, cf: StoreColumnFamily, key: Vec<u8>) {
        self.keys_to_del.insert((cf, key));
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoreResponse {}

const NODE_ID_KEY: &[u8] = b"node_id";
const CLUSTER_UUID_KEY: &[u8] = b"cluster_uuid";

fn common_db_option() -> Options {
    let mut db_opts = Options::default();
    db_opts.set_atomic_flush(true);
    db_opts.set_use_direct_reads(true);
    db_opts.set_use_direct_io_for_flush_and_compaction(true);
    db_opts
}

pub(crate) async fn open_storage<P: AsRef<Path>>(
    id: NodeId, path: P,
) -> anyhow::Result<(Arc<RocksDB>, Uuid, LogStore, StateMachineStore)> {
    let db = RocksDB::open_cf_descriptors(
        &common_db_option(), path.as_ref(),
        StoreColumnFamily::all_descriptors(),
    )?;

    let uuid = db.get(CLUSTER_UUID_KEY)?
        .and_then(|val| Uuid::from_slice(val.as_slice()).ok())
        .unwrap_or(Uuid::default());

    let store_id = db.get(NODE_ID_KEY)?
        .and_then(|val| bin_to_id(&val));
    if let Some(store_id) = store_id {
        if store_id != id {
            let msg = format!("node_id param '{}' does not match store '{}'", id, store_id);
            return Err(anyhow::Error::msg(msg));
        }
    } else {
        return Err(anyhow::Error::msg("failed to get node_id from db"));
    }

    let db = Arc::new(db);

    let log_store = LogStore::new(db.clone());
    let sm_store = StateMachineStore::new(id, db.clone())
        .await
        .unwrap();

    Ok((db, uuid, log_store, sm_store))
}

pub(crate) async fn create_storage<P: AsRef<Path>>(
    id: NodeId, uuid: Uuid, path: P
) -> anyhow::Result<()> {
    let mut db_opts = common_db_option();
    db_opts.set_error_if_exists(true);
    db_opts.create_if_missing(true);
    db_opts.create_missing_column_families(true);

    let db = RocksDB::open_cf_descriptors(
        &db_opts, path.as_ref(),
        StoreColumnFamily::all_descriptors(),
    )?;

    let mut batch = rocksdb::WriteBatch::default();
    batch.put(NODE_ID_KEY, id_to_bin(id));
    batch.put(CLUSTER_UUID_KEY, uuid.as_bytes());
    db.write(batch)?;
    db.flush_wal(true)?;

    Ok(())
}
