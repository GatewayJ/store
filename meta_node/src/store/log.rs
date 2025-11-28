use std::{
    fmt::Debug,
    sync::Arc,
    time::Duration,
    ops::RangeBounds,
    collections::VecDeque,
};
use crate::types::{
    NodeId, TypeConfig, RaftVote, RaftStorageError
};
use openraft::{
    storage::{ LogFlushed, LogState, RaftLogStorage },
    AnyError, OptionalSend, RaftLogReader, StorageIOError,
};
use super::{
    map_io_err, bin_to_id, id_to_bin, RaftLogId, RaftEntry,
    StorageResult, StoreColumnFamily,
};
use rocksdb::{
    ColumnFamily, Direction, WriteBatch, DB as RocksDB
};
use tokio::sync::mpsc;

pub struct LogStore {
    db: Arc<RocksDB>,
    flusher: mpsc::Sender<LogFlushed<TypeConfig>>,
}

impl LogStore {
    const PURGED_LOG_ID_KEY: &'static [u8] = b"purged_log_id";
    const COMMITTED_LOG_ID_KEY: &'static [u8] = b"committed_log_id";
    const VOTE_KEY: &'static [u8] = b"vote";

    pub fn new(db: Arc<RocksDB>) -> Self {
        let (flusher, flush_rx) = mpsc::channel::<LogFlushed<TypeConfig>>(1024);
        let db1 = db.clone();
        let _ = std::thread::spawn(move || Self::flush_work(db1, flush_rx));
        Self { db, flusher }
    }

    fn flush_work(
        db: Arc<RocksDB>, mut flush_rx: mpsc::Receiver<LogFlushed<TypeConfig>>
    ) {
        let mut callbacks = VecDeque::new();
        while let Some(cb) = flush_rx.blocking_recv() {
            callbacks.push_back(cb);
            while let Ok(cb) = flush_rx.try_recv() {
                callbacks.push_back(cb);
            }
            let res = db.flush_wal(true);
            while let Some(cb) = callbacks.pop_front() {
                cb.log_io_completed(res.clone().map_err(std::io::Error::other));
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    fn log_cf(&self) -> &ColumnFamily {
        self.db.cf_handle(StoreColumnFamily::RaftLog.name()).unwrap()
    }

    fn get_last_purged_(&self) -> StorageResult<Option<RaftLogId>> {
        Ok(self.db
            .get(Self::PURGED_LOG_ID_KEY)
            .map_err(|e| StorageIOError::read(&e))?
            .and_then(|v| bincode::deserialize(&v).ok()))
    }

    fn set_committed_(
        &self,
        committed: &Option<RaftLogId>,
    ) -> Result<(), StorageIOError<NodeId>> {
        match committed {
            Some(committed) => {
                let encoded = bincode::serialize(committed).unwrap();
                self.db.put(Self::COMMITTED_LOG_ID_KEY, encoded)
            }
            None => {
                self.db.delete(Self::COMMITTED_LOG_ID_KEY)
            }
        }
            .map_err(|e| StorageIOError::write(&e))?;
        Ok(())
    }

    fn get_committed_(&self) -> StorageResult<Option<RaftLogId>> {
        Ok(self.db
            .get(Self::COMMITTED_LOG_ID_KEY)
            .map_err(|e| map_io_err(StorageIOError::read(&e)))?
            .and_then(|v| bincode::deserialize(&v).ok()))
    }

    fn set_vote_(&self, vote: &RaftVote) -> StorageResult<()> {
        let encoded = bincode::serialize(vote).unwrap();
        self.db.put(Self::VOTE_KEY, encoded)
            .map_err(|e| map_io_err(StorageIOError::write_vote(&e)))?;
        Ok(())
    }

    fn get_vote_(&self) -> StorageResult<Option<RaftVote>> {
        Ok(self.db
            .get(Self::VOTE_KEY)
            .map_err(|e| map_io_err(StorageIOError::write_vote(&e)))?
            .and_then(|v| bincode::deserialize(&v).ok()))
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<RaftEntry>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(x) => id_to_bin(*x),
            std::ops::Bound::Excluded(x) => id_to_bin(*x + 1),
            std::ops::Bound::Unbounded => id_to_bin(0),
        };
        self.db.iterator_cf(
                self.log_cf(),
                rocksdb::IteratorMode::From(&start, Direction::Forward),
            )
            .map(|res| {
                let (id, val) = res.unwrap();
                let entry: StorageResult<RaftEntry> =
                    bincode::deserialize(&val).map_err(
                        |e| map_io_err(StorageIOError::read_logs(&e))
                    );
                let id = bin_to_id(&id).unwrap();
                assert_eq!(Ok(id), entry.as_ref().map(|e| e.log_id.index));
                (id, entry)
            })
            .take_while(|(id, _)| range.contains(id))
            .map(|x| x.1)
            .collect()
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> StorageResult<LogState<TypeConfig>> {
        let last = self.db
            .iterator_cf(self.log_cf(), rocksdb::IteratorMode::End)
            .next()
            .and_then(|res| {
                let (_, val) = res.unwrap();
                let ent = bincode::deserialize::<RaftEntry>(&val);
                Some(ent.ok()?.log_id)
            });

        let last_purged_log_id = self.get_last_purged_()?;

        let last_log_id = match last {
            None => last_purged_log_id,
            Some(x) => Some(x),
        };
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn save_committed(
        &mut self,
        _committed: Option<RaftLogId>,
    ) -> Result<(), RaftStorageError> {
        self.set_committed_(&_committed)?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<RaftLogId>, RaftStorageError> {
        let c = self.get_committed_()?;
        Ok(c)
    }

    #[tracing::instrument(level = "trace", skip(self, callback))]
    async fn save_vote(
        &mut self, vote: &RaftVote, callback: LogFlushed<TypeConfig>
    ) -> Result<(), RaftStorageError> {
        self.set_vote_(vote)?;
        self.flusher.send(callback).await
            .map_err(|_| StorageIOError::write_logs(AnyError::error("flusher exited")))?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<RaftVote>, RaftStorageError> {
        self.get_vote_()
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> StorageResult<()>
    where
        I: IntoIterator<Item = RaftEntry> + Send,
        I::IntoIter: Send,
    {
        let mut batch = WriteBatch::default();
        for entry in entries {
            let id = id_to_bin(entry.log_id.index);
            let encoded = bincode::serialize(&entry)
                .map_err(|e| StorageIOError::write_logs(&e))?;
            batch.put_cf(self.log_cf(), id, encoded);
                
        }
        self.db.write(batch)
            .map_err(|e| StorageIOError::write_logs(&e))?;

        self.flusher.send(callback).await
            .map_err(|_| StorageIOError::write_logs(AnyError::error("flusher exited")))?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn truncate(&mut self, log_id: RaftLogId) -> StorageResult<()> {
        let from = id_to_bin(log_id.index);
        let to = id_to_bin(0xff_ff_ff_ff_ff_ff_ff_ff);
        self.db.delete_range_cf(self.log_cf(), from, to)
            .map_err(|e| StorageIOError::write_logs(&e).into())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge(&mut self, log_id: RaftLogId) -> Result<(), RaftStorageError> {
        let mut batch = WriteBatch::default();

        let encoded = bincode::serialize(&log_id).unwrap();
        batch.put(Self::PURGED_LOG_ID_KEY, encoded);
        batch.delete_range_cf(
            self.log_cf(), id_to_bin(0), id_to_bin(log_id.index + 1)
        );
        self.db.write(batch)
            .map_err(|e| StorageIOError::write_logs(&e).into())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        Self::new(self.db.clone())
    }
}

