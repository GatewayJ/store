use std::{
    hash::{Hash, Hasher},
    cmp::{Ordering, Ord, PartialOrd, PartialEq, Eq},
};
tonic::include_proto!("common_proto");

impl DataNodeId {
    pub fn new(id: u16) -> Self {
        let id = (id as u32) << 16;
        Self { id }
    }

    pub fn seq(&self) -> u16 {
        (self.id >> 16) as u16
    }
}

impl DataNodeInfo {
    pub fn id(&self) -> DataNodeId {
        self.node_id.unwrap()
    }
}

impl Ord for DataNodeId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl PartialOrd for DataNodeId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for DataNodeId {}

impl VolumeId {
    pub fn new(node_id: u16, seq: u16) -> Self {
        let mut id = (node_id as u32) << 16;
        id |= seq as u32;
        Self { id }
    }

    pub fn data_node(&self) -> DataNodeId {
        DataNodeId::new((self.id >> 16) as u16)
    }

    pub fn seq(&self) -> u16 {
        (self.id & 0xffff) as u16
    }
}

impl Ord for VolumeId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl PartialOrd for VolumeId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for VolumeId {}

impl LogStreamId {
    pub const MIN: Self = Self { id: u32::MIN };
    pub const MAX: Self = Self { id: u32::MAX };

    pub fn new(id: u32) -> Self {
        Self { id }
    }
}

impl Hash for LogStreamId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Ord for LogStreamId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl PartialOrd for LogStreamId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for LogStreamId {}

impl ExtentId {
    const SEQ_PREALLOC: u64 = u64::MAX;
    pub const MIN: Self = Self { stream_id: u32::MIN, log_seq: u64::MIN };
    pub const MAX: Self = Self { stream_id: u32::MAX, log_seq: u64::MAX };

    pub fn new(stream_id: LogStreamId, log_seq: u64) -> Self {
        Self { stream_id: stream_id.id, log_seq }
    }

    pub fn stream_id(&self) -> LogStreamId {
        LogStreamId::new(self.stream_id)
    }

    pub fn prealloc(stream_id: LogStreamId) -> Self {
        Self { stream_id: stream_id.id, log_seq: Self::SEQ_PREALLOC }
    }
}

impl Hash for ExtentId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.stream_id.hash(state);
        self.log_seq.hash(state);
    }
}

impl Ord for ExtentId {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.stream_id.cmp(&other.stream_id) {
            Ordering::Equal => self.log_seq.cmp(&other.log_seq),
            ret => ret,
        }
    }
}

impl PartialOrd for ExtentId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for ExtentId {}

impl ExtentVersion {
    pub fn new(epoch: u32, log_seq: u64) -> Self {
        Self { epoch, log_seq }
    }

    pub fn inc_epoch(&self) -> Self {
        Self {
            epoch: self.epoch + 1,
            log_seq: self.log_seq,
        }
    }
    pub fn prev(&self) -> Self {
        let log_seq = if self.log_seq > 0 { self.log_seq - 1} else { 0 };
        Self {
            epoch: self.epoch,
            log_seq,
        }
    }
}

impl ExtentPlacement {
    pub fn new(volume_id: VolumeId, epoch: u32) -> Self {
        Self { volume_id: Some(volume_id), epoch }
    }
    pub fn volume(&self) -> VolumeId {
        self.volume_id.unwrap()
    }
    pub fn data_node(&self) -> DataNodeId {
        self.volume_id.unwrap().data_node()
    }
}

pub trait ExtentPlacements {
    fn subtract(&mut self, other: &Self);
}

impl ExtentPlacements for Vec<ExtentPlacement> {
    fn subtract(&mut self, other: &Self) {
        let orig_len = self.len();
        self.retain(|p1| {
            !other.iter().any(|p2| p1.volume_id == p2.volume_id)
        });
        assert!(orig_len == self.len() + other.len());
    }
}

impl ClientId {
    pub fn new(id: u64) -> Self {
        Self { id }
    }
}

impl Eq for ClientId {}

impl Hash for ClientId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}