use super::{
    meta_service::{MetaBatchWriter, MetaRequestGuard},
    recovery::RecoveryManager,
    utils::{systime_from_proto, systime_to_proto}
};
use crate::{
    store::{StoreColumnFamily, StoreRequest},
    types::{
        common_proto::{MetaStoreKey, VolumeIdStoreKey},
        mngmt_service::{VolumeMngmtRequest, VolumeMngmtResponse},
        ClientWriteError,
    },
    DataNodeBeacon, DataNodeBeaconAck,
};
use common_proto::{DataNodeId, DataNodeInfo, VolumeId};
use prost::Message;
use rand::{thread_rng, Rng};
use rocksdb::{ErrorKind, MergeOperands, DB as RocksDB};
use std::{
    cell::RefCell, rc::{Rc, Weak}, sync::Arc, net::SocketAddr, fmt::Debug, str::FromStr,
    collections::{btree_map::Entry, BTreeMap}, time::{Duration, SystemTime},
};
use tokio::sync::{mpsc, oneshot, Semaphore};
use intrusive_collections::{
    intrusive_adapter, LinkedList, LinkedListLink
};
use uuid::Uuid;
use tracing::{error, info};

#[derive(Clone, PartialEq, Message)]
pub struct DataNodeMeta {
    #[prost(string, tag = "1")]
    pub uuid: String,
    #[prost(uint32, tag = "2")]
    pub flags: u32,
    #[prost(float, optional, tag = "3")]
    pub reweight: Option<f32>,
    #[prost(string, tag = "4")]
    pub rpc_addr: String,
    #[prost(message, tag = "5")]
    pub up_since: Option<prost_types::Timestamp>,
    #[prost(message, tag = "6")]
    pub last_seem: Option<prost_types::Timestamp>,
}

impl From<VolumeMngmtRequest> for DataNodeMeta {
    fn from(req: VolumeMngmtRequest) -> Self {
        Self {
            uuid: req.uuid,
            flags: 0,
            reweight: req.reweight.map(|v| v.value),
            rpc_addr: String::new(),
            up_since: None,
            last_seem: None,
        }
    }
}

impl From<(&Uuid, &DataNodeMut)> for DataNodeMeta {
    fn from((uuid, node_mut): (&Uuid, &DataNodeMut)) -> Self {
        let rpc_addr = node_mut
            .rpc_addr
            .as_ref()
            .map_or(String::new(), ToString::to_string);
        let up_since = systime_to_proto(&node_mut.up_since);
        let last_seem = systime_to_proto(&node_mut.last_seem);

        Self {
            rpc_addr,
            up_since,
            last_seem,
            uuid: uuid.to_string(),
            flags: node_mut.flags,
            reweight: node_mut.reweight,
        }
    }
}

#[repr(u32)]
pub enum DataNodeFlags {
    Up = 1,
}

#[derive(Clone, PartialEq, Message)]
pub struct VolumeMeta {
    #[prost(string, tag = "1")]
    pub uuid: String,
    #[prost(uint32, tag = "2")]
    pub flags: u32,
    #[prost(uint64, tag = "3")]
    pub capacity: u64,
    #[prost(float, optional, tag = "4")]
    pub reweight: Option<f32>,
}

impl VolumeMeta {
    const KEY_SUBFIX: u8 = b'M';
}

impl From<VolumeMngmtRequest> for VolumeMeta {
    fn from(req: VolumeMngmtRequest) -> Self {
        Self {
            uuid: req.uuid,
            flags: 0,
            capacity: req.capacity,
            reweight: req.reweight.map(|v| v.value),
        }
    }
}

impl From<(&Uuid, &VolumeMut)> for VolumeMeta {
    fn from((uuid, vol_mut): (&Uuid, &VolumeMut)) -> Self {
        Self {
            uuid: uuid.to_string(),
            flags: vol_mut.flags,
            capacity: vol_mut.capacity,
            reweight: vol_mut.reweight,
        }
    }
}

#[repr(u32)]
pub enum VolumeFlags {
    Up = 1,
}

#[derive(Clone, PartialEq, Message)]
pub struct VolumeStats {
    #[prost(int64, tag = "1")]
    pub used_bytes: i64,
    #[prost(int32, tag = "2")]
    pub num_extents: i32,
}

impl VolumeStats {
    pub const KEY_SUBFIX: u8 = b'S';

    pub fn merge(&self, vol: &VolumeId, req: &mut StoreRequest) {
        req.merge(
            StoreColumnFamily::DataNode,
            vol.store_key(Self::KEY_SUBFIX).to_vec(),
            self.encode_to_vec(),
        );
    }

    pub fn with_extent(size: u64) -> Self {
        Self {
            used_bytes: size as i64,
            num_extents: 1,
        }
    }

    pub fn negtive(&self) -> Self {
        Self {
            used_bytes: -self.used_bytes,
            num_extents: -self.num_extents,
        }
    }

    fn checked_add(&self, other: &Self) -> Result<Self, String> {
        self.used_bytes
            .checked_add(other.used_bytes)
            .zip(self.num_extents.checked_add(other.num_extents))
            .ok_or_else(|| format!("overflow {:?} + {:?}", self, other))
            .map(|(used_bytes, num_extents)| Self {
                used_bytes,
                num_extents,
            })
    }

    fn validate(&self) -> Result<(), String> {
        if self.used_bytes >= 0 && self.num_extents >= 0 {
            Ok(())
        } else {
            Err(format!("invalid {:?}", self))
        }
    }
}

pub fn merge_volume_stats(
    key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    if key.len() < VolumeId::KEY_LEN || key[0] != VolumeId::KEY_PREFIX {
        return None;
    }

    let mut stats = match existing {
        Some(val) => VolumeStats::decode(val)
            .map_err(|err| {
                eprintln!("failed to decode existing stats {:?}, {:?}", key, err);
            })
            .ok()?,
        None => VolumeStats::default(),
    };

    for val in operands {
        let delta = VolumeStats::decode(val)
            .map_err(|err| {
                eprintln!("failed to decode delta stats {:?}, {:?}", key, err);
            })
            .ok()?;
        stats = stats
            .checked_add(&delta)
            .map_err(|err| {
                eprintln!("failed to merge stats {:?} {:?}, {:?}", stats, delta, err);
            })
            .ok()?;
    }

    if existing.is_some() {
        stats
            .validate()
            .map_err(|err| {
                eprintln!("failed to validate resulting stats {:?}, {}", stats, err);
            })
            .ok()?;
    }

    let mut buf = Vec::new();
    stats.encode(&mut buf).ok()?;
    Some(buf)
}

trait WeightedNode {
    type Id: Debug + Eq + Clone;
    fn weight(&self) -> Option<u64>;
}

struct WeightMap<N: WeightedNode> {
    weight_map: BTreeMap<u64, (Rc<N>, RefCell<u64>)>,
    total_weight: u64,
    choose_seq: RefCell<u64>,
}

impl<N: WeightedNode> WeightMap<N> {
    fn new() -> Self {
        Self {
            weight_map: BTreeMap::new(),
            total_weight: 0,
            choose_seq: RefCell::new(0),
        }
    }

    fn len(&self) -> usize {
        self.weight_map.len()
    }

    fn rebuild(&mut self, nodes: &BTreeMap<N::Id, Rc<N>>) {
        self.weight_map.clear();
        self.total_weight = 0;
        for (_, node) in nodes.iter() {
            if let Some(weight) = node.weight() {
                self.insert(node.clone(), weight);
            }
        }
    }

    fn insert(&mut self, node: Rc<N>, weight: u64) {
        if weight > 0 {
            self.weight_map
                .insert(self.total_weight, (node, RefCell::new(0)));
            self.total_weight += weight
        }
    }
    
    fn prepare_choose(&mut self) -> u64 {
        let choose_seq = self.choose_seq.get_mut();
        *choose_seq += 1;
        *choose_seq
    }

    fn choose<F>(&self, checked: &mut usize, check_node: F) -> Option<Rc<N>>
    where
        F: Fn(&N) -> Option<u64>,
    {
        let choose_seq = *self.choose_seq.borrow();
        loop {
            if *checked >= self.weight_map.len() {
                break;
            }

            let rand_val = thread_rng().gen_range(1_u64..=self.total_weight);
            if let Some((start, (node, seq))) = self.weight_map.range(0..rand_val).last() {
                if choose_seq == *seq.borrow() {
                    continue;
                }

                match check_node(node) {
                    Some(weight) => {
                        if start + weight < rand_val {
                            continue;
                        }

                        *seq.borrow_mut() = choose_seq;
                        *checked += 1;
                        return Some(node.clone());
                    }
                    None => {
                        *seq.borrow_mut() = choose_seq;
                        *checked += 1;
                        continue;
                    }
                };
            }
        }
        None
    }
}

pub struct DataNode {
    pub id: DataNodeId,
    pub uuid: Uuid,
    link: Rc<NodeListLink<DataNode>>,
    mutable: RefCell<DataNodeMut>,
}

impl DataNode {
    fn with_meta(id: DataNodeId, meta: DataNodeMeta) -> Rc<Self> {
        let uuid = Uuid::from_str(&meta.uuid).unwrap_or(Uuid::default());
        Rc::new_cyclic(|me| {
            Self {
                id,
                uuid,
                link: NodeListLink::new(me.clone()),
                mutable: RefCell::new(DataNodeMut {
                    flags: meta.flags,
                    reweight: meta.reweight,
                    rpc_addr: meta.rpc_addr.parse().ok(),
                    up_since: systime_from_proto(&meta.up_since),
                    last_seem: systime_from_proto(&meta.last_seem),
                    avail_capacity: 0,
                    avail_free_bytes: 0,
                    volumes: BTreeMap::new(),
                    weight_map: WeightMap::new(),
                    last_volume_id: 0,
                }),
            }
        })
    }

    fn is_up(&self) -> bool {
        self.mutable.borrow().is_up()
    }

    fn last_seem(&self) -> SystemTime {
        self.mutable.borrow().last_seem.map_or(SystemTime::UNIX_EPOCH, |time| time)
    }
}

impl WeightedNode for DataNode {
    type Id = DataNodeId;
    fn weight(&self) -> Option<u64> {
        self.mutable.borrow().weight()
    }
}

struct DataNodeMut {
    flags: u32,
    reweight: Option<f32>,
    rpc_addr: Option<SocketAddr>,
    up_since: Option<SystemTime>,
    last_seem: Option<SystemTime>,
    avail_capacity: u64,
    avail_free_bytes: u64,
    volumes: BTreeMap<VolumeId, Rc<Volume>>,
    weight_map: WeightMap<Volume>,
    last_volume_id: u16,
}

impl DataNodeMut {
    fn is_up(&self) -> bool {
        self.flags & DataNodeFlags::Up as u32 != 0
    }
    fn mark_up(&mut self, now: SystemTime) {
        self.flags |= DataNodeFlags::Up as u32;
        self.up_since.replace(now);
    }
    fn mark_down(&mut self) {
        self.flags &= !(DataNodeFlags::Up as u32);
        self.up_since.take();
    }
    fn is_in(&self) -> bool {
        self.reweight
            .as_ref()
            .map_or(true, |reweight| *reweight > 0.0)
    }
    fn weight(&self) -> Option<u64> {
        if !self.is_up() || !self.is_in() {
            return None;
        }
        let avail_free_bytes = match &self.reweight {
            Some(reweight) => (self.avail_free_bytes as f32 * reweight) as u64,
            None => self.avail_free_bytes,
        };
        let weight = avail_free_bytes >> 20;
        (weight > 0).then_some(weight)
    }
    fn prepare_alloc(&mut self) {
        let seq = self.weight_map.prepare_choose() as usize;
        if seq % std::cmp::min(self.weight_map.len(), 16) == 0 {
            self.weight_map.rebuild(&self.volumes);
        }
    }
}

pub struct Volume {
    pub id: VolumeId,
    pub uuid: Uuid,
    mutable: RefCell<VolumeMut>,
}

impl Volume {
    fn with_meta(id: VolumeId, meta: VolumeMeta, stats: Option<VolumeStats>) -> Self {
        let uuid = Uuid::from_str(&meta.uuid).unwrap_or(Uuid::default());
        Volume {
            id,
            uuid,
            mutable: RefCell::new(VolumeMut::new(meta, stats)),
        }
    }
}

impl WeightedNode for Volume {
    type Id = VolumeId;
    fn weight(&self) -> Option<u64> {
        self.mutable.borrow().weight()
    }
}

struct VolumeMut {
    flags: u32,
    capacity: u64,
    reweight: Option<f32>,
    stats: Option<VolumeStats>,
}

impl VolumeMut {
    fn new(meta: VolumeMeta, stats: Option<VolumeStats>) -> Self {
        Self {
            flags: meta.flags,
            capacity: meta.capacity,
            reweight: meta.reweight,
            stats,
        }
    }
    fn is_up(&self) -> bool {
        self.flags & VolumeFlags::Up as u32 != 0
    }
    fn mark_up(&mut self) {
        self.flags |= VolumeFlags::Up as u32;
    }
    fn mark_down(&mut self) {
        self.flags &= !(VolumeFlags::Up as u32);
    }

    fn is_in(&self) -> bool {
        self.reweight
            .as_ref()
            .map_or(true, |reweight| *reweight > 0.0)
    }
    fn weight(&self) -> Option<u64> {
        if !self.is_up() || !self.is_in() {
            return None;
        }
        let mut free_bytes = self.free_bytes();
        if let Some(reweight) = &self.reweight {
            assert!(*reweight >= 0.0);
            free_bytes = (free_bytes as f32 * reweight) as u64
        }
        let weight = free_bytes >> 20;
        (weight > 0).then_some(weight)
    }

    fn free_bytes(&self) -> u64 {
        self.stats.as_ref().map_or(0, |stats| {
            self.capacity.saturating_sub(stats.used_bytes as u64)
        })
    }
}

struct NodeListLink<N> {
    node: Weak<N>,
    link: LinkedListLink,
}

unsafe impl<N> Sync for NodeListLink<N> {}

impl<N> NodeListLink<N> {
    fn new(node: Weak<N>) -> Rc<Self> {
        Rc::new(Self {
            node, link: LinkedListLink::new(),
        })
    }
    fn is_linked(&self) -> bool {
        self.link.is_linked()
    }
}

intrusive_adapter!(NodeListAdapter<Id> = Rc<NodeListLink<Id>>: NodeListLink<Id> { link: LinkedListLink });

struct DataNodeMap {
    recovery_mgr: Arc<RecoveryManager>,
    node_map: BTreeMap<DataNodeId, Rc<DataNode>>,
    weight_map: WeightMap<DataNode>,
    node_list: LinkedList<NodeListAdapter<DataNode>>,

    avail_capacity: u64,
    avail_free_bytes: u64,
    last_node_id: u16,
}

unsafe impl Send for DataNodeMap {}

impl DataNodeMap {
    const BEACON_TIMEOUT_MS: u64 = 1000 * 30;

    pub fn new(recovery_mgr: Arc<RecoveryManager>) -> Self {
        Self {
            node_map: BTreeMap::new(),
            weight_map: WeightMap::new(),
            node_list: LinkedList::new(NodeListAdapter::new()),
            avail_capacity: 0,
            avail_free_bytes: 0,
            last_node_id: 0,
            recovery_mgr,
        }
    }

    fn prepare_create(
        &mut self,
        node_id: Option<DataNodeId>,
        uuid: Uuid,
    ) -> anyhow::Result<VolumeId> {
        if let Some(node_id) = node_id {
            let data_node = self.node_map.get(&node_id).ok_or_else(|| {
                let msg = format!("no data node {:?}", node_id);
                anyhow::Error::msg(msg)
            })?;
            let mut node_mut = data_node.mutable.borrow_mut();
            if let Some((id, _)) = node_mut.volumes.iter().find(|(_, vol)| vol.uuid == uuid) {
                let msg = format!("uuid is duplicated with {:?}", id);
                return Err(anyhow::Error::msg(msg));
            }
            let seq = node_mut.last_volume_id.checked_add(1).ok_or_else(|| {
                let msg = format!("volume id for {:?} exhausted", node_id);
                anyhow::Error::msg(msg)
            })?;
            println!("create volume {}", seq);
            node_mut.last_volume_id = seq;
            Ok(VolumeId::new(node_id.seq(), seq))
        } else {
            if let Some((id, _)) = self.node_map.iter().find(|(_, node)| node.uuid == uuid) {
                let msg = format!("uuid is duplicated with {:?}", id);
                return Err(anyhow::Error::msg(msg));
            }
            let node_id = self.last_node_id.checked_add(1).ok_or_else(|| {
                anyhow::Error::msg("data node id exhausted")
            })?;
            self.last_node_id = node_id;
            Ok(VolumeId::new(node_id, 0))
        }
    }

    fn insert_volume(&mut self, vol: Volume) -> anyhow::Result<()> {
        let vol = Rc::new(vol);
        if let Some(data_node) = self.node_map.get(&vol.id.data_node()) {
            let vol_mut = vol.mutable.borrow();
            let (capacity, free_bytes) = if vol_mut.is_up() && vol_mut.is_in() {
                let free_bytes = vol_mut.stats.as_ref().map_or(0, |stats| {
                    vol_mut.capacity.saturating_sub(stats.used_bytes as u64)
                });
                (vol_mut.capacity, free_bytes)
            } else {
                (0, 0)
            };
            drop(vol_mut);
            let node_mut = &mut *data_node.mutable.borrow_mut();
            match node_mut.volumes.entry(vol.id) {
                Entry::Occupied(_) => {
                    return Err(anyhow::anyhow!("volume {:?} already exists", vol.id));
                }
                Entry::Vacant(v) => {
                    if vol.id.seq() > node_mut.last_volume_id {
                        node_mut.last_volume_id = vol.id.seq();
                    }
                    if let Some(weight) = vol.weight() {
                        node_mut.weight_map.insert(vol.clone(), weight);
                    }
                    v.insert(vol);
                }
            }
            if capacity > 0 {
                node_mut.avail_free_bytes = node_mut.avail_free_bytes.saturating_add(free_bytes);
                node_mut.avail_capacity = node_mut.avail_capacity.saturating_add(capacity);
                self.avail_free_bytes = node_mut.avail_free_bytes.saturating_add(free_bytes);
                self.avail_capacity = node_mut.avail_capacity.saturating_add(capacity);
            }
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "data node for volume {:?} not exists",
                vol.id
            ))
        }
    }

    fn insert_data_node(&mut self, node: Rc<DataNode>) -> anyhow::Result<()> {
        match self.node_map.entry(node.id) {
            Entry::Occupied(_) => {
                return Err(anyhow::anyhow!("data node {:?} already exists", node.id));
            }
            Entry::Vacant(v) => {
                if node.id.seq() > self.last_node_id {
                    self.last_node_id = node.id.seq();
                }
                if let Some(weight) = node.weight() {
                    self.weight_map.insert(node.clone(), weight);
                }
                v.insert(node);
            }
        };

        Ok(())
    }

    fn load_node_map(&mut self, db: &RocksDB) -> anyhow::Result<()> {
        for item in db.prefix_iterator_cf(
            StoreColumnFamily::DataNode.cf_handle(db),
            [DataNodeId::KEY_PREFIX],
        ) {
            let (key, val) = item.map_err(anyhow::Error::new)?;
            if key[0] != DataNodeId::KEY_PREFIX {
                break;
            }

            let id = match DataNodeId::parse(key.as_ref()) {
                Some(id) => id,
                None => {
                    eprintln!("failed to parse DataNodeId {:?}", key);
                    continue;
                }
            };

            let meta = match DataNodeMeta::decode(val.as_ref()) {
                Ok(meta) => meta,
                Err(err) => {
                    eprintln!("failed to decode {:?}, {:?}", id, err);
                    continue;
                }
            };

            self.insert_data_node(
                DataNode::with_meta(id, meta)
            ).unwrap();
        }

        let mut node_list: Vec<_> = self.node_map
            .iter()
            .filter(|(_, node)| node.is_up())
            .map(|(_, node)| (node.last_seem(), node.link.clone()))
            .collect();
        node_list.sort_by(|a, b| a.0.cmp(&b.0));
        for (_, link) in node_list.into_iter() {
            self.node_list.push_back(link);
        }

        Ok(())
    }

    fn load_volumes(&mut self, db: &RocksDB) -> anyhow::Result<()> {
        let mut last_vol = None::<Volume>;
        for item in db.prefix_iterator_cf(
            StoreColumnFamily::DataNode.cf_handle(db),
            [VolumeId::KEY_PREFIX],
        ) {
            let (key, val) = match item {
                Err(err) => {
                    if let ErrorKind::Corruption = err.kind() {
                        continue;
                    }
                    return Err(anyhow::Error::new(err));
                }
                Ok(item) => item,
            };

            if key[0] != VolumeId::KEY_PREFIX {
                break;
            }
            let (id, subfix) = match VolumeId::parse(key.as_ref()) {
                Some(pair) => pair,
                None => {
                    eprintln!("failed to parse VolumeId {:?}", key);
                    continue;
                }
            };

            if last_vol.as_ref().map_or(false, |vol| id != vol.id) {
                let _ = self
                    .insert_volume(last_vol.take().unwrap())
                    .map_err(|e| eprintln!("{}", e));
            }

            if subfix == VolumeMeta::KEY_SUBFIX {
                let meta = match VolumeMeta::decode(val.as_ref()) {
                    Ok(meta) => meta,
                    Err(err) => {
                        eprintln!("failed to decode meta {:?}, {:?}", id, err);
                        continue;
                    }
                };

                let vol = Volume::with_meta(id, meta, None);
                last_vol.replace(vol);
            } else if subfix == VolumeStats::KEY_SUBFIX {
                let stats = match VolumeStats::decode(val.as_ref()) {
                    Ok(meta) => meta,
                    Err(err) => {
                        eprintln!("failed to decode stats {:?}, {:?}", id, err);
                        continue;
                    }
                };
                if let Some(vol) = last_vol.as_ref() {
                    vol.mutable.borrow_mut().stats.replace(stats);
                } else {
                    eprintln!("volume not found for {:?} {:?}", id, stats)
                }
            } else {
                eprintln!("unknown volume key subfix {:?} {}", id, subfix);
            }
        }

        if let Some(vol) = last_vol.take() {
            let _ = self.insert_volume(vol).map_err(|e| eprintln!("{}", e));
        }
        Ok(())
    }

    pub async fn reload(&mut self, db: Arc<RocksDB>) -> anyhow::Result<()> {
        let recovery_mgr = self.recovery_mgr.clone();
        let this = tokio::task::spawn_blocking(move || {
            let mut this = Self::new(recovery_mgr);
            this.load_node_map(&db)?;
            this.load_volumes(&db)?;
            this.rebuild_weight_map();
            Ok::<_, anyhow::Error>(this)
        })
        .await
        .map_err(anyhow::Error::new)??;

        let _ = std::mem::replace(self, this);

        let mut up_volumes = Vec::new();
        let mut down_volumes = Vec::new();
        for (_, node) in self.node_map.iter() {
            let node_mut = node.mutable.borrow();
            let node_is_up = node_mut.is_up();
            for (_, volume) in node_mut.volumes.iter() {
                if node_is_up && volume.mutable.borrow().is_up() {
                    up_volumes.push(volume.id);
                } else {
                    down_volumes.push(volume.id)
                }
            }
        }
        self.recovery_mgr.notify_volumes_up_down(up_volumes, down_volumes);

        Ok(())
    }

    fn prepare_alloc(&mut self) {
        let seq = self.weight_map.prepare_choose() as usize;
        info!("seq {:?}, weight map len: {:?}", seq, self.weight_map.len());
        if seq % std::cmp::max(self.weight_map.len(), 1) == 0 {
            self.weight_map.rebuild(&self.node_map);
        }
    }

    fn alloc_spaces(&mut self, alloc: SpaceAllocParams) -> anyhow::Result<SpaceAllocResult> {
        let mut result = SpaceAllocResult {
            volumes: Vec::new(),
            stats: VolumeStats::with_extent(alloc.extent_size),
            nodes_info: Vec::new(),
        };
        info!("to alloc {:?} extents, extent size {:?} replicas {:?}", alloc.num_extents, alloc.extent_size, alloc.num_replicas);
        let mut checked_nodes = 0;
        let mut extent_index = 0;

        self.prepare_alloc();
        while let Some(node) = self.weight_map.choose(&mut checked_nodes, |node| {
            if result.volumes
                .iter()
                .skip(alloc.num_replicas * extent_index)
                .any(|existing| node.id == existing.data_node())
            {
                return None;
            }

            let node_mut = node.mutable.borrow();
            if node_mut.avail_free_bytes >= alloc.extent_size {
                node_mut.weight()
            } else {
                None
            }
        }) {
            info!("find a valid datanode {:?}", node.id);
            let mut checked_vols = 0;
            let mut node_mut = node.mutable.borrow_mut();

            node_mut.prepare_alloc();
            let vol = node_mut.weight_map.choose(&mut checked_vols, |vol| {
                if result.volumes
                    .iter()
                    .skip(alloc.num_replicas * extent_index)
                    .any(|existing| vol.id == *existing)
                {
                    return None;
                }

                let vol_mut = vol.mutable.borrow();
                if vol_mut.free_bytes() >= alloc.extent_size {
                    vol_mut.weight()
                } else {
                    None
                }
            });

            let vol_id = match vol {
                None => continue,
                Some(vol) => {
                    info!("find a valid volume {:?}", vol.id);
                    let mut vol_mut = vol.mutable.borrow_mut();
                    if let Some(vol_stats) = &mut vol_mut.stats {
                        vol_stats.used_bytes =
                            vol_stats.used_bytes.saturating_add(alloc.extent_size as i64);
                        vol_stats.num_extents = vol_stats.num_extents.saturating_add(1);
                    }
                    vol.id
                }
            };

            node_mut.avail_free_bytes = node_mut.avail_free_bytes.saturating_sub(alloc.extent_size);
            self.avail_free_bytes = self.avail_free_bytes.saturating_sub(alloc.extent_size);
            
            if !result.nodes_info.iter().any(|n| n.node_id.unwrap() == node.id) {
                result.nodes_info.push(DataNodeInfo {
                    node_id: Some(node.id),
                    epoch: 0,
                    rpc_addr: node_mut.rpc_addr.map_or(String::new(), |addr| addr.to_string())
                });
            }
            drop(node_mut);

            result.volumes.push(vol_id);
            if result.volumes.len() % alloc.num_replicas == 0 {
                extent_index += 1;
                if extent_index == alloc.num_extents {
                    break;
                }

                checked_nodes = 0;
                self.prepare_alloc();
            }
        } 

        if extent_index != alloc.num_extents {
            // FIXME:
            self.free_spaces(result.volumes, result.stats);
            info!("no enough space for alloc {:?}, {:?}, have alloc {:?}", alloc.num_extents, alloc.extent_size, extent_index);
            return Err(anyhow::anyhow!("no space available"));
        }

        Ok(result)
    }

    fn free_spaces(&mut self, volumes: Vec<VolumeId>, stats: VolumeStats) {
        for vol in volumes.into_iter() {
            if let Some(node) = self.node_map.get(&vol.data_node()) {
                let mut node_mut = node.mutable.borrow_mut();
                let mut update_avail = false;
                if let Some(vol) = node_mut.volumes.get(&vol) {
                    let mut vol_mut = vol.mutable.borrow_mut();
                    if let Some(vol_stats) = &mut vol_mut.stats {
                        vol_stats.used_bytes =
                            vol_stats.used_bytes.saturating_sub(stats.used_bytes);
                        vol_stats.num_extents =
                            vol_stats.num_extents.saturating_sub(stats.num_extents);
                        update_avail = true;
                    }
                    if !vol_mut.is_up() || !vol_mut.is_in() {
                        update_avail = false;
                    }
                }
                if update_avail {
                    node_mut.avail_free_bytes = node_mut
                        .avail_free_bytes
                        .saturating_add(stats.used_bytes as u64);
                    self.avail_free_bytes = self
                        .avail_free_bytes
                        .saturating_add(stats.used_bytes as u64);
                }
            }
        }
    }

    fn handle_beacon(
        &mut self, mut writer: MetaBatchWriter, beacon: DataNodeBeacon,
        resp_tx: oneshot::Sender<Result<DataNodeBeaconAck, tonic::Status>>
    ) {
        
        let node_id = beacon.node_id.unwrap();
        let node = match self.node_map.get(&node_id) {
            Some(node) => node,
            None => {
                error!(?node_id, "beacon node not found");
                let _ = resp_tx.send(
                    Err(tonic::Status::not_found(format!("{:?}", node_id)))
                );
                return;
            }
        };
        if node.uuid.to_string() != beacon.node_uuid {
            error!(
                ?node_id,
                "beacon node uuid mismatch {}/{}", node.uuid, beacon.node_uuid
            );
            let _ = resp_tx.send(
                Err(tonic::Status::not_found("uuids mismatch"))
            );
            return;
        }

        let mut up_volumes = Vec::new();
        let mut down_volumes = Vec::new();
        let mut store_req = StoreRequest::default();

        let now = SystemTime::now();
        let node_mut = &mut *node.mutable.borrow_mut();
        let node_was_up = node_mut.is_up();

        let handle_missing_volume = 
        |
            volume: &Volume,
            store_req: &mut StoreRequest,
            down_volumes: &mut Vec<VolumeId>
        | {
            let mut volume_mut = volume.mutable.borrow_mut();
            if volume_mut.is_up() {
                error!(?node_id, ?volume.id, "beacon does not contain volume");
                volume_mut.mark_down();
                let volume_meta = VolumeMeta::from((&volume.uuid, &*volume_mut));
                store_req.set(
                    StoreColumnFamily::DataNode,
                    volume.id.store_key(VolumeMeta::KEY_SUBFIX).to_vec(),
                    volume_meta.encode_to_vec(),
                );
                if node_was_up {
                    down_volumes.push(volume.id);
                }
            }
        };

        let mut vol_iter = node_mut.volumes.iter();
        let mut next_vol = vol_iter.next();

        for vol_stats in beacon.volumes.iter() {
            let volume_id = match vol_stats.volume_id {
                Some(id) => id,
                None => continue,
            };

            while let Some((id, volume)) = next_vol {
                if *id >= volume_id {
                    break;
                }
                handle_missing_volume(volume, &mut store_req, &mut down_volumes);
                next_vol = vol_iter.next();
            }

            if next_vol.map_or(true, |(id, _)| *id > volume_id) {
                error!(?node_id, ?volume_id, "beacon volume not found");
                continue;
            }

            let (_, volume) = next_vol.unwrap();
            if volume.uuid.to_string() != vol_stats.volume_uuid {
                error!(
                    ?node_id,
                    ?volume_id,
                    "beacon volume uuid mismatch {}/{}",
                    volume.uuid,
                    vol_stats.volume_uuid
                );
                continue;
            }
            next_vol = vol_iter.next();

            let volume_mut = &mut *volume.mutable.borrow_mut();
            if volume_mut.is_up() != vol_stats.is_up() {
                if vol_stats.is_up() {
                    info!(?node_id, ?volume_id, "beacon marks volume up");
                    volume_mut.mark_up();
                    up_volumes.push(volume.id);
                } else {
                    info!(?node_id, ?volume_id, "beacon marks volume down");
                    volume_mut.mark_down();
                    if node_was_up {
                        down_volumes.push(volume.id);
                    }
                }

                let volume_meta = VolumeMeta::from((&volume.uuid, &*volume_mut));
                store_req.set(
                    StoreColumnFamily::DataNode,
                    volume.id.store_key(VolumeMeta::KEY_SUBFIX).to_vec(),
                    volume_meta.encode_to_vec(),
                );
            } else if !node_was_up && volume_mut.is_up() {
                up_volumes.push(volume.id);
            }
        }

        for (_, volume) in vol_iter {
            handle_missing_volume(volume, &mut store_req, &mut down_volumes);
        }

        node_mut.weight_map.rebuild(&node_mut.volumes);

        let mut lazy_update = store_req.is_empty();

        let rpc_addr: SocketAddr = beacon.rpc_addr.parse().unwrap();
        if !node_was_up || node_mut.rpc_addr.map_or(true, |addr| addr != rpc_addr) {
            info!(?node_id, ?rpc_addr, "data node up");
            node_mut.rpc_addr = Some(rpc_addr);
            node_mut.mark_up(now);
            lazy_update = false;
        }

        self.recovery_mgr.notify_volumes_up_down(up_volumes, down_volumes);

        let last_seem = node_mut.last_seem.replace(now);
        if node.link.is_linked() {
            unsafe {
                self.node_list
                    .cursor_mut_from_ptr(Rc::as_ptr(&node.link))
                    .remove();
            }
        }
        self.node_list.push_back(node.link.clone());

        let node_meta = DataNodeMeta::from((&node.uuid, &*node_mut));
        store_req.set(
            StoreColumnFamily::DataNode,
            node_id.store_key().to_vec(),
            node_meta.encode_to_vec(),
        );

        if lazy_update && last_seem.map_or(false, |last_seem| {
            last_seem < writer.get_flushed_upto() &&
            now.duration_since(last_seem).map_or(false, |elapsed| {
                (elapsed.as_millis() as u64) < Self::BEACON_TIMEOUT_MS * 2 / 3
            })
        }) {
            writer.queue_write(store_req);
            let _ = resp_tx.send(Ok(DataNodeBeaconAck::default()));
        } else {
            let res_fut = writer.client_write(store_req);
            tokio::spawn(async move {
                let _ = resp_tx.send(
                    res_fut.await.map(|_| DataNodeBeaconAck::default())
                );
            });
        }
    }

    fn check_down_nodes(&mut self, now: SystemTime) -> StoreRequest {
        let expire_stamp = now.checked_sub(
            Duration::from_millis(Self::BEACON_TIMEOUT_MS)
        ).unwrap();

        let mut store_req = StoreRequest::default();
        let mut cursor = self.node_list.cursor_mut();
        let mut down_volumes = Vec::new();
        cursor.move_next();
        while let Some(link) = cursor.get() {
            let node = match link.node.upgrade() {
                None => {
                    cursor.remove();
                    continue;
                }
                Some(node) => node,
            };
            if node.last_seem() > expire_stamp {
                break;
            }
            if node.is_up() {
                let node_id = node.id;
                info!(?node_id, "data node down");
                let mut node_mut = node.mutable.borrow_mut();
                node_mut.mark_down();

                for (_, volume) in node_mut.volumes.iter() {
                    if volume.mutable.borrow().is_up() {
                        down_volumes.push(volume.id);
                    }
                }

                let node_meta = DataNodeMeta::from((&node.uuid, &*node_mut));
                store_req.set(
                    StoreColumnFamily::DataNode,
                    node.id.store_key().to_vec(),
                    node_meta.encode_to_vec(),
                );
            }
            cursor.remove();
        }
        if !down_volumes.is_empty() {
            self.recovery_mgr.notify_volumes_up_down(Vec::new(), down_volumes);
        }
        store_req
    }

    fn rebuild_weight_map(&mut self) {
        self.weight_map.rebuild(&self.node_map);
    }
}

#[derive(Clone)]
pub struct SpaceAllocParams {
    pub extent_size: u64,
    pub num_replicas: usize,
    pub num_extents: usize,
}

pub struct SpaceAllocResult {
    pub volumes: Vec<VolumeId>,
    pub stats: VolumeStats,
    pub nodes_info: Vec<DataNodeInfo>,
}

enum DataNodeCommand {
    Reload {
        db: Arc<RocksDB>,
        resp_tx: oneshot::Sender<anyhow::Result<()>>,
    },
    AllocSpaces {
        alloc: SpaceAllocParams,
        resp_tx: oneshot::Sender<anyhow::Result<SpaceAllocResult>>,
    },
    FreeSpaces {
        volumes: Vec<VolumeId>,
        stats: VolumeStats,
    },
    PrepareCreate {
        node_id: Option<DataNodeId>,
        uuid: Uuid,
        resp_tx: oneshot::Sender<anyhow::Result<VolumeId>>,
    },
    CreateDataNode {
        id: DataNodeId,
        meta: DataNodeMeta,
    },
    CreateVolume {
        id: VolumeId,
        meta: VolumeMeta,
    },
    DNodeBeacon {
        writer: MetaBatchWriter,
        beacon: DataNodeBeacon,
        resp_tx: oneshot::Sender<Result<DataNodeBeaconAck, tonic::Status>>,
    },
    Tick {
        now: SystemTime,
        writer: MetaBatchWriter,
        resp_tx: oneshot::Sender<Result<(), ClientWriteError>>,
    }
}

pub struct DataNodeManager {
    db: Arc<RocksDB>,
    command_tx: mpsc::UnboundedSender<DataNodeCommand>,
    create_sem: Semaphore,
}

impl DataNodeManager {
    pub fn new(db: Arc<RocksDB>, recovery_mgr: Arc<RecoveryManager>) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            Self::handle_commands(recovery_mgr, command_rx).await;
        });
        Self {
            db, command_tx,
            create_sem: Semaphore::new(1),
        }
    }

    async fn handle_commands(
        recovery_mgr: Arc<RecoveryManager>,
        mut command_rx: mpsc::UnboundedReceiver<DataNodeCommand>
    ) {
        let mut node_map = DataNodeMap::new(recovery_mgr);
        while let Some(cmd) = command_rx.recv().await {
            match cmd {
                DataNodeCommand::Reload { db, resp_tx } => {
                    let res = node_map.reload(db).await;
                    let _ = resp_tx.send(res);
                }
                DataNodeCommand::AllocSpaces { alloc, resp_tx } => {
                    let res = node_map.alloc_spaces(alloc);
                    let _ = resp_tx.send(res);
                }
                DataNodeCommand::FreeSpaces { volumes, stats } => {
                    node_map.free_spaces(volumes, stats);
                }
                DataNodeCommand::PrepareCreate { node_id, uuid, resp_tx } => {
                    let res = node_map.prepare_create(node_id, uuid);
                    let _ = resp_tx.send(res);
                }
                DataNodeCommand::CreateDataNode { id, meta } => {
                    let _ = node_map
                        .insert_data_node(DataNode::with_meta(id, meta))
                        .map_err(|err| eprintln!("failed to insert data node: {:?}", err));
                }
                DataNodeCommand::CreateVolume { id, meta } => {
                    let _ = node_map
                        .insert_volume(Volume::with_meta(id, meta, Some(VolumeStats::default())))
                        .map_err(|err| eprintln!("failed to insert data node: {:?}", err));
                }
                DataNodeCommand::DNodeBeacon { beacon, writer, resp_tx } => {
                    node_map.handle_beacon( writer,  beacon, resp_tx);
                }
                DataNodeCommand::Tick { now, mut writer, resp_tx } => {
                    let store_req = node_map.check_down_nodes(now);
                    node_map.rebuild_weight_map();
                    if store_req.is_empty() {
                        let _ = resp_tx.send(Ok(()));
                    } else {
                        tokio::spawn(async move {
                            let res = writer.raft_write(store_req).await;
                            let _ = resp_tx.send(res);
                        });
                    }
                }
            }
        }
    }

    pub async fn tick(
        &self, writer: MetaBatchWriter, now: SystemTime,
    ) -> Result<(), ClientWriteError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(DataNodeCommand::Tick { now, writer, resp_tx }).unwrap();
        resp_rx.await.unwrap()?;
        Ok(())
    }

    pub async fn reload(&self) -> anyhow::Result<()> {
        let db = self.db.clone();
        let (resp_tx, res_rx) = oneshot::channel();
        self.command_tx.send(DataNodeCommand::Reload { db, resp_tx })?;
        res_rx.await?
    }

    pub async fn alloc_spaces(
        &self, alloc: SpaceAllocParams
    ) -> anyhow::Result<SpaceAllocResult> {
        let (resp_tx, res_rx) = oneshot::channel();
        self.command_tx
            .send(DataNodeCommand::AllocSpaces { alloc, resp_tx })?;
        res_rx.await?
    }

    pub fn free_spaces(&self, volumes: Vec<VolumeId>, stats: VolumeStats) {
        let _ = self.command_tx.send(DataNodeCommand::FreeSpaces { volumes, stats });
    }

    pub async fn create_data_node(
        &self,
        req: VolumeMngmtRequest,
        guard: &mut MetaRequestGuard<'_>,
    ) -> Result<VolumeMngmtResponse, tonic::Status> {
        let uuid = match Uuid::from_str(&req.uuid) {
            Err(err) => {
                return Err(tonic::Status::failed_precondition(err.to_string()));
            }
            Ok(uuid) => uuid,
        };

        let _permit = self.create_sem.acquire().await.unwrap();

        let (resp_tx, res_rx) = oneshot::channel();
        let _ = self.command_tx.send(DataNodeCommand::PrepareCreate {
            node_id: None,
            uuid, resp_tx,
        });
        let id = res_rx.await
            .unwrap()
            .map_err(|err| tonic::Status::internal(err.to_string()))?
            .data_node();

        let meta = DataNodeMeta::from(req);

        let mut store_req = StoreRequest::default();
        store_req.set(
            StoreColumnFamily::DataNode,
            id.store_key().to_vec(),
            meta.encode_to_vec(),
        );

        guard.client_write(store_req).await?;

        let _ = self
            .command_tx
            .send(DataNodeCommand::CreateDataNode { id, meta });

        Ok(VolumeMngmtResponse {
            volume_id: Some(VolumeId::new(id.seq(), 0)),
        })
    }

    pub async fn create_volume(
        &self,
        req: VolumeMngmtRequest,
        guard: &mut MetaRequestGuard<'_>,
    ) -> Result<VolumeMngmtResponse, tonic::Status> {
        let node_id = match req.volume_id {
            None => {
                return Err(tonic::Status::failed_precondition("no volume id"));
            }
            Some(id) => id.data_node(),
        };

        let uuid = match Uuid::from_str(&req.uuid) {
            Err(err) => {
                return Err(tonic::Status::failed_precondition(err.to_string()));
            }
            Ok(uuid) => uuid,
        };

        let _permit = self.create_sem.acquire().await.unwrap();

        let (resp_tx, res_rx) = oneshot::channel();
        let _ = self.command_tx.send(DataNodeCommand::PrepareCreate {
            node_id: Some(node_id),
            uuid,
            resp_tx,
        });
        let id = res_rx.await
            .unwrap()
            .map_err(|err| tonic::Status::internal(err.to_string()))?;

        let meta = VolumeMeta::from(req);

        let mut store_req = StoreRequest::default();
        store_req.set(
            StoreColumnFamily::DataNode,
            id.store_key(VolumeMeta::KEY_SUBFIX).to_vec(),
            meta.encode_to_vec(),
        );
        store_req.set(
            StoreColumnFamily::DataNode,
            id.store_key(VolumeStats::KEY_SUBFIX).to_vec(),
            VolumeStats::default().encode_to_vec(),
        );

        guard.client_write(store_req).await?;

        let _ = self
            .command_tx
            .send(DataNodeCommand::CreateVolume { id, meta });

        Ok(VolumeMngmtResponse {
            volume_id: Some(id),
        })
    }

    pub async fn handle_beacon(
        &self, writer: MetaBatchWriter, beacon: DataNodeBeacon,
    ) -> Result<DataNodeBeaconAck, tonic::Status> {
        beacon
            .node_id
            .ok_or_else(|| tonic::Status::failed_precondition("no node id"))?;
        Uuid::from_str(&beacon.node_uuid)
            .map_err(|err| tonic::Status::failed_precondition(err.to_string()))?;
        beacon
            .rpc_addr
            .parse::<SocketAddr>()
            .map_err(|err| tonic::Status::failed_precondition(err.to_string()))?;

        let (resp_tx, resp_rx) = oneshot::channel();
        let _ = self.command_tx
            .send(DataNodeCommand::DNodeBeacon { writer, beacon, resp_tx });
        resp_rx.await.unwrap()
    }
}
