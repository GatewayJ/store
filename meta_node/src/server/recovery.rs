use std::{
    vec::Vec, sync::Weak, collections::BTreeSet,
};
use crate::store::StoreRequest;
use common_proto::{ClientId, ExtentId, ExtentPlacement, LogStreamId, VolumeId};
use super::meta_service::MetaNodeService;
use tracing::error;

pub struct RecoveryManager {
    meta_service: Weak<MetaNodeService>
}

impl RecoveryManager {
    pub fn new(meta_service: Weak<MetaNodeService>) -> Self {
        Self { meta_service }
    }

    pub fn begin_reload(&self) {
    }
    pub fn finish_reload(&self, succeed: bool) {
    }

    pub fn add_unclosed_streams(&self, stream_ids: BTreeSet<LogStreamId>, client_id: ClientId) {
        error!("adding unclosed stream {:?} {:?}", stream_ids, client_id);
    }

    pub fn add_stale_extent(
        &self, store_req: &mut StoreRequest,
        places: Vec<ExtentPlacement>, extent_id: ExtentId,
    ) {
        error!("adding stale extent {:?} {:?}", places, extent_id);
    }

    pub fn add_unsealed_extent(
        &self, store_req: &mut StoreRequest,
        places: Vec<ExtentPlacement>, extent_id: ExtentId
    ) {
        error!("adding unsealed extent {:?} {:?}", places, extent_id);
    }

    pub fn notify_volumes_up_down(
        &self, up_volumes: Vec<VolumeId>, down_volumes: Vec<VolumeId>
    ) {
        error!("notify volumes up {:?} down {:?}", up_volumes, down_volumes);
    }
}
