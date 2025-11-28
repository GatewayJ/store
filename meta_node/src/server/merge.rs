use rocksdb::MergeOperands;
use crate::{
    server::{
        data_node::{VolumeStats, merge_volume_stats},
        client_session::merge_last_session,
    },
    types::common_proto::{ClientId, ClientIdStoreKey, VolumeId, VolumeIdStoreKey},
};

pub fn merge_cf_log_stream(
    key: &[u8], existing: Option<&[u8]>, operands: &MergeOperands
) -> Option<Vec<u8>> {
    if key.cmp(&ClientId::last_id_key()).is_eq() {
        merge_last_session(key, existing, operands)
    } else {
        None
    }
}

pub fn merge_cf_data_node(
    key: &[u8], existing: Option<&[u8]>, operands: &MergeOperands
) -> Option<Vec<u8>> {
    if VolumeId::parse(key).map_or(false, |(_, subfix)| subfix == VolumeStats::KEY_SUBFIX) {
        merge_volume_stats(key, existing, operands)
    } else {
        None
    }
}
