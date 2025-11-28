use std::{
    fmt::{Debug, Display},
    time::{Duration, SystemTime, UNIX_EPOCH}
};
use serde::ser::Serialize;
use crate::types::RaftError;

pub fn systime_to_proto(
    time: &Option<SystemTime>
) -> Option<prost_types::Timestamp> {
    time.and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| prost_types::Timestamp {
                seconds: d.as_secs() as _,
                nanos: d.subsec_nanos() as _,
            })
}

pub fn systime_from_proto(
    ts: &Option<prost_types::Timestamp>
) -> Option<SystemTime> {
    ts.and_then(|ts| {
        SystemTime::UNIX_EPOCH.checked_add(
            Duration::new(ts.seconds as _, ts.nanos as _)
        )
    })
}

pub fn map_raft_error<E: Serialize + Display + Debug>(e: RaftError<E>) -> tonic::Status {
    serde_json::to_vec(&e)
        .map_or_else(
            |_| tonic::Status::internal(e.to_string()),
            |v| tonic::Status::with_details(tonic::Code::Internal, e.to_string(), v.into())
        )
}
