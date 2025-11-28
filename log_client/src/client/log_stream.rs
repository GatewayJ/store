use common_proto::{LogStreamId, ExtentId, ExtentPlacement, ExtentVersion, VolumeId};
use msc_meta_node::client::{LogStream as LogStreamMeta, LastExtent};
use crate::types::extent_service::{
    ExtentRequest, ExtentRequestKind, ExtentResponse, ExtentResponseKind,
    CreateParams, AppendParams, DeleteParams, SealParams, SealResponse
};
use super::data_session::{DataNodeSession, DataSessionManager};
use nix::errno::Errno;
use std::{
    collections::{btree_map::Entry, BTreeMap, VecDeque},
    sync::{ atomic::{AtomicU64, Ordering}, Arc,}
};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, trace, warn};

enum LogStreamCommand {
    Append {
        params: AppendParams,
        resp_tx: oneshot::Sender<Result<(ExtentId, u64), Errno>>,
    },
    Close {
        resp_tx: oneshot::Sender<anyhow::Result<()>>
    },
    LastExtent {
        resp_tx: oneshot::Sender<LastExtent>
    }
}

struct LogWriteRequest {
    request: ExtentRequestKind,
    responses: BTreeMap<VolumeId, ExtentResponseKind>,
    resp_tx: Option<oneshot::Sender<Result<(ExtentId, u64), Errno>>>,
    log_seq: u64,
    pending_send: u32,
}

impl LogWriteRequest {
    fn is_create(&self) -> bool {
        matches!(self.request, ExtentRequestKind::Create(_))
    }
    fn is_append(&self) -> bool {
        matches!(self.request, ExtentRequestKind::Append(_))
    }
    fn is_seal(&self) -> bool {
        matches!(self.request, ExtentRequestKind::Seal(_))
    }
    fn data_size(&self) -> usize {
        if let ExtentRequestKind::Append(params) = &self.request {
            params.payload.len()
        } else {
            0
        }
    }
}

struct LogExtentWriter {
    epoch: u32,
    session: Arc<DataNodeSession>,
    first_error: Option<(u64, Errno)>,
    sealed: Option<SealResponse>,
}

impl LogExtentWriter {
    fn new(epoch: u32, session: Arc<DataNodeSession>) -> Self {
        Self {
            epoch, session,
            first_error: None,
            sealed: None,
        }
    }
}
struct LogStreamImpl {
    log_stream: LogStreamMeta,
    data_sessions: DataSessionManager,
    last_request_seq: Arc<AtomicU64>,
    closed_tx: Option<oneshot::Sender<anyhow::Result<()>>>,

    extent_writers: BTreeMap<VolumeId, LogExtentWriter>,
    inflight_writes: BTreeMap<u64, LogWriteRequest>,
    pending_writes: VecDeque<LogWriteRequest>,
    last_extent_id: ExtentId,
    next_write_version: ExtentVersion,
    extent_avail_bytes: Option<usize>, 
    curr_used_bytes: usize,
    curr_num_appends: usize,
}

impl LogStreamImpl {
    const EXTENT_IO_UNIT: usize = 4096;
    const MAX_WRITE_REQUESTS: usize = 256;

    async fn run(&mut self, mut cmd_rx: mpsc::Receiver<LogStreamCommand>) {
        let (resp_tx, mut resp_rx) = mpsc::unbounded_channel::<ExtentResponse>();
        loop {
            if self.needs_alloc_extent() {
                if self.inflight_writes.is_empty() {
                    self.do_alloc_extent(&resp_tx).await;
                }
            } else {
                self.maybe_dequeue_pending();
                if self.inflight_writes.is_empty() && self.closed_tx.is_some() {
                    break;
                }
            }

            tokio::select! {
                biased;
                resp = resp_rx.recv() => {
                    self.handle_response(resp.unwrap());
                }
                Some(cmd) = cmd_rx.recv(), if self.should_recv_commands() => {
                    self.handle_command(cmd);
                }
                Some(do_seal) = self.do_send_request() => {
                    if do_seal {
                        self.start_sealing_extent();
                    }
                }
            }
        }
    }

    fn should_recv_commands(&self) -> bool {
        self.inflight_writes.len() + self.pending_writes.len() <
        Self::MAX_WRITE_REQUESTS
    }

    fn align_io_unit_size(val: usize) -> usize {
        val.next_multiple_of(Self::EXTENT_IO_UNIT)
    }

    fn next_request_seq(&self) -> u64 {
        self.last_request_seq.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn needs_retry_write(&self, log_seq: u64) -> bool {
        self.extent_writers.iter()
            .any(|x| {
                x.1.first_error
                    .map_or(false, |(seq, _)| log_seq >= seq)
            })
    }

    fn maybe_dequeue_pending(&mut self) {
        if self.pending_writes.is_empty() {
            return;
        }
        if self.inflight_writes
                .last_key_value()
                .map_or(false, |(_, req)| req.pending_send > 0) {
            return;
        }

        let mut req = self.pending_writes.pop_front().unwrap();
        if req.log_seq > 0 {
            assert!(req.log_seq != self.next_write_version.log_seq);
        }
        req.log_seq = self.next_write_version.log_seq;
        req.pending_send = (1 << self.extent_writers.len()) - 1;
        self.curr_used_bytes +=
            Self::align_io_unit_size(req.data_size()) + Self::EXTENT_IO_UNIT;
        self.curr_num_appends += 1;
        self.inflight_writes.insert(self.next_request_seq(), req);
    }

    fn start_sealing_extent(&mut self) {
        self.inflight_writes.insert(
            self.next_request_seq(),
            LogWriteRequest{
                request: ExtentRequestKind::Seal(SealParams{}),
                responses: BTreeMap::new(),
                resp_tx: None,
                log_seq: self.next_write_version.log_seq - 1,
                pending_send: (1 << self.extent_writers.len()) - 1,
            }
        );
    }

    async fn do_send_request(&mut self) -> Option<bool> {
        if let Some(mut entry) = self.inflight_writes.last_entry() {
            let request_seq = *entry.key();
            let req = entry.get_mut();
            if req.pending_send == 0 {
                return None;
            }

            let extent_version = ExtentVersion::new(
                self.next_write_version.epoch, req.log_seq
            );

            for (idx, (&volume_id, writer)) in self.extent_writers.iter().enumerate() {
                if req.pending_send & (1 << idx) == 0 {
                    continue;
                }
                let ext_req = ExtentRequest {
                    request_seq,
                    volume_id: Some(volume_id),
                    extent_id: Some(self.last_extent_id),
                    extent_version: Some(extent_version),
                    kind: Some(req.request.clone())
                };
                writer.session.send_request(ext_req, None).await;
                req.pending_send &= !(1 << idx);
            }
            
            if req.is_append() {
                assert!(req.log_seq == self.next_write_version.log_seq);
                self.next_write_version.log_seq += 1;
                Some(!self.any_write_error() && !self.has_enough_space())
            } else {
                Some(false)
            }
        } else {
            None
        }
    }
    
    fn has_enough_space(&self) -> bool {
        let want_bytes = if let Some(next) = self.pending_writes.front() {
            Self::align_io_unit_size(next.data_size()) + Self::EXTENT_IO_UNIT
        } else if self.curr_num_appends > 0 {
            Self::align_io_unit_size(self.curr_used_bytes / self.curr_num_appends)
        } else {
            Self::EXTENT_IO_UNIT * 2
        };
        if let Some(avail_bytes) = self.extent_avail_bytes {
            self.curr_used_bytes + want_bytes <= avail_bytes
        } else {
            let extent_size = self.log_stream.get_config().extent_size as usize;
            self.curr_used_bytes + want_bytes + (extent_size >> 9) <= extent_size
        }
    }

    fn any_write_error(&self) -> bool {
        self.extent_writers.iter().any(|x| x.1.first_error.is_some())
    }

    fn needs_alloc_extent(&self) -> bool {
        !self.log_stream.last_extent().is_writable() ||
        self.any_write_error() ||
        !self.has_enough_space()
    }

    async fn do_seal_extent(&mut self) -> bool {
        let stream_id = self.log_stream.get_id();
        if !self.log_stream.last_extent().is_writable() {
            return true;
        }

        let mut extent_used_bytes = 0;
        let mut num_sealed = 0;
        let mut error_seq = None;
        let mut error_vols = Vec::new();

        for (volume_id, writer) in self.extent_writers.iter() {
            writer.session.detach_responder(stream_id).await;

            if let Some((log_seq, _)) = &writer.first_error {
                error_vols.push(ExtentPlacement::new(*volume_id, writer.epoch));
                if error_seq.map_or(true, |seq| seq > *log_seq) {
                    error_seq.replace(*log_seq);
                }
            } else if let Some(resp) = writer.sealed {
                num_sealed += 1;
                extent_used_bytes = resp.used_bytes;
            }
        }

        if num_sealed < self.extent_writers.len() {
            let next_version = &mut self.next_write_version;
            if let Some(err_seq) = error_seq {
                next_version.log_seq = std::cmp::max(err_seq, self.last_extent_id.log_seq);
            };
            let extent_version = if next_version.log_seq > self.last_extent_id.log_seq {
                self.next_write_version.prev()
            } else {
                ExtentVersion::new(next_version.epoch, 0)
            };
            let req_kind = if extent_version.log_seq > 0 {
                ExtentRequestKind::Seal(SealParams{})
            } else {
                ExtentRequestKind::Delete(DeleteParams{})
            };

            let mut resp_rx_vec = Vec::new();
            for (volume_id, writer) in self.extent_writers.iter() {
                if error_vols.iter().any(|place| place.volume() == *volume_id) {
                    continue;
                }

                let (resp_tx, resp_rx) = oneshot::channel();
                writer.session.send_request(
                    ExtentRequest {
                        request_seq: self.next_request_seq(),
                        volume_id: Some(*volume_id),
                        extent_id: Some(self.last_extent_id),
                        extent_version: Some(extent_version),
                        kind: Some(req_kind.clone())
                    },
                    Some(resp_tx)
                ).await;
                resp_rx_vec.push(resp_rx);
            }

            num_sealed = 0;
            for resp_rx in resp_rx_vec.into_iter() {
                let resp = resp_rx.await.unwrap();
                match resp.ok() {
                    Ok(_) => {
                        num_sealed += 1;
                        if let Some(ExtentResponseKind::Seal(seal)) = resp.kind {
                            if extent_used_bytes < seal.used_bytes {
                                extent_used_bytes = seal.used_bytes;
                            }
                        }
                    }
                    Err(errno) => {
                        if errno == Errno::ENOENT &&
                           matches!(req_kind, ExtentRequestKind::Delete(_)) {
                            num_sealed += 1;
                            continue;
                        }
                        error_vols.push(ExtentPlacement::new(
                            resp.volume_id.unwrap(), extent_version.epoch
                        ));
                    }
                }
            }
        }

        println!("sealing {:?} {} {:?}", self.next_write_version, extent_used_bytes, error_vols);
        self.log_stream.prepare_seal_extent(
            self.next_write_version.log_seq, extent_used_bytes, error_vols
        );
        
        let all_sealed = self.extent_writers.len() == num_sealed;
        self.extent_writers.clear();

        all_sealed
    }

    async fn do_alloc_extent(&mut self, resp_tx: &mpsc::UnboundedSender<ExtentResponse>) {
        let stream_id = self.log_stream.get_id();

        let mut all_sealed = self.do_seal_extent().await;
        
        let last_extent = loop {
            let prealloc_async = all_sealed;
            all_sealed = false;
            let data_sessions = self.data_sessions.clone();
            if let Err(err) = self.log_stream.alloc_extent(
                prealloc_async, data_sessions
            ).await {
                let stream_id = self.log_stream.get_id();
                error!(?stream_id, "error allocating extent {:?}", err);
                continue;
            }
    
            let last_extent = self.log_stream.last_extent();
            assert!(!last_extent.places.is_empty());
            let node_ids = last_extent.places.iter().map(|p| p.data_node()).collect();
            let sessions = self.data_sessions.multi_get(node_ids).await;
            if let Some((idx, _)) = sessions.iter().enumerate().find(|(_, s)| s.is_none()) {
                error!(
                    ?stream_id, "failed to get session for {:?}",
                    last_extent.places[idx].data_node()
                );
                continue
            }

            for (session, place) in sessions.into_iter().zip(last_extent.places.iter()) {
                let session = session.unwrap();
                session.attach_responder(stream_id, resp_tx.clone()).await;
                self.extent_writers.insert(
                    place.volume(), LogExtentWriter::new(place.epoch, session)
                );
            }
            break last_extent;
        };

        let extent_size = self.log_stream.get_config().extent_size;

        self.last_extent_id = last_extent.id;
        self.next_write_version = last_extent.next_version;
        self.curr_used_bytes = extent_size as usize / Self::EXTENT_IO_UNIT * 4;
        self.curr_num_appends = 0;

        self.inflight_writes.insert(
            self.next_request_seq(),
            LogWriteRequest {
                request: ExtentRequestKind::Create(CreateParams {
                    size: extent_size,
                }),
                responses: BTreeMap::new(),
                resp_tx: None,
                log_seq: 0,
                pending_send: (1 << self.extent_writers.len()) - 1,
            }
        );
    }

    fn handle_response(&mut self, resp: ExtentResponse) {
        let stream_id = self.log_stream.get_id();
        let request_seq = resp.request_seq;
        if !resp.is_valid() {
            warn!(?stream_id, ?request_seq, "invalid response");
            return;
        }
        let mut entry = match self.inflight_writes.entry(resp.request_seq) {
            Entry::Vacant(_) => {
                trace!(?stream_id, ?request_seq, "stale response");
                return;
            }
            Entry::Occupied(entry) => entry,
        };

        let volume_id = resp.volume_id.unwrap();
        let extent_id = resp.extent_id.unwrap();
        match self.extent_writers.entry(volume_id) {
            Entry::Occupied(mut o) => {
                match resp.ok() {
                    Ok(_) => {
                        if entry.get().is_seal() {
                            if let Some(ExtentResponseKind::Seal(seal)) = resp.kind {
                                let writer = o.get_mut();
                                writer.sealed.replace(seal);
                            } else {
                                error!(
                                    ?extent_id, ?request_seq,
                                    "unexpected response kind for seal {:?}", entry.get().request
                                );
                            }
                        } else if entry.get().is_create() {
                            if let Some(ExtentResponseKind::Create(create)) = resp.kind {
                                let avail_bytes = create.avail_bytes as usize;
                                if self.extent_avail_bytes.map_or(true, |v| v > avail_bytes) {
                                    self.extent_avail_bytes.replace(avail_bytes);
                                    trace!(
                                        ?extent_id, ?request_seq,
                                        "update extent_avail_bytes to {}", avail_bytes
                                    );
                                }
                            } else {
                                error!(
                                    ?extent_id, ?request_seq,
                                    "unexpected response kind for create"
                                );
                            }
                        }
                    }
                    Err(errno) => {
                        if entry.get().is_seal() {
                            warn!(
                                ?extent_id, ?request_seq,
                                "seal error for volume {:?}, {:?}", volume_id, errno
                            );
                        } else {
                            let writer = o.get_mut();
                            if writer.first_error.is_none() {
                                warn!(
                                    ?extent_id, ?request_seq,
                                    "first error for volume {:?}, {:?}", volume_id, errno
                                );
                                writer.first_error.replace((entry.get().log_seq, errno));
                            }
                        }
                    }
                }
            }
            Entry::Vacant(_) => {
                warn!(
                    ?extent_id, ?request_seq,
                    "unexpected response for volume {:?}", volume_id
                );
                return;
            }
        }

        let req = entry.get_mut();
        let req_log_seq = req.log_seq;
        let old = req.responses.insert(volume_id, resp.kind.unwrap());
        if old.is_some() {
            warn!(
                ?extent_id, ?req_log_seq,
                "duplicated response for volume {:?}", volume_id
            );
            return;
        }

        let more = self.extent_writers.len() - req.responses.len();
        if more > 0 {
            trace!(
                ?extent_id, ?req_log_seq,
                "waiting for {} more responses", more
            );
            return;
        }

        let mut fin_req = entry.remove();
        if self.needs_retry_write(fin_req.log_seq) {
            if let Some(next_req) = self.inflight_writes.first_entry() {
                assert!(next_req.get().log_seq > fin_req.log_seq);
                while let Some((_, other_req)) = self.inflight_writes.pop_last() {
                    if other_req.is_append() {
                        fin_req.responses.clear();
                        self.pending_writes.push_front(other_req);
                    }
                }
            }
            if fin_req.is_append() {
                fin_req.responses.clear();
                self.pending_writes.push_front(fin_req);
            }
        } else if let Some(resp_tx) = fin_req.resp_tx {
            let _ = resp_tx.send(Ok((self.last_extent_id, fin_req.log_seq))); 
            trace!(?extent_id, ?req_log_seq, "request finished");
        }
    }

    fn handle_command(&mut self, cmd: LogStreamCommand) {
        match cmd {
            LogStreamCommand::Append { params, resp_tx } => {
                self.pending_writes.push_back(LogWriteRequest{
                    request: ExtentRequestKind::Append(params),
                    responses: BTreeMap::new(),
                    resp_tx: Some(resp_tx),
                    log_seq: 0,
                    pending_send: 0,
                });
            }
            LogStreamCommand::Close { resp_tx } => {
                self.closed_tx.replace(resp_tx);
            }
            LogStreamCommand::LastExtent { resp_tx } => {
                let _ = resp_tx.send(self.log_stream.last_extent().clone());
            }
        }
    }
}

pub struct LogStream {
    stream_id: LogStreamId,
    command_tx: mpsc::Sender<LogStreamCommand>,
}

impl LogStream {
    pub(super) fn new(
        log_stream: LogStreamMeta,
        data_sessions: DataSessionManager,
        last_request_seq: Arc<AtomicU64>
    ) -> Self {
        let stream_id = log_stream.get_id();
    
        let last_extent = log_stream.last_extent();
        let mut lst_impl = LogStreamImpl {
            extent_writers: BTreeMap::new(),
            inflight_writes: BTreeMap::new(),
            pending_writes: VecDeque::new(),
            last_extent_id: last_extent.id,
            next_write_version: last_extent.next_version,
            extent_avail_bytes: None,
            curr_used_bytes: usize::MAX,
            curr_num_appends: 0,
            log_stream,
            data_sessions,
            last_request_seq,
            closed_tx: None,
        };

        let (command_tx, cmd_rx) = mpsc::channel(256);
        tokio::spawn(async move {
            lst_impl.run(cmd_rx).await;
            lst_impl.do_seal_extent().await;
            let res = lst_impl.log_stream.close().await;
            if let Some(closed_tx) = lst_impl.closed_tx {
                let _ = closed_tx.send(res);
            }
        });

        Self { stream_id, command_tx }
    }

    pub async fn last_extent(&self) -> LastExtent {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(LogStreamCommand::LastExtent { resp_tx }).await.unwrap();
        resp_rx.await.unwrap()
    }

    pub async fn write(&self, user_header: Vec<u8>, payload: Vec<u8>) -> Result<(ExtentId, u64), Errno> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(LogStreamCommand::Append {
            params: AppendParams { user_header, payload },
            resp_tx,
        }).await.unwrap();
        resp_rx.await.map_err(|_| Errno::EBADFD).and_then(|v| v)
    }

    pub async fn close(self) -> anyhow::Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(LogStreamCommand::Close { resp_tx }).await.unwrap();
        resp_rx.await.map_err(|_| Errno::EBADFD.into()).and_then(|v| v)
    }
}
