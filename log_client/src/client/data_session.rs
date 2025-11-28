use crate::types::extent_service::{
    ExtentRequest, ExtentResponse, ExtentResponseKind, ExtentServiceClient
};
use common_proto::{ DataNodeId, DataNodeInfo, VolumeId, ExtentId, LogStreamId };
use msc_meta_node::client::DataNodeConnector;
use futures::StreamExt;
use nix::errno::Errno;
use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Endpoint, Channel};
use tracing::{trace, warn, error};

enum SessionCommand {
    Connect {
        node_info: DataNodeInfo,
    },
    SendRequest {
        req: ExtentRequest,
        resp_tx: Option<oneshot::Sender<ExtentResponse>>
    },
    AttachResponder {
        stream_id: LogStreamId,
        responder: mpsc::UnboundedSender<ExtentResponse>,
    },
    DetachResponder {
        stream_id: LogStreamId,
    }
}

struct ExtentServiceChannel {
    request_tx: mpsc::Sender<ExtentRequest>,
    response_rx: tonic::Streaming<ExtentResponse>,
    _client: ExtentServiceClient<Channel>,
}

type RequestMapValue = (Option<ExtentId>, Option<oneshot::Sender<ExtentResponse>>);

struct DataNodeSessionImpl {
    node_info: DataNodeInfo,
    service_chan: Option<ExtentServiceChannel>,
    responder_map: BTreeMap<LogStreamId, mpsc::UnboundedSender<ExtentResponse>>,
    request_map: BTreeMap<(u64, Option<VolumeId>), RequestMapValue>
}

impl DataNodeSessionImpl {
    const URL_SCHEME: &'static str = "http://";
    const REQUEST_CHANNEL_CAP: usize = 256;

    fn new(node_info: DataNodeInfo) -> Self {
        Self {
            node_info,
            service_chan: None,
            responder_map: BTreeMap::new(),
            request_map: BTreeMap::new(),
        }
    }

    async fn run(&mut self, mut cmd_rx: mpsc::Receiver<SessionCommand>) {
        loop {
            if self.service_chan.is_none() {
                if let Err(err) = self.connect().await {
                    error!(?self.node_info, "error connecting {:?}", err);
                }
            }

            tokio::select! {
                biased;
                resp = self.service_chan.as_mut().unwrap().response_rx.next(), if self.service_chan.is_some() => {
                    match resp {
                        Some(Ok(resp)) => {
                            let key = &(resp.request_seq, resp.volume_id);
                            if let Some((_, resp_tx)) = self.request_map.remove(key) {
                                if let Some(resp_tx) = resp_tx {
                                    let _ = resp_tx.send(resp);
                                } else if let Some(resp_tx) = resp.extent_id.and_then(
                                    |extent_id| self.responder_map.get(&extent_id.stream_id())
                                ) {
                                    let _ = resp_tx.send(resp);
                                } else {
                                    warn!(
                                        ?self.node_info, "no channel for response {}/{:?}",
                                        resp.request_seq, resp.extent_id,
                                    );
                                }
                            } else {
                                warn!(
                                    ?self.node_info, "got unexpected response {}/{:?}",
                                    resp.request_seq, resp.extent_id,
                                );
                            }
                        }
                        Some(Err(err)) => {
                            warn!(?self.node_info, "got err from response stream {:?}", err);
                        }
                        None => {
                            trace!(?self.node_info, "response stream is closed");
                            self.service_chan.take();
                            self.fault_requests();
                        }
                    }
                }
                cmd = cmd_rx.recv(), if self.service_chan.as_ref().map_or(true, |chan| !chan.request_tx.is_closed()) => {
                    match cmd {
                        Some(SessionCommand::Connect { node_info }) => {
                            if node_info.epoch > self.node_info.epoch {
                                self.service_chan.take();
                                self.fault_requests();
                            }
                            self.node_info = node_info;
                        }
                        Some(SessionCommand::SendRequest { req, mut resp_tx }) => {
                            let unsent = if let Some(svc_chan) = &self.service_chan {
                                let request_seq = req.request_seq;
                                let extent_id = req.extent_id;
                                let volume_id = req.volume_id;
                                match svc_chan.request_tx.send(req).await {
                                    Ok(_) => {
                                        self.request_map.insert(
                                            (request_seq, volume_id),
                                            (extent_id, resp_tx.take())
                                        );
                                        None
                                    }
                                    Err(err) => {
                                        trace!(?self.node_info, "failed to send {:?}", err);
                                        Some(err.0)
                                    }
                                }
                            } else {
                                Some(req)
                            };
                            if let Some(req) = unsent {
                                let resp = ExtentResponse::errno(&req, Errno::EPIPE);
                                if let Some(resp_tx) = resp_tx {
                                    let _ = resp_tx.send(resp);
                                } else if let Some(resp_tx) = resp.extent_id.and_then(
                                    |extent_id| self.responder_map.get(&extent_id.stream_id())
                                ) {
                                    let _ = resp_tx.send(resp);
                                } 
                            }
                        }
                        Some(SessionCommand::AttachResponder { stream_id, responder }) => {
                            self.responder_map.insert(stream_id, responder);
                        }
                        Some(SessionCommand::DetachResponder { stream_id }) => {
                            self.responder_map.remove(&stream_id);
                        }
                        None => {
                            trace!(?self.node_info, "session closed");
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        let ep = Endpoint::from_shared(
            String::from(Self::URL_SCHEME) + &self.node_info.rpc_addr
        )?;
        let mut client = ExtentServiceClient::connect(ep).await?;
        let (request_tx, req_rx) = mpsc::channel(Self::REQUEST_CHANNEL_CAP);
        let resp = client.client_session(ReceiverStream::new(req_rx)).await?;
        self.service_chan.replace(ExtentServiceChannel {
            _client: client,
            request_tx, 
            response_rx: resp.into_inner(),
        });
        Ok(())
    }

    fn fault_requests(&mut self) {
        while let Some(((request_seq, volume_id), (extent_id, resp_tx))) = self.request_map.pop_first() {
            let resp = ExtentResponse {
                request_seq,
                extent_id,
                volume_id,
                kind: Some(ExtentResponseKind::errno(Errno::EPIPE))
            };
            if let Some(resp_tx) = resp_tx {
                let _ = resp_tx.send(resp);
            } else if let Some(resp_tx) = resp.extent_id.and_then(
                |extent_id| self.responder_map.get(&extent_id.stream_id())
            ) {
                let _ = resp_tx.send(resp);
            }
        }
    }
}

pub struct DataNodeSession {
    node_id: DataNodeId,
    command_tx: mpsc::Sender<SessionCommand>,
}

impl DataNodeSession {
    pub async fn send_request(
        &self, req: ExtentRequest,
        resp_tx: Option<oneshot::Sender<ExtentResponse>>
    ) {
        self.command_tx.send(
            SessionCommand::SendRequest { req, resp_tx }
        ).await.unwrap();
    }

    pub async fn kick_connect(&self, node_info: DataNodeInfo) {
        self.command_tx.send(SessionCommand::Connect { node_info }).await.unwrap();
    }

    pub async fn attach_responder(
        &self, stream_id: LogStreamId, responder: mpsc::UnboundedSender<ExtentResponse>
    ) {
        self.command_tx.send(
            SessionCommand::AttachResponder { stream_id, responder }
        ).await.unwrap();
    }

    pub async fn detach_responder(&self, stream_id: LogStreamId) {
        self.command_tx.send(
            SessionCommand::DetachResponder { stream_id }
        ).await.unwrap();
    }
}

enum SessionMngmtCommand {
    MultiGet {
        node_ids: Vec<DataNodeId>,
        resp_tx: oneshot::Sender<Vec<Option<Arc<DataNodeSession>>>>,
    },
    Connect {
        nodes_info: Vec<DataNodeInfo>,
    },
    Tick,
}

pub struct DataSessionManager {
    command_tx: mpsc::Sender<SessionMngmtCommand>,
}

impl Clone for DataSessionManager {
    fn clone(&self) -> Self {
        Self { command_tx: self.command_tx.clone() }
    }
}

impl DataSessionManager {
    const COMMAND_CHANNEL_CAP: usize = 128;

    pub fn new() -> Self {
        let (command_tx, cmd_rx) = mpsc::channel(Self::COMMAND_CHANNEL_CAP);
        tokio::spawn(async move {
            Self::run(cmd_rx).await;
        });
        Self { command_tx }
    }

    pub async fn multi_get(&self, node_ids: Vec<DataNodeId>) -> Vec<Option<Arc<DataNodeSession>>> {
        let (resp_tx, resp) = oneshot::channel();
        self.command_tx.send(
            SessionMngmtCommand::MultiGet{ node_ids, resp_tx }
        ).await.unwrap();
        resp.await.unwrap()
    }

    pub async fn connect(&self, nodes_info: Vec<DataNodeInfo>) {
        self.command_tx.send(
            SessionMngmtCommand::Connect { nodes_info }
        ).await.unwrap();
    }

    async fn run(mut cmd_rx: mpsc::Receiver<SessionMngmtCommand>) {
        let mut session_map: BTreeMap<DataNodeId, Arc<DataNodeSession>> = BTreeMap::new();

        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                SessionMngmtCommand::MultiGet { node_ids, resp_tx } => {
                    let mut sessions = Vec::with_capacity(node_ids.len());
                    for node_id in node_ids.iter() {
                        sessions.push(session_map.get(node_id).cloned());
                    }
                    let _ = resp_tx.send(sessions);
                }
                SessionMngmtCommand::Connect { nodes_info } => {
                    for node_info in nodes_info.into_iter() {
                        let node_id = node_info.id();
                        let (session, sess_cmd_rx) = match session_map.entry(node_id) {
                            Entry::Vacant(v) => {
                                let (command_tx, cmd_rx) = mpsc::channel(Self::COMMAND_CHANNEL_CAP);
                                let session = Arc::new(DataNodeSession { node_id, command_tx });
                                v.insert(session.clone());
                                (session, Some(cmd_rx))
                            }
                            Entry::Occupied(o) => {
                                let session = o.get().clone();
                                (session, None)
                            }
                        };
                        if let Some(sess_cmd_rx) = sess_cmd_rx {
                            tokio::spawn(async move {
                                let mut sess_impl = DataNodeSessionImpl::new(node_info);
                                sess_impl.run(sess_cmd_rx).await;
                            });
                        } else {
                            session.kick_connect(node_info).await;
                        }
                    }
                }
                SessionMngmtCommand::Tick => {
                    todo!("cleanup idle session")
                }
            }
        }
    }
}

impl DataNodeConnector for DataSessionManager {
    async fn call(self, nodes_info: Vec<DataNodeInfo>)  {
        self.connect(nodes_info).await
    }
}
