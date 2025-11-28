use std::sync::{Arc, Weak};
use msc_meta_node::DataVolumeStats;
use tokio::sync::mpsc; 
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use super::client_session::ClientSession;
use crate::{
    store::{SpdkStoreApp, CreateVolumeGuard},
    types::extent_service::{
        ExtentService, ExtentRequest, ExtentResponse,
    }
};
use tracing::{info, error};

pub struct LogExtentService {
    this: Weak<Self>,
    spdk_store: SpdkStoreApp,
}

impl LogExtentService {
    pub fn new(spdk_store: SpdkStoreApp) -> Arc<Self> {
        Arc::new_cyclic(|me| {
            Self { this: me.clone(), spdk_store }
        })
    }

    pub async fn load_volumes(&self) -> anyhow::Result<usize> {
        let mut loaded = 0;
        let mut last_err = None;
        let results = self.spdk_store.load_volumes().await;
        for (bdev, res) in results.into_iter() {
            match res {
                Ok(id) => {
                    loaded += 1;
                    info!("succeed in loading {}, {:?}", bdev, id);
                }
                Err(err) => {
                    error!("failed to load {}, {:?}", bdev, err);
                    last_err.replace(err);
                }
            }
        }
        if loaded > 0 {
            Ok(loaded)
        } else if let Some(err) = last_err {
            Err(anyhow::Error::new(err))
        } else {
            Ok(0)
        }
    }

    pub async fn gather_volumes_stats(&self) -> Vec<DataVolumeStats> {
        self.spdk_store.gather_volumes_stats().await
    }

    pub async fn prepare_create_volume(
        &self, bdev: String
    ) -> anyhow::Result<CreateVolumeGuard> {
        let guard = self.spdk_store.prepare_create_volume(bdev, None).await?;
        Ok(guard)
    }

    pub async fn handle_requests<S>(
        &self, session: ClientSession, mut req_stream: S,
        responder: mpsc::Sender<Result<ExtentResponse, tonic::Status>>
    ) 
    where
        S: futures::Stream<Item=Result<ExtentRequest, tonic::Status>> + Unpin
    {
        let (resp_tx, resp_rx) = flume::bounded::<ExtentResponse>(session.max_inflight);
        loop {
            tokio::select! {
                Some(req) = req_stream.next(), if session.has_request_cap() => {
                    match req {
                        Ok(req) => {
                            session.inc_inflight();
                            self.spdk_store.handle_extent_op(req, resp_tx.clone()).await;
                        }
                        Err(_) => {
                            break;
                        }
                    }
                }
                Ok(resp) = resp_rx.recv_async() => {
                    let res = responder.send(Ok(resp)).await;
                    session.dec_inflight();
                    if res.is_err() {
                        break;
                    }
                }
            }
        }
    }
}

#[tonic::async_trait]
impl ExtentService for LogExtentService {
    type ClientSessionStream = ReceiverStream<Result<ExtentResponse, tonic::Status>>;

    async fn client_session(
        &self,
        request: tonic::Request<tonic::Streaming<ExtentRequest>>,
    ) -> std::result::Result<
        tonic::Response<Self::ClientSessionStream>,
        tonic::Status,
    > {
        let session = ClientSession::new(256);
        let (resp_tx, resp_rx) = mpsc::channel(session.max_inflight);
        let req_stream = request.into_inner();
        let this = self.this.upgrade().unwrap();
        tokio::spawn(async move {
            this.handle_requests(session, req_stream, resp_tx).await;
        });
        Ok(tonic::Response::new(ReceiverStream::new(resp_rx)))
    }
}
