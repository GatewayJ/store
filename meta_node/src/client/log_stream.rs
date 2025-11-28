use std::sync::{Arc, Mutex};
use crate::types::stream_service::{ 
    AllocExtentRequest, SealExtentRequest, CloseRequest, StreamServiceClient
};
use common_proto::{
    DataNodeInfo, ExtentId, ExtentVersion, LogStreamConfig, LogStreamId,
    ExtentPlacement, ExtentPlacements, 
};
use tokio::sync::Semaphore;
use super::meta_client::ClientMetaSession;

struct PreallocatedPlaces {
    places: Mutex<anyhow::Result<Vec<ExtentPlacement>>>,
}

impl PreallocatedPlaces {
    fn new() -> Self {
        Self { places: Mutex::new(Ok(Vec::new())) }
    }

    fn take(&self) -> anyhow::Result<Vec<ExtentPlacement>> {
        match &mut *self.places.lock().unwrap() {
            Err(err) => Err(anyhow::Error::msg(err.to_string())),
            Ok(places) => Ok(std::mem::take(places)),
        }
    }
    fn update(&self, val: Vec<ExtentPlacement>) {
        assert!(!val.is_empty());
        let mut places = self.places.lock().unwrap();
        assert!(matches!(&*places, Ok(v) if v.is_empty()));
        *places = Ok(val);
    }
    fn mark_error(&self, err: anyhow::Error) {
        let mut places = self.places.lock().unwrap();
        assert!(matches!(&*places, Ok(v) if v.is_empty()));
        *places = Err(err);
    }
}

pub struct LogStream {
    id: LogStreamId,
    config: LogStreamConfig,
    version: ExtentVersion,
    session: Arc<ClientMetaSession>,
    alloc_sem: Arc<Semaphore>,
    last_extent: LastExtent,
    prealloc_places: Arc<PreallocatedPlaces>,
}

pub trait DataNodeConnector {
    fn call(self, nodes_info: Vec<DataNodeInfo>) -> impl futures::Future<Output = ()> + Send + 'static;
}

impl LogStream {
    pub(super) fn new(
        session: Arc<ClientMetaSession>, id: LogStreamId, config: LogStreamConfig, version: ExtentVersion
    ) -> Self {
        LogStream {
            session, id, config, version,
            last_extent: LastExtent::new(id, version),
            alloc_sem: Arc::new(Semaphore::new(1)),
            prealloc_places: Arc::new(PreallocatedPlaces::new())
        }
    }

    pub fn get_id(&self) -> LogStreamId {
        self.id
    }

    pub fn get_version(&self) -> ExtentVersion {
        self.version
    }

    pub fn get_config(&self) -> &LogStreamConfig {
        &self.config
    }

    pub fn last_extent(&self) -> &LastExtent {
        &self.last_extent
    }

    pub fn prepare_seal_extent(
        &mut self, log_seq: u64, used_bytes: u64,
        error_vols: Vec<ExtentPlacement>,
    ) {
        assert!(log_seq >= self.last_extent.id.log_seq);
        assert!(self.last_extent.is_writable());
        self.last_extent.next_version.log_seq = log_seq;
        self.last_extent.used_bytes = used_bytes;
        
        if !error_vols.is_empty() {
            self.last_extent.places.subtract(&error_vols);
        }
    }

    pub async fn close(self) -> anyhow::Result<()> {
        let next_log_seq = self.last_extent.next_version.log_seq;
        let seal_version = if next_log_seq == self.version.log_seq {
            ExtentVersion::new(self.version.epoch, 0)
        } else {
            ExtentVersion::new(self.version.epoch, next_log_seq - 1)
        };

        let req = CloseRequest {
            request_seq: self.session.next_request_seq(),
            stream_id: Some(self.id),
            seal_request: Some(SealExtentRequest {
                extent_version: Some(seal_version),
                extent_places: self.last_extent.places.clone(),
                used_bytes: self.last_extent.used_bytes,
            }),
        };

        let _alloc_perm = self.alloc_sem.clone().acquire_owned().await?;

        let resp = self.session.send_rpc(
            req, None,
            |mut client: StreamServiceClient<_>, req| async move {
                client.close_stream(req).await.map(|resp| resp.into_inner())
            }
        ).await?;
        println!("closed with {:?}", resp.extent_version);

        Ok(())
    }

    pub async fn alloc_extent<C>(
        &mut self, prealloc_async: bool, connector: C
    ) -> anyhow::Result<()> 
    where
        C: DataNodeConnector + Send + 'static
    {
        let next_log_seq = self.last_extent.next_version.log_seq;
        let (seal_version, next_version) = if next_log_seq == self.version.log_seq {
            (ExtentVersion::new(self.version.epoch, 0), self.version.inc_epoch())
        } else {
            (ExtentVersion::new(self.version.epoch, next_log_seq - 1),
             ExtentVersion::new(1, next_log_seq))
        };

        let req = AllocExtentRequest {
            request_seq: self.session.next_request_seq(),
            extent_id: Some(ExtentId::new(self.id, next_log_seq)),
            seal_request: Some(SealExtentRequest {
                extent_version: Some(seal_version),
                extent_places: self.last_extent.places.clone(),
                used_bytes: self.last_extent.used_bytes,
            }),
        };

        let alloc_perm = self.alloc_sem.clone().acquire_owned().await?;

        let mut places = self.prealloc_places.take()?;
        if !places.is_empty() {
            self.version = next_version;
            for place in places.iter_mut() {
                place.epoch = next_version.epoch;
            }
            self.last_extent = LastExtent::with_places(self.id, next_version, places.clone());

            let session = self.session.clone();
            let prealloc_places = self.prealloc_places.clone();
            let rpc_task = tokio::spawn(async move {
                match session.send_rpc(
                    req, None,
                    |mut client: StreamServiceClient<_>, req| async move {
                        client.alloc_extent(req).await.map(|resp| resp.into_inner())
                    }
                ).await {
                    Ok(resp) => {
                        assert!(resp.new_version.map_or(false, |v| v == next_version));
                        assert!(resp.extent_places == places);
                        prealloc_places.update(resp.prealloc_places);
                        connector.call(resp.nodes_info).await;
                    }
                    Err(err) => {
                        eprintln!("failed to alloc extent {:?}", err);
                        prealloc_places.mark_error(err);
                    }
                }
                drop(alloc_perm);
            });
            if !prealloc_async {
                let _ = rpc_task.await;
            }
        } else {
            let resp = self.session.send_rpc(
                req, None,
                |mut client: StreamServiceClient<_>, req| async move {
                    client.alloc_extent(req).await.map(|resp| resp.into_inner())
                }
            ).await?;
            assert!(resp.new_version.map_or(false, |v| v == next_version));
            self.version = next_version;
            self.last_extent = LastExtent::with_places(self.id, next_version, resp.extent_places);
            self.prealloc_places.update(resp.prealloc_places);
            connector.call(resp.nodes_info).await;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct LastExtent {
    pub id: ExtentId,
    pub next_version: ExtentVersion,
    pub places: Vec<ExtentPlacement>,
    pub used_bytes: u64,
}

impl LastExtent {
    fn new(stream_id: LogStreamId, verion: ExtentVersion) -> Self {
        Self {
            id: ExtentId::new(stream_id, verion.log_seq),
            next_version: verion,
            places: Vec::new(),
            used_bytes: 0,
        }
    }

    fn with_places(
        stream_id: LogStreamId, verion: ExtentVersion,
        places: Vec<ExtentPlacement>, 
    )  -> Self {
        Self {
            id: ExtentId::new(stream_id, verion.log_seq),
            next_version: verion,
            places,
            used_bytes: 0,
        }
    }

    pub fn is_writable(&self) -> bool {
        !self.places.is_empty()
    }
}

