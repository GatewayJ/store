
use std::sync::Arc;
use rocksdb::DB as RocksDB;
use super::{
    meta_service::MetaRequestGuard,
    client_session::ClientSession,
    data_node::{DataNodeManager, SpaceAllocParams},
    extent::ExtentCache,
    log_stream::{ PreparedOpen, PreparedUpdate, LogStreamCache},
    meta_cache::{CacheEntry, CacheError},
    recovery::RecoveryManager,
};
use crate::{
    store::StoreRequest,
    types::stream_service::{
        CreateRequest, CreateResponse, OpenRequest, OpenResponse,
        CloseRequest, CloseResponse, AllocExtentRequest, AllocExtentResponse,
        StatExtentRequest, StatExtentResponse,
    }
};
use common_proto::{ExtentId, ExtentPlacement, ExtentPlacements, ExtentVersion};

pub(crate) struct LogStreamService {
    log_streams: LogStreamCache,
    extents: ExtentCache,
    recovery_mgr: Arc<RecoveryManager>,
}

impl LogStreamService {
    pub fn new(db: Arc<RocksDB>, recovery_mgr: Arc<RecoveryManager>) -> Self {
        let log_streams = LogStreamCache::new(db.clone(), 100000);
        let extents = ExtentCache::new(db.clone(), 1000000);
        Self {
            log_streams, extents, recovery_mgr
        }
    }

    pub(super) fn update_raft_term(&self, term: u64) {
        self.log_streams.set_raft_term(term);
        self.extents.set_raft_term(term);
    }

    pub(super) async fn invalidate_all(&self) {
        let _ = tokio::join!(
            self.log_streams.invalidate_all(),
            self.extents.invalidate_all()
        );
    }

    pub(super) async fn reload(&self) -> anyhow::Result<()> {
        self.log_streams.reload().await
    }

    pub async fn handle_create_stream(
        &self, req: CreateRequest,
        session: &ClientSession, guard: &mut MetaRequestGuard<'_>,
    ) -> Result<CreateResponse, tonic::Status> 
    {
        let config = req.config.ok_or_else(|| {
            tonic::Status::invalid_argument("no config")
        })?;

        if let Some(stream_id) = session.check_created_stream(req.request_seq) {
            let open_req = OpenRequest {
                request_seq: req.request_seq,
                stream_id: Some(stream_id),
            };
            let res = self.handle_open_stream(open_req, session, guard).await?;
            return Ok(CreateResponse {
                stream_id: Some(stream_id),
                extent_version: res.extent_version,
            })
        }
        
        let log_stream = self.log_streams.create()
            .await
            .map_err(|err| tonic::Status::internal(err.to_string()))?;

        let extent = match self.extents.create(ExtentId::new(log_stream.id, 1)).await {
            Ok(extent) => extent,
            Err(err) => {
                log_stream.finish_update(Err(CacheError::Invalidated));
                return Err(tonic::Status::internal(err.to_string()));
            }
        };

        let log_stream = guard.add_locked_stream(log_stream);
        let extent = guard.add_locked_extent(extent);

        let mut store_req = StoreRequest::default();

        let (epoch, _) = extent.update(&mut store_req, |ext_inner| {
            ext_inner.reset(Vec::new())
        });
        
        let extent_version = ExtentVersion::new(epoch, extent.id.log_seq);
        log_stream.update(&mut store_req, |lst_inner| {
            lst_inner.init_new(&config, &extent_version, session.id, req.request_seq);
        });

        let stream_id = log_stream.id;
        session.open_stream(stream_id, Some(req.request_seq), &mut store_req);

        guard.client_write(store_req).await?;

        Ok(CreateResponse{
            stream_id: Some(stream_id),
            extent_version: Some(extent_version),
        })
    }

    pub async fn handle_open_stream(
        &self, req: OpenRequest,
        session: &ClientSession, guard: &mut MetaRequestGuard<'_>,
    ) -> Result<OpenResponse, tonic::Status> {
        let stream_id = req.stream_id.ok_or_else(|| {
            tonic::Status::invalid_argument("no stream id")
        })?;

        let log_stream = self.log_streams.get(stream_id)
            .await
            .map_err(|err| tonic::Status::internal(err.to_string()))?;

        let prep_res = log_stream.prepare_open(
            session.id, req.request_seq
        ).await?;
        let log_stream = guard.add_locked_stream(log_stream);

        let (config, version) = match prep_res {
            PreparedOpen::Duplicated((config, version)) => {
                return Ok(OpenResponse::new(version, config));
            }
            PreparedOpen::Ready(res) => res,
        };

        let mut store_req = StoreRequest::default();
        log_stream.update(&mut store_req, |lst_inner| {
            lst_inner.open(session.id, req.request_seq);
        });

        session.open_stream(log_stream.id, None, &mut store_req);

        guard.client_write(store_req).await?;

        Ok(OpenResponse::new(version, config))
    }

    pub async fn handle_close_stream(
        &self, req: CloseRequest,
        session: &ClientSession, guard: &mut MetaRequestGuard<'_>,
    ) -> Result<CloseResponse, tonic::Status> {
        let stream_id = req.stream_id.ok_or_else(|| {
            tonic::Status::invalid_argument("no stream id")
        })?;
        let seal_request = req.seal_request.ok_or_else(|| {
            tonic::Status::invalid_argument("no seal request")
        })?;

        let log_stream = self.log_streams.get(stream_id)
            .await
            .map_err(|err| tonic::Status::internal(err.to_string()))?;
        
        let prep_res = log_stream.prepare_close(
            session.id, req.request_seq, &seal_request
        ).await?;
        let log_stream = guard.add_locked_stream(log_stream);

        let (config, mut prealloc_places, prev_extent_id) = match prep_res {
            PreparedUpdate::Duplicated(version) => {
                return Ok(CloseResponse::new(version));
            }
            PreparedUpdate::Ready(res) => res,
        };

        let seal_version = seal_request.extent_version.unwrap();

        let mut prev_extent = match self.extents.get(prev_extent_id).await {
            Err(err) => {
                return Err(tonic::Status::internal(err.to_string()));
            }
            Ok(extent) => {
                let _ = extent.prepare_update(|_| Ok::<_, ()>(())).await;
                Some(guard.add_locked_extent(extent))
            }
        };

        let extent = if seal_version.log_seq == 0 {
            prev_extent.take().unwrap()
        } else {
            let extent_id = ExtentId::new(stream_id, seal_version.log_seq + 1);
            match self.extents.create(extent_id).await {
                Err(err) => {
                    return Err(tonic::Status::internal(err.to_string()));
                }
                Ok(extent) => {
                    guard.add_locked_extent(extent)
                }
            }
        };

        let mut store_req = StoreRequest::default();

        if let Some(prev_extent) = &prev_extent {
            let unsealed = prev_extent.update(&mut store_req, |ext_inner| {
                ext_inner.mark_sealed(seal_version, &seal_request.extent_places)
            });
            if !unsealed.is_empty() {
                self.recovery_mgr.add_unsealed_extent(
                    &mut store_req, unsealed, prev_extent_id
                )
            }
        }

        let (epoch, mut freed_places) = extent.update(&mut store_req, |ext_inner| {
            ext_inner.reset(Vec::new())
        });

        if !freed_places.is_empty() {
            freed_places.subtract(&seal_request.extent_places);
            self.recovery_mgr.add_stale_extent(&mut store_req, freed_places, extent.id);

            freed_places = seal_request.extent_places;
            freed_places.append(&mut prealloc_places);
        } else {
            freed_places = prealloc_places;
        }
        if !freed_places.is_empty() {
            guard.free_spaces(&mut store_req, freed_places, &config)
        }

        let extent_version = ExtentVersion::new(epoch, extent.id.log_seq);
        log_stream.update(&mut store_req, |lst_inner| {
            lst_inner.close(req.request_seq, &extent_version);
        });

        session.close_stream(log_stream.id, &mut store_req);

        guard.client_write(store_req).await?;
        
        Ok(CloseResponse::new(extent_version))
    }

    pub async fn handle_alloc_extent(
        &self, req: AllocExtentRequest,
        session: &ClientSession, guard: &mut MetaRequestGuard<'_>,
        data_nodes: &DataNodeManager, 
    ) -> Result<AllocExtentResponse, tonic::Status> {
        let extent_id = req.extent_id.ok_or_else(|| {
            tonic::Status::invalid_argument("no extent id")
        })?;
        let seal_request = req.seal_request.ok_or_else(|| {
            tonic::Status::invalid_argument("no seal request")
        })?;

        let log_stream = self.log_streams.get(extent_id.stream_id())
            .await
            .map_err(|err| tonic::Status::internal(err.to_string()))?;

        let prep_res = log_stream.prepare_new_extent(
            session.id, req.request_seq, &extent_id, &seal_request
        ).await?;
        let log_stream = guard.add_locked_stream(log_stream);
        
        let (config, mut extent_places, prev_extent_id) = match prep_res {
            PreparedUpdate::Duplicated((prealloc_places, last_extent_id)) => {
                let last_extent = self.extents.get(last_extent_id)
                    .await
                    .map_err(|err| tonic::Status::internal(err.to_string()))?;
                let stats = last_extent.stat();
                let new_version = stats.version.map(
                    |v| ExtentVersion::new(v.epoch, last_extent_id.log_seq)
                );
                return Ok(AllocExtentResponse{
                    new_version,
                    extent_places: stats.places,
                    prealloc_places,
                    nodes_info: stats.nodes_info,
                })
            }
            PreparedUpdate::Ready(res) => res,
        };

        let (mut prev_extent, mut next_epoch) = match self.extents.get(prev_extent_id).await {
            Err(err) => {
                return Err(tonic::Status::internal(err.to_string()));
            }
            Ok(extent) => {
                let epoch = extent.prepare_update(|ext_inner| {
                    Ok::<_, ()>(ext_inner.next_epoch())
                }).await;
                (Some(guard.add_locked_extent(extent)), epoch.unwrap())
            }
        };

        let spaces = match data_nodes.alloc_spaces(SpaceAllocParams {
            extent_size: config.extent_size,
            num_replicas: config.num_replicas as usize,
            num_extents: if extent_places.is_empty() { 2 } else { 1 },
        }).await {
            Err(err) => {
                return Err(tonic::Status::internal(err.to_string()));
            }
            Ok(spaces) => spaces,
        };

        let extent = if prev_extent.as_ref().map_or(false, |ext| ext.id == extent_id) {
            prev_extent.take().unwrap()
        } else {
            match self.extents.create(extent_id).await {
                Err(err) => {
                    data_nodes.free_spaces(spaces.volumes, spaces.stats);
                    log_stream.finish_update(Ok(()));
                    return Err(tonic::Status::internal(err.to_string()));
                }
                Ok(extent) => {
                    next_epoch = 1;
                    guard.add_locked_extent(extent)
                }
            }
        };

        let mut store_req = StoreRequest::default();

        if let Some(prev_extent) = prev_extent {
            let unsealed = prev_extent.update(&mut store_req, |ext_inner| {
                ext_inner.mark_sealed(
                    seal_request.extent_version.unwrap(),
                    &seal_request.extent_places,
                )
            });
            if !unsealed.is_empty() {
                self.recovery_mgr.add_unsealed_extent(
                    &mut store_req, unsealed, prev_extent_id
                );
            }
        }

        for vol in spaces.volumes.iter() {
            spaces.stats.merge(vol, &mut store_req);
        }

        let prealloc_places = if !extent_places.is_empty() {
            spaces.volumes
                .iter()
                .map(|v| ExtentPlacement::new(*v, 0))
                .collect::<Vec<_>>()
        } else {
            extent_places = spaces.volumes
                .iter()
                .map(|v| ExtentPlacement::new(*v, 0))
                .collect::<Vec<_>>();
            extent_places.split_off(config.num_replicas as usize)
        };
        for place in extent_places.iter_mut() {
            place.epoch = next_epoch;
        }

        let (epoch, mut freed_places) = extent.update(&mut store_req, |ext_inner| {
            assert!(!extent_places.is_empty());
            ext_inner.reset(extent_places.clone())
        });
        assert!(next_epoch == epoch);

        if !freed_places.is_empty() {
            freed_places.subtract(&seal_request.extent_places);
            self.recovery_mgr.add_stale_extent(&mut store_req, freed_places, extent_id);
            guard.free_spaces(&mut store_req, seal_request.extent_places, &config);
        }

        let extent_version = ExtentVersion::new(epoch, extent.id.log_seq);
        log_stream.update(&mut store_req, |lst_inner| {
            lst_inner.update(&extent_version, &prealloc_places, extent);
        });

        guard.client_write(store_req).await?;

        Ok(AllocExtentResponse{
            new_version: Some(extent_version),
            extent_places,
            prealloc_places,
            nodes_info: spaces.nodes_info
        })
    }

    pub async fn handle_stat_extent(
        &self, req: StatExtentRequest,
    ) -> Result<StatExtentResponse, tonic::Status> {
        let extent_id = req.extent_id.ok_or_else(|| {
            tonic::Status::invalid_argument("no extent id")
        })?;
        
        match self.extents.get(extent_id).await {
            Ok(extent) => {
                Ok(StatExtentResponse {
                    stats: Some(extent.stat())
                })
            }
            Err(err) => {
                if matches!(err, CacheError::MetaNotFound) {
                    Ok(StatExtentResponse::default())
                } else {
                    Err(tonic::Status::internal(err.to_string()))
                }
            }
        }
    }
}



