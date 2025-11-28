use std::{sync::{Arc, atomic::AtomicU64}, vec::Vec};
use msc_meta_node::{
    MetaNode, client::MetaClient
};
use common_proto::{LogStreamConfig, LogStreamId, ExtentId, ExtentPlacement};
use super::{log_stream::LogStream, data_session::DataSessionManager};
use quick_cache::sync::Cache as QuickCache;

pub struct LogClientBuilder {
    meta_client: MetaClient,
    extent_places_caps: usize,
}

impl LogClientBuilder {
    pub fn new(nodes: Vec<MetaNode>) -> Self {
        let meta_client = MetaClient::new(
            nodes.into_iter().map(|n| (0, n)).collect()
        );
        Self {
            meta_client,
            extent_places_caps: 10000,
        }
    }
    pub fn extent_places_cache_size(self, caps: usize) -> Self {
        Self {
            extent_places_caps: caps,
            ..self
        }
    }
    pub async fn build(self) -> anyhow::Result<LogClient> {
        let mut meta_client = self.meta_client;
        meta_client.open_session().await?;

        Ok(LogClient { 
            meta_client,
            data_sessions: DataSessionManager::new(),
            extent_places: Arc::new(QuickCache::new(self.extent_places_caps)),
            last_request_seq: Arc::new(AtomicU64::new(1)),
        })
    }
}

pub struct LogClient {
    meta_client: MetaClient,
    data_sessions: DataSessionManager,
    extent_places: Arc<QuickCache<ExtentId, Vec<ExtentPlacement>>>,
    last_request_seq: Arc<AtomicU64>
}

impl LogClient {
    pub async fn open_stream(&mut self, id: LogStreamId) -> anyhow::Result<LogStream> {
        let log_stream = self.meta_client.open_stream(id).await?;
        Ok(LogStream::new(
            log_stream,
            self.data_sessions.clone(),
            self.last_request_seq.clone()
        ))
    }

    pub async fn create_stream(&mut self, config: LogStreamConfig) -> anyhow::Result<LogStream> {
        let log_stream = self.meta_client.create_stream(config).await?;
        Ok(LogStream::new(
            log_stream,
            self.data_sessions.clone(),
            self.last_request_seq.clone()
        ))
    }

    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        self.meta_client.close_session().await?;
        Ok(())
    }
}
