mod config;
mod extent_service;
mod client_session;
use std::{path::Path, sync::Arc, time::{Duration, Instant}};
use extent_service::LogExtentService;
use tokio::sync::oneshot;
use uuid::Uuid;
use tracing::{error, info};
use crate::{
    store::{SpdkStoreApp, SpdkStoreBuilder},
    types::extent_service::{
        ExtentServiceServer
    },
};
use common_proto::{DataNodeId, VolumeId};
use msc_meta_node::{
    MetaNode, DataNodeBeacon, client::MetaClient,
};

pub use config::DataNodeConfig;

pub struct Server {
    config: DataNodeConfig,
    meta_client: Arc<MetaClient>,
    extent_svc: Arc<LogExtentService>,
}

impl Server {
    pub async fn serve(mut self) -> anyhow::Result<()> {
        let rpc_addr = self.config.get_rpc_addr();

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let extent_svc = self.extent_svc.clone();

        tokio::spawn(async move {
            let interval = Duration::from_secs(5);
            loop {
                let next_beacon = Instant::now() + interval;
                if let Err(err) = self.send_beacon(interval).await {
                    error!("error sending beacon {:?}", err);
                }
                if shutdown_tx.is_closed() {
                    break;
                }
                tokio::time::sleep_until(next_beacon.into()).await;
            }
        });

        tonic::transport::Server::builder()
            .add_service(ExtentServiceServer::from_arc(extent_svc))
            .serve_with_shutdown(rpc_addr, async move { let _ = shutdown_rx.await; })
            .await
            .map_err(|err| {
                eprintln!("failed to start rpc service, {:?}", err);
                err
            })?;

        Ok(())
    }

    pub async fn send_beacon(&mut self, timeout: Duration) -> anyhow::Result<()> {
        let volumes = self.extent_svc.gather_volumes_stats().await;
        let beacon = DataNodeBeacon {
            node_id: Some(self.config.node_id),
            node_uuid: self.config.node_uuid.to_string(),
            rpc_addr: self.config.get_rpc_addr().to_string(),
            volumes,
        };
        info!("sending {:?}", beacon);
        self.meta_client.data_node_beacon(beacon, timeout).await?;
        Ok(())
    }

    pub async fn load_volumes(&mut self) -> anyhow::Result<()> {
        self.extent_svc.load_volumes().await?;
        Ok(())
    }

    pub async fn create_volume(&self, bdev: String) -> anyhow::Result<VolumeId> {
        let guard = self.extent_svc.prepare_create_volume(bdev).await?;
        let new_id = self.meta_client.create_volume(
            guard.get_uuid().to_string(), self.config.node_id, guard.get_capacity()
        ).await?;
        let volume_id = VolumeId { id: new_id.id };
        guard.created(volume_id).await?;
        Ok(volume_id)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        eprintln!("shutting down");
        SpdkStoreApp::shutdown();
    }
}

pub struct ServerBuilder {
    config: DataNodeConfig,
}

impl ServerBuilder {
    pub fn new(config: DataNodeConfig) -> Self {
        Self { config }
    }

    fn new_meta_client(&self) -> MetaClient {
        let nodes = self.config.meta_nodes
            .iter()
            .map(|addr| (0, MetaNode::new(addr.to_string())))
            .collect();
        MetaClient::new(nodes)
    }

    pub async fn create<P: AsRef<Path>>(&mut self, conf_path: P) -> anyhow::Result<DataNodeId> {
        if self.config.is_registered() {
            return Err(anyhow::Error::msg(
                format!("node has already been registered {:?}", self.config.node_id)
            ))
        }

        let mut client = self.new_meta_client();

        let node_uuid = Uuid::new_v4();
        let node_id = client.create_date_node(
            node_uuid.to_string(), self.config.node_addr.clone()
        ).await?;

        self.config.node_id = DataNodeId { id: node_id.id };
        self.config.node_uuid = node_uuid;
        self.config.save(conf_path).await?;
        Ok(self.config.node_id)
    }

    pub async fn build(self, runtime: tokio::runtime::Handle) -> anyhow::Result<Server> {
        if !self.config.is_registered() {
            return Err(anyhow::Error::msg("node hasn't been registered yet"));
        }

        let meta_client = Arc::new(self.new_meta_client());

        let config = self.config;
        let spdk_store = SpdkStoreBuilder::new(config.node_id, config.node_uuid)
            .app_name(&format!("dnode-{}", config.node_id.id))
            .config_file(&config.spdk_confg)
            .reactor_mask(&config.reactor_mask)
            .build(runtime)
            .await?;
        let extent_svc = LogExtentService::new(spdk_store);
        Ok(Server { 
            config, meta_client, extent_svc
        })
    }
}
