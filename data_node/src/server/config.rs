use std::{ 
    path::Path, net::SocketAddr, str::FromStr, io::BufReader, marker::PhantomData,
};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;
use crate::types::common_proto::DataNodeId;

#[derive(Serialize, Deserialize, Default)]
struct DataNodeConfigImpl {
    node_id: u32,
    node_uuid: String,
    node_addr: String,
    rpc_port: u32,
    spdk_conf: String,
    reactor_mask: String,
    meta_nodes: Vec<String>,
}

impl DataNodeConfigImpl {
    fn load<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let file = std::fs::File::open(path)?;
        let conf = serde_json::from_reader(BufReader::new(file))?;
        Ok(conf)
    }

    async fn save<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        let mut file = tokio::fs::File::create(path).await?;
        let buf = serde_json::to_vec_pretty(&self)?;
        file.write_all(buf.as_slice()).await?;
        file.flush().await?;
        Ok(())
    }

    fn validate(self) -> anyhow::Result<DataNodeConfig> {
        let node_id = if (self.node_id & 0xffff) == 0 {
            DataNodeId { id: self.node_id }
        } else {
            return Err(anyhow::Error::msg("invalid node id"));
        };
        let node_uuid = if self.node_uuid.is_empty() {
            Uuid::default()
        } else {
            Uuid::from_str(&self.node_uuid)?
        };

        if self.rpc_port > u16::MAX as u32 {
            return Err(anyhow::Error::msg("invalid rpc port"));
        }

        let mut meta_nodes = Vec::new();
        for addr in self.meta_nodes.iter() {
            meta_nodes.push(addr.parse::<SocketAddr>()?);
        }

        Ok(DataNodeConfig {
            node_id, node_uuid, meta_nodes,
            node_addr: self.node_addr,
            rpc_port: self.rpc_port as u16,
            spdk_confg: self.spdk_conf,
            reactor_mask: self.reactor_mask,
            ..Default::default()
        })
    }
}

impl From<&DataNodeConfig> for DataNodeConfigImpl {
    fn from(config: &DataNodeConfig) -> Self {
        Self {
            node_id: config.node_id.id,
            node_uuid: config.node_uuid.to_string(),
            node_addr: config.node_addr.clone(),
            rpc_port: config.rpc_port as u32,
            spdk_conf: config.spdk_confg.clone(),
            reactor_mask: config.reactor_mask.clone(),
            meta_nodes: config.meta_nodes.iter().map(|addr| addr.to_string()).collect()
        }
    }
}

#[derive(Default)]
pub struct DataNodeConfig {
    pub node_id: DataNodeId,
    pub node_uuid: Uuid,
    pub node_addr: String,
    pub rpc_port: u16,
    pub spdk_confg: String,
    pub reactor_mask: String,
    pub meta_nodes: Vec<SocketAddr>,
    _marker: PhantomData<()>,
}

impl DataNodeConfig {
    pub fn load<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        DataNodeConfigImpl::load(path)?.validate()
    }

    pub async fn save<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        DataNodeConfigImpl::from(self).save(path).await
    }

    pub fn is_registered(&self) -> bool {
        self.node_id.id != 0 && !self.node_uuid.is_nil()
    }

    pub fn get_rpc_addr(&self) -> SocketAddr {
        format!("{}:{}", self.node_addr, self.rpc_port).parse().unwrap()
    }
}
