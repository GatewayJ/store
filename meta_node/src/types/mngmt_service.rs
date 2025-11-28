use std::collections::{BTreeMap, BTreeSet};
use crate::types::{Node, NodeId, RaftChangeMembers};

tonic::include_proto!("mngmt_service");

pub use mngmt_service_server::MngmtServiceServer;
pub use mngmt_service_client::MngmtServiceClient;
pub use raft_mngmt_request::{
    Flags as RaftMngmtRequestFlags,
    Op as RaftMngmtRequestOp,
};

impl From<(NodeId, Node)> for RaftNode {
    fn from(node: (NodeId, Node)) -> Self {
        Self {
            id: node.0,
            rpc_addr: node.1.rpc_addr,
        }
    }
}

impl From<RaftChangeMembers> for RaftMngmtRequest {
    fn from(members: RaftChangeMembers) -> Self {
        match members {
            RaftChangeMembers::AddVoterIds(ids) => {
                Self {
                    opcode: RaftMngmtRequestOp::AddVoters as i32,
                    flags: 0,
                    ids: ids.into_iter().collect(),
                    nodes: Vec::new(),
                }
            }
            RaftChangeMembers::AddVoters(nodes) => {
                Self {
                    opcode: RaftMngmtRequestOp::AddVoters as i32,
                    flags: 0,
                    ids: Vec::new(),
                    nodes: nodes.into_iter().map(|n| n.into()).collect(),
                }
            }
            RaftChangeMembers::RemoveVoters(ids) => {
                Self {
                    opcode: RaftMngmtRequestOp::RemoveVoters as i32,
                    flags: 0,
                    ids: ids.into_iter().collect(),
                    nodes: Vec::new(),
                }
            }
            RaftChangeMembers::ReplaceAllVoters(ids) =>  {
                Self {
                    opcode: RaftMngmtRequestOp::ReplaceAllVoters as i32,
                    flags: 0,
                    ids: ids.into_iter().collect(),
                    nodes: Vec::new(),
                }
            }
            RaftChangeMembers::AddNodes(nodes) => {
                Self {
                    opcode: RaftMngmtRequestOp::AddNodes as i32,
                    flags: 0,
                    ids: Vec::new(),
                    nodes: nodes.into_iter().map(|n| n.into()).collect(),
                }
            }
            RaftChangeMembers::SetNodes(nodes) => {
                Self {
                    opcode: RaftMngmtRequestOp::SetNodes as i32,
                    flags: 0,
                    ids: Vec::new(),
                    nodes: nodes.into_iter().map(|n| n.into()).collect(),
                }
            }
            RaftChangeMembers::RemoveNodes(ids) => {
                Self {
                    opcode: RaftMngmtRequestOp::RemoveNodes as i32,
                    flags: 0,
                    ids: ids.into_iter().collect(),
                    nodes: Vec::new(),
                }
            }
            RaftChangeMembers::ReplaceAllNodes(nodes) => {
                Self {
                    opcode: RaftMngmtRequestOp::ReplaceAllNodes as i32,
                    flags: 0,
                    ids: Vec::new(),
                    nodes: nodes.into_iter().map(|n| n.into()).collect(),
                }
            }
        }
    }
}

impl TryFrom<RaftMngmtRequest> for RaftChangeMembers {
    type Error = ();
    fn try_from(req: RaftMngmtRequest) -> Result<Self, Self::Error> {
        let ids: BTreeSet<_> = req.ids.into_iter().collect();
        let nodes: BTreeMap<_, _> = req.nodes.into_iter()
            .map(|n| (n.id, Node{ rpc_addr: n.rpc_addr }))
            .collect();
        match req.opcode {
            op if op == RaftMngmtRequestOp::AddVoters as i32 => {
                if nodes.is_empty() {
                    Ok(RaftChangeMembers::AddVoterIds(ids))
                } else {
                    Ok(RaftChangeMembers::AddVoters(nodes))
                }
            }
            op if op == RaftMngmtRequestOp::RemoveVoters as i32 => {
                Ok(RaftChangeMembers::RemoveVoters(ids))
            }
            op if op == RaftMngmtRequestOp::ReplaceAllVoters as i32 => {
                Ok(RaftChangeMembers::ReplaceAllVoters(ids))
            }
            op if op == RaftMngmtRequestOp::AddNodes as i32 => {
                Ok(RaftChangeMembers::AddNodes(nodes))
            }
            op if op == RaftMngmtRequestOp::SetNodes as i32 => {
                Ok(RaftChangeMembers::SetNodes(nodes))
            }
            op if op == RaftMngmtRequestOp::RemoveNodes as i32 => {
                Ok(RaftChangeMembers::RemoveNodes(ids))
            }
            op if op == RaftMngmtRequestOp::ReplaceAllNodes as i32 => {
                Ok(RaftChangeMembers::ReplaceAllNodes(nodes))
            }
            _ => {
                Err(())
            }
        }
    }
}

impl RaftMngmtRequest {
    pub fn new() -> Self {
        Self {
            opcode: RaftMngmtRequestOp::Unspecified.into(),
            flags: 0,
            ids: Vec::new(),
            nodes: Vec::new(),
        }
    }
    pub fn add_voters(nodes: &[RaftNode]) -> Self {
        Self {
            opcode: RaftMngmtRequestOp::AddVoters.into(),
            flags: 0,
            ids: Vec::new(),
            nodes: nodes.to_vec()
        }
    }
    pub fn remove_voters(ids: &[u64]) -> Self {
        Self {
            opcode: RaftMngmtRequestOp::RemoveVoters.into(),
            flags: 0,
            ids: ids.to_vec(),
            nodes: Vec::new(),
        }
    }
    pub fn replace_all_voters(ids: &[u64]) -> Self {
        Self {
            opcode: RaftMngmtRequestOp::ReplaceAllVoters.into(),
            flags: 0,
            ids: ids.to_vec(),
            nodes: Vec::new(),
        }
    }
    pub fn add_nodes(nodes: &[RaftNode]) -> Self {
        Self {
            opcode: RaftMngmtRequestOp::AddNodes.into(),
            flags: 0,
            ids: Vec::new(),
            nodes: nodes.to_vec()
        }
    }
    pub fn set_nodes(nodes: &[RaftNode]) -> Self {
        Self {
            opcode: RaftMngmtRequestOp::SetNodes.into(),
            flags: 0,
            ids: Vec::new(),
            nodes: nodes.to_vec()
        }
    }
    pub fn remove_nodes(ids: &[u64]) -> Self {
        Self {
            opcode: RaftMngmtRequestOp::RemoveNodes.into(),
            flags: 0,
            ids: ids.to_vec(),
            nodes: Vec::new(),
        }
    }
    pub fn replace_all_nodes(nodes: &[RaftNode]) -> Self {
        Self {
            opcode: RaftMngmtRequestOp::SetNodes.into(),
            flags: 0,
            ids: Vec::new(),
            nodes: nodes.to_vec()
        }
    }
    pub fn set_flags(&mut self, flags: u32) {
        self.flags |= flags;
    }
}

impl RaftMngmtResponse {
    pub fn new(data: String) -> Self {
        Self { data }
    }
}
