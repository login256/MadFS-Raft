//! ChiselStore RPC module Via MadSim.

use crate::rpc_struct;
use crate::rpc_struct::*;
use crate::{Consistency, StoreCommand, StoreServer, StoreTransport};
use async_trait::async_trait;
use derivative::Derivative;
use little_raft::message::Message;
use madsim::net::NetLocalHandle;
use madsim::net::rpc::Request;
use std::net::SocketAddr;
use std::sync::Arc;

type NodeAddrFn = dyn Fn(usize) -> SocketAddr + Send + Sync;

/// RPC transport.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct RpcTransport {
    /// Node address mapping function.
    #[derivative(Debug = "ignore")]
    node_addr: Box<NodeAddrFn>,
}

pub async fn call<R: Request>(peer: SocketAddr, request: R) {
    NetLocalHandle::current().call(peer, request).await;
}

impl RpcTransport {
    /// Creates a new RPC transport.
    pub fn new(node_addr: Box<NodeAddrFn>) -> Self {
        RpcTransport { node_addr }
    }
}

#[async_trait]
impl StoreTransport for RpcTransport {
    fn send(&self, to_id: usize, msg: Message<StoreCommand>) {
        match msg {
            Message::AppendEntryRequest {
                from_id,
                term,
                prev_log_index,
                prev_log_term,
                entries,
                commit_index,
            } => {
                let from_id = from_id as u64;
                let term = term as u64;
                let prev_log_index = prev_log_index as u64;
                let prev_log_term = prev_log_term as u64;
                let entries = entries
                    .iter()
                    .map(|entry| {
                        let id = entry.transition.id as u64;
                        let index = entry.index as u64;
                        let sql = entry.transition.sql.clone().as_bytes().to_vec();
                        let term = entry.term as u64;
                        LogEntry {
                            id,
                            sql,
                            index,
                            term,
                        }
                    })
                    .collect();
                let commit_index = commit_index as u64;
                let request = AppendEntriesRequest {
                    from_id,
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    commit_index,
                };
                let peer = (self.node_addr)(to_id);
                madsim::task::spawn(call(peer, request));
            }
            Message::AppendEntryResponse {
                from_id,
                term,
                success,
                last_index,
                mismatch_index,
            } => {
                let from_id = from_id as u64;
                let term = term as u64;
                let last_index = last_index as u64;
                let mismatch_index = mismatch_index.map(|idx| idx as u64);
                let request = AppendEntriesResponse {
                    from_id,
                    term,
                    success,
                    last_index,
                    mismatch_index,
                };
                let peer = (self.node_addr)(to_id);
                madsim::task::spawn(call(peer, request));
            }
            Message::VoteRequest {
                from_id,
                term,
                last_log_index,
                last_log_term,
            } => {
                let from_id = from_id as u64;
                let term = term as u64;
                let last_log_index = last_log_index as u64;
                let last_log_term = last_log_term as u64;
                let request = VoteRequest {
                    from_id,
                    term,
                    last_log_index,
                    last_log_term,
                };
                let peer = (self.node_addr)(to_id);
                madsim::task::spawn(call(peer, request));
            }
            Message::VoteResponse {
                from_id,
                term,
                vote_granted,
            } => {
                let peer = (self.node_addr)(to_id);
                let from_id = from_id as u64;
                let term = term as u64;
                let request = VoteResponse {
                    from_id,
                    term,
                    vote_granted,
                };
                madsim::task::spawn(call(peer, request));
            }
        }
    }

    async fn delegate(
        &self,
        to_id: usize,
        sql: String,
        consistency: Consistency,
    ) -> std::result::Result<crate::server::QueryResults, crate::StoreError> {
        let addr = (self.node_addr)(to_id);
        let consistency = match consistency {
            Consistency::Strong => rpc_struct::Consistency::STRONG,
            Consistency::RelaxedReads => rpc_struct::Consistency::RELAXED_READS,
        };
        let request = rpc_struct::Query {
            sql: sql.as_bytes().to_vec(),
            consistency: consistency,
        };
        let response = NetLocalHandle::current()
            .call(addr, request)
            .await
            .unwrap()?;
        let mut rows = vec![];
        for row in response.rows {
            rows.push(crate::server::QueryRow { values: row.values });
        }
        Ok(crate::server::QueryResults { rows })
    }
}

/// RPC service.
#[derive(Debug, Clone)]
pub struct RpcService {
    /// The ChiselStore server access via this RPC service.
    pub server: Arc<StoreServer<RpcTransport>>,
}

#[madsim::service]
impl RpcService {
    /// Creates a new RPC service.
    pub fn new(server: Arc<StoreServer<RpcTransport>>) -> Self {
        let me = Self { server };
        let srv = me.clone();
        srv.add_rpc_handler();
        me
    }

    #[rpc]
    async fn append_entry_request(&self, req: AppendEntriesRequest) -> Result<()> {
        let msg = req;
        let from_id = msg.from_id as usize;
        let term = msg.term as usize;
        let prev_log_index = msg.prev_log_index as usize;
        let prev_log_term = msg.prev_log_term as usize;
        let entries: Vec<little_raft::message::LogEntry<StoreCommand>> = msg
            .entries
            .iter()
            .map(|entry| {
                let id = entry.id as usize;
                let sql = String::from_utf8(entry.sql.clone()).unwrap();
                let transition = StoreCommand { id, sql };
                let index = entry.index as usize;
                let term = entry.term as usize;
                little_raft::message::LogEntry {
                    transition,
                    index,
                    term,
                }
            })
            .collect();
        let commit_index = msg.commit_index as usize;
        let msg = little_raft::message::Message::AppendEntryRequest {
            from_id,
            term,
            prev_log_index,
            prev_log_term,
            entries,
            commit_index,
        };
        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(())
    }

    #[rpc]
    async fn append_entry_response(&self, request: AppendEntriesResponse) -> Result<()> {
        let msg = request;
        let from_id = msg.from_id as usize;
        let term = msg.term as usize;
        let success = msg.success;
        let last_index = msg.last_index as usize;
        let mismatch_index = msg.mismatch_index.map(|idx| idx as usize);
        let msg = little_raft::message::Message::AppendEntryResponse {
            from_id,
            term,
            success,
            last_index,
            mismatch_index,
        };
        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(())
    }

    #[rpc]
    async fn vote(&self, request: VoteRequest) -> Result<()> {
        let msg = request;
        let from_id = msg.from_id as usize;
        let term = msg.term as usize;
        let last_log_index = msg.last_log_index as usize;
        let last_log_term = msg.last_log_term as usize;
        let msg = little_raft::message::Message::VoteRequest {
            from_id,
            term,
            last_log_index,
            last_log_term,
        };
        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(())
    }

    #[rpc]
    async fn respond_to_vote(&self, request: VoteResponse) -> Result<()> {
        let msg = request;
        let from_id = msg.from_id as usize;
        let term = msg.term as usize;
        let vote_granted = msg.vote_granted;
        let msg = little_raft::message::Message::VoteResponse {
            from_id,
            term,
            vote_granted,
        };
        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(())
    }

    #[rpc]
    async fn execute(
        &self,
        request: Query,
    ) -> std::result::Result<QueryResults, crate::StoreError> {
        let query = request;
        let consistency = match query.consistency {
            rpc_struct::Consistency::STRONG => Consistency::Strong,
            rpc_struct::Consistency::RELAXED_READS => Consistency::RelaxedReads,
        };
        let sql = String::from_utf8(query.sql).unwrap();
        let server = self.server.clone();
        let results = match server.query(sql, consistency).await {
            Ok(results) => results,
            Err(e) => return Err(e),
        };
        let mut rows = vec![];
        for row in results.rows {
            rows.push(QueryRow { values: row.values })
        }
        Ok(QueryResults { rows })
    }
}
