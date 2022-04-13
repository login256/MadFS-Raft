//! ChiselStore RPC module Via MadSim.

use crate::rpc_struct::*;
use crate::{Consistency, StoreCommand, StoreServer, StoreTransport};
use async_mutex::Mutex;
use async_trait::async_trait;
use crossbeam::queue::ArrayQueue;
use derivative::Derivative;
use futures::join;
use little_raft::message::Message;
use madsim::net::NetLocalHandle;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

type NodeAddrFn = dyn Fn(usize) -> String + Send + Sync;

/// RPC transport.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct RpcTransport {
    /// Node address mapping function.
    #[derivative(Debug = "ignore")]
    node_addr: Box<NodeAddrFn>,
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
                        let sql = entry.transition.sql.clone();
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
                NetLocalHandle::current().call(peer, request).await?;
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
                NetLocalHandle::current().call(peer, request).await?;
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
                NetLocalHandle::current().call(peer, request).await?;
            }
            Message::VoteResponse {
                from_id,
                term,
                vote_granted,
            } => {
                let peer = (self.node_addr)(to_id);
                let from_id = from_id as u64;
                let term = term as u64;
                let response = VoteResponse {
                    from_id,
                    term,
                    vote_granted,
                };
                NetLocalHandle::current().call(peer, request).await?;ß
            }
        }
    }

    async fn delegate(
        &self,
        to_id: usize,
        sql: String,
        consistency: Consistency,
    ) -> Result<crate::server::QueryResults, crate::StoreError> {
        let addr = (self.node_addr)(to_id);
        let mut client = self.connections.connection(addr.clone()).await;
        let query = tonic::Request::new(Query {
            sql,
            consistency: consistency as i32,
        });
        let response = client.conn.execute(query).await.unwrap();
        let response = response.into_inner();
        let mut rows = vec![];
        for row in response.rows {
            rows.push(crate::server::QueryRow { values: row.values });
        }
        Ok(crate::server::QueryResults { rows })
    }
}

/// RPC service.
#[derive(Debug)]
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
        madsim::task::spawn(async move {
            let mut started = false;
            loop {
                if srv.ctl.self_info().is_in() && !started {
                    started = true;
                    srv.add_rpc_handler();
                }

                let prev_map = srv.ctl.current_map();
                srv.peers.update(prev_map.as_ref());
                let map = srv.ctl.wait_map(prev_map.version).await;
                if map.version.major > 1 {
                    panic!("don't support add/remove server dynamically");
                }
            }
        })
        .detach();
        let srv = me.clone();
        madsim::task::spawn(async move {
            // start recovery in background
            let txns = srv.kv.recovery().await;
            for (id, commit) in txns {
                srv.ended_txns.lock().unwrap().insert(id, (commit, None));
            }
            madsim::time::sleep(Duration::from_secs(10)).await;
            join!(srv.txn_background(), async {
                loop {
                    srv.kv.purge_deleted();
                    madsim::time::sleep(Duration::from_secs(10)).await;
                }
            },)
        })
        .detach();
        me
    }
}
