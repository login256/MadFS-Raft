use crate::rpc::proto::rpc_client::RpcClient;
use crate::rpc::proto::rpc_server::RpcServer;
use crate::rpc::proto::{Consistency, Query};
use crate::{
    rpc::{RpcService, RpcTransport},
    StoreServer,
};
use log::info;
use log::{debug, trace};
use log_derive::{logfn, logfn_inputs};
use madsim::net::NetSim;
use madsim::plugin::simulator;
use madsim::task::NodeId;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tonic::transport::Server;

use madsim::runtime::{Handle, NodeHandle};

use madsim::time::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;

/// Node authority (host and port) in the cluster.
fn node_authority(id: usize) -> (String, u16) {
    let host = "10.2.3.".to_string() + id.to_string().as_str();
    let port = 50001;
    (host, port)
}

/// Node RPC address in cluster.
fn node_rpc_addr(id: usize) -> String {
    let (host, port) = node_authority(id);
    format!("http://{}:{}", host, port)
}

fn rpc_listen_addr(id: usize) -> String {
    let (host, port) = node_authority(id);
    format!("{}:{}", host, port)
}

#[derive(Clone)]
struct MadRaft {
    pub id: usize,
    pub peers: Vec<usize>,
    pub server: Arc<StoreServer<RpcTransport>>,
}

impl MadRaft {
    pub async fn build(id: usize, peers: Vec<usize>, path: Option<PathBuf>) -> Self {
        let transport = RpcTransport::new(Box::new(node_rpc_addr));
        let server = StoreServer::start(id, peers.clone(), transport, path)
            .await
            .unwrap();
        let server = Arc::new(server);
        MadRaft { id, peers, server }
    }

    pub fn get_rpc_service(&self) -> RpcService {
        RpcService::new(self.server.clone())
    }

    pub async fn run(&self) {
        let addr = rpc_listen_addr(self.id).parse::<SocketAddr>().unwrap();
        let rpc = self.get_rpc_service();
        let g = {
            let id = self.id;
            tokio::task::spawn(async move {
                info!("RPC listening to {} ...", rpc_listen_addr(id));
                let ret = Server::builder()
                    .add_service(RpcServer::new(rpc))
                    .serve(addr)
                    .await;
                ret
            })
        };
        let f = {
            let server = self.server.clone();
            tokio::task::spawn(async move {
                server.run().await;
            })
        };
        let results = tokio::try_join!(f, g).unwrap();
        results.1.unwrap();
    }
}

pub struct Tester {
    handle: Handle,
    temp_path: PathBuf,
    n: usize,
    ids: Vec<usize>,
    node_ids: Vec<NodeId>,
    client_nodeid: Mutex<HashMap<usize, NodeId>>,
    servers: Mutex<Vec<Option<Arc<MadRaft>>>>,
    next_client_id: AtomicUsize,
    maxraftstate: Option<usize>,

    // begin()/end() statistics
    t0: Instant,
    // rpc_total() at start of test
    rpcs0: u64,
    // number of agreements
    ops: Arc<AtomicUsize>,
}

impl Tester {
    pub async fn new(n: usize, unreliable: bool, maxraftstate: Option<usize>) -> Tester {
        info!("Create Raft. n:{n}, unreliable:{unreliable}");
        let handle = Handle::current();
        let temp_dir = tempfile::TempDir::new_in("./test_data").unwrap();
        let temp_path = temp_dir.into_path(); //temp_dir.path().to_path_buf();
        info!("{:?}", temp_path);
        if unreliable {
            madsim::plugin::simulator::<madsim::net::NetSim>().update_config(|cfg| {
                cfg.packet_loss_rate = 0.1;
                cfg.send_latency = Duration::from_millis(1)..Duration::from_millis(27);
            });
        }
        let mut servers = vec![];
        servers.resize_with(n, || None);
        let mut tester = Tester {
            handle,
            temp_path,
            n,
            ids: (1..=n).collect(),
            node_ids: vec![],
            client_nodeid: Default::default(),
            servers: Mutex::new(servers),
            next_client_id: AtomicUsize::new(0),
            maxraftstate,
            t0: Instant::now(),
            rpcs0: 0,
            ops: Arc::new(AtomicUsize::new(0)),
        };
        tester.rpcs0 = tester.rpc_total();
        // create a full set of KV servers.
        for i in 0..n {
            let node = tester
                .handle
                .create_node()
                .name(format!("server{}", tester.ids[i]))
                .ip(rpc_listen_addr(tester.ids[i])
                    .parse::<SocketAddr>()
                    .unwrap()
                    .ip())
                .build();
            tester.node_ids.push(node.id());
            tester.start_server(i).await;
        }
        let temp_node = tester
            .handle
            .create_node()
            .name("init client")
            .ip([11, 11, 11, 11].into())
            .build();
        let cl = KvClient::new(
            tester
                .ids
                .clone()
                .iter()
                .map(|&x| rpc_listen_addr(x).parse::<SocketAddr>().unwrap())
                .collect(),
        );
        let x = temp_node.spawn(async move {
            cl.init().await;
        });
        tokio::join!(x);
        tester
    }

    fn rpc_total(&self) -> u64 {
        simulator::<NetSim>().stat().msg_count / 2
    }

    fn check_timeout(&self) {
        // enforce a two minute real-time limit on each test
        if self.t0.elapsed() > Duration::from_secs(120) {
            panic!("test took longer than 120 seconds");
        }
    }

    /// Maximum log size across all servers
    pub fn log_size(&self) -> usize {
        /*
        self.addrs
            .iter()
            .map(|&addr| self.handle.fs.ge100t_file_size(addr, "state").unwrap())
            .max()
            .unwrap() as usize
            */
        0
    }

    /// Maximum snapshot size across all servers
    pub fn snapshot_size(&self) -> usize {
        /*self.addrs
        .iter()
        .map(|&addr| self.handle.fs.get_file_size(addr, "snapshot").unwrap())
        .max()
        .unwrap() as usize
        */
        0
    }

    /// Attach server i to servers listed in to
    fn connect(&self, i: usize, to: &[usize]) {
        debug!("connect peer {} to {:?}", i, to);
        for &j in to {
            simulator::<NetSim>().connect2(self.node_ids[i], self.node_ids[j]);
            //self.handle.net.connect2(self.addrs[i], self.addrs[j]);
        }
    }

    /// Detach server i from the servers listed in from
    fn disconnect(&self, i: usize, from: &[usize]) {
        debug!("disconnect peer {} from {:?}", i, from);
        for &j in from {
            simulator::<NetSim>().disconnect2(self.node_ids[i], self.node_ids[j]);
            //self.handle.net.disconnect2(self.addrs[i], self.addrs[j]);
        }
    }

    pub fn all(&self) -> Vec<usize> {
        (0..self.n).collect()
    }

    pub fn connect_all(&self) {
        for i in 0..self.n {
            self.connect(i, &self.all());
        }
    }

    /// Sets up 2 partitions with connectivity between servers in each  partition.
    pub fn partition(&self, p1: &[usize], p2: &[usize]) {
        debug!("partition servers into: {:?} {:?}", p1, p2);
        for &i in p1 {
            self.disconnect(i, p2);
            self.connect(i, p1);
        }
        for &i in p2 {
            self.disconnect(i, p1);
            self.connect(i, p2);
        }
    }

    // Create a clerk with clerk specific server names.
    // Give it connections to all of the servers, but for
    // now enable only connections to servers in to[].
    pub async fn make_client(&self, to: &[usize]) -> Clerk {
        let id = ClerkId(self.next_client_id.fetch_add(1, Ordering::SeqCst));
        let node = self.handle.create_node().ip(id.to_addr().ip()).build();
        self.client_nodeid.lock().await.insert(id.0, node.id());
        self.connect_client(id, to).await;
        Clerk {
            id,
            handle: node,
            ck: Arc::new(KvClient::new(
                self.ids
                    .clone()
                    .iter()
                    .map(|&x| rpc_listen_addr(x).parse::<SocketAddr>().unwrap())
                    .collect(),
            )),
            ops: self.ops.clone(),
        }
    }

    pub async fn connect_client(&self, id: ClerkId, to: &[usize]) {
        debug!("connect {:?} to {:?}", id, to);
        let nodeid = self.client_nodeid.lock().await.get(&id.0).unwrap().clone();
        simulator::<NetSim>().connect(nodeid.clone());
        for i in 0..self.n {
            simulator::<NetSim>().disconnect2(nodeid.clone(), self.node_ids[i]);
        }
        for &i in to {
            simulator::<NetSim>().connect2(nodeid.clone(), self.node_ids[i]);
        }
    }

    /// Shutdown a server.
    pub async fn shutdown_server(&self, i: usize) {
        debug!("shutdown_server({})", i);
        self.handle.kill(self.node_ids[i]);
        self.servers.lock().await[i] = None;
    }

    /// Start a server.
    /// If restart servers, first call shutdown_server
    pub async fn start_server(&self, i: usize) {
        debug!("start_server({})", i);
        self.handle.restart(self.node_ids[i]);
        let mut peers = self.ids.clone();
        peers.retain(|x| *x != self.ids[i]);
        let raft = Arc::new(MadRaft::build(self.ids[i], peers, Some(self.temp_path.clone())).await);
        let node = self.handle.get_node(self.node_ids[i]).unwrap();
        let traft = raft.clone();
        node.spawn(async move {
            traft.run().await;
        });
        self.servers.lock().await[i] = Some(raft);
    }

    /// get the leader
    pub async fn leader(&self) -> Option<usize> {
        let servers = self.servers.lock().await;
        for (i, raft) in servers.iter().enumerate() {
            if let Some(raft) = raft {
                if raft.server.is_leader().await {
                    return Some(i);
                }
            }
        }
        None
    }

    /// Partition servers into 2 groups and put current leader in minority
    pub async fn make_partition(&self) -> (Vec<usize>, Vec<usize>) {
        let leader = self.leader().await.unwrap_or(0);
        let mut p1 = (0..self.n).collect::<Vec<usize>>();
        p1.swap_remove(leader);
        let mut p2 = p1.split_off(self.n / 2 + 1);
        p2.push(leader);
        (p1, p2)
    }

    /// End a Test -- the fact that we got here means there
    /// was no failure.
    /// print the Passed message,
    /// and some performance numbers.
    pub fn end(&self) {
        self.check_timeout();

        // real time
        let t = self.t0.elapsed();
        // number of Raft peers
        let npeers = self.n;
        // number of RPC sends
        let nrpc = self.rpc_total() - self.rpcs0;
        // number of clerk get/put/append calls
        let nops = self.ops.load(Ordering::Relaxed);

        info!("  ... Passed --");
        info!("  {:?}  {} {} {}", t, npeers, nrpc, nops);
    }
}

pub struct KvClient {
    servers: Vec<SocketAddr>,
}

impl KvClient {
    pub fn new(servers: Vec<SocketAddr>) -> Self {
        KvClient { servers }
    }

    async fn send(&self, sql: String) -> String {
        let mut cur = 0;
        loop {
            let mut client = RpcClient::connect(self.servers[cur].to_string())
                .await
                .unwrap();
            let query = tonic::Request::new(Query {
                sql: sql.clone(),
                consistency: Consistency::Strong as i32,
            });
            let response = client.execute(query).await;
            if let Ok(response) = response {
                let response = response.into_inner();
                if response.rows.len() >= 1 {
                    return response.rows[0].values[0].clone();
                } else {
                    return "".to_string();
                }
            }
            cur += 1;
            cur %= self.servers.len();
        }
    }

    pub async fn init(&self) {
        info!("init");
        self.send("CREATE TABLE kvtable(key TEXT PRIMARY KEY, value TEXT);".to_string())
            .await;
        info!("init finish");
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    pub async fn get(&self, key: String) -> String {
        debug!("get {key}");
        let res = self
            .send(format!("SELECT value FROM kvtable WHERE key=\"{}\";", key).to_string())
            .await;
        debug!("get result {res}");
        res
    }

    pub async fn put(&self, key: String, value: String) {
        debug!("put {key}: {value}");
        self.send(
            format!(
                "INSERT INTO kvtable (key, value) \
                VALUES (\"{}\", \"{}\") \
                ON CONFLICT(key) DO UPDATE SET value = \"{}\";",
                key, value, value,
            )
            .to_string(),
        )
        .await;
        debug!("put finish");
    }

    pub async fn append(&self, key: String, value: String) {
        debug!("append {key} {value}");
        self.send(
            format!(
                "INSERT INTO kvtable (key, value) \
                VALUES (\"{}\", \"{}\") \
                ON CONFLICT(key) DO UPDATE SET value = value || \"{}\";",
                key, value, value,
            )
            .to_string(),
        )
        .await;
        debug!("append finish!");
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClerkId(usize);

impl ClerkId {
    fn to_addr(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 2, self.0 as u8], 1))
    }
}

pub struct Clerk {
    handle: NodeHandle,
    id: ClerkId,
    ck: Arc<KvClient>,
    ops: Arc<AtomicUsize>,
}

impl Clerk {
    pub const fn id(&self) -> ClerkId {
        self.id
    }

    pub async fn put(&self, key: &str, value: &str) {
        self.op();
        let ck = self.ck.clone();
        let key = key.to_owned();
        let value = value.to_owned();
        self.handle
            .spawn(async move {
                ck.put(key, value).await;
            })
            .await
            .unwrap()
    }

    pub async fn append(&self, key: &str, value: &str) {
        self.op();
        let ck = self.ck.clone();
        let key = key.to_owned();
        let value = value.to_owned();
        self.handle
            .spawn(async move {
                ck.append(key, value).await;
            })
            .await
            .unwrap()
    }

    pub async fn get(&self, key: &str) -> String {
        self.op();
        let ck = self.ck.clone();
        let key = key.to_owned();
        self.handle
            .spawn(async move { ck.get(key).await })
            .await
            .unwrap()
    }

    pub async fn check(&self, key: &str, value: &str) {
        let ck = self.ck.clone();
        let key1 = key.to_owned();
        let actual = self
            .handle
            .spawn(async move { ck.get(key1).await })
            .await
            .unwrap();
        assert_eq!(actual, value, "get({}) check failed", key);
    }

    fn op(&self) {
        self.ops.fetch_add(1, Ordering::Relaxed);
    }
}
