#[cfg(test)]
#[cfg(madsim)]
mod test {
    use anyhow::Result;
    use chiselstore::rpc::proto::rpc_client::RpcClient;
    use chiselstore::rpc::proto::rpc_server::RpcServer;
    use chiselstore::rpc::proto::{Consistency, Query};
    use chiselstore::{
        rpc::{RpcService, RpcTransport},
        StoreServer,
    };
    use log::info;
    use madsim::net::rpc;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use structopt::StructOpt;
    use tonic::transport::Server;

    use madsim::{runtime::Handle, time::sleep};

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
    struct MadRaft {
        id: usize,
        peers: Vec<usize>,
        server: Arc<StoreServer<RpcTransport>>,
    }

    impl MadRaft {
        pub async fn build(id: usize, peers: Vec<usize>) -> Result<Self> {
            let transport = RpcTransport::new(Box::new(node_rpc_addr));
            let server = StoreServer::start(id, peers.clone(), transport).await?;
            let server = Arc::new(server);
            Ok(MadRaft { id, peers, server })
        }

        pub fn get_rpc_service(&self) -> RpcService {
            RpcService::new(self.server.clone())
        }

        pub async fn run(self) {
            let f = {
                let server = self.server.clone();
                tokio::task::spawn(async move {
                    server.run().await;
                })
            };
            let addr = rpc_listen_addr(self.id).parse::<SocketAddr>().unwrap();
            let rpc = self.get_rpc_service();
            let g = tokio::task::spawn(async move {
                println!("RPC listening to {} ...", rpc_listen_addr(self.id));
                let ret = Server::builder()
                    .add_service(RpcServer::new(rpc))
                    .serve(addr)
                    .await;
                ret
            });
            let results = tokio::try_join!(f, g).unwrap();
            results.1.unwrap();
        }
    }

    #[madsim::test]
    async fn test() {
        let handle = Handle::current();
        let addr1 = rpc_listen_addr(1).parse::<SocketAddr>().unwrap();
        let addr2 = rpc_listen_addr(2).parse::<SocketAddr>().unwrap();
        let addr3 = rpc_listen_addr(3).parse::<SocketAddr>().unwrap();
        let node1 = handle.create_node().name("server1").ip(addr1.ip()).build();
        let node2 = handle.create_node().name("server2").ip(addr2.ip()).build();
        let node3 = handle.create_node().name("server3").ip(addr3.ip()).build();
        let task1 = node1.spawn(async move {
            let raft1 = MadRaft::build(1, vec![2, 3]).await.unwrap();
            raft1.run().await;
        });
        let task2 = node2.spawn(async move {
            let raft2 = MadRaft::build(2, vec![1, 3]).await.unwrap();
            raft2.run().await;
        });
        let task3 = node3.spawn(async move {
            let raft3 = MadRaft::build(3, vec![1, 2]).await.unwrap();
            raft3.run().await;
        });

        let client_addr1 = "10.2.4.1:11451".parse::<SocketAddr>().unwrap();
        let cl_node1 = handle
            .create_node()
            .name("client1")
            .ip(client_addr1.ip())
            .build();
        let cl_task1 = cl_node1.spawn(async move {
            let mut client = RpcClient::connect(addr1.clone().to_string()).await.unwrap();
            let query = tonic::Request::new(Query {
                sql: "select * from users".to_string(),
                consistency: Consistency::Strong as i32,
            });
            let response = client.execute(query).await.unwrap();
            let response = response.into_inner();
            for row in response.rows {
                println!("{:?}", row.values);
            }
        });

        task1.await.unwrap();
        task2.await.unwrap();
        task3.await.unwrap();
        cl_task1.await.unwrap();
    }
}
