use anyhow::Result;
use madfs_raft::rpc::proto::rpc_server::RpcServer;
use madfs_raft::{
    rpc::{RpcService, RpcTransport},
    StoreServer,
};
use log::info;
use std::sync::Arc;
use structopt::StructOpt;
use tonic::transport::Server;

#[derive(StructOpt, Debug)]
#[structopt(name = "gouged")]
struct Opt {
    /// The ID of this server.
    #[structopt(short, long)]
    id: usize,
    /// The IDs of peers.
    #[structopt(short, long, required = false)]
    peers: Vec<usize>,
}

/// Node authority (host and port) in the cluster.
fn node_authority(id: usize) -> (&'static str, u16) {
    let host = "127.0.0.1";
    let port = 50000 + (id as u16);
    (host, port)
}

/// Node RPC address in cluster.
fn node_rpc_addr(id: usize) -> String {
    let (host, port) = node_authority(id);
    format!("http://{}:{}", host, port)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    info!("run");
    let opt = Opt::from_args();
    let (host, port) = node_authority(opt.id);
    let rpc_listen_addr = format!("{}:{}", host, port).parse().unwrap();
    let transport = RpcTransport::new(Box::new(node_rpc_addr));
    let server = StoreServer::start(opt.id, opt.peers, transport, None).await?;
    let server = Arc::new(server);
    info!("begin");
    let f = {
        let server = server.clone();
        tokio::task::spawn(async move {
            server.run().await;
        })
    };
    let rpc = RpcService::new(server);
    let g = tokio::task::spawn(async move {
        println!("RPC listening to {} ...", rpc_listen_addr);
        let ret = Server::builder()
            .add_service(RpcServer::new(rpc))
            .serve(rpc_listen_addr)
            .await;
        ret
    });
    let results = tokio::try_join!(f, g)?;
    results.1?;
    Ok(())
}
