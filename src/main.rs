use crate::connection::{run_listener, ConnectionMessage};
use crate::replication::ReplicationConfig;
use crate::resp::Resp;
use crate::server::{run_server, Server};
use crate::storage::Storage;
use clap::Parser;
use tokio::sync::mpsc;

mod commands;

mod connection;
mod replication;
mod request;
mod resp;
mod resp_result;
mod server;
mod server_result;
mod set;
mod storage;
mod storage_result;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(
        short = 'H',
        long,
        help = "The host address to bind the server to",
        default_value_t = String::from("127.0.0.1")
    )]
    host: String,

    #[arg(
        short,
        long,
        help = "The TCP port to use for the server",
        default_value_t = 6379
    )]
    port: u16,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Parse the command line arguments.
    let args = Args::parse();

    // Create the replication configuration.
    let replication_config = ReplicationConfig::new_master();

    // Create the storage and activate expiry.
    let mut storage = Storage::new();
    storage.set_active_expiry(true);

    // Create the server and attach the storage.
    let mut server = Server::new();
    server.set_storage(storage);
    server.set_replication(replication_config);

    // Create the MPSC channel to communicate with the server.
    let (server_sender, server_receiver) = mpsc::channel::<ConnectionMessage>(32);

    // Spawn the server task.
    tokio::spawn(run_server(server, server_receiver));

    // Run the listener on the configured address and port.
    run_listener(args.host, args.port, server_sender).await;

    Ok(())
}
