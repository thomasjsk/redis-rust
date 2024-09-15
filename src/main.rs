mod command_handler;
mod server;
mod storage;
mod commands;
mod util;
mod connection;

use std::collections::HashMap;
use std::env;
use crate::server::{Server, ServerStartupConfig};

#[tokio::main]
async fn main() {
    let args: HashMap<String, Vec<String>> = HashMap::from([
        (String::from("port"), vec![String::from("--port"), String::from("-p")]),
        (String::from("replicaof"), vec![String::from("--replicaof")]),
    ]);


    let runtime_args: Vec<String> = env::args().collect();

    let host = String::from("127.0.0.1");
    let mut port = String::from("6379");
    let mut replica_of: Option<String> = None;

    for (i, arg) in runtime_args.iter().enumerate() {
        if args["port"].contains(&arg) {
            port = String::from(&runtime_args[i + 1]);
        }

        if args["replicaof"].contains(&arg) {
            replica_of = Some(String::from(&runtime_args[i + 1]))
        }
    }

    Server::new(ServerStartupConfig {
        host,
        port,
        replica_of,
    }).await;
}
