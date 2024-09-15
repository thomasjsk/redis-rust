use crate::command_handler::{Command, CommandHandler};
use crate::commands::{echo_command, get_command, info_command, ping_command, psync_command, replconf_command, set_command};
use crate::connection::Connection;
use crate::storage::Storage;
use crate::util::generate_random_string;
use std::collections::HashSet;
use std::io::Error;
use std::sync::Arc;
use tokio::{
    net::TcpListener,
    sync::Mutex,
};

pub struct ServerStartupConfig {
    pub host: String,
    pub port: String,
    pub replica_of: Option<String>,
}

#[derive(Debug)]
pub struct ServerConfig {
    pub port: String,
    pub replica_of: Option<ServerReplicaOf>,
    pub replication_id: String,
    pub replication_offset: i32,
    pub replicas: HashSet<String>,
}

#[derive(Debug, Clone)]
pub struct ServerReplicaOf {
    pub host: String,
    pub port: String,
}

pub struct RedisConnection {
    pub host: String,
    pub port: String,
    pub connection: Option<Connection>,
}

impl Clone for RedisConnection {
    fn clone(&self) -> Self {
        RedisConnection {
            host: self.host.clone(),
            port: self.port.clone(),
            connection: None,
        }
    }
}

pub struct Server {
    pub config: Arc<Mutex<ServerConfig>>,
    listener: TcpListener,
    pub storage: Arc<Mutex<Storage>>,
}

impl Server {
    pub async fn new(startup_config: ServerStartupConfig) -> Self {
        let address = vec![startup_config.host.as_str(), ":", startup_config.port.as_str()].concat();


        let config = ServerConfig {
            port: startup_config.port,
            replication_id: generate_random_string(40),
            replication_offset: 0,
            replica_of: match &startup_config.replica_of {
                Some(val) => {
                    let (host, port) = val.split_once(" ").unwrap();

                    Some(ServerReplicaOf {
                        host: String::from(host),
                        port: String::from(port),
                    })
                }
                None => None
            },
            replicas: HashSet::new(),
        };

        let storage = Arc::new(Mutex::new(Storage::new()));
        let mut server = Server {
            config: Arc::new(Mutex::new(config)),
            listener: TcpListener::bind(&address).await.unwrap(),
            storage,
        };

        if startup_config.replica_of.is_some() {
            match server.sync_with_master().await {
                Ok(connection) => Some(connection),
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                    None
                }
            };
        }

        println!("Server started on: {}", address);

        server.listen().await;
        server
    }

    async fn listen(&mut self) {
        println!("Awaiting new connections...");

        loop {
            let connection = self.listener.accept().await;
            let storage = Arc::clone(&self.storage);
            let config = Arc::clone(&self.config);

            match connection {
                Ok((stream, _)) => {
                    tokio::spawn(async move {
                        let mut command_handler = CommandHandler::new(stream);
                        handle_connection(&mut command_handler, storage, config).await
                    });
                }
                Err(e) => {
                    handle_error(e)
                }
            }
        }
    }

    async fn sync_with_master(&mut self) -> Result<(), String> {
        println!("Connecting to master...");
        println!("Handshake...");
        // let storage = Arc::clone(&self.storage);
        let config = Arc::clone(&self.config);
        let port = config.lock().await.port.clone();
        let replica_of = config.lock().await.replica_of.clone().unwrap();


        let master_address = format!("{}:{}", replica_of.clone().host, replica_of.clone().port);

        let mut connection = Connection::new(master_address.clone()).await;
        let mut handshake_steps = vec![
            vec![
                Command::Array(vec![Command::BulkString(String::from("PING"))]),
                Command::SimpleString(String::from("PONG"))
            ],
            vec![
                Command::Array(vec![
                    Command::BulkString(String::from("REPLCONF")),
                    Command::BulkString(String::from("listening-port")),
                    Command::BulkString(port)
                ]),
                Command::SimpleString(String::from("OK"))
            ],
            vec![
                Command::Array(vec![
                    Command::BulkString(String::from("REPLCONF")),
                    Command::BulkString(String::from("capa")),
                    Command::BulkString(String::from("psync2")),
                ]),
                Command::SimpleString(String::from("OK"))
            ],
            vec![
                Command::Array(vec![
                    Command::BulkString(String::from("PSYNC")),
                    Command::BulkString(String::from("?")),
                    Command::BulkString(String::from("-1")),
                ]),
                Command::SimpleString(String::from("FULLRESYNC"))
            ]
        ];

        while handshake_steps.len() > 0 {
            let step = handshake_steps.remove(0);
            let request = step[0].clone();
            let expected_response = step[1].clone();

            println!("Handshake step: {:?}", &request);
            connection.write(request).await;
            let master_response = connection.read().await;
            match master_response {
                Some(cmd) => {
                    println!("Response from master: {:?}", cmd);

                    let received_command = cmd.serialize();
                    let expected_command = expected_response.serialize();

                    if expected_command == Command::SimpleString(String::from("FULLRESYNC")).serialize() {
                        if !received_command.contains("FULLRESYNC") {
                            return Err(String::from("Cannot connect to master"));
                        }
                    } else if received_command != expected_command {
                        return Err(String::from("Cannot connect to master"));
                    }
                }
                None => break
            }
        }

        println!("Connected to master at: {}", master_address);
        Ok(())
    }
}

fn handle_error(e: Error) {
    println!("error: {}", e);
}

pub async fn handle_connection(command_handler: &mut CommandHandler, storage: Arc<Mutex<Storage>>, config: Arc<Mutex<ServerConfig>>) {
    println!("Handling new connection...");

    loop {
        let command_read = command_handler.read().await.unwrap_or_else(|e| {
            eprintln!("Error: {:?}", e);
            None
        });

        match command_read {
            Some(cmd) => {
                println!("Command {:?}", cmd);
                let (command, args) = unpack_command(cmd.clone()).unwrap();

                match command.to_lowercase().as_str() {
                    "ping" => ping_command(command_handler).await,
                    "echo" => echo_command(command_handler, &args).await,
                    "set" => set_command(command_handler, &args, &storage).await,
                    "get" => get_command(command_handler, &unpack_bulk_str(args[0].clone()).unwrap(), &storage).await,
                    "info" => info_command(command_handler, &config, &args).await,
                    "replconf" => replconf_command(command_handler, &config, &args).await,
                    "psync" => psync_command(command_handler).await,
                    _ => ()
                };

                if vec!["get", "set"].contains(&command.to_lowercase().as_str()) {
                    let command = cmd.clone();
                    for replica in config.lock().await.replicas.iter() {
                        eprintln!("Replicating command to: {}", replica.to_string());
                        let mut c = Connection::new(replica.to_string()).await;
                        c.write(command.clone()).await;
                        c.read().await;
                    }
                }
            }
            None => ()
        }
    }
}


pub fn unpack_command(command: Command) -> Result<(String, Vec<Command>), anyhow::Error> {
    match command {
        Command::Array(arr) => {
            Ok((
                unpack_bulk_str(arr.first().unwrap().clone())?,
                arr.into_iter().skip(1).collect(),
            ))
        }
        _ => Err(anyhow::anyhow!("Unknown command format"))
    }
}

pub fn unpack_bulk_str(command: Command) -> Result<String, anyhow::Error> {
    match command {
        Command::BulkString(bulk_string) => Ok(bulk_string),
        _ => Err(anyhow::anyhow!("Expected command to be a bulk string"))
    }
}


