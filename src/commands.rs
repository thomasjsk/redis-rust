use crate::command_handler::{Command, CommandHandler, WriteData};
use crate::server::{unpack_bulk_str, ServerConfig};
use crate::storage::Storage;
use crate::util::generate_random_string;
use chrono::Local;
use itertools::join;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn ping_command(command_handler: &mut CommandHandler) {
    command_handler.write(WriteData::Command(Command::SimpleString("PONG".to_string()))).await.unwrap()
}

pub async fn echo_command(command_handler: &mut CommandHandler, args: &Vec<Command>) {
    command_handler.write(WriteData::Command(args.first().unwrap().clone())).await.unwrap()
}

pub async fn set_command(command_handler: &mut CommandHandler, args: &Vec<Command>, storage: &Arc<Mutex<Storage>>) {
    let k = unpack_bulk_str(args[0].clone()).unwrap();
    let v = unpack_bulk_str(args[1].clone()).unwrap();
    let mut exp_at: i64 = 0;

    if 2 < args.len() {
        let next_arg = unpack_bulk_str(args[2].clone()).unwrap();

        if next_arg == String::from("px") {
            let ttl_arg = unpack_bulk_str(args[3].clone()).unwrap();
            exp_at = Local::now().timestamp_millis() + ttl_arg.parse::<i64>().unwrap_or_else(|_| 0)
        }
    }
    storage.lock().await.set((k, v), exp_at);


    command_handler.write(WriteData::Command(Command::SimpleString("OK".to_string()))).await.unwrap();
}

pub async fn get_command(command_handler: &mut CommandHandler, key: &String, storage: &Arc<Mutex<Storage>>) {
    let command = match storage.lock().await.get(key) {
        Some(record) => {
            Command::SimpleString(record.value.clone())
        }
        None => Command::BulkString(String::from(""))
    };

    command_handler.write(WriteData::Command(command)).await.unwrap()
}

pub async fn info_command(command_handler: &mut CommandHandler, config: &Arc<Mutex<ServerConfig>>, args: &Vec<Command>) {
    let mut sections: Vec<String> = vec![];
    for section in args.iter() {
        sections.push(unpack_bulk_str(section.clone()).unwrap())
    }
    let section_map = HashMap::from([
        (String::from("replication"), get_replication_info(config).await)
    ]);


    let command = if sections.len() == 1 {
        Command::BulkString(section_map.get(&sections[0]).unwrap().clone())
    } else {
        Command::BulkString(String::from(""))
    };

    command_handler.write(WriteData::Command(command)).await.unwrap()
}

pub async fn replconf_command(command_handler: &mut CommandHandler, config: &Arc<Mutex<ServerConfig>>, args: &Vec<Command>) {
    let arg = unpack_bulk_str(args[0].clone()).unwrap();
    let val = unpack_bulk_str(args[1].clone()).unwrap();

    if arg == String::from("listening-port") {
        let host = command_handler.stream.peer_addr().unwrap();
        config.lock().await.replicas.insert(format!("{}:{}", host.ip(), val));
    }

    command_handler.write(WriteData::Command(Command::SimpleString("OK".to_string()))).await.unwrap()
}

pub async fn psync_command(command_handler: &mut CommandHandler) {
    let data = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
    let rdb = hex::decode(data).unwrap_or_else(|_| panic!());

    command_handler.write(WriteData::Command(Command::SimpleString(format!("FULLRESYNC {} {}", generate_random_string(40), 0)))).await.unwrap();
    command_handler.write(WriteData::String(String::from(format!("${}\r\n", rdb.len())))).await.unwrap();
    command_handler.write(WriteData::Raw(rdb)).await.unwrap();
}

async fn get_replication_info(config: &Arc<Mutex<ServerConfig>>) -> String {
    let config = config.lock().await;

    let role = match config.replica_of {
        Some(_) => String::from("role:slave"),
        None => String::from("role:master")
    };

    join(vec![
        role,
        format!("master_replid:{}", config.replication_id),
        format!("master_repl_offset:{}", config.replication_offset)
    ], "\n")
}