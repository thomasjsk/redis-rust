use crate::command_handler::{Command, CommandHandler, WriteData};
use tokio::net::TcpStream;

pub struct Connection {
    pub command_handler: CommandHandler,
}

impl Connection {
    pub async fn new(address: String) -> Self {
        Connection {
            command_handler: CommandHandler::new(match TcpStream::connect(address.clone()).await {
                Ok(stream) => stream,
                Err(e) => {
                    eprintln!("Error creating command handler {}", e);
                    std::process::exit(1);
                }
            })
        }
    }

    pub async fn write(&mut self, command: Command) {
        self.command_handler.write(WriteData::Command(command)).await.unwrap()
    }

    pub async fn read(&mut self) -> Option<Command> {
        self.command_handler.read().await.unwrap()
    }
}
