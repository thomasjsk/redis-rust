use bytes::BytesMut;
use tokio::{net::TcpStream, io::{AsyncReadExt, AsyncWriteExt}};
use anyhow::Result;
use itertools::join;

#[derive(Clone, Debug)]
pub enum Command {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Command>),
}

impl Command {
    pub fn serialize(self) -> String {
        match self {
            Command::SimpleString(s) => format!("+{}\r\n", s),
            Command::BulkString(s) => if s.len() == 0 {
                String::from("$-1\r\n")
            } else {
                format!("${}\r\n{}\r\n", s.chars().count(), s)
            },
            Command::Array(commands) => {
                let number_of_elements = commands.len();
                let mut elements: Vec<String> = vec![];

                for command in commands.iter() {
                    elements.push(command.clone().serialize())
                }

                format!("*{}\r\n{}", number_of_elements, join(elements, ""))
            }
        }
    }
}


pub struct CommandHandler {
    pub stream: TcpStream,
    buffer: BytesMut,
}

impl CommandHandler {
    pub fn new(stream: TcpStream) -> Self {
        CommandHandler {
            stream,
            buffer: BytesMut::with_capacity(512),
        }
    }

    pub async fn read(&mut self) -> Result<Option<Command>> {
        let cursor = self.stream.read_buf(&mut self.buffer).await?;

        if cursor == 0 {
            return Ok(None);
        }

        let (command, _) = to_command(self.buffer.split())?;
        Ok(Some(command))
    }

    // pub async fn writeQ(&mut self, bytes: &[u8]) -> Result<()> {
    //     self.stream.write(bytes).await?;
    //
    //     Ok(())
    // }

    pub async fn write(&mut self, data: WriteData) -> Result<()> {
        let bytes = match data {
            WriteData::Command(command) => command.serialize().into_bytes(),
            WriteData::String(string) => string.into_bytes(),
            WriteData::Raw(data) => data
        };
        self.stream.write(bytes.as_slice()).await?;

        Ok(())
    }

    // pub async fn write2(&mut self, value: Command) -> Result<()> {
    //     self.stream.write(value.serialize().as_bytes()).await?;
    //
    //     Ok(())
    // }
}

pub enum WriteData {
    Command(Command),
    String(String),
    Raw(Vec<u8>),
}

fn to_command(buffer: BytesMut) -> Result<(Command, usize)> {
    // println!("to_command-->");
    match buffer[0] as char {
        '+' => parse_simple_string(buffer),
        '$' => parse_bulk_string(buffer),
        '*' => parse_array(buffer),
        _ => Err(anyhow::anyhow!("Unknown value type {:?}", buffer))
    }
}


fn parse_simple_string(buffer: BytesMut) -> Result<(Command, usize)> {
    // println!("parse_simple_string {:?}", buffer);
    if let Some((line, len)) = read_line(&buffer[1..]) {
        let string = String::from_utf8(line.to_vec()).unwrap();

        return Ok((Command::SimpleString(string), len + 1));
    } else {
        return Err(anyhow::anyhow!("Invalid string format {:?}", buffer));
    };
}

fn parse_bulk_string(buffer: BytesMut) -> Result<(Command, usize)> {
    // println!("parse_bulk_string {:?}", buffer);
    let (bulk_str_len, cursor) = if let Some((next_line, cursor)) = read_line(&buffer[1..]) {
        let bulk_str_len = buffer_to_int(next_line)?;

        (bulk_str_len, cursor + 1)
    } else {
        return Err(anyhow::anyhow!("Invalid array format {:?}", buffer));
    };

    let end_of_bulk_str = cursor + bulk_str_len as usize;
    let total_parsed = end_of_bulk_str + 2;

    Ok((Command::BulkString(String::from_utf8(buffer[cursor..end_of_bulk_str].to_vec())?), total_parsed))
}

fn parse_array(buffer: BytesMut) -> Result<(Command, usize)> {
    // println!("parse_array {:?}", buffer);

    // Get number of items in array, and move cursor
    let (array_length, mut cursor) = if let Some((next_line, cursor)) = read_line(&buffer[1..]) {
        let arr_len = buffer_to_int(next_line)?;
        (arr_len, cursor + 1)
    } else {
        return Err(anyhow::anyhow!("Invalid array format {:?}", buffer));
    };

    let mut commands = vec![];

    // Read each command in array
    for _ in 0..array_length {
        let (command, _cursor) = to_command(BytesMut::from(&buffer[cursor..]))?;
        commands.push(command);
        cursor += _cursor;
    }

    return Ok((Command::Array(commands), cursor));
}

fn read_line(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((&buffer[0..(i - 1)], i + 1));
        }
    }

    return None;
}

fn buffer_to_int(buffer: &[u8]) -> Result<i64> {
    Ok(String::from_utf8(buffer.to_vec())?.parse::<i64>()?)
}
