use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::resp::simple_string;

pub async fn handle_ping(stream: &mut TcpStream)-> Result<(), ()>{
    match stream.write_all(b"+PONG\r\n").await {
        Ok(_) => Ok(()),
        Err(_) => Err(()),
    }
}

pub async fn handle_echo(stream: &mut TcpStream, parts: &Vec<String>) -> Result<(), ()>{
    let mut output= String::new();
    for words in parts{
        output.push_str(&words);
    }
    let resp_output = simple_string(&output);
    match stream.write_all(resp_output.as_bytes()).await {
        Ok(_) => Ok(()),
        Err(_) => Err(()),
    }
}