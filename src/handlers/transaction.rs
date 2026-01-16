use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::resp::{error_message, simple_string};

pub async fn handle_multi(stream: &mut TcpStream) -> Result<(), ()>{
    let output = simple_string("OK");
    return stream.write_all(output.as_bytes()).await.map_err(|_| ());
}

pub async fn handle_exec(stream: &mut TcpStream) -> Result<(), ()>{
    let output = error_message("ERR EXEC without MULTI");
    return stream.write_all(output.as_bytes()).await.map_err(|_| ());
}