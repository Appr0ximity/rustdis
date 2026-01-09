#![allow(unused_imports)]
use std::{collections::HashMap, fmt::format, io::{Read, Write}, sync::Arc, thread, time::Duration};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener, sync::Mutex};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Logs from your program will appear here!");
    let store: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop{
        let (mut stream, _)  = listener.accept().await?;
        let store_clone = store.clone();
        tokio::spawn(async move {
            let mut buf = [0;1024];

            loop {
                let n = match stream.read(&mut buf).await {
                    Ok(0) => return ,
                    Ok(n) => n ,
                    Err(e) => {
                        eprintln!("Error while reading from socket: {:?}", e);
                        return ;
                    },
                };
                let input = String::from_utf8_lossy(&buf[..n]); 
                if let Some(index) = input.find("ECHO"){
                    if let Some(output) = input.get(index+6..){
                        if let Err(_e) = stream.write_all(output.as_bytes()).await{
                            break ;
                        }
                    }
                }else if let Some(_index) = input.find("PING"){
                    let output = b"+PONG\r\n";
                    if let Err(_e) = stream.write_all(output).await{
                        break ;
                    }
                }else if let Some(_index) = input.find("SET"){
                    let parts: Vec<&str> = input.split("\r\n").collect();
                    let key = parts[4];
                    let value = parts[6];

                    let mut map = store_clone.lock().await;
                    map.insert(key.to_string(), value.to_string());
                    let output = b"+OK\r\n";
                    if let Err(_e) = stream.write_all(output).await{
                        break ;
                    }
                }else if let Some(_index) = input.find("GET"){
                    let parts: Vec<&str> = input.split("\r\n").collect();
                    let key = parts[4];

                    let map = store_clone.lock().await;
                    if let Some(value) = map.get(key){
                        let output = format!("${}\r\n{}\r\n", value.len(), value);
                        if let Err(_e) = stream.write_all(output.as_bytes()).await{
                            break ;
                        }
                    }else{
                        let output = b"#-1\r\n";
                        if let Err(_e) = stream.write_all(output).await{
                            break ;
                        }
                    }
                }
            }
        });
    }
}
