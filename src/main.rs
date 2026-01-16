use std::{collections::HashMap, sync::Arc, time::SystemTime};
use tokio::{io::AsyncWriteExt, net::TcpListener, sync::{Mutex, broadcast}};

use crate::parser::parse_command;

mod parser;
mod handlers;
mod resp;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Logs from your program will appear here!");
    let store: Arc<Mutex<HashMap<String, (String, Option<SystemTime>)>>> = Arc::new(Mutex::new(HashMap::new()));
    let lists: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
    let streams: Arc<Mutex<HashMap<String, Vec<(String, HashMap<String, String>)>>>> = Arc::new(Mutex::new(HashMap::new()));
    let stream_channels:Arc<Mutex<HashMap<String, broadcast::Sender<()>>>> = Arc::new(Mutex::new(HashMap::new()));
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop{
        let (mut stream, _)  = listener.accept().await?;
        let store_clone = store.clone();
        let list_clone = lists.clone();
        let stream_clone= streams.clone();
        let stream_channels_clone = stream_channels.clone();
        tokio::spawn(async move {
            if let Some(parts) = parse_command(&mut stream).await{
                loop {
                    match parts[0].as_str(){
                        "PING" => {
                            if handlers::string::handle_ping(&mut stream).await.is_err(){
                                break;
                            }
                        },
                        "ECHO" => {
                            if handlers::string::handle_echo(&mut stream, &parts).await.is_err(){
                                break;
                            }
                        },
                        "SET" => {
                            if handlers::string::handle_set(&mut stream, &parts, &store_clone).await.is_err(){
                                break;
                            }
                        },
                        "GET" => {
                            if handlers::string::handle_get(&mut stream, &parts, &store_clone).await.is_err(){
                                break;
                            }
                        },
                        "RPUSH" =>{
                            if handlers::list::handle_rpush(&mut stream, &parts, &list_clone).await.is_err(){
                                break;
                            }
                        },
                        "LRANGE" =>{
                            if handlers::list::handle_lrange(&mut stream, &parts, &list_clone).await.is_err(){
                                break;
                            }
                        },
                        "LPUSH" =>{
                            if handlers::list::handle_lpush(&mut stream, &parts, &list_clone).await.is_err(){
                                break;
                            }
                        },
                        "LLEN" =>{
                            if handlers::list::handle_llen(&mut stream, &parts, &list_clone).await.is_err(){
                                break;
                            }
                        },
                        "BLPOP" =>{
                            if handlers::list::handle_blpop(&mut stream, &parts, &list_clone).await.is_err(){
                                break;
                            }
                        },
                        "LPOP" =>{
                            if handlers::list::handle_lpop(&mut stream, &parts, &list_clone).await.is_err(){
                                break;
                            }
                        },
                        "TYPE" =>{
                            if handlers::list::handle_type(&mut stream, &parts, &store_clone, &list_clone, &stream_clone).await.is_err(){
                                break;
                            }
                        },
                        "XADD" =>{
                            if handlers::stream::handle_xadd(&mut stream, &parts, &stream_clone, &stream_channels_clone).await.is_err(){
                                break;
                            }
                        },
                        "XRANGE" =>{
                            if handlers::stream::handle_xrange(&mut stream, &parts, &stream_clone).await.is_err(){
                                break;
                            }
                        },
                        "XREAD" =>{
                            if handlers::stream::handle_xread(&mut stream, &parts, &stream_clone, &stream_channels_clone).await.is_err(){
                                break;
                            }
                        },
                        _ => {
                            let _ = stream.write_all(b"-ERR Invalid input").await;
                            break;
                        }
                    }

                    if parts[0] == "INCR"{
                        let mut store_map = store_clone.lock().await;
                        let output ;
                        if parts.len() < 5{
                            output = "-ERR Invlid input\r\n".to_string();
                            let _ = stream.write_all(output.as_bytes()).await;
                            continue;
                        }
                        let key = &parts[4];
                        if let Some((value, _)) = store_map.get_mut(key){
                            let num = match value.parse::<isize>(){
                                Ok(result) => result,
                                Err(_e) => {
                                    output = "-ERR value is not an integer or out of range\r\n".to_string();
                                    let _ = stream.write_all(output.as_bytes()).await;
                                    continue;
                                },
                            };
                            let num_string = (num + 1).to_string();
                            *value = num_string;
                            output = format!(":{}\r\n", num+1);
                        }else{
                            store_map.insert(parts[4].to_string(), ("1".to_string(), None));
                            output = ":1\r\n".to_string();
                        }
                        let _ = stream.write_all(output.as_bytes()).await;
                    }else if parts[0] == "MULTI"{
                        let output = "+OK\r\n".to_string();
                        let _ = stream.write_all(output.as_bytes()).await;
                    }else if parts[0] == "EXEC"{
                        let output = "-ERR EXEC without MULTI".to_string();
                        let _ = stream.write_all(output.as_bytes());
                    }
                }
            }
        });
    }
}