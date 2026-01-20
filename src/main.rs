use std::{collections::HashMap, env, sync::Arc, time::SystemTime};
use tokio::{io::AsyncWriteExt, net::{TcpListener, TcpStream}, sync::{Mutex, broadcast}};

use crate::{parser::parse_command, resp::{bulk_string_array, error_message, simple_string}};

mod parser;
mod handlers;
mod resp;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut port: u32 = 6379;
    let mut replica_of  = String::new();
    println!("Logs from your program will appear here!");
    let args: Vec<String> = env::args().collect();
    for (i,arg) in args.iter().enumerate(){
        if arg == "--port" && args.len()> i+1{
            port = args[i+1].parse::<u32>().unwrap();
        }
        if arg == "--replicaof" && args.len() > i + 1{
            replica_of = args[i+1].to_string();
        }
    }
    let server_info: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap:: new()));
    {
        let mut info = server_info.lock().await;
        info.insert("port".to_string(), port.to_string());
        if !replica_of.is_empty(){
            info.insert("replicaof".to_string(), replica_of.to_string());
        }
        info.insert("master_replid".to_string(),"8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string());
        info.insert("master_repl_offset".to_string(), "0".to_string());
    }
    let store: Arc<Mutex<HashMap<String, (String, Option<SystemTime>)>>> = Arc::new(Mutex::new(HashMap::new()));
    let lists: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
    let streams: Arc<Mutex<HashMap<String, Vec<(String, HashMap<String, String>)>>>> = Arc::new(Mutex::new(HashMap::new()));
    let stream_channels:Arc<Mutex<HashMap<String, broadcast::Sender<()>>>> = Arc::new(Mutex::new(HashMap::new()));
    if !replica_of.is_empty(){
        let replica_port = port.clone();
        tokio::spawn(async move{
            let _ = connect_to_master(&replica_of, &replica_port).await;
        });
    }
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    loop{
        let (mut stream, _)  = listener.accept().await?;
        let store_clone = store.clone();
        let list_clone = lists.clone();
        let stream_clone= streams.clone();
        let stream_channels_clone = stream_channels.clone();
        let server_info_clone = server_info.clone();
        tokio::spawn(async move {
            let mut multi_enabled = false;
            let mut queued_commands: Vec<Vec<String>> = Vec::new();
            loop {
                let parts = match parse_command(&mut stream).await{
                    Some(p) => p,
                    None => break,
                };
                match parts[0].as_str(){
                    "MULTI"=>{
                        if multi_enabled{
                            let _ = stream.write_all(error_message("ERR MULTI calls can not be nested").as_bytes()).await;
                            continue;
                        }else{
                            multi_enabled = true;
                        }
                        if handlers::transaction::handle_multi(&mut stream).await.is_err(){
                            break;
                        }
                    },
                    "EXEC" =>{
                        if handlers::transaction::handle_exec(
                                &mut stream,
                                &mut multi_enabled,
                                &mut queued_commands,
                                &store_clone,
                                &list_clone,
                                &stream_clone,
                                &stream_channels_clone,
                                &server_info_clone
                            ).await.is_err(){
                            break;
                        }
                    },
                    "DISCARD" =>{
                        if handlers::transaction::handle_discard(
                                &mut stream,
                                &mut multi_enabled,
                                &mut queued_commands
                            ).await.is_err(){
                            break;
                        }
                    },
                    cmd =>{
                        if multi_enabled{
                            queued_commands.push(parts.clone());
                            let _ = stream.write_all(simple_string("QUEUED").as_bytes()).await;
                            continue;
                        }
                        let command = cmd;
                        let result: String = run_command(&command, &parts, &store_clone, &list_clone, &stream_clone, &stream_channels_clone, &server_info_clone).await;
                        let _ = stream.write_all(result.as_bytes()).await;
                        continue;
                    }
                }
            }
        });
    }
}

pub async fn run_command(
        command: &str,
        parts: &Vec<String>,
        store_clone: &Arc<Mutex<HashMap<String, (String, Option<SystemTime>)>>>,
        list_clone: &Arc<Mutex<HashMap<String, Vec<String>>>>,
        stream_clone: &Arc<Mutex<HashMap<String, Vec<(String, HashMap<String, String>)>>>>,
        stream_channels_clone: &Arc<Mutex<HashMap<String, broadcast::Sender<()>>>>,
        server_info_clone: &Arc<Mutex<HashMap<String, String>>>
    )-> String{
    match command{
        "PING" =>{
            handlers::string::handle_ping().await
        },
        "ECHO" =>{
            handlers::string::handle_echo(parts).await
        },
        "SET" =>{
            handlers::string::handle_set(parts, store_clone).await
        },
        "GET" =>{
            handlers::string::handle_get(parts, store_clone).await
        },
        "RPUSH" =>{
            handlers::list::handle_rpush(parts, list_clone).await
        },
        "LRANGE" =>{
            handlers::list::handle_lrange(parts, list_clone).await
        },
        "LPUSH" =>{
            handlers::list::handle_lpush(parts, list_clone).await
        },
        "LLEN" =>{
            handlers::list::handle_llen(parts, list_clone).await
        },
        "BLPOP" =>{
            handlers::list::handle_blpop(parts, list_clone).await
        },
        "LPOP" =>{
            handlers::list::handle_lpop(parts, list_clone).await
        },
        "TYPE" =>{
            handlers::list::handle_type(parts, store_clone, list_clone, stream_clone).await
        },
        "XADD" =>{
            handlers::stream::handle_xadd(parts, stream_clone, stream_channels_clone).await
        },
        "XRANGE" =>{
            handlers::stream::handle_xrange(parts, stream_clone).await
        },
        "XREAD" =>{
            handlers::stream::handle_xread(parts, stream_clone, stream_channels_clone).await
        },
        "INCR" =>{
            handlers::string::handle_incr(parts, store_clone).await
        },
        "INFO" =>{
            handlers::info::handle_info(&server_info_clone, parts).await
        },
        "REPLCONF" =>{
            handlers::replication::handle_replconf().await
        },
        "PSYNC" =>{
            handlers::replication::handle_psync(&server_info_clone).await
        },
        _ => {
            "ERR Invalid input".to_string()
        }
    }
}


use tokio::io::{AsyncReadExt};

async fn connect_to_master(replica_of: &str, port: &u32) -> Result<(), Box<dyn std::error::Error>> {
    let parts: Vec<&str> = replica_of.split_whitespace().collect();
    let host = parts[0];
    let master_port = parts[1];
    let mut master_stream = TcpStream::connect(format!("{}:{}", host, master_port)).await?;
    let ping_cmd = bulk_string_array(&vec!["PING".to_string()]);
    master_stream.write_all(ping_cmd.as_bytes()).await?;
    let mut buf = [0u8; 1024];
    let _ = master_stream.read(&mut buf).await;
    let cmd = bulk_string_array(&vec!["REPLCONF".to_string(), "listening-port".to_string(), port.to_string()]);
    master_stream.write_all(cmd.as_bytes()).await?;
    let _ = master_stream.read(&mut buf).await;
    let cmd = bulk_string_array(&vec!["REPLCONF".to_string(), "capa".to_string(), "psync".to_string()]);
    master_stream.write_all(cmd.as_bytes()).await?;
    let _ = master_stream.read(&mut buf).await;
    let cmd = bulk_string_array(&vec!["PSYNC".to_string(), "?".to_string(), "-1".to_string()]);
    master_stream.write_all(cmd.as_bytes()).await?;
    let _ = master_stream.read(&mut buf).await;
    Ok(())
}


