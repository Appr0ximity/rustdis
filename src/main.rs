use std::{collections::HashMap, sync::Arc, time::SystemTime};
use tokio::{io::AsyncWriteExt, net::TcpListener, sync::{Mutex, broadcast}};

use crate::{parser::parse_command, resp::{error_message, simple_string}};

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
                                &stream_channels_clone
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
                        let result: String = run_command(&command, &parts, &store_clone, &list_clone, &stream_clone, &stream_channels_clone).await;
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
        stream_channels_clone: &Arc<Mutex<HashMap<String, broadcast::Sender<()>>>>
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
        _ => {
            "ERR Invalid input".to_string()
        }
    }
}