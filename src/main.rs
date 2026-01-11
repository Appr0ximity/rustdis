#![allow(unused_imports)]
use std::{collections::HashMap, env::args, fmt::{Error, format}, io::{Read, Write}, num::ParseIntError, sync::Arc, thread, time::{Duration, SystemTime}};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener, sync::Mutex};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Logs from your program will appear here!");
    let store: Arc<Mutex<HashMap<String, (String, Option<SystemTime>)>>> = Arc::new(Mutex::new(HashMap::new()));
    let lists: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop{
        let (mut stream, _)  = listener.accept().await?;
        let store_clone = store.clone();
        let list_clone = lists.clone();
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
                let parts: Vec<&str> = input.split("\r\n").collect();
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
                }else if let Some(_index) = input.find("SET"){          //*5\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n$2\r\nPX\r\n$2\r\n10\r\n
                    let key;                                              //0.    1.    2.    3.      4.     5.     6.      7.    8.     9.   10.
                    let value;
                    if parts.len() >= 7{ 
                        key = parts[4];
                        value = parts[6];
                    }else{
                        break;
                    }

                    if parts.len() >= 11{
                        if parts[8].eq_ignore_ascii_case("px"){
                            let mut map = store_clone.lock().await;
                            let ms = match parts[10].parse(){
                                Ok(parsed) => parsed,
                                Err(e) => {
                                    eprintln!("Error while parsing: {}", e);
                                    continue;
                                },
                            };
                            let expiry = SystemTime::now() + Duration::from_millis(ms);
                            map.insert(key.to_string(), (value.to_string(), Some(expiry)));
                            let output = b"+OK\r\n";
                            if let Err(_e) = stream.write_all(output).await{
                                break ;
                            }
                        }
                    }else{
                        let mut map = store_clone.lock().await;
                        map.insert(key.to_string(), (value.to_string(), None));
                        let output = b"+OK\r\n";
                        if let Err(_e) = stream.write_all(output).await{
                            break ;
                        }
                    }
                }else if let Some(_index) = input.find("GET"){
                    let parts: Vec<&str> = input.split("\r\n").collect();
                    let key = parts[4];

                    let mut map = store_clone.lock().await;
                    if let Some((value, expiry)) = map.get(key){
                        if let Some(exp_time) = expiry{
                            if *exp_time < SystemTime::now(){
                                map.remove(key);
                                if let Err(e) = stream.write_all(b"$-1\r\n").await{
                                    eprintln!("{}", e);
                                }
                                continue;
                            }
                        }
                        let output = format!("${}\r\n{}\r\n", value.len(), value);
                        if let Err(_e) = stream.write_all(output.as_bytes()).await{
                            break ;
                        }
                    }else{
                        let output = b"$-1\r\n";
                        if let Err(_e) = stream.write_all(output).await{
                            break ;
                        }
                    }
                }else if let Some(_index) = input.find("RPUSH"){
                    let mut lists_map = list_clone.lock().await;
                    let mut values: Vec<String> = Vec::new();
                    let length = parts.len();
                    let mut index = 6;
                    while index < length{
                        values.push(String::from(parts[index]));
                        index = index + 2;
                    }
                    lists_map.entry(parts[4].to_string())
                        .or_insert_with(Vec::new)
                        .extend(values);

                    let output = format!(":{}\r\n", lists_map.get(parts[4]).unwrap().len());
                    if let Err(_e) = stream.write_all(output.as_bytes()).await{
                        break ;
                    }
                }else if let Some(_index) = input.find("LRANGE"){
                    let lists_map = list_clone.lock().await;

                    let output_vec = lists_map.get(parts[4]);
                    let start:i32 = parts[6].parse().unwrap_or(0);
                    let end:i32 = parts[8].parse().unwrap_or(-1);
                    match output_vec{
                        Some(output_vec) => {
                            let mut output: String = String::new();
                            let len = output_vec.len() as i32;
                            let start_index: usize = if start < 0{
                                (len + start).max(0)
                            }else{
                                start.min(len)
                            } as usize;
                            let end_index: usize = if end < 0{
                                (len + end).max(-1) + 1
                            }else{
                                (end+1).min(len)
                            } as usize;
                            let slice = &output_vec[start_index..end_index];
                            output.push_str(&format!("*{}\r\n",slice.len()));
                            for arg in slice{
                                output.push_str(&format!("${}\r\n{}\r\n", &arg.len(), arg));
                            }

                            if let Err(_e) = stream.write_all(output.as_bytes()).await{
                                break ;
                            }
                        },
                        None => {
                            let output = "*0\r\n";
                            if let Err(_e) = stream.write_all(output.as_bytes()).await{
                                break ;
                            }
                        }
                    }

                }else if let Some(_index) = input.find("LPUSH"){
                    let mut lists_map = list_clone.lock().await;
                    let mut values: Vec<String> = Vec::new();
                    let length = parts.len();
                    let mut index = 6;
                    while index < length{
                        values.push(String::from(parts[index]));
                        index = index + 2;
                    }
                    let list = lists_map.entry(parts[4].to_string()).or_insert_with(Vec::new);

                    for value in values{
                        list.insert(0, value);
                    }

                    let output = format!(":{}\r\n", lists_map.get(parts[4]).unwrap().len());
                    if let Err(_e) = stream.write_all(output.as_bytes()).await{
                        break ;
                    }
                }else if let Some(_index) = input.find("LLEN"){
                    let lists_map = list_clone.lock().await;
                    if let Some(list) = lists_map.get(parts[4]){
                        let output = format!(":{}\r\n", list.len());
                        if let Err(_e) = stream.write_all(output.as_bytes()).await{
                            break ;
                        }
                    }else{
                        let output = format!(":0\r\n");
                        if let Err(_e) = stream.write_all(output.as_bytes()).await{
                            break ;
                        }
                    }
                }else if let Some(_index) = input.find("LPOP"){
                    let mut lists_map = list_clone.lock().await;
                    let mut output = String::new();
                    if let Some(list) = lists_map.get_mut(parts[4]){
                        if parts.len() >= 7 && let Ok(mut iterations) = parts[6].parse::<i32>(){
                            output.push_str(&format!("*{}\r\n", iterations));
                            while !list.is_empty()  && iterations != 0{
                                let removed = list.remove(0);
                                output.push_str(&format!("${}\r\n{}\r\n", &removed.len(), removed));
                                iterations = iterations-1;
                            }
                        }else {
                            let removed = list.remove(0);
                            output = format!("${}\r\n{}\r\n", &removed.len(), removed);
                        }
                    }else{
                        output = format!("$-1\r\n");
                    }
                    if let Err(_e) = stream.write_all(output.as_bytes()).await{
                        break ;
                    }
                }
            }
        });
    }
}
