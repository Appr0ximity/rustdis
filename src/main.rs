#![allow(unused_imports)]
use std::{collections::HashMap, fmt::format, io::{Read, Write}, sync::Arc, thread, time::{Duration, SystemTime}};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener, sync::Mutex};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Logs from your program will appear here!");
    let store: Arc<Mutex<HashMap<String, (String, Option<SystemTime>)>>> = Arc::new(Mutex::new(HashMap::new()));
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
                }else if let Some(_index) = input.find("SET"){          //*5\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n$2\r\nPX\r\n$2\r\n10\r\n
                    let parts: Vec<&str> = input.split("\r\n").collect();       //0.    1.    2.    3.      4.     5.     6.      7.    8.     9.   10. 
                    let key = parts[4];
                    let value = parts[6];

                    if parts.len() >= 11{
                        if parts[8].eq_ignore_ascii_case("px"){
                            let mut map = store_clone.lock().await;
                            let ms = parts[10].parse().unwrap();
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
                }
            }
        });
    }
}
