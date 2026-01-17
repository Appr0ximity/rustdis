use std::{collections::HashMap, sync::Arc, time::{Duration, SystemTime}};

use tokio::{io::AsyncWriteExt, net::TcpStream, sync::Mutex};

use crate::resp::{bulk_string, error_message, integer, nil_bulk, simple_string};

 

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

pub async fn handle_set(stream: &mut TcpStream, parts: &Vec<String>, store_clone: &Arc<Mutex<HashMap<String, (String, Option<SystemTime>)>>>) -> Result<(), ()>{
    let key = parts.get(1).ok_or(())?;
    let value= parts.get(2).ok_or(())?;

    if parts.len() >= 6 && parts[3].eq_ignore_ascii_case("px"){
        let ms = parts[5].parse().map_err(|_| ())?;
        let mut map = store_clone.lock().await;
        let expiry = SystemTime::now() + Duration::from_millis(ms);
        map.insert(key.to_string(), (value.to_string(), Some(expiry)));
    }else if parts.len() >= 3{
        let mut map = store_clone.lock().await;
        map.insert(key.to_string(), (value.to_string(), None));
    }else{
        return Err(());
    }
    let resp_output = simple_string("OK");
    stream.write_all(resp_output.as_bytes()).await.map_err(|_|())
}

pub async fn handle_get(stream: &mut TcpStream, parts: &Vec<String>, store_clone: &Arc<Mutex<HashMap<String, (String, Option<SystemTime>)>>>)->Result<(), ()>{
    let key = &parts[1];
    let output;

    let mut map = store_clone.lock().await;
    if let Some((value, expiry)) = map.get(key){
        if let Some(exp_time) = expiry{
            if *exp_time < SystemTime::now(){
                map.remove(key);
                return stream.write_all(nil_bulk().as_bytes()).await.map_err(|_|());
            }
        }
        output = value.clone();
    }else{
        return stream.write_all(nil_bulk().as_bytes()).await.map_err(|_|());
    }
    let resp_output = bulk_string(&output);
    stream.write_all(resp_output.as_bytes()).await.map_err(|_|())
}

pub async fn handle_incr(stream: &mut TcpStream, parts: &Vec<String>, store_clone: &Arc<Mutex<HashMap<String, (String, Option<SystemTime>)>>>)-> Result<(), ()>{
    let mut store_map = store_clone.lock().await;
    let output ;
    if parts.len() < 2{
        output = error_message("ERR Invlid input");
        return stream.write_all(output.as_bytes()).await.map_err(|_| ());
    }
    let key = &parts[1];
    if let Some((value, _)) = store_map.get_mut(key){
        let num = match value.parse::<isize>(){
            Ok(result) => result,
            Err(_e) => {
                output = error_message("ERR value is not an integer or out of range");
                return stream.write_all(output.as_bytes()).await.map_err(|_| ());
            },
        };
        let num_string = (num + 1).to_string();
        *value = num_string;
        output = integer((num+1) as i64);
    }else{
        store_map.insert(parts[4].to_string(), ("1".to_string(), None));
        output = integer(1 as i64);
    }
    return stream.write_all(output.as_bytes()).await.map_err(|_| ());
}