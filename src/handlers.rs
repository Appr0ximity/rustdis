use std::{collections::HashMap, sync::Arc, time::{Duration, SystemTime}};

use tokio::{io::AsyncWriteExt, net::TcpStream, sync::Mutex, time::sleep};

use crate::resp::{self, bulk_string, bulk_string_array, integer, nil_bulk, nil_array, simple_string};

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
    let key = &parts[4];
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

pub async fn handle_rpush(stream: &mut TcpStream, parts: &Vec<String>, list_clone: &Arc<Mutex<HashMap<String, Vec<String>>>>)-> Result<(), ()>{
    let mut lists_map = list_clone.lock().await;
    let mut values: Vec<String> = Vec::new();
    let length = parts.len();
    let mut index = 6;
    while index < length{
        values.push(String::from(&parts[index]));
        index = index + 2;
    }
    lists_map.entry(parts[4].to_string())
        .or_insert_with(Vec::new)
        .extend(values);

    let output = lists_map.get(&parts[4]).unwrap().len() as i64;
    let resp_output = integer(output);
    stream.write_all(resp_output.as_bytes()).await.map_err(|_|())
}

pub async fn handle_lrange(stream: &mut TcpStream, parts: &Vec<String>, list_clone: &Arc<Mutex<HashMap<String, Vec<String>>>>)-> Result<(), ()>{
    let lists_map = list_clone.lock().await;

    let output_vec = lists_map.get(&parts[1]);
    let start:i32 = parts[2].parse().unwrap_or(0);
    let end:i32 = parts[3].parse().unwrap_or(-1);
    match output_vec{
        Some(output_vec) => {
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
            let resp_output = bulk_string_array(slice);
            return stream.write_all(resp_output.as_bytes()).await.map_err(|_|())
        },
        None => {
            let output = [];
            let resp_output = bulk_string_array(&output);
            return stream.write_all(resp_output.as_bytes()).await.map_err(|_|())
        }
    }
}

pub async fn handle_lpush(stream: &mut TcpStream, parts: &Vec<String>, list_clone: &Arc<Mutex<HashMap<String, Vec<String>>>>)-> Result<(), ()>{
    let key = parts.get(1).ok_or(())?;
    let values = parts.get(2..).ok_or(())?;
    let mut lists_map = list_clone.lock().await;
    let list = lists_map.entry(key.clone()).or_insert_with(Vec::new);

    for value in values{
        list.insert(0, value.to_string());
    }
    let resp_output = integer(lists_map.get(&parts[1]).unwrap().len() as i64);
    stream.write_all(resp_output.as_bytes()).await.map_err(|_|())
}

pub async fn handle_llen(stream: &mut TcpStream, parts: &Vec<String>, list_clone: &Arc<Mutex<HashMap<String, Vec<String>>>>)-> Result<(), ()>{
    let lists_map = list_clone.lock().await;
    if let Some(list) = lists_map.get(&parts[1]){
        let resp_output = integer(list.len() as i64);
        return stream.write_all(resp_output.as_bytes()).await.map_err(|_|());
    }else{
        let resp_output = integer(0);
        return stream.write_all(resp_output.as_bytes()).await.map_err(|_|());
    }
}

pub async fn handle_blpop(stream: &mut TcpStream, parts: &Vec<String>, list_clone: &Arc<Mutex<HashMap<String, Vec<String>>>>)-> Result<(), ()>{
    let key = parts[1].to_string();
    if let Ok(timeout) = parts[2].parse::<f64>(){
        if timeout == 0.0 {
            let mut lists_map = list_clone.lock().await;
            let output ;
            if let Some(list) = lists_map.get_mut(&key){
                if !list.is_empty(){
                    let removed = list.remove(0);
                    drop(lists_map);
                    output = [key, removed];
                    let resp_output = bulk_string_array(&output);
                    return stream.write_all(resp_output.as_bytes()).await.map_err(|_| ());
                }else{
                    drop(lists_map);
                    loop{
                        sleep(Duration::from_millis(100)).await;
                        let mut lists_map = list_clone.lock().await;
                        if let Some(list) = lists_map.get_mut(&key) && !list.is_empty(){
                            let removed = list.remove(0);
                            drop(lists_map);
                            output = [key.clone(), removed];
                            let resp_output = bulk_string_array(&output);
                            return stream.write_all(resp_output.as_bytes()).await.map_err(|_| ());
                        }
                        drop(lists_map);
                    }
                }
            }else{
                drop(lists_map);
                loop{
                    sleep(Duration::from_millis(100)).await;
                    let mut lists_map = list_clone.lock().await;
                    if let Some(list) = lists_map.get_mut(&key) && !list.is_empty(){
                        let removed = list.remove(0);
                        drop(lists_map);
                        output = [key.clone(), removed];
                        let resp_output = bulk_string_array(&output);
                        return stream.write_all(resp_output.as_bytes()).await.map_err(|_| ());
                    }
                    drop(lists_map);
                }
            }
        }else {
            let mut remaining_ms = (timeout * 1000.0) as u64;
            let mut found = false;
            let output ;
            let mut resp_output = String::new();
            
            while remaining_ms > 0 {
                let sleep_time = std::cmp::min(100, remaining_ms);
                sleep(Duration::from_millis(sleep_time as u64)).await;
                
                let mut lists_map = list_clone.lock().await;
                if let Some(list) = lists_map.get_mut(&key) && !list.is_empty(){
                    let removed = list.remove(0);
                    drop(lists_map);
                    output = [key, removed];
                    resp_output = bulk_string_array(&output);
                    found = true;
                    break;
                }
                drop(lists_map);
                remaining_ms -= sleep_time;
            }
            
            if !found {
                return stream.write_all(nil_array().as_bytes()).await.map_err(|_| ());
            }else{
                return stream.write_all(resp_output.as_bytes()).await.map_err(|_| ());
            }
        }
    }else{
        return stream.write_all(nil_array().as_bytes()).await.map_err(|_| ());
    }
}

pub async fn handle_lpop(stream: &mut TcpStream, parts: &Vec<String>, list_clone: &Arc<Mutex<HashMap<String, Vec<String>>>>)-> Result<(),()>{
    let mut lists_map = list_clone.lock().await;
    let mut resp_output ;
    if let Some(list) = lists_map.get_mut(&parts[1]){
        let mut output: Vec<&str>;
        if parts.len() >= 4 && let Ok(mut iterations) = parts[3].parse::<i32>(){

            while !list.is_empty()  && iterations != 0{
                let removed = list.remove(0);
                // output.push(&removed);
                iterations = iterations-1;
            }
            // resp_output = bulk_string_array(output);
        }else {
            let removed = list.remove(0);
            resp_output = bulk_string(&removed);
        }
    }else{
        resp_output = nil_bulk().to_string();
    }
    return stream.write_all(nil_array().as_bytes()).await.map_err(|_| ());
}