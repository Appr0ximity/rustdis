use std::{collections::HashMap, sync::Arc, time::{Duration, SystemTime}};

use tokio::{sync::Mutex, time::sleep};

use crate::resp::{bulk_string, bulk_string_array, integer, nil_array, simple_string};

pub async fn handle_rpush(parts: &Vec<String>, list_clone: &Arc<Mutex<HashMap<String, Vec<String>>>>)-> String{
    let mut lists_map = list_clone.lock().await;
    let mut values: Vec<String> = Vec::new();
    let length = parts.len();
    let mut index = 2;
    while index < length{
        values.push(String::from(&parts[index]));
        index += 1;
    }
    lists_map.entry(parts[1].to_string())
        .or_insert_with(Vec::new)
        .extend(values);

    let output = lists_map.get(&parts[1]).unwrap().len() as i64;
    let resp_output = integer(output);
    resp_output
}

pub async fn handle_lrange(parts: &Vec<String>, list_clone: &Arc<Mutex<HashMap<String, Vec<String>>>>)-> String{
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
            return resp_output
        },
        None => {
            let output = [];
            let resp_output = bulk_string_array(&output);
            return resp_output
        }
    }
}

pub async fn handle_lpush(parts: &Vec<String>, list_clone: &Arc<Mutex<HashMap<String, Vec<String>>>>)-> String{
    let key = parts.get(1).unwrap();
    let values = parts.get(2..).unwrap();
    let mut lists_map = list_clone.lock().await;
    let list = lists_map.entry(key.clone()).or_insert_with(Vec::new);

    for value in values{
        list.insert(0, value.to_string());
    }
    let resp_output = integer(lists_map.get(&parts[1]).unwrap().len() as i64);
    resp_output
}

pub async fn handle_llen(parts: &Vec<String>, list_clone: &Arc<Mutex<HashMap<String, Vec<String>>>>)-> String{
    let lists_map = list_clone.lock().await;
    if let Some(list) = lists_map.get(&parts[1]){
        let resp_output = integer(list.len() as i64);
        return resp_output;
    }else{
        let resp_output = integer(0);
        return resp_output;
    }
}

pub async fn handle_blpop(parts: &Vec<String>, list_clone: &Arc<Mutex<HashMap<String, Vec<String>>>>)-> String{
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
                    return resp_output
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
                            return resp_output
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
                        return resp_output
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
                return nil_array().to_string()
            }else{
                return resp_output
            }
        }
    }else{
        return nil_array().to_string()
    }
}

pub async fn handle_lpop(parts: &Vec<String>, list_clone: &Arc<Mutex<HashMap<String, Vec<String>>>>)-> String{
    let mut lists_map = list_clone.lock().await;
    let resp_output ;
    if let Some(list) = lists_map.get_mut(&parts[1]){
        let mut output = Vec::new();
        if parts.len() >= 3 && let Ok(mut iterations) = parts[2].parse::<i32>(){

            while !list.is_empty()  && iterations != 0{
                let removed = list.remove(0);
                output.push(removed);
                iterations = iterations-1;
            }
            resp_output = bulk_string_array(&output);
        }else {
            let removed = list.remove(0);
            resp_output = bulk_string(&removed);
        }
    }else{
        resp_output = nil_array().to_string();
    }
    return resp_output
}

pub async fn handle_type(
        parts: &Vec<String>,
        store_clone: &Arc<Mutex<HashMap<String, (String, Option<SystemTime>)>>>,
        list_clone: &Arc<Mutex<HashMap<String, Vec<String>>>>,
        stream_clone: &Arc<Mutex<HashMap<String, Vec<(String, HashMap<String, String>)>>>>
    ) -> String {
    let store_map = store_clone.lock().await;
    let lists_map = list_clone.lock().await;
    let streams_map = stream_clone.lock().await;
    let key: &str = &parts[1];
    let output;
    if store_map.contains_key(key){
        output = format!("string");
    }else if streams_map.contains_key(key){
        output = format!("stream");
    }else if lists_map.contains_key(key){
        output = format!("list");
    }else{
        output = format!("none");
    }
    let resp_output = simple_string(&output);
    return resp_output
}