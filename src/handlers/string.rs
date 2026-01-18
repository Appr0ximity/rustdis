use std::{collections::HashMap, sync::Arc, time::{Duration, SystemTime}};

use tokio::sync::Mutex;

use crate::resp::{bulk_string, error_message, integer, nil_bulk, simple_string};

 

pub async fn handle_ping()-> String{
    simple_string("PONG")
}

pub async fn handle_echo(parts: &Vec<String>) -> String{
    let mut output= String::new();
    for words in parts.iter().skip(1){
        output.push_str(&words);
    }
    let resp_output = bulk_string(&output);
    resp_output
}

pub async fn handle_set(parts: &Vec<String>, store_clone: &Arc<Mutex<HashMap<String, (String, Option<SystemTime>)>>>) -> String{
    let key = parts.get(1).unwrap();
    let value= parts.get(2).unwrap();

    if parts.len() >= 5 && parts[3].eq_ignore_ascii_case("px"){
        let ms = parts[4].parse().map_err(|_| ()).unwrap();
        let mut map = store_clone.lock().await;
        let expiry = SystemTime::now() + Duration::from_millis(ms);
        map.insert(key.to_string(), (value.to_string(), Some(expiry)));
    }else if parts.len() >= 3{
        let mut map = store_clone.lock().await;
        map.insert(key.to_string(), (value.to_string(), None));
    }else{
        return error_message("ERR Invalid Input format");
    }
    let resp_output = simple_string("OK");
    resp_output
}

pub async fn handle_get(parts: &Vec<String>, store_clone: &Arc<Mutex<HashMap<String, (String, Option<SystemTime>)>>>)->String{
    let key = &parts[1];
    let output;

    let mut map = store_clone.lock().await;
    if let Some((value, expiry)) = map.get(key){
        if let Some(exp_time) = expiry{
            if *exp_time < SystemTime::now(){
                map.remove(key);
                return nil_bulk().to_string();
            }
        }
        output = value.clone();
    }else{
        return nil_bulk().to_string();
    }
    let resp_output = bulk_string(&output);
    resp_output
}

pub async fn handle_incr(parts: &Vec<String>, store_clone: &Arc<Mutex<HashMap<String, (String, Option<SystemTime>)>>>)-> String{
    let mut store_map = store_clone.lock().await;
    let output ;
    if parts.len() < 2{
        output = error_message("ERR Invlid input");
        return output;
    }
    let key = &parts[1];
    if let Some((value, _)) = store_map.get_mut(key){
        let num = match value.parse::<isize>(){
            Ok(result) => result,
            Err(_e) => {
                output = error_message("ERR value is not an integer or out of range");
                return output;
            },
        };
        let num_string = (num + 1).to_string();
        *value = num_string;
        output = integer((num+1) as i64);
    }else{
        store_map.insert(key.to_string(), ("1".to_string(), None));
        output = integer(1 as i64);
    }
    return output;
}