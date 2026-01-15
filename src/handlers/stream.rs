use std::{collections::HashMap, sync::Arc, time::{SystemTime, UNIX_EPOCH}};

use futures::TryFutureExt;
use tokio::{io::AsyncWriteExt, net::TcpStream, sync::{Mutex, broadcast}};

use crate::resp::{bulk_string, error_message};

pub async fn handle_xadd(
        stream: &mut TcpStream,
        parts: &Vec<String>,
        stream_clone: &Arc<Mutex<HashMap<String, Vec<(String, HashMap<String, String>)>>>>,
        stream_channels_clone: &Arc<Mutex<HashMap<String, broadcast::Sender<()>>>>
    )->Result<(), ()>{

    let output;
    let stream_key = parts[1].to_string();
    let stream_id = parts[2].to_string();
    let id_elements: Vec<String> = stream_id.split("-").into_iter().map(|x| x.to_string()).collect();
    if id_elements.len() > 2 {
        output = format!("ERR Invalid ID type");
        let resp_output = error_message(&output);
        return stream.write_all(resp_output.as_bytes()).await.map_err(|_| ());
    }
    if id_elements.len() == 2 && id_elements[0] == "0" && id_elements[1] ==  "0"{
        output = format!("The ID specified in XADD must be greater than 0-0");
        let resp_output = error_message(&output);
        return stream.write_all(resp_output.as_bytes()).await.map_err(|_| ());
    }
    let mut fields: HashMap<String, String> = HashMap::new();
    for i in (3..parts.len()).step_by(2){
        fields.insert(parts[i].to_string(), parts[i+1].to_string());
    }
    let mut streams_map = stream_clone.lock().await;
    let curr_stream = streams_map.entry(stream_key.clone()).or_default();
    let actual_id = match get_id(&stream_id, curr_stream.last()){
        Ok(id) => id,
        Err (msg) => {
            let resp_output = error_message(&msg);
            return stream.write_all(resp_output.as_bytes()).await.map_err(|_| ())
        }
    };

    curr_stream.push((actual_id.clone(), fields));
    drop(streams_map);

    let stream_channels_map = stream_channels_clone.lock().await;
    if let Some(tx) = stream_channels_map.get(&stream_key){
        let _ = tx.send(());
    }

    let resp = bulk_string(&actual_id);
    stream.write_all(resp.as_bytes()).await.map_err(|_| ())
}

fn get_id (id_str: &str, last_entry: Option<&(String, HashMap<String, String>)>)-> Result<String, String>{
    let parts: Vec<&str> = id_str.split('-').collect();
    let ms_input = parts[0];
    let seq_input = parts.get(1).copied();

    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();

    let ms = if ms_input == "*"{
        now_ms
    }else{
        ms_input.parse().map_err(|_| "ERR Invalid milliseconds".to_string())?
    };

    let seq: u128 = if let Some(s) = seq_input{
        if s == "*"{
            if let Some((prev_id, _)) = last_entry{
                let prev: Vec<&str>= prev_id.split('-').collect();
                let prev_ms = prev[0].parse().unwrap();
                let prev_seq: u128 = prev[1].parse().unwrap();
                if ms == prev_ms { prev_seq + 1}else{ 0 }
            }else{
                if ms == 0 { 1 }else{ 0 }
            }
        }else{
            s.parse().map_err(|_| "ERR Invalid sequence".to_string())?
        }
    }else{
        if ms == 0 { 1 }else { 0 }
    };

    if let Some((prev_id, _)) = last_entry{
        let prev: Vec<&str>= prev_id.split('-').collect();
        let prev_ms: u128 = prev[0].parse().unwrap();
        let prev_seq: u128 = prev[1].parse().unwrap();

        if ms < prev_ms || (ms == prev_ms && seq <= prev_seq){
            return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string())
        }

    }
    Ok(format!("{}-{}", ms, seq))
}