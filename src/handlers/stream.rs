use std::{collections::HashMap, sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}, vec};

use futures::future::select_all;
use tokio::{io::AsyncWriteExt, net::TcpStream, sync::{Mutex, broadcast}};

use crate::{resp::{bulk_string, bulk_string_array, error_message, nil_array}};

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

pub async fn handle_xrange(stream: &mut TcpStream, parts: &Vec<String>, stream_clone: &Arc<Mutex<HashMap<String, Vec<(String, HashMap<String, String>)>>>>)-> Result<(), ()>{
    let mut output: Vec<String> = Vec::new();
    if parts.len() < 4{
        let resp_output = error_message("ERR Invalid Range");
        return stream.write_all(resp_output.as_bytes()).await.map_err(|_| ())
    }

    let stream_key = parts[1].to_string();
    let stream_id_from = parts[2].to_string();
    let mut stream_id_to = parts[3].to_string();
    if stream_id_to == "+"{
        stream_id_to = String::from("~");
    }
    let streams_map = stream_clone.lock().await;
    let mut resp_array  = nil_array().to_string();
    if let Some(stream) = streams_map.get(&stream_key){
        for (entry_id, entry_map) in stream{
            if entry_id >= &stream_id_from && entry_id <= &stream_id_to{    //Currently comparing lexiographically which is not 100% correct. 
                let mut stream_vec = Vec::new();
                stream_vec.push(bulk_string(entry_id));
                let mut elements_vec = Vec::new();
                for entry in entry_map{
                    elements_vec.push(bulk_string(entry.0));
                    elements_vec.push(bulk_string(entry.1));
                }
                stream_vec.push(bulk_string_array(&elements_vec));
                output.push(bulk_string_array(&stream_vec));
            }
        }
        resp_array = bulk_string_array(&output);
    }
    stream.write_all(resp_array.as_bytes()).await.map_err(|_| ())
}

pub async fn handle_xread(
        stream: &mut TcpStream,
        parts: &Vec<String>,
        stream_clone: &Arc<Mutex<HashMap<String, Vec<(String, HashMap<String, String>)>>>>,
        stream_channels_clone: &Arc<Mutex<HashMap<String, broadcast::Sender<()>>>>
    )->Result<(), ()>{
    let base_id = "0-0".to_string();
    let output ;
    if parts.len() < 4{
        output = "ERR Invalid input";
        return stream.write_all(error_message(output).as_bytes()).await.map_err(|_| ());
    }
    let mut parts_idx = 1;
    let mut block_flag = false;
    let mut block_millli = Duration::new(0, 0);
    if parts.len() > parts_idx + 1 && parts[parts_idx].eq_ignore_ascii_case("block"){
        block_flag = true;
        if let Ok(timeout) = parts[parts_idx + 1].parse::<u64>(){
            block_millli = Duration::from_millis(timeout);
        }
        parts_idx += 2;
    }
    if parts.len() <= parts_idx || !parts[parts_idx].eq_ignore_ascii_case("streams"){
        let output = "ERR Invalid input";
        return stream.write_all(error_message(output).as_bytes()).await.map_err(|_|());
    }
    parts_idx += 1;

    let remainings_args = parts.len() - parts_idx;
    if remainings_args % 2 != 0 || remainings_args == 0 {
        let output = "ERR Invlid input";
        return stream.write_all(error_message(output).as_bytes()).await.map_err(|_| ());
    }

    let num_streams = remainings_args/2;
    let stream_keys: Vec<String> = parts[parts_idx..parts_idx + num_streams].iter().map(|x| x.to_string()).collect();
    let mut start_ids: Vec<String>= parts[parts_idx + num_streams..].iter().map(|x| x.to_string()).collect();
    let streams_map = stream_clone.lock().await;
    let mut all_results= Vec::new();

    for (stream_key, start_id) in stream_keys.iter().zip(start_ids.iter_mut()){
        let mut matching_entries = Vec::new();

        if let Some(streams) = streams_map.get(stream_key){
            let actual_start = if *start_id == "$"{
                if let Some((last_id, _)) = streams.iter().rev().next(){
                    last_id.clone()
                }else{
                    base_id.clone()
                }
            }else{
                start_id.clone()
            };
            let start_parsed = parse_stream_id(&actual_start);
            for (entry_id, entry_map) in streams{
                let entry_parsed = parse_stream_id(entry_id);
                if entry_parsed > start_parsed{
                    matching_entries.push((entry_id, entry_map));
                }
            }
        }

        if !matching_entries.is_empty(){
            all_results.push((stream_key.clone(), matching_entries));
        }
    }
    let output = if all_results.is_empty() && block_flag == true{
        drop(streams_map);
        let mut receivers = Vec::new();
        let mut channels_map = stream_channels_clone.lock().await;

        for stream_key in &stream_keys{
            let tx = channels_map.entry(stream_key.clone()).or_insert_with(|| broadcast::channel(100).0);
            receivers.push(tx.subscribe());
        }

        drop(channels_map);

        if block_millli.is_zero(){
            let _ = wait_for_any_receiver(receivers).await;
        }else{
            let _ = tokio::time::timeout(block_millli, wait_for_any_receiver(receivers)).await;
        }

        let streams_map = stream_clone.lock().await;
        let mut new_results = Vec::new();

        for(stream_key, start_id) in stream_keys.iter().zip(start_ids.iter()){
            let mut matching_entries = Vec::new();


            if let Some(streams) = streams_map.get(stream_key){
                let actual_start = if *start_id == "$"{
                    streams.last().map(|(id, _)| id.clone()).unwrap_or(base_id.clone())
                }else{
                    start_id.clone()
                };
                let start_parsed = parse_stream_id(&actual_start);
                for (entry_id, entry_map) in streams{
                    let entry_parsed = parse_stream_id(entry_id);
                    if entry_parsed > start_parsed{
                        matching_entries.push((entry_id, entry_map));
                    }
                }
            }

            if !matching_entries.is_empty(){
                new_results.push((stream_key.clone(), matching_entries));
            }
        }

        xread_result_formatter(&new_results)
    }else if all_results.is_empty(){
        nil_array().to_string()
    } else{
        xread_result_formatter(&all_results)
    };

    stream.write_all(output.as_bytes()).await.map_err(|_| ())
}

async fn wait_for_any_receiver(mut receivers: Vec<broadcast::Receiver<()>>) {
    if receivers.is_empty() {
        return;
    }
    let futures: Vec<_> = receivers.iter_mut().map(|rx| Box::pin(rx.recv())).collect();
    let _ = select_all(futures).await;
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

fn xread_result_formatter(results: &Vec<(String, Vec<(&String, &HashMap<String, String>)>)> )->String{
    if results.is_empty(){
        return nil_array().to_string();
    }
    let mut result_vec: Vec<String> = Vec::new();

    for (stream_key, matching_entries) in results{
        let mut entries_vec= Vec::new();
        for(entry_id, entry_map) in matching_entries{
            let mut fields_vec = Vec::new();
            for entry in *entry_map{
                fields_vec.push(bulk_string(entry.0));
                fields_vec.push(bulk_string(entry.1));
            }
            let entry = vec![bulk_string(entry_id), bulk_string_array(&fields_vec)];
            entries_vec.push(bulk_string_array(&entry));
        }
        let stream_result = vec![bulk_string(stream_key), bulk_string_array(&entries_vec)];
        result_vec.push(bulk_string_array(&stream_result));
    }
    bulk_string_array(&result_vec)
}

fn parse_stream_id(id: &str) -> (u128, u128) {
    let parts: Vec<&str> = id.split('-').collect();
    let ms = parts[0].parse().unwrap_or(0);
    let seq = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    (ms, seq)
}