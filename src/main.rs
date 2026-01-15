#![allow(unused_imports)]
use std::{cmp::min, collections::HashMap, env::args, fmt::{Error, format}, io::{Read, Write}, num::ParseIntError, sync::Arc, thread, time::{Duration, SystemTime, UNIX_EPOCH}, u64};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener, sync::{Mutex, broadcast}, time::sleep};
use futures::future::select_all;

use crate::parser::parse_command;

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
            if let Some(parts) = parse_command(&mut stream).await{
                loop {
                    match parts[0].as_str(){
                        "PING" => {
                            if handlers::handle_ping(&mut stream).await.is_err(){
                                break;
                            }
                        },
                        "ECHO" => {
                            if handlers::handle_echo(&mut stream, &parts).await.is_err(){
                                break;
                            }
                        },
                        "SET" => {
                            if handlers::handle_set(&mut stream, &parts, &store_clone).await.is_err(){
                                break;
                            }
                        },
                        "GET" => {
                            if handlers::handle_get(&mut stream, &parts, &store_clone).await.is_err(){
                                break;
                            }
                        },
                        "RPUSH" =>{
                            if handlers::handle_rpush(&mut stream, &parts, &list_clone).await.is_err(){
                                break;
                            }
                        },
                        "LRANGE" =>{
                            if handlers::handle_lrange(&mut stream, &parts, &list_clone).await.is_err(){
                                break;
                            }
                        },
                        "LPUSH" =>{
                            if handlers::handle_lpush(&mut stream, &parts, &list_clone).await.is_err(){
                                break;
                            }
                        },
                        "LLEN" =>{
                            if handlers::handle_llen(&mut stream, &parts, &list_clone).await.is_err(){
                                break;
                            }
                        },
                        "BLPOP" =>{
                            if handlers::handle_blpop(&mut stream, &parts, &list_clone).await.is_err(){
                                break;
                            }
                        },
                        _ => {
                            let _ = stream.write_all(b"-ERR Invalid input").await;
                            break;
                        }
                    }

                    if parts[0] == "LPOP"{
                        let mut lists_map = list_clone.lock().await;
                        let mut output = String::new();
                        if let Some(list) = lists_map.get_mut(&parts[4]){
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
                    }else if parts[0] == "TYPE"{
                        let store_map = store_clone.lock().await;
                        let lists_map = list_clone.lock().await;
                        let streams_map = stream_clone.lock().await;
                        let key: &str = &parts[4];
                        let output;
                        if store_map.contains_key(key){
                            output = format!("+string\r\n");
                        }else if streams_map.contains_key(key){
                            output = format!("+stream\r\n");
                        }else if lists_map.contains_key(key){
                            output = format!("+list\r\n");
                        }else{
                            output = format!("+none\r\n");
                        }
                        if let Err(_e) = stream.write_all(output.as_bytes()).await{
                            break ;
                        }
                    }else if parts[0] == "XADD"{
                        let output;
                        let stream_key = parts[4].to_string();
                        let stream_id = parts[6].to_string();
                        let id_elements: Vec<String> = stream_id.split("-").into_iter().map(|x| x.to_string()).collect();
                        if id_elements.len() > 2 {
                            output = format!("-ERR Invalid ID type\r\n");
                            if let Err(_e) = stream.write_all(output.as_bytes()).await{
                                break ;
                            }
                            continue;
                        }
                        if id_elements.len() == 2 && id_elements[0] == "0" && id_elements[1] ==  "0"{
                            output = format!("-ERR The ID specified in XADD must be greater than 0-0\r\n");
                            if let Err(_e) = stream.write_all(output.as_bytes()).await{
                                break ;
                            }
                            continue;
                        }
                        let mut fields: HashMap<String, String> = HashMap::new();
                        for i in (8..parts.len()).step_by(4){
                            fields.insert(parts[i].to_string(), parts[i+2].to_string());
                        }
                        let mut streams_map = stream_clone.lock().await;
                        if let Some(curr_stream) = streams_map.get_mut(&stream_key) && let Some((prev_stream_id, _)) = curr_stream.last(){
                            let milliseconds_time = if id_elements[0] == "*"{
                                &SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_millis()
                            } else{
                                &id_elements[0].parse::<u128>().unwrap()
                            };
                            let prev_id_elements: Vec<String> = prev_stream_id.split("-").into_iter().map(|x| x.to_string()).collect();
                            let prev_milliseconds_time = &prev_id_elements[0].parse::<u128>().unwrap();
                            let prev_sequence = &prev_id_elements[1].parse::<u128>().unwrap();
                            let sequence = if id_elements.len() > 1 && id_elements[1] != "*"{
                                &id_elements[1].parse::<u128>().unwrap()
                            }else{
                                if prev_milliseconds_time == milliseconds_time{
                                    &(prev_sequence + 1)
                                }else{
                                    &0
                                }
                            };
                            let actual_id = format!("{}-{}", milliseconds_time, sequence);
                            if milliseconds_time < prev_milliseconds_time {
                                output = format!("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
                            }else if milliseconds_time == prev_milliseconds_time{
                                if prev_sequence >= sequence{
                                    output = format!("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
                                }else{
                                    curr_stream.push((actual_id.clone(), fields));
                                    let channels = stream_channels_clone.lock().await;
                                    if let Some(tx) = channels.get(&stream_key){
                                        let _ = tx.send(());
                                    }
                                    drop(channels);
                                    output = format!("${}\r\n{}\r\n", &actual_id.len(), actual_id);
                                }
                            }else {
                                curr_stream.push((actual_id.clone(), fields));
                                let channels = stream_channels_clone.lock().await;
                                if let Some(tx) = channels.get(&stream_key){
                                    let _ = tx.send(());
                                }
                                drop(channels);
                                output = format!("${}\r\n{}\r\n", &actual_id.len(), actual_id);
                            }
                        }else{
                            let milliseconds_time = if id_elements[0] == "*"{
                                &SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_millis()
                            } else{
                                &id_elements[0].parse::<u128>().unwrap()
                            };
                            let sequence = if id_elements.len() > 1 && id_elements[1] != "*" {
                                id_elements[1].parse::<usize>().unwrap()
                            } else {
                                if milliseconds_time == &0 { 1 } else { 0 }
                            };
                            let actual_id = format!("{}-{}", milliseconds_time, sequence);
                            streams_map.entry(stream_key.clone()).or_default().push((actual_id.clone(), fields));
                            drop(streams_map);
                            let channels = stream_channels_clone.lock().await;
                            if let Some(tx) = channels.get(&stream_key){
                                let _ = tx.send(());
                            }
                            drop(channels);
                            output = format!("${}\r\n{}\r\n", actual_id.len(), actual_id);
                        }
                        if let Err(_e) = stream.write_all(output.as_bytes()).await{
                            break ;
                        }
                    }else if parts[0] == "XRANGE"{
                        let mut output;
                        if parts.len() < 9{
                            output = String::from("-ERR Invalid Range\r\n");
                            if let Err(_e) = stream.write_all(output.as_bytes()).await{
                                break ;
                            }
                            continue;
                        }
                        //TODO: Parsing IDs and comparing it in this way works for now. But it will crash eventually.
                        let stream_key = parts[4].to_string();
                        let stream_id_from = parts[6].to_string();
                        let mut stream_id_to = parts[8].to_string();
                        if stream_id_to == "+"{
                            stream_id_to = String::from("~");
                        }
                        let streams_map = stream_clone.lock().await;
                        let mut output_len = 0;
                        output = format!("*{}\r\n", 0);
                        if let Some(stream) = streams_map.get(&stream_key){
                            for (entry_id, entry_map) in stream{
                                if entry_id >= &stream_id_from && entry_id <= &stream_id_to{
                                    output_len = output_len + 1;
                                    output.push_str(&format!("*2\r\n${}\r\n{}\r\n", entry_id.len(), entry_id));
                                    output.push_str(&format!("*{}\r\n",entry_map.len()*2));
                                    for entry in entry_map{
                                        output.push_str(&format!("${}\r\n{}\r\n", entry.0.len(), entry.0));
                                        output.push_str(&format!("${}\r\n{}\r\n", entry.1.len(), entry.1));
                                    }
                                }
                            }
                            output.replace_range(1..2, &output_len.to_string());
                        }
                        if let Err(_e) = stream.write_all(output.as_bytes()).await{
                            break ;
                        }
                    }else if parts[0] == "XREAD"{
                        let base_id = "0-0".to_string();
                        let output ;
                        if parts.len() < 9{
                            output = String::from("-ERR Invalid input");
                            if let Err(_e) = stream.write_all(output.as_bytes()).await{
                                break ;
                            }
                        }
                        let mut parts_idx = 4;
                        let mut block_flag = false;
                        let mut block_millli = Duration::new(0, 0);
                        if parts.len() > parts_idx + 1 && parts[parts_idx].eq_ignore_ascii_case("block"){
                            block_flag = true;
                            if let Ok(timeout) = parts[6].parse::<u64>(){
                                block_millli = Duration::from_millis(timeout);
                            }
                            parts_idx += 4;
                        }
                        if parts.len() <= parts_idx || !parts[parts_idx].eq_ignore_ascii_case("streams"){
                            let output = "-ERR Invalid input\r\n".to_string();
                            let _ = stream.write_all(output.as_bytes()).await;
                            continue;
                        }
                        parts_idx +=2;

                        let remainings_args = parts.len() - parts_idx;
                        if remainings_args % 2 != 0 || remainings_args == 0 {
                            let output = "-ERR Invlid input\r\n".to_string();
                            let _ = stream.write_all(output.as_bytes()).await;
                            continue;
                        }

                        let num_streams = remainings_args/2;
                        let stream_keys: Vec<String> = parts[parts_idx..parts_idx + num_streams].iter().map(|x| x.to_string()).collect();
                        let mut start_ids: Vec<String>= parts[parts_idx + num_streams..].iter().map(|x| x.to_string()).collect();
                        let streams_map = stream_clone.lock().await;
                        let mut all_results = Vec::new();

                        for (stream_key, start_id) in stream_keys.iter().zip(start_ids.iter_mut()){
                            let mut matching_entries = Vec::new();

                            if let Some(streams) = streams_map.get(stream_key){
                                if *start_id == "$"{
                                    if let Some((last_id, _)) = streams.iter().rev().next(){
                                        *start_id = last_id.clone();
                                    }else{
                                        *start_id = base_id.clone();
                                    }
                                }
                                for (entry_id, entry_map) in streams{
                                    if entry_id > start_id && start_id != "$"{
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

                            for (stream_key, start_id) in stream_keys.iter().zip(start_ids.iter()){
                                let mut matching_entries = Vec::new();

                                if let Some(streams) = streams_map.get(stream_key){
                                    for (entry_id, entry_map) in streams{
                                        if entry_id > start_id{
                                            matching_entries.push((entry_id, entry_map));
                                        }
                                    }
                                }

                                if !matching_entries.is_empty(){
                                    new_results.push((stream_key.clone(), matching_entries));
                                }
                            }

                            if new_results.is_empty(){
                                "*-1\r\n".to_string()
                            }else{
                                let mut result = format!("*{}\r\n", new_results.len());

                                for (stream_key, matching_entries) in new_results{
                                    result.push_str(&format!("*2\r\n${}\r\n{}\r\n", stream_key.len(), stream_key));
                                    result.push_str(&format!("*{}\r\n", matching_entries.len()));

                                    for(entry_id, entry_map) in matching_entries{
                                        result.push_str(&format!("*2\r\n${}\r\n{}\r\n", entry_id.len(), entry_id));
                                        result.push_str(&format!("*{}\r\n", entry_map.len() * 2));

                                        for (key, value) in entry_map{
                                            result.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
                                            result.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
                                        }
                                    }
                                }
                                result
                            }


                        }else if all_results.is_empty(){
                            "*-1\r\n".to_string()
                        } else{
                            let mut result = format!("*{}\r\n", all_results.len());

                            for (stream_key, matching_entries) in all_results{
                                result.push_str(&format!("*2\r\n${}\r\n{}\r\n", stream_key.len(), stream_key));
                                result.push_str(&format!("*{}\r\n", matching_entries.len()));

                                for(entry_id, entry_map) in matching_entries{
                                    result.push_str(&format!("*2\r\n${}\r\n{}\r\n", entry_id.len(), entry_id));
                                    result.push_str(&format!("*{}\r\n", entry_map.len() * 2));

                                    for (key, value) in entry_map{
                                        result.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
                                        result.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
                                    }
                                }
                            }
                            result
                        };
                        if let Err(_e) = stream.write_all(output.as_bytes()).await{
                            break ;
                        }
                    }else if parts[0] == "INCR"{
                        let mut store_map = store_clone.lock().await;
                        let output ;
                        if parts.len() < 5{
                            output = "-ERR Invlid input\r\n".to_string();
                            let _ = stream.write_all(output.as_bytes()).await;
                            continue;
                        }
                        let key = &parts[4];
                        if let Some((value, _)) = store_map.get_mut(key){
                            let num = match value.parse::<isize>(){
                                Ok(result) => result,
                                Err(_e) => {
                                    output = "-ERR value is not an integer or out of range\r\n".to_string();
                                    let _ = stream.write_all(output.as_bytes()).await;
                                    continue;
                                },
                            };
                            let num_string = (num + 1).to_string();
                            *value = num_string;
                            output = format!(":{}\r\n", num+1);
                        }else{
                            store_map.insert(parts[4].to_string(), ("1".to_string(), None));
                            output = ":1\r\n".to_string();
                        }
                        let _ = stream.write_all(output.as_bytes()).await;
                    }else if parts[0] == "MULTI"{
                        let output = "+OK\r\n".to_string();
                        let _ = stream.write_all(output.as_bytes()).await;
                    }else if parts[0] == "EXEC"{
                        let output = "-ERR EXEC without MULTI".to_string();
                        let _ = stream.write_all(output.as_bytes());
                    }
                }
            }
        });
    }
}

async fn wait_for_any_receiver(receivers: Vec<broadcast::Receiver<()>>) {
    if receivers.is_empty(){
        return ;
    }
    let futures: Vec<_> = receivers.into_iter().map(|mut rx| Box::pin(async move{rx.recv().await})).collect();

    let _ = select_all(futures).await;
}