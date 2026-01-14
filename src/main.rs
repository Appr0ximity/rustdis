#![allow(unused_imports)]
use std::{cmp::min, collections::HashMap, env::args, fmt::{Error, format}, io::{Read, Write}, num::ParseIntError, sync::Arc, thread, time::{Duration, SystemTime, UNIX_EPOCH}, u64};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener, sync::{Mutex, broadcast}, time::sleep};
use futures::future::select_all;

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
        let stream_clone = streams.clone();
        let stream_channels_clone = stream_channels.clone();
        tokio::spawn(async move {
            let mut buf = [0;1024];

            'shell: loop {
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
                }else if let Some(_index) = input.find("BLPOP"){
                    let key = parts[4].to_string();
                    if let Ok(timeout) = parts[6].parse::<f64>(){
                        if timeout == 0.0 {
                            let mut lists_map = list_clone.lock().await;
                            let output ;
                            if let Some(list) = lists_map.get_mut(&key){
                                if !list.is_empty(){
                                    let removed = list.remove(0);
                                    drop(lists_map);
                                    output = format!("*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n", &key.len(), key, &removed.len(), removed);
                                    if let Err(_e) = stream.write_all(output.as_bytes()).await{
                                        break ;
                                    }
                                }else{
                                    drop(lists_map);
                                    loop{
                                        sleep(Duration::from_millis(100)).await;
                                        let mut lists_map = list_clone.lock().await;
                                        if let Some(list) = lists_map.get_mut(&key) && !list.is_empty(){
                                            let removed = list.remove(0);
                                            let key = &key.to_string();
                                            drop(lists_map);
                                            output = format!("*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n", &key.len(), key, &removed.len(), removed);
                                            if let Err(_e) = stream.write_all(output.as_bytes()).await{
                                                break 'shell;
                                            }
                                            break ;
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
                                        let key = &key.to_string();
                                        drop(lists_map);
                                        output = format!("*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n", &key.len(), key, &removed.len(), removed);
                                        if let Err(_e) = stream.write_all(output.as_bytes()).await{
                                            break 'shell;
                                        }
                                        break ;
                                    }
                                    drop(lists_map);
                                }
                            }
                        }else {
                            let mut remaining_ms = (timeout * 1000.0) as u64;
                            let mut found = false;
                            
                            while remaining_ms > 0 {
                                let sleep_time = std::cmp::min(100, remaining_ms);
                                sleep(Duration::from_millis(sleep_time as u64)).await;
                                
                                let mut lists_map = list_clone.lock().await;
                                if let Some(list) = lists_map.get_mut(&key) && !list.is_empty(){
                                    let removed = list.remove(0);
                                    drop(lists_map);
                                    let output = format!("*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n", 
                                        key.len(), key, removed.len(), removed);
                                    if let Err(_e) = stream.write_all(output.as_bytes()).await{
                                        break 'shell;
                                    }
                                    found = true;
                                    break;
                                }
                                drop(lists_map);
                                remaining_ms -= sleep_time;
                            }
                            
                            if !found {
                                let output = b"*-1\r\n";
                                if let Err(_e) = stream.write_all(output).await{
                                    break 'shell;
                                }
                            }
                        }
                    }else{
                        let output = format!("*-1\r\n");
                        if let Err(_e) = stream.write_all(output.as_bytes()).await{
                            break ;
                        }
                    }
                }
                else if let Some(_index) = input.find("LPOP"){
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
                }else if let Some(_index) = input.find("TYPE"){
                    let store_map = store_clone.lock().await;
                    let lists_map = list_clone.lock().await;
                    let streams_map = stream_clone.lock().await;
                    let key: &str = parts[4];
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
                }else if let Some(_index) = input.find("XADD"){
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
                }else if let Some(_index) = input.find("XRANGE"){
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
                }else if let Some(_index) = input.find("XREAD"){
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