use std::{collections::HashMap, sync::Arc};

use tokio::{io::AsyncWriteExt, net::TcpStream, sync::{Mutex, broadcast}};

use crate::{resp::{error_message, simple_array, simple_string}, run_command};

pub async fn handle_multi(stream: &mut TcpStream) -> Result<(), ()>{
    let output = simple_string("OK");
    return stream.write_all(output.as_bytes()).await.map_err(|_| ());
}

pub async fn handle_exec(
        stream: &mut TcpStream,
        multi_enabled: &mut bool,
        queued_commands: &mut Vec<Vec<String>>,
        store_clone: &Arc<Mutex<std::collections::HashMap<String, (String, Option<std::time::SystemTime>)>>>,
        list_clone: &Arc<Mutex<std::collections::HashMap<String, Vec<String>>>>,
        stream_clone: &Arc<Mutex<std::collections::HashMap<String, Vec<(String, std::collections::HashMap<String, String>)>>>>,
        stream_channels_clone: &Arc<Mutex<std::collections::HashMap<String, broadcast::Sender<()>>>>,
        server_info_clone: &Arc<Mutex<HashMap<String, String>>>
    ) -> Result<(), ()>{
    let output ;
    if *multi_enabled == false{
        output = error_message("ERR EXEC without MULTI");
        return stream.write_all(output.as_bytes()).await.map_err(|_| ());
    }
    let mut output_vec: Vec<String> = Vec::new();
    for queued_command in queued_commands.iter(){
        let cmd = queued_command.get(0).unwrap();
        output_vec.push(run_command(cmd, queued_command, store_clone, list_clone, stream_clone, stream_channels_clone, server_info_clone).await);
    }
    queued_commands.clear();
    output = simple_array(&output_vec);
    *multi_enabled = false;
    return stream.write_all(output.as_bytes()).await.map_err(|_| ());
}

pub async fn handle_discard(stream: &mut TcpStream, multi_enabled: &mut bool, queued_commands: &mut Vec<Vec<String>>)-> Result<(), ()>{
    let output;
    if *multi_enabled == false{
        output = error_message("ERR DISCARD without MULTI");
        return stream.write_all(output.as_bytes()).await.map_err(|_| ());
    }else{
        queued_commands.clear();
        *multi_enabled = false;
        return stream.write_all(simple_string("OK").as_bytes()).await.map_err(|_| ());
    }
}