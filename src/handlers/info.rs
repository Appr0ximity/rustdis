use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;

use crate::resp::{bulk_string};

pub async fn handle_info(server_info_clone: &Arc<Mutex<HashMap<String, String>>>, parts: &Vec<String> )-> String{
    let mut output: String;
    let server_info_map = server_info_clone.lock().await;
    let default = String::new();
    let role = server_info_map.get("replicaof").unwrap_or(&default);
    if role.is_empty(){
        output = "role:master".to_string();
    }else{
        output = "role:slave".to_string();
    }
    if parts.len() > 1 && parts[1] == "replication".to_string(){
        let repl_id = server_info_map.get("master_replid").unwrap();
        output.push_str(&("\r\n".to_string() + "master_replid:"+ repl_id));
        let repl_offset = server_info_map.get("master_repl_offset").unwrap();
        output.push_str(&("\r\n".to_string() + "master_repl_offset:" + repl_offset));
    }
    bulk_string(&output)
}