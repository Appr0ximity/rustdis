use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;

use crate::resp::simple_string;

pub async fn handle_replconf()-> String{
    simple_string("OK")
}

pub async fn handle_psync(server_info_clone: &Arc<Mutex<HashMap<String, String>>>)->String{
    let server_info_map = server_info_clone.lock().await;
    let output;
    let repl_id = server_info_map.get("master_replid").unwrap();
    output = "FULLRESYNC ".to_string() + repl_id + " 0";
    simple_string(&output)
}