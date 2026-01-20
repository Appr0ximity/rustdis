use std::{collections::HashMap, fs::read, sync::Arc};

use tokio::sync::Mutex;

use crate::resp::simple_string;

pub async fn handle_replconf()-> String{
    simple_string("OK")
}

pub async fn handle_psync(server_info_clone: &Arc<Mutex<HashMap<String, String>>>)->String{
    let server_info_map = server_info_clone.lock().await;
    let fullresync;
    let repl_id = server_info_map.get("master_replid").unwrap();
    fullresync = "FULLRESYNC ".to_string() + repl_id + " 0";
    let rdb_bytes = read("empty.rdb").expect("Failed to read empty.rdb");
    let mut output = simple_string(&fullresync);
    output.push_str(&format!("${}\r\n", rdb_bytes.len()));
    unsafe {
        output.push_str(&String::from_utf8_unchecked(rdb_bytes));
    }
    output
}