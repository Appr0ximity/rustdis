#![allow(unused_imports)]
use std::{io::{Read, Write}, net::TcpListener};

fn main() {
    println!("Logs from your program will appear here!");
    
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut buffer = [0; 1024];
                if let Ok(_read) = stream.read(&mut buffer){
                    stream.write_all(b"+PONG\r\n").unwrap();
                }

            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
