#![allow(unused_imports)]
use std::{io::{Read, Write}, net::TcpListener, thread, time::Duration};

fn main() {
    println!("Logs from your program will appear here!");
    
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        thread::spawn(||{
            match stream {
                Ok(mut stream) => {
                    loop{
                        let mut buffer = [0; 1024];
                        if let Ok(read) = stream.read(&mut buffer){
                            let input = String::from_utf8_lossy(&buffer[..read]); 
                            if let Some(index) = input.find("ECHO"){
                                if let Some(output) = input.get(index+6..){
                                    if let Err(_e) = stream.write_all(output.as_bytes()){
                                        break ;
                                    }
                                }
                            }else if let Some(_index) = input.find("PING"){
                                let output = b"+PONG\r\n";
                                if let Err(_e) = stream.write_all(output){
                                    break ;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        });
    }
}
