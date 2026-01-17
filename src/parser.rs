use tokio::net::TcpStream;
use tokio::io::AsyncReadExt;


pub async fn parse_command(stream: &mut TcpStream) -> Option<Vec<String>>{
    let mut buffer = Vec::new();
    loop{
        let mut tmp = [0u8; 4096];
        let n = match stream.read(&mut tmp).await {
            Ok(0) => break,
            Ok(n) => n ,
            Err(e) => {
                eprintln!("Error while reading from socket: {:?}", e);
                return None;
            },
        };
        buffer.extend_from_slice(&tmp[..n]);
        if buffer_is_complete(&buffer){
            break;
        }
    }
    let input = String::from_utf8_lossy(&buffer);
    let parts: Vec<String> = input.split("\r\n").into_iter().skip(2).step_by(2).map(|x| x.to_string()).collect();
    if parts.is_empty(){
        None
    }else{
        Some(parts)
    }
}

fn buffer_is_complete(buffer: &[u8]) -> bool {
    let temp_input = String::from_utf8_lossy(&buffer);
    let parts: Vec<&str> = temp_input.split("\r\n").collect();
    if parts.is_empty() || !parts[0].starts_with('*'){
        return false;
    }
    if let Ok(num) = parts[0][1..].parse::<usize>(){
        return parts.len() == (num+1)*2
    }
    false
}