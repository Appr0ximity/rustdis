pub fn simple_string(s: &str) -> String{
    format!("+{}\r\n", s)
}

pub fn bulk_string(s: &str)->String{
    format!("${}\r\n{}\r\n", s.len(), s)
}

pub fn nil_bulk()-> &'static str{
    "$-1\r\n"
}