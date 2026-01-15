pub fn simple_string(s: &str) -> String{
    format!("+{}\r\n", s)
}

pub fn bulk_string(s: &str)->String{
    format!("${}\r\n{}\r\n", s.len(), s)
}

pub fn nil_bulk()-> &'static str{
    "$-1\r\n"
}

pub fn integer(n: i64) -> String{
    format!(":{}\r\n", n)
}

pub fn bulk_string_array(arr: &[String])-> String{
    let mut resp_array = format!("*{}\r\n", arr.len());
    for words in arr{
        resp_array.push_str(&bulk_string(words));
    }
    resp_array
}

pub fn nil_array()-> &'static str{
    "*-1\r\n"
}

pub fn error_message(s: &str)->String{
    format!("-{}\r\n", s)
}