use crate::resp::simple_string;

pub async fn handle_replconf()-> String{
    simple_string("OK")
}