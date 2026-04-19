use crate::request::Request;
use crate::resp::RESP;
use crate::server::Server;
use crate::server_result::ServerValue;

pub async fn command(_server: &Server, request: &Request, _command: &Vec<String>) {
    request
        .data(ServerValue::RESP(RESP::SimpleError(
            "ERR EXEC without MULTI".to_string(),
        )))
        .await;
}
