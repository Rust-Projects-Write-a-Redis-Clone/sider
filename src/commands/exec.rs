use crate::request::Request;
use crate::resp::Resp;
use crate::server::Server;
use crate::server_result::ServerValue;

pub async fn command(_server: &Server, request: &Request, _command: &[String]) {
    request
        .data(ServerValue::Resp(Resp::simple_error(
            "ERR EXEC without MULTI",
        )))
        .await;
}
