use crate::request::Request;
use crate::resp::Resp;
use crate::server::Server;
use crate::server_result::ServerValue;

pub async fn command(_server: &mut Server, request: &Request, _command: &[String]) {
    let result = ServerValue::Resp(Resp::Integer(42));

    request.data(result).await;
}
