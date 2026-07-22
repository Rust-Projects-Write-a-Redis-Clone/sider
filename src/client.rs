use crate::request::Request;

pub struct Client {
    #[expect(dead_code)]
    pub client_id: u64,
    pub requests: Vec<Request>,
    pub multi: bool,
}

impl Client {
    pub fn new(client_id: u64) -> Self {
        Self {
            client_id,
            requests: Vec::new(),
            multi: false,
        }
    }
}
