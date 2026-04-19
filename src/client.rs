pub struct Client {
    pub client_id: u64,
    pub multi: bool,
}

impl Client {
    pub fn new(client_id: u64) -> Self {
        Self {
            client_id: client_id,
            multi: false,
        }
    }
}
