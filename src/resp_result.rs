#[derive(Debug)]
pub enum RespError {}

pub type RespResult<T> = Result<T, RespError>;
