use crate::storage::Storage;
use crate::storage_result::{StorageError, StorageResult};
use crate::RESP;
use std::sync::{Arc, Mutex};

// Process an incoming request and return a result.
pub fn process_request(request: RESP, storage: Arc<Mutex<Storage>>) -> StorageResult<RESP> {
    // Check if the request is expressed using
    // a RESP array and extract the elements.
    let elements = match request {
        RESP::Array(v) => v,
        _ => {
            return Err(StorageError::IncorrectRequest);
        }
    };

    // The vector that contains all the commands we need to process.
    let mut command = Vec::new();

    // Check that each element of the array is a
    // bulk string, extract the content, and add it
    // to the vector.
    for elem in elements.iter() {
        match elem {
            RESP::BulkString(v) => command.push(v.clone()),
            _ => {
                return Err(StorageError::IncorrectRequest);
            }
        }
    }

    // Acquire a lock on the storage.
    let mut guard = storage.lock().unwrap();

    // Process the command contained in the request.
    let response = guard.process_command(&command);

    // Return the response.
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // Test that the function process_request
    // processes a PING request and that it
    // responds with a PONG.
    fn test_process_request_ping() {
        let request = RESP::Array(vec![RESP::BulkString(String::from("PING"))]);
        let storage = Arc::new(Mutex::new(Storage::new()));

        let output = process_request(request, storage).unwrap();

        assert_eq!(output, RESP::SimpleString(String::from("PONG")));
    }

    #[test]
    // Test that the function process_request
    // returns the correct error when it is
    // given a request that doesn't contain
    // a RESP array.
    fn test_process_request_not_array() {
        let request = RESP::BulkString(String::from("PING"));
        let storage = Arc::new(Mutex::new(Storage::new()));

        let error = process_request(request, storage).unwrap_err();

        assert_eq!(error, StorageError::IncorrectRequest);
    }

    #[test]
    // Test that the function process_request
    // returns the correct error when it is
    // given a request that contains a RESP array
    // but the content of the array is not
    // a bulk string.
    fn test_process_request_not_bulkstrings() {
        let request = RESP::Array(vec![RESP::SimpleString(String::from("PING"))]);
        let storage = Arc::new(Mutex::new(Storage::new()));

        let error = process_request(request, storage).unwrap_err();

        assert_eq!(error, StorageError::IncorrectRequest);
    }

    #[test]
    // Test that the function process_request
    // processes an ECHO request and that it
    // responds with a copy of the input.
    fn test_process_request_echo() {
        let request = RESP::Array(vec![
            RESP::BulkString(String::from("ECHO")),
            RESP::BulkString(String::from("42")),
        ]);
        let storage = Arc::new(Mutex::new(Storage::new()));

        let output = process_request(request, storage).unwrap();

        assert_eq!(output, RESP::BulkString(String::from("42")));
    }
}
