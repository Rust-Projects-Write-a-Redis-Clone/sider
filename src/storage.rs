use crate::resp::Resp;
use crate::storage_result::{StorageError, StorageResult};
use std::collections::HashMap;

#[derive(Debug, PartialEq)]
pub enum StorageValue {
    String(String),
}

pub struct Storage {
    store: HashMap<String, StorageValue>,
}

impl Storage {
    pub fn new() -> Self {
        let store: HashMap<String, StorageValue> = HashMap::new();

        Self { store }
    }

    // Process an incoming command with its parameters.
    pub fn process_command(&mut self, command: &[String]) -> StorageResult<Resp> {
        match command[0].to_lowercase().as_str() {
            "ping" => self.command_ping(command),
            "echo" => self.command_echo(command),
            "get" => self.command_get(command),
            "set" => self.command_set(command),
            _ => Err(StorageError::CommandNotAvailable(command[0].clone())),
        }
    }

    // Implement the `set` operation for the storage.
    fn set(&mut self, key: String, value: String) -> StorageResult<String> {
        self.store.insert(key, StorageValue::String(value));

        Ok(String::from("OK"))
    }

    // Implement the `get` operation for the storage.
    fn get(&self, key: &str) -> StorageResult<Option<String>> {
        match self.store.get(key) {
            Some(StorageValue::String(v)) => Ok(Some(v.clone())),
            None => Ok(None),
        }
    }

    // The command `PING` responds with a simple string
    // that contains the value `PONG`.
    fn command_ping(&self, _command: &[String]) -> StorageResult<Resp> {
        Ok(Resp::simple_string("PONG"))
    }

    // The command `ECHO` responds with a bulk string
    // that contains the same value passed to `ECHO`.
    fn command_echo(&self, command: &[String]) -> StorageResult<Resp> {
        Ok(Resp::bulk_string(&command[1]))
    }

    // The command `SET` stores the given key and value
    // pair and responds with `OK`.
    fn command_set(&mut self, command: &[String]) -> StorageResult<Resp> {
        // Check the command length. The command
        // requires at least 2 parameters.
        if command.len() != 3 {
            return Err(StorageError::CommandSyntaxError(command.join(" ")));
        }

        // Use the function set to store the key and value pair.
        let _ = self.set(command[1].clone(), command[2].clone());

        Ok(Resp::simple_string("OK"))
    }

    // The command `GET` retrieves the value of the given key
    // and responds with a bulk string that contains it.
    fn command_get(&self, command: &[String]) -> StorageResult<Resp> {
        // Check the command length. The command
        // requires at least 1 parameter.
        if command.len() != 2 {
            return Err(StorageError::CommandSyntaxError(command.join(" ")));
        }

        // Use the function get to retrieve the value of the given key.
        let output = self.get(&command[1]);

        match output {
            // If the key corresponds to a value, return
            // it as a bulk string.
            Ok(Some(v)) => Ok(Resp::BulkString(v)),
            // If the key is not in the storage, return
            // a Resp null value.
            Ok(None) => Ok(Resp::Null),
            Err(_) => Err(StorageError::CommandInternalError(command.join(" "))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // Test that a new storage can be created
    // and that it is empty.
    fn test_create_new() {
        let storage: Storage = Storage::new();

        assert_eq!(storage.store.len(), 0);
    }

    #[test]
    // Test that the function set works as expected.
    // When a key and value pair is stored the
    // output is the value, the storage contains
    // an element, and the value can be retrieved.
    fn test_set_value() {
        let mut storage: Storage = Storage::new();
        let avalue = StorageValue::String(String::from("avalue"));

        let output = storage
            .set(String::from("akey"), String::from("avalue"))
            .unwrap();

        assert_eq!(output, "OK");
        assert_eq!(storage.store.len(), 1);
        match storage.store.get("akey") {
            Some(value) => assert_eq!(value, &avalue),
            None => panic!(),
        }
    }

    #[test]
    // Test that the function get works as expected.
    // When a key value is retrieved, the output
    // is the value, and the key is not deleted
    // from the storage.
    fn test_get_value() {
        let mut storage: Storage = Storage::new();
        storage.store.insert(
            String::from("akey"),
            StorageValue::String(String::from("avalue")),
        );

        let result = storage.get("akey").unwrap();

        assert_eq!(storage.store.len(), 1);
        assert_eq!(result, Some(String::from("avalue")));
    }

    #[test]
    // Test that the function get works as expected.
    // When a key doesn't exist the output is None, and
    // the storage is left unchanged.
    fn test_get_value_key_does_not_exist() {
        let storage: Storage = Storage::new();

        let result = storage.get("akey").unwrap();

        assert_eq!(storage.store.len(), 0);
        assert_eq!(result, None);
    }

    #[test]
    // Test that the storage provides the function
    // command_ping, and that its output is correct.
    // Check the command in lowercase format.
    fn test_command_ping() {
        let command = vec![String::from("ping")];
        let storage: Storage = Storage::new();

        let output = storage.command_ping(&command).unwrap();

        assert_eq!(output, Resp::simple_string("PONG"));
    }

    #[test]
    // Test that the storage provides the function
    // command_ping, and that its output is correct.
    // Check the command in uppercase format.
    fn test_command_ping_uppercase() {
        let command = vec![String::from("PING")];
        let storage: Storage = Storage::new();

        let output = storage.command_ping(&command).unwrap();

        assert_eq!(output, Resp::simple_string("PONG"));
    }

    #[test]
    // Test that the storage provides the function
    // command_echo and that its output is correct.
    fn test_command_echo() {
        let command = vec![String::from("echo"), String::from("42")];
        let storage: Storage = Storage::new();

        let output = storage.command_echo(&command).unwrap();

        assert_eq!(output, Resp::bulk_string("42"));
    }

    #[test]
    // Test that the storage provides the function
    // command_set and that its output is correct.
    fn test_process_command_set() {
        let mut storage: Storage = Storage::new();
        let command = vec![
            String::from("set"),
            String::from("key"),
            String::from("value"),
        ];

        let output = storage.process_command(&command).unwrap();

        assert_eq!(output, Resp::simple_string("OK"));
        assert_eq!(storage.store.len(), 1);
    }

    #[test]
    // Test that the storage provides the function
    // command_get and that its output is correct.
    fn test_process_command_get() {
        let mut storage: Storage = Storage::new();
        storage.store.insert(
            String::from("akey"),
            StorageValue::String(String::from("avalue")),
        );
        let command = vec![String::from("get"), String::from("akey")];

        let output = storage.process_command(&command).unwrap();

        assert_eq!(output, Resp::bulk_string("avalue"));
        assert_eq!(storage.store.len(), 1);
    }
}
