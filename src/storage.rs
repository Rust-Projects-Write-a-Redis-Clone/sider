use crate::resp::Resp;
use crate::storage_result::{StorageError, StorageResult};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

#[derive(Debug, PartialEq)]
pub enum StorageValue {
    String(String),
}

#[derive(Debug)]
pub struct StorageData {
    pub value: StorageValue,
    pub creation_time: SystemTime,
    pub expiry: Option<Duration>,
}

impl StorageData {
    pub fn add_expiry(&mut self, expiry: Duration) {
        self.expiry = Some(expiry);
    }
}

impl From<String> for StorageData {
    fn from(s: String) -> Self {
        Self {
            value: StorageValue::String(s),
            creation_time: SystemTime::now(),
            expiry: None,
        }
    }
}

impl PartialEq for StorageData {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.expiry == other.expiry
    }
}

pub struct Storage {
    store: HashMap<String, StorageData>,
    expiry: HashMap<String, SystemTime>,
    active_expiry: bool,
}

impl Storage {
    pub fn new() -> Self {
        let store: HashMap<String, StorageData> = HashMap::new();

        Self {
            store,
            expiry: HashMap::<String, SystemTime>::new(),
            active_expiry: true,
        }
    }

    // Turns on the storage active expiry.
    pub fn set_active_expiry(&mut self, value: bool) {
        self.active_expiry = value;
    }

    // Check all keys with an expiry time.
    // If the key has expired remove it from the storage.
    pub fn expire_keys(&mut self) {
        // This function works only if active
        // expiry is turned on.
        if !self.active_expiry {
            return;
        }

        // Get the current time.
        let now = SystemTime::now();

        // Filter all expiry keys and find
        // those that expired in the past.
        let expired_keys: Vec<String> = self
            .expiry
            .iter()
            .filter_map(|(key, &value)| if value < now { Some(key.clone()) } else { None })
            .collect();

        // Remove all expired keys from the storage
        // and from the list of expiry keys.
        for k in expired_keys {
            self.store.remove(&k);
            self.expiry.remove(&k);
        }
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
        self.store.insert(key, StorageData::from(value));

        Ok(String::from("OK"))
    }

    // Implement the `get` operation for the storage.
    fn get(&self, key: &str) -> StorageResult<Option<String>> {
        match self.store.get(key) {
            Some(StorageData {
                value: StorageValue::String(v),
                creation_time: _,
                expiry: _,
            }) => Ok(Some(v.clone())),
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
        assert_eq!(storage.expiry.len(), 0);
        assert_eq!(storage.expiry, HashMap::<String, SystemTime>::new());
        assert!(storage.active_expiry);
    }

    #[test]
    // Test that the function set works as expected.
    // When a key and value pair is stored the
    // output is the value, the storage contains
    // an element, and the value can be retrieved.
    fn test_set_value() {
        let mut storage: Storage = Storage::new();
        let avalue = StorageData::from(String::from("avalue"));

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
            StorageData::from(String::from("avalue")),
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
            StorageData::from(String::from("avalue")),
        );
        let command = vec![String::from("get"), String::from("akey")];

        let output = storage.process_command(&command).unwrap();

        assert_eq!(output, Resp::bulk_string("avalue"));
        assert_eq!(storage.store.len(), 1);
    }

    #[test]
    // Test that the function expire_keys removes
    // keys that have an expiry time in the past.
    fn test_expire_keys() {
        let mut storage: Storage = Storage::new();

        storage
            .set(String::from("akey"), String::from("avalue"))
            .unwrap();

        storage.expiry.insert(
            String::from("akey"),
            SystemTime::now() - Duration::from_secs(5),
        );

        storage.expire_keys();
        assert_eq!(storage.store.len(), 0);
    }

    #[test]
    // Test that the function expire_keys doesn't remove
    // keys that have an expiry time in the past.
    // if active expiry is turned off.
    fn test_expire_keys_deactivated() {
        let mut storage: Storage = Storage::new();
        storage.set_active_expiry(false);

        storage
            .set(String::from("akey"), String::from("avalue"))
            .unwrap();

        storage.expiry.insert(
            String::from("akey"),
            SystemTime::now() - Duration::from_secs(5),
        );

        storage.expire_keys();
        assert_eq!(storage.store.len(), 1);
    }
}
