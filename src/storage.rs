use crate::set::{KeyExpiry, SetArgs};
use crate::storage_result::StorageResult;
use std::collections::HashMap;
use std::ops::Add;
use std::time::{Duration, SystemTime};

#[derive(Debug, PartialEq)]
pub enum StorageValue {
    String(String),
    List(Vec<String>),
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

    // Implement the `set` operation for the storage.
    pub fn set(&mut self, key: String, value: String, args: SetArgs) -> StorageResult<String> {
        let mut data = StorageData::from(value);

        if let Some(value) = args.expiry {
            let expiry = match value {
                KeyExpiry::EX(v) => Duration::from_secs(v),
                KeyExpiry::PX(v) => Duration::from_millis(v),
            };

            data.add_expiry(expiry);
            self.expiry
                .insert(key.clone(), data.creation_time.add(expiry));
        }

        self.store.insert(key, data);

        Ok(String::from("OK"))
    }

    // Implement the `get` operation for the storage.
    pub fn get(&mut self, key: &str) -> StorageResult<Option<StorageValue>> {
        if let Some(&expiry) = self.expiry.get(key) {
            if SystemTime::now() >= expiry {
                self.expiry.remove(key);
                self.store.remove(key);
                return Ok(None);
            }
        }

        match self.store.get(key) {
            Some(StorageData {
                value: StorageValue::String(v),
                creation_time: _,
                expiry: _,
            }) => Ok(Some(StorageValue::String(v.clone()))),
            _ => Ok(None),
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
            .set(String::from("akey"), String::from("avalue"), SetArgs::new())
            .unwrap();

        assert_eq!(output, "OK");
        assert_eq!(storage.store.len(), 1);
        match storage.store.get("akey") {
            Some(value) => assert_eq!(value, &avalue),
            None => panic!(),
        }
    }

    #[test]
    // Test that the function set works as expected
    // when a key is created with passive expiration.
    // This doesn't test the expiry mechanism itself,
    // only that the key is created with the correct
    // parameters.
    fn test_set_value_with_px() {
        let mut storage: Storage = Storage::new();
        let mut avalue = StorageData::from(String::from("avalue"));
        avalue.add_expiry(Duration::from_millis(100));

        let output = storage
            .set(
                String::from("akey"),
                String::from("avalue"),
                SetArgs {
                    expiry: Some(KeyExpiry::PX(100)),
                    existence: None,
                    get: false,
                },
            )
            .unwrap();

        assert_eq!(output, "OK");
        assert_eq!(storage.store.len(), 1);
        match storage.store.get("akey") {
            Some(value) => assert_eq!(value, &avalue),
            None => panic!(),
        }

        storage.expiry.get("akey").unwrap();
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
        assert_eq!(result, Some(StorageValue::String(String::from("avalue"))));
    }

    #[test]
    // Test that the function get works as expected.
    // When a key doesn't exist the output is None, and
    // the storage is left unchanged.
    fn test_get_value_key_does_not_exist() {
        let mut storage: Storage = Storage::new();

        let result = storage.get("akey").unwrap();

        assert_eq!(storage.store.len(), 0);
        assert_eq!(result, None);
    }

    #[test]
    // Test that the function expire_keys removes
    // keys that have an expiry time in the past.
    fn test_expire_keys() {
        let mut storage: Storage = Storage::new();

        storage
            .set(String::from("akey"), String::from("avalue"), SetArgs::new())
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
            .set(String::from("akey"), String::from("avalue"), SetArgs::new())
            .unwrap();

        storage.expiry.insert(
            String::from("akey"),
            SystemTime::now() - Duration::from_secs(5),
        );

        storage.expire_keys();
        assert_eq!(storage.store.len(), 1);
    }
}
