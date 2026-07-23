use crate::set::{KeyExpiry, SetArgs};
use crate::storage_result::{StorageError, StorageResult};
use std::cmp::min;
use std::collections::HashMap;
use std::ops::Add;
use std::time::{Duration, SystemTime};

mod expiry;
mod get;
mod llen;
mod lpop;
mod lpush;
mod lrange;
mod rpush;
mod set;

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

    pub fn string(s: impl Into<String>) -> Self {
        Self::from(StorageValue::String(s.into()))
    }

    pub fn list<I, T>(items: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        Self::from(StorageValue::List(
            items.into_iter().map(Into::into).collect(),
        ))
    }
}

impl From<StorageValue> for StorageData {
    fn from(value: StorageValue) -> Self {
        Self {
            value,
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
}
