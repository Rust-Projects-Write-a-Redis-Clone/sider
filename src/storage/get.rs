use super::*;

impl Storage {
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
    // Test that the function get works as expected.
    // When a key value is retrieved, the output
    // is the value, and the key is not deleted
    // from the storage.
    fn test_get_value() {
        let mut storage: Storage = Storage::new();
        storage
            .store
            .insert(String::from("akey"), StorageData::string("avalue"));

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
}
