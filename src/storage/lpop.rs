use super::*;

impl Storage {
    // Implement the `lpop` operation for the storage.
    pub fn lpop(&mut self, key: &str, num_items: i64) -> StorageResult<Vec<String>> {
        // Get a mutable reference to the value
        // of the key and act according to its
        // nature.
        let item = match self.store.get_mut(key) {
            // The value is a string, we cannot
            // use lpop on it.
            Some(StorageData {
                value: StorageValue::String(_),
                ..
            }) => return Err(StorageError::WrongType),

            // The value is an empty list.
            // Return an empty vector.
            Some(StorageData {
                value: StorageValue::List(v),
                ..
            }) if v.is_empty() => vec![],

            // The value is a list.
            // Return the requested elements.
            Some(StorageData {
                value: StorageValue::List(v),
                ..
            }) => {
                // Make sure the provided number of
                // items can be actually fetched.
                let max = (num_items.max(0) as usize).min(v.len());

                // Extract the items.
                v.drain(0..max).collect()
            }

            // The key doesn't exist.
            // Return an empty vector.
            None => vec![],
        };

        Ok(item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // Test that the function lpop works as expected
    // when the key doesn't exist.
    fn test_lpop_key_does_not_exist() {
        let mut storage: Storage = Storage::new();

        let output = storage.lpop("akey", 1).unwrap();

        assert_eq!(output, Vec::<String>::new());
        assert_eq!(storage.store.len(), 0);
    }

    #[test]
    // Test that the function lpop works as expected
    // when the key exists and is a list.
    // We pop zero items.
    fn test_lpop_key_exists_and_is_list_pop_zero_items() {
        let mut storage: Storage = Storage::new();

        storage.store.insert(
            String::from("akey"),
            StorageData::list(["value0", "value1"]),
        );

        let output = storage.lpop("akey", 0).unwrap();

        assert_eq!(output, Vec::<String>::new());
        assert_eq!(storage.store.len(), 1);
        match storage.store.get("akey") {
            Some(value) => assert_eq!(value, &StorageData::list(["value0", "value1"])),
            None => panic!(),
        }
    }

    #[test]
    // Test that the function lpop works as expected
    // when the key exists and is a list.
    // We pop a single item.
    fn test_lpop_key_exists_and_is_list_pop_single_item() {
        let mut storage: Storage = Storage::new();

        storage.store.insert(
            String::from("akey"),
            StorageData::list(["value0", "value1"]),
        );

        let output = storage.lpop("akey", 1).unwrap();

        assert_eq!(output, vec![String::from("value0")]);
        assert_eq!(storage.store.len(), 1);
        match storage.store.get("akey") {
            Some(value) => assert_eq!(value, &StorageData::list(["value1"])),
            None => panic!(),
        }
    }

    #[test]
    // Test that the function lpop works as expected
    // when the key exists and is a list.
    // We pop multiple items.
    fn test_lpop_key_exists_and_is_list_pop_multiple_items() {
        let mut storage: Storage = Storage::new();

        storage.store.insert(
            String::from("akey"),
            StorageData::list(["value0", "value1", "value2"]),
        );

        let output = storage.lpop("akey", 2).unwrap();

        assert_eq!(output, vec![String::from("value0"), String::from("value1")]);
        assert_eq!(storage.store.len(), 1);
        match storage.store.get("akey") {
            Some(value) => assert_eq!(value, &StorageData::list(["value2"])),
            None => panic!(),
        }
    }

    #[test]
    // Test that the function lpop works as expected
    // when the key exists and is an empty list.
    fn test_lpop_key_exists_and_is_empty() {
        let mut storage: Storage = Storage::new();

        storage
            .store
            .insert(String::from("akey"), StorageData::list(["value0"]));

        // Pop the only value.
        let _ = storage.lpop("akey", 1).unwrap();
        let output = storage.lpop("akey", 1).unwrap();

        assert_eq!(output, Vec::<String>::new());
        assert_eq!(storage.store.len(), 1);
        match storage.store.get("akey") {
            Some(value) => assert_eq!(value, &StorageData::list::<[_; 0], String>([])),
            None => panic!(),
        }
    }

    #[test]
    // Test that the function lpop
    // returns the correct error when
    // the key exists and is not a list.
    fn test_lpop_key_exists_and_is_not_list() {
        let mut storage: Storage = Storage::new();

        storage
            .store
            .insert(String::from("akey"), StorageData::string("avalue"));

        let error = storage.lpop("akey", 1);

        assert_eq!(error, Err(StorageError::WrongType));
    }
}
