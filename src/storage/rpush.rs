use super::*;

impl Storage {
    // Implement the `rpush` operation for the storage.
    pub fn rpush(&mut self, key: &str, values: &[String]) -> StorageResult<usize> {
        // Get a mutable reference to the value
        // of the key and act according to its
        // nature.
        let length = match self.store.get_mut(key) {
            // The value is a string, we cannot
            // use rpush on it.
            Some(StorageData {
                value: StorageValue::String(_),
                ..
            }) => return Err(StorageError::WrongType),

            // The value is already a list. Add
            // the new element and return the
            // length of the whole list.
            Some(StorageData {
                value: StorageValue::List(v),
                ..
            }) => {
                // Create an iterator of &String and own each
                // element cloning it into a String.
                v.extend(values.iter().cloned());
                v.len()
            }

            // The key doesn't exist. Create it as
            // a list of a single element and
            // return its length.
            None => {
                self.store
                    .insert(key.to_string(), StorageData::list(values.to_vec()));
                values.len()
            }
        };

        Ok(length)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // Test that the function rpush works as expected
    // when the key doesn't exist.
    // The key is created as a list and the value is
    // added to it.
    fn test_rpush_key_does_not_exist() {
        let mut storage: Storage = Storage::new();
        let avalue = StorageData::list(["avalue"]);

        let output = storage.rpush("akey", &[String::from("avalue")]).unwrap();

        assert_eq!(output, 1);
        assert_eq!(storage.store.len(), 1);
        match storage.store.get("akey") {
            Some(value) => assert_eq!(value, &avalue),
            None => panic!(),
        }
    }

    #[test]
    // Test that the function rpush works as expected
    // when the key exists and is an empty list.
    // The value is added to it.
    fn test_rpush_key_exists_and_is_list_empty() {
        let mut storage: Storage = Storage::new();

        storage.store.insert(
            String::from("akey"),
            StorageData::list(Vec::<String>::new()),
        );

        let list = StorageData::list(["value1"]);

        let output = storage.rpush("akey", &[String::from("value1")]).unwrap();

        assert_eq!(output, 1);
        assert_eq!(storage.store.len(), 1);
        match storage.store.get("akey") {
            Some(value) => assert_eq!(value, &list),
            None => panic!(),
        }
    }

    #[test]
    // Test that the function rpush works as expected
    // when the key exists and is a non-empty list.
    // The value is added to it.
    fn test_rpush_key_exists_and_is_list_not_empty() {
        let mut storage: Storage = Storage::new();

        storage
            .store
            .insert(String::from("akey"), StorageData::list(["value1"]));

        let list = StorageData::list(["value1", "value2"]);

        let output = storage.rpush("akey", &[String::from("value2")]).unwrap();

        assert_eq!(output, 2);
        assert_eq!(storage.store.len(), 1);
        match storage.store.get("akey") {
            Some(value) => assert_eq!(value, &list),
            None => panic!(),
        }
    }

    #[test]
    // Test that the function rpush works as expected
    // when the key exists and multiple values are
    // pushed.
    fn test_rpush_multiple_elements() {
        let mut storage: Storage = Storage::new();

        storage
            .store
            .insert(String::from("akey"), StorageData::list(["value1"]));

        let list = StorageData::list(["value1", "value2", "value3"]);

        let output = storage
            .rpush("akey", &[String::from("value2"), String::from("value3")])
            .unwrap();

        assert_eq!(output, 3);
        assert_eq!(storage.store.len(), 1);
        match storage.store.get("akey") {
            Some(value) => assert_eq!(value, &list),
            None => panic!(),
        }
    }

    #[test]
    // Test that the function rpush
    // returns the correct error when
    // the key exists and is not a list.
    fn test_rpush_key_exists_and_is_not_list() {
        let mut storage: Storage = Storage::new();

        storage
            .store
            .insert(String::from("akey"), StorageData::string("avalue"));

        let error = storage.rpush("akey", &[String::from("value2")]);

        assert_eq!(error, Err(StorageError::WrongType));
    }
}
