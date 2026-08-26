use super::*;

impl Storage {
    // Implement the `set` operation for the storage.
    pub fn set(&mut self, key: String, value: String, args: SetArgs) -> StorageResult<String> {
        let mut data = StorageData::string(value);

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // Test that the function set works as expected.
    // When a key and value pair is stored the
    // output is the value, the storage contains
    // an element, and the value can be retrieved.
    fn test_set_value() {
        let mut storage: Storage = Storage::new();
        let avalue = StorageData::string("avalue");

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
        let mut avalue = StorageData::string("avalue");
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
}
