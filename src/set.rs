use crate::storage_result::{StorageError, StorageResult};

#[derive(Debug, PartialEq)]
pub enum KeyExistence {
    NX,
    XX,
}

#[derive(Debug, PartialEq)]
pub enum KeyExpiry {
    EX(u64),
    PX(u64),
}

#[derive(Debug, PartialEq)]
pub struct SetArgs {
    pub expiry: Option<KeyExpiry>,
    pub existence: Option<KeyExistence>,
    pub get: bool,
}

impl SetArgs {
    pub fn new() -> Self {
        Self {
            expiry: None,
            existence: None,
            get: false,
        }
    }
}

// Parse the arguments passed to the command SET and
// collect them into a SetArgs struct.
pub fn parse_set_arguments(arguments: &[String]) -> StorageResult<SetArgs> {
    // Create a new SetArgs struct.
    let mut args = SetArgs::new();

    // An index to keep track of the argument we processed.
    let mut idx: usize = 0;

    // Loop through all arguments.
    loop {
        // If we processed all arguments stop the loop.
        if idx >= arguments.len() {
            break;
        }

        // Process the current argument.
        match arguments[idx].to_lowercase().as_str() {
            "nx" => {
                // NX and XX are mutually exclusive.
                if args.existence == Some(KeyExistence::XX) {
                    return Err(StorageError::CommandSyntaxError(arguments.join(" ")));
                }

                args.existence = Some(KeyExistence::NX);

                idx += 1;
            }
            "xx" => {
                // XX and NX are mutually exclusive.
                if args.existence == Some(KeyExistence::NX) {
                    return Err(StorageError::CommandSyntaxError(arguments.join(" ")));
                }

                args.existence = Some(KeyExistence::XX);

                idx += 1;
            }
            "get" => {
                args.get = true;

                idx += 1;
            }
            "ex" => {
                // EX and PX are mutually exclusive.
                if let Some(KeyExpiry::PX(_)) = args.expiry {
                    return Err(StorageError::CommandSyntaxError(arguments.join(" ")));
                }

                // EX requires an argument, check that it is present.
                if idx + 1 == arguments.len() {
                    return Err(StorageError::CommandSyntaxError(arguments.join(" ")));
                }

                // The argument passed to EX must be a u64.
                let value: u64 = arguments[idx + 1]
                    .parse()
                    .map_err(|_| StorageError::CommandSyntaxError(arguments.join(" ")))?;

                args.expiry = Some(KeyExpiry::EX(value));

                idx += 2;
            }
            "px" => {
                // PX and EX are mutually exclusive.
                if let Some(KeyExpiry::EX(_)) = args.expiry {
                    return Err(StorageError::CommandSyntaxError(arguments.join(" ")));
                }

                // PX requires an argument, check that it is present.
                if idx + 1 == arguments.len() {
                    return Err(StorageError::CommandSyntaxError(arguments.join(" ")));
                }

                // The argument passed to PX must be a u64.
                let value: u64 = arguments[idx + 1]
                    .parse()
                    .map_err(|_| StorageError::CommandSyntaxError(arguments.join(" ")))?;

                args.expiry = Some(KeyExpiry::PX(value));

                idx += 2;
            }
            _ => {
                return Err(StorageError::CommandSyntaxError(arguments.join(" ")));
            }
        }
    }

    Ok(args)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // Check that the function parse_set_arguments
    // processes the arguments NX correctly.
    fn test_parse_nx() {
        let commands: Vec<String> = vec![String::from("NX")];

        let args = parse_set_arguments(&commands).unwrap();

        assert_eq!(args.existence, Some(KeyExistence::NX));
    }

    #[test]
    // Check that the function parse_set_arguments
    // processes the arguments NX correctly (lowercase)
    fn test_parse_nx_lowercase() {
        let commands: Vec<String> = vec![String::from("nx")];

        let args = parse_set_arguments(&commands).unwrap();

        assert_eq!(args.existence, Some(KeyExistence::NX));
    }

    #[test]
    // Check that the function parse_set_arguments
    // processes the arguments XX correctly.
    fn test_parse_xx() {
        let commands: Vec<String> = vec![String::from("XX")];

        let args = parse_set_arguments(&commands).unwrap();

        assert_eq!(args.existence, Some(KeyExistence::XX));
    }

    #[test]
    // Check that the function parse_set_arguments
    // returns the correct error when we pass XX and
    // NX together.
    fn test_parse_xx_and_nx() {
        let commands: Vec<String> = vec![String::from("XX"), String::from("NX")];

        assert!(matches!(
            parse_set_arguments(&commands),
            Err(StorageError::CommandSyntaxError(_))
        ));
    }

    #[test]
    // Check that the function parse_set_arguments
    // returns the correct error when we pass XX and
    // NX together. (reverse order)
    fn test_parse_nx_and_xx() {
        let commands: Vec<String> = vec![String::from("NX"), String::from("XX")];

        assert!(matches!(
            parse_set_arguments(&commands),
            Err(StorageError::CommandSyntaxError(_))
        ));
    }

    #[test]
    // Check that the function parse_set_arguments
    // processes the arguments GET correctly.
    fn test_parse_get() {
        let commands: Vec<String> = vec![String::from("GET")];

        let args = parse_set_arguments(&commands).unwrap();

        assert!(args.get);
    }

    #[test]
    // Check that the function parse_set_arguments
    // behaves correctly when we pass NX and GET.
    fn test_parse_nx_and_get() {
        let commands: Vec<String> = vec![String::from("NX"), String::from("GET")];

        let args = parse_set_arguments(&commands).unwrap();

        assert_eq!(
            args,
            SetArgs {
                existence: Some(KeyExistence::NX),
                expiry: None,
                get: true
            }
        );
    }

    #[test]
    // Check that the function parse_set_arguments
    // behaves correctly when we pass XX and GET.
    fn test_parse_xx_and_get() {
        let commands: Vec<String> = vec![String::from("XX"), String::from("GET")];

        let args = parse_set_arguments(&commands).unwrap();

        assert_eq!(
            args,
            SetArgs {
                existence: Some(KeyExistence::XX),
                expiry: None,
                get: true
            }
        );
    }

    #[test]
    // Check that the function parse_set_arguments
    // processes the arguments EX correctly.
    fn test_parse_ex() {
        let commands: Vec<String> = vec![String::from("EX"), String::from("100")];

        let args = parse_set_arguments(&commands).unwrap();

        assert_eq!(args.expiry, Some(KeyExpiry::EX(100)));
    }

    #[test]
    // Check that the function parse_set_arguments
    // returns the correct error when the argument
    // of EX is not a u64.
    fn test_parse_ex_wrong_value() {
        let commands: Vec<String> = vec![String::from("EX"), String::from("value")];

        assert!(matches!(
            parse_set_arguments(&commands),
            Err(StorageError::CommandSyntaxError(_))
        ));
    }

    #[test]
    // Check that the function parse_set_arguments
    // returns the correct error when EX doesn't
    // have an argument.
    fn test_parse_ex_end_of_vector() {
        let commands: Vec<String> = vec![String::from("EX")];

        assert!(matches!(
            parse_set_arguments(&commands),
            Err(StorageError::CommandSyntaxError(_))
        ));
    }

    #[test]
    // Check that the function parse_set_arguments
    // processes the arguments PX correctly.
    fn test_parse_px() {
        let commands: Vec<String> = vec![String::from("PX"), String::from("100")];

        let args = parse_set_arguments(&commands).unwrap();

        assert_eq!(args.expiry, Some(KeyExpiry::PX(100)));
    }

    #[test]
    // Check that the function parse_set_arguments
    // returns the correct error when the argument
    // of PX is not a u64.
    fn test_parse_px_wrong_value() {
        let commands: Vec<String> = vec![String::from("PX"), String::from("value")];

        assert!(matches!(
            parse_set_arguments(&commands),
            Err(StorageError::CommandSyntaxError(_))
        ));
    }

    #[test]
    // Check that the function parse_set_arguments
    // returns the correct error when PX doesn't
    // have an argument.
    fn test_parse_px_end_of_vector() {
        let commands: Vec<String> = vec![String::from("PX")];

        assert!(matches!(
            parse_set_arguments(&commands),
            Err(StorageError::CommandSyntaxError(_))
        ));
    }

    #[test]
    // Check that the function parse_set_arguments
    // returns the correct error when we pass EX and
    // PX together.
    fn test_parse_ex_and_px() {
        let commands: Vec<String> = vec![
            String::from("EX"),
            String::from("100"),
            String::from("PX"),
            String::from("100"),
        ];

        assert!(matches!(
            parse_set_arguments(&commands),
            Err(StorageError::CommandSyntaxError(_))
        ));
    }
}
