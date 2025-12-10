use std::{io::Read, process};

use datastore::dbms::{KVStore, KVStoreConfig, KVStoreEntryReader, KVStoreIterator};

pub fn main() {
    let mut config: KVStoreConfig = Default::default();
    config.page_count = 4;
    config.block_size = 64;

    let mut kvstore = match KVStore::open("dev/example-kvstore", config) {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Failed to open KVStore: {}", e);
            process::exit(1);
        }
    };

    // arguments:
    // - "key value" to insert key-value pair
    // - "key" to get value for key
    // - no arguments to iterate all key-value pairs
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 3 {
        let key = &args[1];
        let value = &args[2];
        if let Err(e) = kvstore.insert(key.as_bytes(), value.as_bytes()) {
            eprintln!("Failed to insert key-value pair: {}", e);
            process::exit(1);
        }
        println!("Inserted key-value pair: {:?} -> {:?}", key, value);
    } else if args.len() == 2 || args.len() == 1 {
        let mut iter = match kvstore.iter(
            if args.len() == 2 {
                Some(args[1].as_bytes())
            } else {
                None
            }
        ) {
            Ok(it) => it,
            Err(e) => {
                eprintln!("Failed to create iterator: {}", e);
                process::exit(1);
            }
        };

        let mut found_any = false;
        loop {
            match iter.next() {
                Ok(Some(mut entry)) => {
                    found_any = true;
                    let key_buf = {
                        let mut key_reader = match entry.key() {
                            Ok(r) => r,
                            Err(e) => {
                                eprintln!("Failed to read key: {}", e);
                                process::exit(1);
                            }
                        };
                        let mut key_buf = Vec::new();
                        if let Err(e) = key_reader.read_to_end(&mut key_buf) {
                            eprintln!("Failed to read key: {}", e);
                            process::exit(1);
                        }
                        key_buf
                    };
                    let value_buf = {
                        let mut value_reader = match entry.value() {
                            Ok(r) => r,
                            Err(e) => {
                                eprintln!("Failed to read value: {}", e);
                                process::exit(1);
                            }
                        };
                        let mut value_buf = Vec::new();
                        if let Err(e) = value_reader.read_to_end(&mut value_buf) {
                            eprintln!("Failed to read value: {}", e);
                            process::exit(1);
                        }
                        value_buf
                    };
                    println!("Key: {:?}, Value: {:?}", String::from_utf8_lossy(&key_buf), String::from_utf8_lossy(&value_buf));
                },
                Ok(None) => break,
                Err(e) => {
                    eprintln!("Iterator error: {}", e);
                    process::exit(1);
                }
            }
        }
        if !found_any {
            println!("No entries found.");
        }
    } else {
        println!("Usage:");
        println!("  To insert: {} <key> <value>", args[0]);
        println!("  To get:    {} <key>", args[0]);
        println!("  To iterate all: {}", args[0]);
        process::exit(1);
    }
}
