use std::{io::Read, process};

use datastore::{dbms::{HashTableConfig, ManagedHashTable}, hash_table::{HashTable, HashTableEntry, HashTableScanFilter, HashTableScanner}};

pub fn main() {
    let mut config: HashTableConfig = Default::default();
    config.page_size = 64;
    config.section_count = 4;

    let mut hash_table = match ManagedHashTable::open("dev/example-hash-table", config) {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Failed to open hash table: {}", e);
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
        if let Err(e) = hash_table.insert(key.as_bytes(), value.as_bytes()) {
            eprintln!("Failed to insert key-value pair: {}", e);
            process::exit(1);
        }
        println!("Inserted key-value pair: {:?} -> {:?}", key, value);
    } else if args.len() == 2 || args.len() == 1 {
        let mut scanner = match hash_table.scan(match args.get(1) {
            Some(key) => HashTableScanFilter::Key(key.as_bytes()),
            None => HashTableScanFilter::All,
        }) {
            Ok(it) => it,
            Err(e) => {
                eprintln!("Failed to create iterator: {}", e);
                process::exit(1);
            }
        };

        let mut found_any = false;
        let mut key_buf = Vec::new();
        let mut value_buf = Vec::new();
        loop {
            let mut entry = match scanner.next() {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(e) => {
                    eprintln!("Scanner error: {}", e);
                    process::exit(1);
                },
            };
            found_any = true;
            let key = {
                let mut key_reader = match entry.key() {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("Failed to read key: {}", e);
                        process::exit(1);
                    }
                };
                
                key_buf.clear();
                if let Err(e) = key_reader.read_to_end(&mut key_buf) {
                    eprintln!("Failed to read key: {}", e);
                    process::exit(1);
                }
                &key_buf[..]
            };
            let value = {
                let mut value_reader = match entry.value() {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("Failed to read value: {}", e);
                        process::exit(1);
                    }
                };

                value_buf.clear();
                if let Err(e) = value_reader.read_to_end(&mut value_buf) {
                    eprintln!("Failed to read value: {}", e);
                    process::exit(1);
                }
                &value_buf[..]
            };
            println!("Key: {:?}, Value: {:?}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&value));
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

    if let Err(e) = hash_table.save() {
        eprintln!("Failed to save hash table: {}", e);
        process::exit(1);
    }
}
