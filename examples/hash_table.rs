use std::{io::Read, process};

use datastore::{dbms::{HashTableConfig, ManagedHashTable}, hash_table::{HashTable, HashTableEntry, HashTableScanFilter, HashTableScanner}};

pub fn main() {
    let mut config: HashTableConfig = Default::default();
    config.page_size = 64;
    config.index_chunk_size = 64;
    config.section_count = 4;

    let mut hash_table = match ManagedHashTable::open("dev/example-hash-table", config) {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Failed to open hash table: {}", e);
            process::exit(1);
        }
    };

    let args: Vec<String> = std::env::args().collect();
    if args.len() == 4 && args[1] == "insert" {
        let key = &args[2];
        let value = &args[3];
        if let Err(e) = hash_table.insert(key.as_bytes(), value.as_bytes()) {
            eprintln!("Failed to insert key-value pair: {}", e);
            process::exit(1);
        }
        println!("Inserted key-value pair: {:?} -> {:?}", key, value);
    } else if (args.len() == 3 && args[1] == "scan-key") || (args.len() == 2 && args[1] == "scan") {
        let mut scanner = match hash_table.scan(match args[1].as_str() {
            "scan-key" => HashTableScanFilter::Key(args[2].as_bytes()),
            "scan" => HashTableScanFilter::All,
            _ => unreachable!(),
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
        println!("  To insert:      {} insert <key> <value>", args[0]);
        println!("  To scan by key: {} scan-key <key>", args[0]);
        println!("  To scan all:    {} scan", args[0]);
        process::exit(1);
    }

    if let Err(e) = hash_table.save() {
        eprintln!("Failed to save hash table: {}", e);
        process::exit(1);
    }
}
