#!/bin/bash

cargo build --example hash_table --release

./target/release/examples/hash_table insert foo bar
./target/release/examples/hash_table insert test-key test-value
./target/release/examples/hash_table insert foo baz
./target/release/examples/hash_table insert sample-key sample-value
./target/release/examples/hash_table insert test-key another-value
./target/release/examples/hash_table insert sample-key different-value
./target/release/examples/hash_table insert joo zap
