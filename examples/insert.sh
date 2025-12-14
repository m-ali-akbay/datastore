#!/bin/bash

cargo run --example hash_table insert foo bar
cargo run --example hash_table insert test-key test-value
cargo run --example hash_table insert foo baz
cargo run --example hash_table insert sample-key sample-value
cargo run --example hash_table insert test-key another-value
cargo run --example hash_table insert sample-key different-value
cargo run --example hash_table insert joo zap
