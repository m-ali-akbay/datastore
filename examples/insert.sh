#!/bin/bash

cargo run --example hash_table foo bar
cargo run --example hash_table test-key test-value
cargo run --example hash_table foo baz
cargo run --example hash_table sample-key sample-value
cargo run --example hash_table test-key another-value
cargo run --example hash_table foo qux
cargo run --example hash_table sample-key different-value
