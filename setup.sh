#!/bin/bash

echo "Installing Rust (if not already installed)..."
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env

echo "⚡ Installing DuckDB CLI (optional, for manual queries)..."
# For Windows with chocolatey
# choco install duckdb

# For Linux/WSL
wget https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip
unzip duckdb_cli-linux-amd64.zip
sudo mv duckdb /usr/local/bin/

echo "Building optimized release version..."
cargo build --release

echo "Ready to run! Execute with:"
echo "cargo run --release"
