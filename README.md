# Snapchain v0

Prototype for the snapchain proposal

## Prerequisites

Before you begin, ensure you have the following installed:
- Rust (latest stable version)
- Cargo (comes with Rust)
- Protocol Buffers compiler (protoc)

## Installation

1. First clone the malachite repo and checkout the correct commit:
   ```
   git clone git@github.com:informalsystems/malachite.git
   cd malachite
   git checkout 8a9f3702eb41199bc8a7f45139adba233a04744a
   cd code && cargo build
   ```
2. Then clone the snapchain repo and build it:
   ```
   cd ..
   git clone https://github.com/farcasterxyz/snapchain-v0.git
   cd snapchain-v0
   cargo build
   ```

## Running the Application

To run a Snapchain node, use the following command:

First setup the node configs:
```
cargo run --bin setup 
```

Then to run the first node:
```
cargo run -- --id 1
```

To run additional nodes:
```
cargo run -- --id 2
cargo run -- --id 3
```
