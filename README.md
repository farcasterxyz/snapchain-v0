# Snapchain v0

Prototype for the snapchain proposal

## Prerequisites

Before you begin, ensure you have the following installed:
- Rust (latest stable version)
- Cargo (comes with Rust)
- Protocol Buffers compiler (protoc)

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/farcasterxyz/snapchain-v0.git
   cd snapchain-v0
   ```

2. Build the project:
   ```
   cargo build
   ```

## Running the Application

To run a Snapchain node, use the following command:

```
cargo run -- --id 0
```

To run additional nodes:
```
cargo run -- --id 1
cargo run -- --id 2
```