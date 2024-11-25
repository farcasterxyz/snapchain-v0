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
   git checkout 8a9f3702eb41199bc8a7f45139adba233a04744a # Remember to update GitHub workflow when changing
   cd code && cargo build
   ```
2. Then clone the snapchain repo and build it:
   ```
   cd ..
   git clone https://github.com/farcasterxyz/snapchain-v0.git
   cd snapchain-v0
   cargo build
   ```

## Testing

After setting up your Rust toolchain above, you can run tests with:

```
cargo test
```

## Running the Application

For development, you can run multiple nodes by running:
```
make dev
```

These will be configured to communicate with each other.

To query a node, you can run `grpcurl` from within the container:

```
docker compose exec node1 grpcurl -import-path proto -proto proto/rpc.proto list
```

## Clean up

You can remove any cached items by running:

```
make clean
```
