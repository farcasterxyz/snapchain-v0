# Snapchain v0

Prototype for the snapchain proposal

## Development Notes

🚨 **We are not accepting external PRs at this time** 🚨

Presently, due to our current development velocity, we don't have time to review or discuss external PRs. Any PRs opened 
against the repo will be closed.

We may accept external PRs at a later date, at which point this note will be removed.

Thank you for your interest in the project!

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

For development, you can run multiple nodes by running:
```
docker compose up --build
```

These will be configured to communicate with each other.

To query a node, you can run `grpcurl` from within the container:

```
docker compose exec node1 grpcurl -import-path proto -proto proto/rpc.proto list
```
