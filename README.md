# Snapchain v0

Prototype for the snapchain proposal

## Prerequisites
- `Rust` (latest stable version) + `Cargo`
- Protocol Buffers compiler (`protoc`)
- `Docker` + `Docker Compose` (for containerized run)
- `malachite` (commit 8a9f3702eb41199bc8a7f45139adba233a04744a)   
   ```bash
   git clone git@github.com:informalsystems/malachite.git
   cd malachite
   git checkout 8a9f3702eb41199bc8a7f45139adba233a04744a # Remember to update GitHub workflow when changing
   cd code && cargo build
   ```

## Running the Application
There are two ways to run the application:

### 1. Using Docker (recommended)
- For development, you can run multiple nodes by running the below command and they will be configured to communicate with each other
   ```bash
   make dev        
   ```
- To query a node, you can run `grpcurl` from within the container:
   ```bash
   docker compose exec node1 grpcurl -import-path proto -proto proto/rpc.proto list
   ```

### 2. Local build without Docker
- Setup dependencies and build
   ```bash
   make local-build   
   ```

## Other useful commands
- Remove all artifacts and containers
   ```bash
   make clean  
   ```
- For all available commands, run:
   ```bash
   make help
   ```