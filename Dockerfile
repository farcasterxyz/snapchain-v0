# Use the base image with Rust
FROM rust:1.82 AS builder

WORKDIR /usr/src/app

# Arguments for the Malachite repository
ARG MALACHITE_GIT_REPO_URL=git@github.com:farcasterxyz/malachite.git
ARG MALACHITE_GIT_REF=8a9f3702eb41199bc8a7f45139adba233a04744a

# Install dependencies for Malachite
RUN --mount=type=ssh <<EOF
set -eu
# Install required packages
apt-get update && apt-get install -y \
    libclang-dev \
    git \
    libjemalloc-dev \
    llvm-dev \
    make \
    protobuf-compiler \
    libssl-dev \
    openssh-client \
  && rm -rf /var/lib/apt/lists/*  # Clean up apt cache to reduce image size

# Clone the Malachite repository
echo "Cloning Malachite repository..."
cd ..
git clone $MALACHITE_GIT_REPO_URL
cd malachite
git checkout $MALACHITE_GIT_REF
cd code
cargo build
EOF

# Copy files for Rust project build
COPY Cargo.toml build.rs ./
COPY src ./src

# Build the project with dependency caching
RUN cargo build --release

# Generate configuration files (presumably for snapchain)
RUN target/release/setup --propose-value-delay=250ms

#################################################################################

# Create final image based on Ubuntu
FROM ubuntu:24.04

# Install gRPC client for debugging
ARG GRPCURL_VERSION=1.9.1
RUN set -eu \
  && apt-get update && apt-get install -y curl \
  && curl -L https://github.com/fullstorydev/grpcurl/releases/download/v${GRPCURL_VERSION}/grpcurl_${GRPCURL_VERSION}_linux_arm64.deb > grpcurl.deb \
  && dpkg -i grpcurl.deb \
  && rm grpcurl.deb \
  && apt-get remove -y curl \
  && apt-get clean -y \
  && rm -rf /var/lib/apt/lists/*  # Remove unnecessary files to reduce image size

WORKDIR /app

# Copy necessary files from the builder image
COPY --from=builder /usr/src/app/src/proto /app/proto
COPY --from=builder /usr/src/app/nodes /app/nodes
COPY --from=builder /usr/src/app/target/release/snapchain /app/

# Set Rust flags to suppress warnings
ENV RUSTFLAGS="-Awarnings"

# Run snapchain with ID 1
CMD ["./snapchain", "--id", "1"]
