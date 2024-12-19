FROM rust:1.83 AS builder

WORKDIR /usr/src/app

# Build Malachite first
ARG MALACHITE_GIT_REPO_URL=git@github.com:farcasterxyz/malachite.git
ENV MALACHITE_GIT_REPO_URL=$MALACHITE_GIT_REPO_URL
ARG MALACHITE_GIT_REF=8a9f3702eb41199bc8a7f45139adba233a04744a
ENV RUST_BACKTRACE=1
RUN --mount=type=ssh <<EOF
set -eu
apt-get update && apt-get install -y libclang-dev git libjemalloc-dev llvm-dev make protobuf-compiler libssl-dev openssh-client
echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config
cd ..
git clone $MALACHITE_GIT_REPO_URL
cd malachite
git checkout $MALACHITE_GIT_REF
cd code
cargo build
EOF

# Unfortunately, we can't prefetch creates without including the source code,
# since the Cargo configuration references files in src.
# This means we'll re-fetch all creates every time the source code changes,
# which isn't ideal.
COPY Cargo.toml build.rs ./
COPY src ./src

ENV RUST_BACKTRACE=full
RUN cargo build --release --bins

## Pre-generate some configurations we can use
# TOOD: consider doing something different here
RUN target/release/setup_local_testnet

#################################################################################

FROM ubuntu:24.04

# Easier debugging within container
ARG GRPCURL_VERSION=1.9.1
ARG TARGETOS
ARG TARGETARCH
RUN <<EOF
  set -eu
  apt-get update && apt-get install -y curl
  curl -L https://github.com/fullstorydev/grpcurl/releases/download/v${GRPCURL_VERSION}/grpcurl_${GRPCURL_VERSION}_${TARGETOS}_${TARGETARCH}.deb > grpcurl.deb
  dpkg -i grpcurl.deb
  rm grpcurl.deb
  apt-get remove -y curl
  apt clean -y
EOF

WORKDIR /app
COPY --from=builder /usr/src/app/src/proto /app/proto
COPY --from=builder /usr/src/app/nodes /app/nodes
COPY --from=builder \
  /usr/src/app/target/release/snapchain \
  /usr/src/app/target/release/follow_blocks \
  /usr/src/app/target/release/setup_local_testnet \
  /usr/src/app/target/release/submit_message \
  /usr/src/app/target/release/perftest \
  /app/

ENV RUSTFLAGS="-Awarnings"
CMD ["./snapchain", "--id", "1"]
