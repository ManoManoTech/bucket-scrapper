FROM 304971447450.dkr.ecr.eu-west-3.amazonaws.com/public/library/rust:1.88.0-slim-bullseye AS builder

# Install build dependencies for aws-lc-rs
RUN apt-get update && apt-get install -y --no-install-recommends \
  cmake \
  clang \
  pkg-config \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy manifests first for dependency caching
COPY Cargo.toml Cargo.lock ./

# Create dummy src to build dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build dependencies only (cached layer)
RUN cargo build --release && rm -rf src target/release/deps/log_consolidator*

# Copy actual source code
COPY src ./src

# Build the real application
RUN cargo build --release

# Runtime stage
FROM debian:bullseye-slim

# Create non-root user
RUN useradd --create-home --user-group log-consolidator-checker-rs

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/log-consolidator-checker-rust .

# Set ownership
RUN chown -R log-consolidator-checker-rs:log-consolidator-checker-rs /app

USER log-consolidator-checker-rs

ENTRYPOINT ["./log-consolidator-checker-rust"]
