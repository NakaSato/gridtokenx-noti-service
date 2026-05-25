# syntax=docker/dockerfile:1
# -----------------------------------------------------------------------------
# Stage 1: Builder
# -----------------------------------------------------------------------------
FROM rust:1.89-bookworm AS builder

# Install build dependencies with cache mount
RUN --mount=type=cache,target=/var/lib/apt/lists <<EOT
    apt-get update
    apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        libssl-dev \
        cmake \
        clang \
        git \
        curl \
        protobuf-compiler
EOT

WORKDIR /app

# Copy dependency manifests and project structure
COPY gridtokenx-noti-service/ gridtokenx-noti-service/
COPY gridtokenx-blockchain-core/ gridtokenx-blockchain-core/

WORKDIR /app/gridtokenx-noti-service

# Build in release mode with cargo cache mounts
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/gridtokenx-noti-service/target \
    cargo build --release --bin noti-server && \
    strip target/release/noti-server && \
    cp target/release/noti-server /app/noti-server-bin

# -----------------------------------------------------------------------------
# Stage 2: Runtime
# -----------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN --mount=type=cache,target=/var/lib/apt/lists <<EOT
    apt-get update
    apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
        tzdata \
        curl
EOT

# Create non-root user
RUN <<EOT
    groupadd -g 1000 appgroup
    useradd -u 1000 -g appgroup -s /bin/sh appuser
EOT

WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /app/noti-server-bin /app/noti-server

# Copy assets
COPY --from=builder /app/gridtokenx-noti-service/templates /app/templates
COPY --from=builder /app/gridtokenx-noti-service/migrations /app/migrations

# Set permissions
RUN chown -R appuser:appgroup /app

USER appuser

# Expose ports (HTTP: 8080, gRPC: 8090)
EXPOSE 8080 8090

# Run the binary
ENTRYPOINT ["/app/noti-server"]
