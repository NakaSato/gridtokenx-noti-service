# -----------------------------------------------------------------------------
# Stage 1: Builder
# -----------------------------------------------------------------------------
FROM rust:1.89-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    cmake \
    clang \
    git \
    curl \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the whole project to maintain structure for sqlx migrations
COPY gridtokenx-noti-service/ gridtokenx-noti-service/

WORKDIR /app/gridtokenx-noti-service

# Build in release mode
# Note: we use --bin noti-server which is defined in crates/noti-server
RUN cargo build --release --bin noti-server

# Strip binary to reduce size
RUN strip target/release/noti-server

# -----------------------------------------------------------------------------
# Stage 2: Runtime
# -----------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    tzdata \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -g 1000 appgroup && \
    useradd -u 1000 -g appgroup -s /bin/sh appuser

WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /app/gridtokenx-noti-service/target/release/noti-server /app/noti-server

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
