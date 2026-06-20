# syntax=docker/dockerfile:1.7
# -----------------------------------------------------------------------------
# Multi-stage build for gridtokenx-noti-service — distroless final image.
#
# Build CONTEXT is the superproject root (gridtokenx-coresystem), because
# noti-server has path dependencies on sibling crates (gridtokenx-blockchain-core,
# gridtokenx-telemetry). Build from there:
#
#   DOCKER_BUILDKIT=1 docker build \
#     -f gridtokenx-noti-service/Dockerfile \
#     -t gridtokenx-noti-service:latest .
#
# The cargo registry/git caches and target/ live ONLY in BuildKit cache mounts —
# never in an image layer. The final image carries only the stripped binary, its
# non-glibc shared libs (collected into /app/lib), the runtime templates, a static
# busybox (for the healthcheck — distroless has no curl/shell), and CA certs +
# glibc from the distroless/cc base. Migrations are embedded into the binary
# (sqlx::migrate!("../../migrations")), so they are NOT shipped separately.
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Stage 1: Builder
# -----------------------------------------------------------------------------
FROM rust:1.89-bookworm AS builder

# Build toolchain: cmake/clang for librdkafka + zstd (vendored), protobuf for the
# noti-protocol codegen, libssl-dev for the Solana SDK's openssl-sys link.
# busybox-static ships into the runtime image for the healthcheck.
RUN <<EOT
    apt-get update
    apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        libssl-dev \
        cmake \
        clang \
        git \
        protobuf-compiler \
        libprotobuf-dev \
        busybox-static
    rm -rf /var/lib/apt/lists/*
EOT

WORKDIR /app

# Path-dependency crates + the service itself. .dockerignore keeps host target/
# and .git out of the context.
COPY gridtokenx-noti-service/ gridtokenx-noti-service/
COPY gridtokenx-blockchain-core/ gridtokenx-blockchain-core/
COPY gridtokenx-telemetry/ gridtokenx-telemetry/

WORKDIR /app/gridtokenx-noti-service

# Cache mounts: cargo registry/git + target persist across builds in BuildKit's
# cache, never in a layer. Copy the binary out before the RUN ends — the mount
# is not visible to a later COPY. --locked builds against the committed Cargo.lock.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/gridtokenx-noti-service/target,sharing=locked \
    cargo build --release --locked --bin noti-server \
    && strip target/release/noti-server \
    && cp target/release/noti-server /app/noti-server-bin

# Collect the binary + its non-glibc shared libs into a flat lib/ folder.
# glibc core + the dynamic loader come from the distroless/cc base — skip them.
RUN set -eux; \
    BIN=/app/noti-server-bin; \
    mkdir -p /out/lib; \
    cp "$BIN" /out/noti-server; \
    cp /bin/busybox /out/busybox; \
    ldd "$BIN" | awk '/=>/{print $3} !/=>/{print $1}' | grep -E '^/' | sort -u | while read -r lib; do \
        case "$lib" in \
            */ld-linux*|*/libc.so*|*/libm.so*|*/libpthread*|*/libdl.so*|*/librt.so*) continue;; \
        esac; \
        cp -Lv "$lib" /out/lib/; \
    done

# -----------------------------------------------------------------------------
# Stage 2: Runtime (distroless, non-root uid 65532)
# -----------------------------------------------------------------------------
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

WORKDIR /app

# Binary, its lib folder, static busybox (healthcheck), and runtime templates.
# No target artifacts, no migrations (embedded), no source.
COPY --from=builder /out/noti-server /app/noti-server
COPY --from=builder /out/lib/ /app/lib/
COPY --from=builder /out/busybox /usr/bin/busybox
COPY --from=builder /app/gridtokenx-noti-service/templates /app/templates

ENV LD_LIBRARY_PATH=/app/lib

# HTTP (PORT, default 8080) + gRPC/ConnectRPC (PORT+10, default 8090).
EXPOSE 8080 8090

ENTRYPOINT ["/app/noti-server"]
