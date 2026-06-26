# ── Stage 1: cargo-chef (shared build environment) ───────────────────────────
FROM rust:1-bookworm AS chef
RUN cargo install cargo-chef --locked
WORKDIR /build

# Native build dependencies:
#   cmake + clang + libclang-dev: required by livekit (WebRTC C++ bindings via bindgen)
#   libopus-dev: audio codec, linked dynamically (libopus0 in runtime stage)
#   protobuf-compiler: required by livekit/prost
#   libssl-dev: build-time header dep for some transitive crates (rustls is used at runtime)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    clang \
    libclang-dev \
    libopus-dev \
    pkg-config \
    protobuf-compiler \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# ── Stage 2: dependency recipe ────────────────────────────────────────────────
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ── Stage 3: build all external dependencies (cached layer) ──────────────────
# This layer is invalidated only when Cargo.toml / Cargo.lock change.
# Note: livekit downloads a pre-built libwebrtc (~400–600 MB) on first build.
FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Compile only the project's own code (deps already compiled above).
COPY . .
RUN cargo build --release --locked -p nether-voicebridge

# ── Stage 4: minimal runtime image ───────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libopus0 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/target/release/nether-voicebridge .

# Config file is bind-mounted at runtime (see docker-compose.yml).
# The binary reads CONFIG_PATH, falling back to ./config.toml.
ENV CONFIG_PATH=/app/config.toml

# Base dir for each bridge's auto-derived matrix-sdk store path. With this set,
# bridges that omit `matrix.store_path` get `/app/data/<bridge>` automatically —
# no per-bridge store config needed. Paired with the VOLUME below so the stores
# (Olm crypto + state) survive container restarts. An explicit `store_path` in
# config.toml still overrides this.
ENV NVB_DATA_DIR=/app/data

# matrix-sdk SQLite store lives here — must be a named/bind volume so it
# survives container restarts.
VOLUME ["/app/data"]

ENTRYPOINT ["./nether-voicebridge"]
