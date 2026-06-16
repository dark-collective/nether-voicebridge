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
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/target/release/nether-voicebridge .

# Config file is bind-mounted at runtime (see docker-compose.yml).
# The binary reads CONFIG_PATH, falling back to ./config.toml.
ENV CONFIG_PATH=/app/config.toml

# matrix-sdk SQLite store lives here — must be a named/bind volume so it
# survives container restarts. Set store_path = "/app/data/<name>" in config.toml.
VOLUME ["/app/data"]

ENTRYPOINT ["./nether-voicebridge"]
