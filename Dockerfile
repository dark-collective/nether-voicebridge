# ── Stage 1: cargo-chef (shared build environment) ───────────────────────────
# Rust pinned (not `rust:1`): a floating tag invalidates every cached layer —
# including the ~20 min dependency build — whenever upstream moves it, and
# keeps release builds reproducible. Bump deliberately.
FROM rust:1.96.0-bookworm AS chef
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
# The [patch.crates-io] songbird fork is a path dependency the recipe can't
# reconstruct — cook needs the real crate present.
COPY vendor vendor
RUN cargo chef cook --release --recipe-path recipe.json

# Compile only the project's own code (deps already compiled above).
COPY . .
RUN cargo build --release --locked -p nether-voicebridge

# ── Static binary (release asset only, not part of the image) ────────────────
# Built only with `--target static` (the release workflow's static jobs export
# it via `--output type=local`); the default target below never runs these.
#
# A fully static glibc executable: +crt-static, non-PIE (glibc's static-pie
# startup crashes with this link), GNU ld instead of rust-lld, glib's whole
# static dependency closure (webrtc-sys links glib/gobject/gio) in one group
# with libc, and packaging/static/static-cc.sh forcing every lib to its .a.
# Runs on musl (Alpine) and old-glibc hosts alike. glibc's NSS still applies
# to getaddrinfo: the built-in `files`/`dns` sources work; other NSS modules
# can't load from a static binary.
FROM chef AS static-builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-dev \
    libffi-dev \
    libpcre2-dev \
    zlib1g-dev \
    libselinux1-dev \
    libsepol-dev \
    libblkid-dev \
    && rm -rf /var/lib/apt/lists/*
# Same flags in both RUNs (cook + final) so the cached dependency layer is
# reused; like `builder`, it's invalidated only by dependency changes.
ENV STATIC_RUSTFLAGS="-C target-feature=+crt-static -C relocation-model=static"
COPY --from=planner /build/recipe.json recipe.json
COPY vendor vendor
RUN RUSTFLAGS="$STATIC_RUSTFLAGS" cargo chef cook --release \
      --target "$(uname -m)-unknown-linux-gnu" --recipe-path recipe.json
COPY . .
# The trailing checks fail the build unless the binary is truly static AND starts.
RUN set -eux; \
    triple="$(uname -m)-unknown-linux-gnu"; \
    export RUSTFLAGS="$STATIC_RUSTFLAGS"; \
    cargo rustc --release --locked -p nether-voicebridge --bin nether-voicebridge \
      --target "$triple" -- \
      -C linker=/build/packaging/static/static-cc.sh -C linker-features=-lld \
      -C "link-args=-Wl,-Bstatic -Wl,--start-group -lgio-2.0 -lgobject-2.0 -lgmodule-2.0 -lglib-2.0 -lffi -lpcre2-8 -lz -lblkid -lselinux -lsepol -lstdc++ -lm -lpthread -ldl -lrt -lc -Wl,--end-group"; \
    cp "target/$triple/release/nether-voicebridge" /nether-voicebridge; \
    if readelf -d /nether-voicebridge | grep -q NEEDED; then echo "not static" >&2; exit 1; fi; \
    cd /tmp && /nether-voicebridge 2>&1 | grep -q "no config.toml found"

FROM scratch AS static
COPY --from=static-builder /nether-voicebridge /nether-voicebridge

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
