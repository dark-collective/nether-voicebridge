# Running with Docker

### Prerequisites

[Docker](https://docs.docker.com/get-docker/) with the Compose plugin (`docker compose version` to verify).

### 1. Create your config file

```bash
cp config.example.toml config.toml
```

Fill in `config.toml` with your credentials. See the comments in `config.example.toml` for where to find each value. **Do not commit `config.toml`** — it contains bot tokens and Matrix access tokens.

You do **not** need to set `store_path`. The image sets `NVB_DATA_DIR=/app/data`, so any bridge that omits `matrix.store_path` gets `/app/data/<bridge>` automatically — landing on the persistent volume (below) with a distinct path per bridge. Only set `store_path` explicitly if you want a custom location.

### 2. Build the image

```bash
docker compose build
```

> **Note:** The first build downloads a pre-built WebRTC library (~400–600 MB) required by the LiveKit SDK. Subsequent builds are fast — the dependency layer is cached and only rebuilt when `Cargo.toml` or `Cargo.lock` change.

### 3. Run

```bash
docker compose up -d
docker compose logs -f
```

`config.toml` is mounted read-only into the container. The matrix-sdk store (session state, crypto) is written to a named Docker volume (`voicebridge-data`) and persists across restarts.

### 4. Stop

```bash
docker compose down
```

To also delete the persisted store (you will need to re-authenticate the bridge bot):

```bash
docker compose down -v
```

### Log verbosity

Set `RUST_LOG` in `docker-compose.yml` to increase detail when diagnosing issues:

```yaml
environment:
  RUST_LOG: "nether_voicebridge=debug,info"
```

### Multi-architecture builds

To build for both `linux/amd64` and `linux/arm64` and push to a registry:

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --push \
  -t yourregistry/nether-voicebridge:latest \
  .
```

A plain `docker compose build` builds only for the host architecture.

## CI image builds (Forgejo Actions)

`.forgejo/workflows/docker.yml` builds and pushes the image to the
`nether.codes` container registry on every push to `master` and on `v*` tags. It
needs a Forgejo runner with a `docker` label and a reachable Docker daemon
(socket-mounted into the job container). The first build is heavy — the WebRTC
download plus a full release compile, doubled for the emulated arm64 leg.

### Limiting runner resources

A heavy build can starve the host the runner shares. Cap it with either (or both)
of these levers:

- **Hard ceiling on the runner container** (e.g. an Incus/LXC container) — protects
  the host regardless of what the runner does:

  ```bash
  incus config set <runner-container> limits.cpu 4
  incus config set <runner-container> limits.memory 8GiB
  # softer alternative to pinning whole cores:
  #   incus config set <runner-container> limits.cpu.allowance 50%
  ```

- **Per-job-container cap** in the runner's `config.yml` (docker-mode runners),
  then restart the runner:

  ```yaml
  container:
    options: "--cpus=4 --memory=8g"
  ```
