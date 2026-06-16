This is a stub for the README.md when created - use as a template for the docker section of the README

## Running with Docker

### Prerequisites

[Docker](https://docs.docker.com/get-docker/) with the Compose plugin (`docker compose version` to verify).

### 1. Create your config file

```bash
cp config.example.toml config.toml
```

Fill in `config.toml` with your credentials. See the comments in `config.example.toml` for where to find each value. **Do not commit `config.toml`** — it contains bot tokens and Matrix access tokens.

One setting requires a change from the example defaults: every `[[bridge]]` block has a `store_path` that defaults to a relative path (`./nether-store/…`). Inside the container, relative paths are lost on restart. Set each one to an absolute path under `/app/data/`:

```toml
[bridge.matrix]
# ...
store_path = "/app/data/general"   # use a unique name per bridge
```

If you have multiple bridges, give each a distinct path:

```toml
# first [[bridge]]
store_path = "/app/data/main"

# second [[bridge]]
store_path = "/app/data/gaming"
```

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
