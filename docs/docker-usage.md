# Running with Docker

### Prerequisites

[Docker](https://docs.docker.com/get-docker/) with the Compose plugin (`docker compose version` to verify).

### 1. Create your config file

```bash
cp config.example.toml config.toml
```

Fill in `config.toml` with your credentials. See the comments in `config.example.toml` for where to find each value. **Do not commit `config.toml`** — it contains bot tokens and Matrix access tokens.

You do **not** need to set `store_path`. The image sets `NVB_DATA_DIR=/app/data`, so any bridge that omits `matrix.store_path` gets `/app/data/<bridge>` automatically — landing on the persistent volume (below) with a distinct path per bridge. Only set `store_path` explicitly if you want a custom location.

`matrix_domain` (or `NETHER_MATRIX_DOMAIN`) is an optional field that sets the
public Matrix domain (server name, e.g. `nether.im`). The bridge uses it to build
the bot's user ID from `sender_localpart`, and to fetch
`https://<domain>/.well-known/matrix/client` for MatrixRTC/LiveKit discovery.
When omitted, the bridge uses the host of `homeserver_url` for both. Set it
whenever the two differ, e.g. `homeserver_url = "https://matrix.example.org"`
for server name `example.org`, or an internal container address such as
`http://synapse:8008`. Otherwise the bot ID lands outside the appservice
namespace (`M_EXCLUSIVE`) and LiveKit discovery fails.

Environment variables (`NETHER_*`) only configure a single fixed bridge, used
when no config file is mounted, and that mode has no management bot. For
self-service linking, mount a `config.toml` with a `[server.<name>]` profile, a
`[management]` block and no `[[bridge]]` blocks.

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

### Diagnostics (#32: Discord→Matrix audio corrosion)

The Discord→Matrix audio path ships two tiers of diagnostics for chasing
intermittent corruption (issue #32).

**Always on — no flag.** Drop counters, a per-ghost health line (~every 5 s), and
frame-shape warnings (short PLC / non-1920-sample frames) are compiled in and always
emitted as `tracing` logs. The only knob is verbosity — raise `RUST_LOG` (see
[Log verbosity](#log-verbosity) above) and read the health lines. Start here.

**Opt-in — WAV taps (`capture_wav`, default off).** For a live A/B bisection, enable
the per-bridge `capture_wav` flag, which writes two raw 48 kHz mono WAVs:

- **Tap A** — `capture_<ssrc>.wav`: what *arrived* from Discord, at ingress.
- **Tap B** — `publish_<key>.wav`: the exact mono PCM we *hand to LiveKit*, at the
  publish boundary.

How to read the pair: if Tap A and Tap B both sound clean but Element Call is
garbled, the fault is **downstream** of our audio core (Opus encode / frame E2EE),
not in the bridge's reframing/mixing. If Tap B is already corrupt, the problem is in
our core.

Enable it via the bridge's Discord section in `config.toml`:

```toml
[bridge.discord]
capture_wav = true
```

…or, in Docker, via the equivalent environment variable (applies to all bridges):

```yaml
environment:
  NETHER_DISCORD_CAPTURE_WAV: "true"
```

**Caveats — leave this off in normal production:**

- Files are written with **bare relative filenames** to the process working
  directory, which in the image is `/app` — **not** the `/app/data` volume. They are
  therefore ephemeral: retrieve them with `docker compose cp` / `docker compose exec`
  while the container is up, or they vanish on `docker compose down`.
- The files grow **unbounded** for the whole session (headers are finalized on a
  clean Ctrl-C / SIGTERM shutdown).
- Filenames are **not** bridge-prefixed, so do not enable `capture_wav` on two
  bridges that might see the same Discord SSRC at the same time — they would collide.

### Multi-architecture builds

The published images and release artifacts cover **amd64 and arm64**: each release
tag is a multi-arch manifest (`docker pull` resolves your architecture
automatically), and the `.deb`/`.rpm`/tarball assets ship in both arches. To build
your own multi-arch image and push it to a registry:

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
`nether.codes` container registry on `v*` tags (and on-demand `:edge` builds via
workflow dispatch). It needs a Forgejo runner with a `docker` label and a
reachable Docker daemon (socket-mounted into the job container), plus a second
runner labeled `linux-arm64` that builds the arm64 image **natively** (no QEMU);
a `manifest` job merges both into the multi-arch tag. The first build is heavy —
the WebRTC download plus a full release compile — a cold amd64 build is ~12 min.

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
