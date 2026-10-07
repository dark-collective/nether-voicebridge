# Native packages (.deb / .rpm) & systemd

Pre-built `.deb` and `.rpm` packages let server admins install the bridge as a
native systemd service without Docker. Packages are attached to each
[release](https://nether.codes/nether/nether-voicebridge/releases) and built by
[`nfpm`](https://nfpm.goreleaser.com/) from a single spec, `packaging/nfpm.yaml`.

## What the package installs

| Path | Purpose |
|---|---|
| `/usr/bin/nether-voicebridge` | the binary |
| `/usr/lib/systemd/system/nether-voicebridge.service` | systemd unit |
| `/etc/nether-voicebridge/config.example.toml` | documented example config (a conffile) |
| `/var/lib/nether-voicebridge/` | matrix-sdk store (Olm crypto + state); created on first start |

A dedicated unprivileged **`nether-voicebridge`** system user/group is created by
the post-install script and owns the state dir. Runtime dependencies are declared
per format (`libopus0` + `ca-certificates` on Debian; `opus` + `ca-certificates`
on Fedora/RHEL).

The unit sets `CONFIG_PATH=/etc/nether-voicebridge/config.toml` and
`NVB_DATA_DIR=/var/lib/nether-voicebridge`, runs as the service user with a set of
systemd hardening directives (`ProtectSystem=strict`, `PrivateTmp`,
`NoNewPrivileges`, …), and stops cleanly: the binary drains on both SIGINT and
SIGTERM, and the unit gives it up to `TimeoutStopSec=15` to leave Discord voice and
retract ghost `m.call.member` before systemd escalates.

## Install & run

```bash
# pick your architecture: amd64/x86_64 or arm64/aarch64
sudo apt install ./nether-voicebridge_<version>_amd64.deb      # Debian/Ubuntu
sudo dnf install ./nether-voicebridge-<version>-1.x86_64.rpm   # Fedora/RHEL

sudo cp /etc/nether-voicebridge/config.example.toml /etc/nether-voicebridge/config.toml
sudo chown root:nether-voicebridge /etc/nether-voicebridge/config.toml
sudo chmod 640 /etc/nether-voicebridge/config.toml             # holds secrets
sudoedit /etc/nether-voicebridge/config.toml

sudo systemctl enable --now nether-voicebridge
journalctl -u nether-voicebridge -f
```

Removing the package stops and disables the unit. On a Debian **purge** the service
user is removed too; the `/var/lib/nether-voicebridge` store is **left in place** in
both cases (it holds Olm crypto keys) — delete it manually if you really mean to.

## Building the packages yourself

Requires a Rust toolchain, the native build deps (`cmake clang libclang-dev
libopus-dev protobuf-compiler libssl-dev pkg-config`), and `nfpm` on `PATH`.

```bash
packaging/build-packages.sh            # version read from Cargo.toml
NVB_VERSION=0.2.0 packaging/build-packages.sh   # or pin it explicitly
# -> dist/nether-voicebridge_0.2.0_amd64.deb
#    dist/nether-voicebridge-0.2.0-1.x86_64.rpm
```

The script builds the release binary, then runs `nfpm` from the repo root (all
paths in `packaging/nfpm.yaml` are root-relative). It packages for the host's
architecture (override with `NVB_ARCH=amd64|arm64`); CI builds both arches per
release, each natively, mirroring the multi-arch Docker image.

## Standalone binaries: tarball, static tarball, AppImage

For distros without `.deb`/`.rpm`, each release carries three more per-arch
(`x86_64` / `aarch64`) downloads. All three bundle `config.example.toml`, the
systemd unit, `LICENSE`, and the README (the AppImage under
`usr/share/doc/nether-voicebridge/`):

| Asset | Binary | Needs on the host |
|---|---|---|
| `nether-voicebridge-<version>-<arch>-linux.tar.gz` | the same glibc binary as the Docker image and packages | glibc ≥ 2.35, libstdc++ from GCC ≥ 12 |
| `nether-voicebridge-<version>-<arch>-linux-static.tar.gz` | **fully static** (no shared libraries, no ELF interpreter) | nothing; runs on Alpine/musl and old-glibc hosts (verified on Alpine 3 and CentOS 7) |
| `nether-voicebridge-<version>-<arch>.AppImage` | the image's glibc binary in a single self-mounting file | same as the tarball, plus FUSE (or run it with `--appimage-extract-and-run`) |

All of them need CA certificates (`ca-certificates`) for TLS to the homeserver and
Discord. Libopus is compiled in, so no system libopus is needed. A `SHA256SUMS`
file covering every release asset is attached too; verify with
`sha256sum -c SHA256SUMS --ignore-missing`.

**Static binary notes.** It's built separately from the image's binary (Dockerfile
`static` target): glibc `+crt-static`, non-PIE, linked by GNU ld with glib's
full static dependency closure (webrtc-sys links glib/gobject/gio), and
`packaging/static/static-cc.sh` forcing every library to its `.a`. The build
fails unless the result has no `NEEDED` entries and starts. Hostname lookups use
glibc's built-in `files` and `dns` sources (`/etc/hosts`, `/etc/resolv.conf`);
other NSS modules from `/etc/nsswitch.conf` (e.g. `resolve`, `mdns`) can't load
into a static binary. It's ~98 MiB unpacked (~40 MB compressed).

**AppImage notes.** `packaging/build-appimage.sh <binary> <arch> <version> <outdir>`
wraps any built binary; it fetches appimagetool and the type2 runtime, both pinned
by version and sha256, and needs `file(1)`. It's a CLI daemon packaged as an
AppImage: run it from a terminal like the plain binary (`CONFIG_PATH` /
`./config.toml` resolve against your working directory).

The end-user install steps are in the README's "Standalone binary" section.

## CI

`.forgejo/workflows/docker.yml` builds the images and every release asset. On a
`v*` tag:

1. **`image`** (amd64) and **`image-arm64`** build natively, in parallel: each
   pushes its per-arch Docker image (the one compile the packages reuse), then
   builds the fully static binary (Dockerfile `static` target, a deliberately
   separate compile) and attaches its `-linux-static.tar.gz` to the Release,
   which is created as a **draft** so nothing is public yet.
2. **`manifest`** merges the two images into the multi-arch tag.
3. **`packages`** pulls each arch image, copies the binary out
   (`/app/nether-voicebridge` → `target/release/`), builds the `.deb`, `.rpm`,
   tarball and AppImage per arch, uploads them, writes `SHA256SUMS` over all
   assets, and **publishes** the release.

That's 13 assets per release (2× `.deb`, 2× `.rpm`, 4× `.tar.gz`, 2×
`.AppImage`, `SHA256SUMS`). Uploads go through `packaging/publish-release.sh`
(create-or-reuse the draft, replace a same-named asset, `--publish` at the
end), so re-runs are idempotent. On `workflow_dispatch` the image jobs build
`:edge` images and the static binaries (self-checked) without releasing.

**Build caching.** Both image jobs keep their BuildKit layer cache between
runs, so a release whose dependencies are unchanged (`Cargo.lock`; cargo-chef
ignores our own version bumps) recompiles only our crates. Both build on
their runner host's Docker daemon's own builder (`driver: docker`), whose
cache persists across jobs (no cache export/import step). The Rust base image is pinned (`rust:1.96.0-bookworm`) because a
moving `rust:1` would invalidate everything; bump it deliberately.

The workflow needs a repo Actions secret **`RELEASE_TOKEN`** — a Forgejo token
with `write:repository` scope (the Docker job's package-scoped `REGISTRY_TOKEN`
can't create releases). Cutting a release is then just: bump the version in
`[workspace.package]`, commit, and push a `v<version>` tag.
