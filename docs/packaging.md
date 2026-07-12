# Native packages (.deb / .rpm) & systemd

Pre-built `.deb` and `.rpm` packages let server admins install the bridge as a
native systemd service without Docker. Packages are attached to each
[release](https://nether.codes/dark/nether-voicebridge/releases) and built by
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

## Standalone binary tarball

For distros without `.deb`/`.rpm`, each release also carries self-contained
`nether-voicebridge-<version>-x86_64-linux.tar.gz` and `…-aarch64-linux.tar.gz`
tarballs — the same glibc binaries the Docker images and packages use, bundled
with `config.example.toml`, the systemd unit, `LICENSE`, and the README. A
`SHA256SUMS` file covering every release asset (debs, rpms, tarballs) is attached
too; verify with `sha256sum -c SHA256SUMS --ignore-missing`.

The tarballs are dynamically linked against **glibc 2.36 + libopus** — not
static/musl builds, so they need those runtime libraries and a glibc host. The
end-user install steps are in the README's "Standalone binary" section.

## CI

`.forgejo/workflows/docker.yml` handles both the images and the packages, so the
binary is compiled **once per (tag, arch)**, each natively (no QEMU). On a `v*`
tag push the `image` (amd64) and `image-arm64` jobs build and push the per-arch
Docker images (the sole compiles) and `manifest` merges them into the multi-arch
tag; then the `packages` job — which `needs: manifest` and is tag-gated — pulls
each arch image, copies the binary out of it (`/app/nether-voicebridge` →
`target/release/`), runs `nfpm`, builds the standalone tarball, and after both
arches generates `SHA256SUMS` and uploads all seven assets (2× `.deb`, 2× `.rpm`,
2× `.tar.gz`, `SHA256SUMS`) to the Forgejo Release. It needs a repo Actions secret **`RELEASE_TOKEN`**
— a Forgejo token with `write:repository` scope (the Docker job's package-scoped
`REGISTRY_TOKEN` can't create releases). Cutting a release is then just: bump the
version in `[workspace.package]`, commit, and push a `v<version>` tag.
