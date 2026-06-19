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
sudo apt install ./nether-voicebridge_<version>_amd64.deb     # Debian/Ubuntu
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
paths in `packaging/nfpm.yaml` are root-relative). amd64 only for now, mirroring the
Docker image.

## CI

`.forgejo/workflows/docker.yml` handles both the image and the packages, so the
binary is compiled **once** per tag. On a `v*` tag push the `image` job builds and
pushes the Docker image (the sole compile), then the `packages` job — which
`needs: image` and is tag-gated — pulls that image, copies the binary out of it
(`/app/nether-voicebridge` → `target/release/`), runs `nfpm`, and uploads both
packages to the Forgejo Release. It needs a repo Actions secret **`RELEASE_TOKEN`**
— a Forgejo token with `write:repository` scope (the Docker job's package-scoped
`REGISTRY_TOKEN` can't create releases). Cutting a release is then just: bump the
version in `[workspace.package]`, commit, and push a `v<version>` tag.
