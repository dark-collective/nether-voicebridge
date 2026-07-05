# arm64 CI runner setup (Oracle Always-Free Ampere)

How to stand up the native arm64 Forgejo Actions runner that
`.forgejo/workflows/docker.yml` targets with the `docker-arm64` label
(issue #49). With it, releases publish a **multi-arch image**
(`linux/amd64` + `linux/arm64`) and arm64 `.deb`/`.rpm`/tarball assets —
compiled natively, no QEMU (the emulated leg was unusably slow, which is why
arm64 was dropped originally).

The moving parts, and why they're shaped this way:

- **Both runners are equal peers.** The existing amd64 runner (Incus container,
  label `docker`) builds `…:<tag>-amd64`; the new arm64 VM (label
  `docker-arm64`) builds `…:<tag>-arm64` in parallel. A `manifest` job then
  stitches the two into the public tags (`:vX.Y.Z`, `:latest`, `:edge`).
- **The runner must be on the tailnet.** CI talks to Forgejo and its registry
  at `http://100.64.0.13:3000` (plain HTTP over tailscale), not the public
  `nether.codes` — Cloudflare's ~100 MB request cap and flaky chunked uploads
  broke large registry pushes from the amd64 runner before, and this VM sits
  off-site so all its pushes would otherwise ride Cloudflare.
- **Job containers use the same image as the amd64 runner**
  (`ghcr.io/catthehacker/ubuntu:act-22.04` — it's multi-arch, arm64/v8
  included), with the host Docker socket automounted.
- **The build itself needs nothing arch-special:** the pinned `webrtc-sys`
  downloads LiveKit's prebuilt `webrtc-linux-arm64` (verified present for the
  pinned `webrtc-51ef663` tag), and the Docker base images are multi-arch.

## 1. Provision the Oracle VM

Issue #49 has the click-by-click with screenshots; the essentials:

1. Compute → Instances → Create instance.
2. **Shape:** "Change shape" → Ampere → `VM.Standard.A1.Flex`. The Always-Free
   allotment is **4 OCPUs / 24 GB total** across A1 instances. If this VM is
   dedicated to CI, take all of it — a release build is a cold full-workspace
   Rust compile, and 4 OCPUs roughly halves the wall clock vs 2. Use
   2 OCPU / 12 GB only if you want headroom for a second free VM later.
3. **Image:** Ubuntu 24.04 (aarch64). The bootstrap script assumes Ubuntu.
4. **Networking:** defaults are fine. A public IPv4 is handy for SSH but the
   runner itself only ever dials out (Forgejo over tailscale, ghcr.io,
   crates.io) — no inbound ports need opening. Oracle's default rules
   (SSH-only inbound) can stay as they are.
5. **SSH key:** upload/paste your public key — password login doesn't exist on
   Oracle VMs.
6. **Boot volume:** bump to ~100 GB (free tier includes 200 GB total). Docker
   images and buildkit layers accumulate between cold builds; the 47 GB
   default fills up annoyingly fast.

Free-tier gotcha: Always-Free accounts often hit "out of capacity" for Ampere
in busy regions — retry later, or convert the account to Pay-As-You-Go (still
$0 within free limits) to unlock provisioning immediately.

## 2. Collect the two secrets

- **Runner registration token** — as the repo owner (`dark`):
  repo **Settings → Actions → Runners → Create new runner** and copy the token
  shown. (An instance-level token from the admin panel works too and would let
  the runner serve other repos. The robocub account can't mint one — the API
  returns 403 for non-owners.)
- **Tailscale auth key** (optional) — https://login.tailscale.com/admin/settings/keys
  → Generate auth key. Without one the script runs `tailscale up`
  interactively and prints a login URL, which is fine for a one-off.

## 3. Bootstrap

Copy `scripts/setup-arm64-runner.sh` to the VM and run it as root:

```bash
scp scripts/setup-arm64-runner.sh ubuntu@<vm-ip>:
ssh ubuntu@<vm-ip>
sudo TS_AUTHKEY=tskey-auth-… NVB_RUNNER_TOKEN=<registration-token> ./setup-arm64-runner.sh
```

It is idempotent (re-run after fixing anything) and does, in order: Docker
Engine; `insecure-registries` for the plain-HTTP tailnet registry; tailscale
join; `forgejo-runner` v12.12.0 (pinned) as a systemd service under a dedicated
`forgejo-runner` user; registration against `http://100.64.0.13:3000` with the
label `docker-arm64:docker://ghcr.io/catthehacker/ubuntu:act-22.04`.

Overridable env: `NVB_FORGEJO_URL`, `NVB_RUNNER_NAME` (default `oracle-arm64`),
`NVB_RUNNER_LABELS` — see the script header.

## 4. Verify

1. `systemctl status forgejo-runner` on the VM shows active; the runner
   appears **Idle** under repo Settings → Actions → Runners.
2. Dispatch **Docker Build & Release** from the Actions UI (`workflow_dispatch`
   → publishes `:edge`). `image-amd64` and `image-arm64` should run in
   parallel on their respective runners, then `manifest` combines them.
   Expect the arm64 leg to be the long pole (cold build, ~20–45 min depending
   on OCPUs).
3. Confirm the multi-arch result from any docker host:

   ```bash
   docker manifest inspect nether.codes/dark/nether-voicebridge:edge
   # → manifests[] should list amd64 AND arm64
   ```

4. On the next `v*` tag, the release should carry twice the assets: `_amd64` +
   `_arm64` debs, `x86_64` + `aarch64` rpms and tarballs, one `SHA256SUMS`.

## Troubleshooting

- **Job stays queued forever** — label mismatch: the workflow asks for
  `docker-arm64`; check the runner's registered labels on the settings page
  (re-register with the right `--labels` if needed). Also remember queued runs
  are invisible in the `/actions/tasks` API — check `/actions/runs`.
- **Push fails with TLS/`http: server gave HTTP response`** — the daemon or
  buildkit doesn't trust the plain-HTTP registry. The daemon side is
  `/etc/docker/daemon.json` (`insecure-registries`, script handles it); the
  buildkit side is the `buildkitd-config-inline` block already in docker.yml.
- **`502`/`connection reset` mid-push** — traffic is riding Cloudflare instead
  of the tailnet. Check `tailscale status` on the VM and that the registry
  host in the workflow is the tailnet address.
- **Disk fills up** — cold builds leave big layers behind. A weekly
  `docker system prune -af` cron (or `--filter until=168h`) keeps the volume
  bounded; nothing on this VM is precious.
- **Runner wedges / jobs stuck** — same symptom class as the amd64 Incus
  runner: `journalctl -u forgejo-runner -f` for repeated fetch failures, then
  `systemctl restart forgejo-runner`.
