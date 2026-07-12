# e2e federation fixture: the `nether.autos` homeserver (#109)

A second, operator-owned Matrix homeserver used as a **test fixture** by the
`nvb-e2e` harness, so the suite can mint genuinely REMOTE users and exercise
the federation surface the #96/#97/#103 incident class lived in: federated DM
invites with real multi-second join windows, cross-server E2EE device
fan-out, and v12 (domainless-id) rooms minted on another server. It also
doubles as the phase-0 prototype for the #108 federation self-test
exploration.

**Why ours and not a friend's server:** a fixture needs an appservice
registration (to mint users), determinism (its updates/wipes are ours), and
tolerance for test churn and future fault injection. Friends' servers are the
field; the fixture must be ours (#106).

## Topology

- Incus container `autos` on the same host as the other infra containers;
  Ubuntu 24.04 LTS, deliberately **prod-matched** (prod's `nether.im` Tuwunel
  runs the same OS + packaging).
- **Tuwunel 1.8.1-1**, the upstream release deb (x86-64-v3 build), installed
  with `apt install ./<deb>` — no apt repo; pin knowingly. Running a
  *different* version than prod is a feature when reproducing version-skew
  bugs; change it deliberately, note it here.
- `server_name = "nether.autos"`, listener on `0.0.0.0:8008`.
- **Cloudflare tunnel fronted, cloudflared inside the container** →
  `http://localhost:8008`, the same pattern as prod (the host has no open
  inbound; federation hairpins via the CF edge). Operator-owned CF-side
  pieces: the DNS record, the tunnel hostname route, and the **`/.well-known/`
  cache exclusion** (CF interference with well-known breaks discovery — the
  nether.im OIDC lesson).
- Tuwunel serves its own well-known: `client = "https://nether.autos"`,
  `server = "nether.autos:443"`.
- `allow_registration = false`, `allow_federation = true`. Nothing else runs
  on it: no RTC stack, no bridges — DM/room federation only.

Config: `/etc/tuwunel/tuwunel.toml`. Database: `/var/lib/tuwunel` (RocksDB).
Unit: `tuwunel.service` (ships with the deb).

## The `autotest` appservice

`/etc/tuwunel/appservices/autotest.yaml` — passive registration (`url: null`,
outbound masquerade only), mirroring the primary harness's `nvbtest`
registration on nether.im:

```yaml
id: autotest
url: null
as_token: <64-hex random>   # SECRET
hs_token: <64-hex random>   # SECRET
sender_localpart: autotest_bot
rate_limited: false
namespaces:
    users:
        - regex: ^@autotest_.*:nether\.autos$
          exclusive: true
```

The stem is deliberately not user-DMable-looking (#96 lesson: never point a
harness at an MXID real users might DM). Tuwunel loads the file at startup —
restart after changes.

Where the `as_token` lives: the Forgejo Actions secret `NVB_E2E_FED_AS_TOKEN`
(CI) and `fed_as_token` in the staging box's `e2e.toml`. Never committed,
never echoed.

## Signing-key persistence and the wipe procedure

**Invariant: the server's ed25519 signing key must survive every wipe.**
Remote homeservers cache the key; a fixture that rotates it on wipe presents
as a key-changed server and sours federation trust with every peer it ever
talked to.

Tuwunel mints the key into the RocksDB database at first boot, so the key is
preserved by restoring a **golden image** taken right after that first boot
(pristine: key minted, no users/rooms/federation yet):

```sh
# taken once, at provisioning time:
systemctl stop tuwunel
tar -C /var/lib -czf /var/lib/tuwunel-golden.tar.gz tuwunel
systemctl start tuwunel
```

**Wipe = restore the golden image** (drops all users/rooms/media/federation
state, keeps key + config):

```sh
systemctl stop tuwunel
rm -rf /var/lib/tuwunel
tar -C /var/lib -xzf /var/lib/tuwunel-golden.tar.gz
chown -R tuwunel:tuwunel /var/lib/tuwunel
systemctl start tuwunel
```

Verify after every wipe: `curl -s http://localhost:8008/_matrix/key/v2/server`
must show the SAME `verify_keys` as before (baseline recorded at
`/root/signing-key-baseline.txt` on the container). Drilled at provisioning
time (2026-07-12): key `ed25519:WiQzDgvI` identical across a full
wipe/restore; a smoke-registered `@autotest_smoke` user was gone afterwards.

Do NOT wipe by deleting the DB without restoring the image — a fresh DB mints
a fresh key (the exact thing the invariant forbids). Config and the
appservice file live in `/etc/tuwunel`, outside the wiped path.

## Harness configuration (`fed_*` group)

The harness reads four optional keys (env-first, `e2e.toml` fallback — same
resolution as everything else in `crates/e2e/src/config.rs`):

| env | e2e.toml | default |
|---|---|---|
| `NVB_E2E_FED_HOMESERVER_URL` | `fed_homeserver_url` | — (group anchor) |
| `NVB_E2E_FED_AS_TOKEN` | `fed_as_token` | — (secret) |
| `NVB_E2E_FED_DOMAIN` | `fed_domain` | host of the url |
| `NVB_E2E_FED_USER_PREFIX` | `fed_user_prefix` | `autotest` |

Semantics: **all absent → the `fed_*` scenarios report `skipped`** (the suite
runs anywhere); **partial → the config load fails loudly** (a typo must never
silently demote a federated run). The staging box's live `e2e.toml` is flat
TOML — append the keys at the end as plain keys, not a `[federation]` table.

The scenarios (`crates/e2e/src/scenarios/federation.rs`; skip-gated via
`E2eConfig::federation_or_skip()`):

- `fed_dm_invite_lifecycle` (Pass) — #97 over real federation;
- `fed_cold_outreach_utd` (KnownFail #104) — operator `msg` to a remote
  crypto user, cross-server megolm share;
- `fed_link_v12` (Pass) — the #103 acceptance test: `link` of a
  remotely-minted v12 room.

Remote actors are the same `MatrixManagementClient`/`MatrixCryptoDmClient`
used everywhere, constructed with `register_on(&fed, ...)` — they talk to the
fixture's client API (Cloudflare-fronted: the browser UA + rustls the actors
already use applies) while DMing the bridge's mgmt bot on nether.im.

## Verification ladder

Run in order after provisioning (and after any wipe, abbreviated). Browser UA
on every probe that crosses Cloudflare (`-A "Mozilla/5.0 (compatible; nvb-e2e)"`).

1. **Local**: `curl -s http://localhost:8008/_matrix/client/versions` on the
   container; `/_matrix/key/v2/server` matches the baseline.
2. **Edge**: `https://nether.autos/.well-known/matrix/server` +
   `/client` + `/_matrix/client/versions` from anywhere — proves DNS, the
   tunnel route, and the well-known cache exclusion.
3. **Federation, both directions** (the tinier.town failure was directional —
   run each probe ON the respective server so direction is real):
   from the fixture, `GET https://nether.im/_matrix/federation/v1/version`;
   from nether.im's container, the same against `nether.autos`; then
   `/_matrix/key/v2/server` each way.
4. **Behavioral round trip, both directions**: as_token-minted user on each
   side invites the other into a room; both invites arrive and both joins
   complete.
5. **v12 cross-join**: create a `room_version: "12"` room on the fixture,
   join it from nether.im (via-hinted — the id is domainless).
6. **Harness**: `nvb-e2e run --scenario fed_dm_invite_lifecycle` — first
   without the `fed_*` keys (expect `skipped`), then with them (expect
   `pass`).

## Ground rules

- The container's **insides are a fixture**: restart, reconfigure, wipe
  (via the golden image) freely. Everything outside it — the host, tunnel
  config, DNS, prod Tuwunel, the prod bridge — is operator-only.
- Registration stays closed; nobody lives on this server. If #108 graduates
  to a public service, that becomes its own deployment decision (issue #108
  phase 2), not a quiet repurposing of the fixture.
