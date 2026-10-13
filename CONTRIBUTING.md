# Contributing

The canonical repository is
**[nether.codes/nether/nether-voicebridge](https://nether.codes/nether/nether-voicebridge)**.
Issues, pull requests and releases all live there. The copy on GitHub is a
one-way, read-only mirror: anything opened on it isn't seen, and pull requests
opened there are closed automatically.

- **Report a bug or request a feature:** [open an issue](https://nether.codes/nether/nether-voicebridge/issues/new).
- **Send a change:** fork on nether.codes and [open a pull request](https://nether.codes/nether/nether-voicebridge/pulls).
- **Account:** nether.codes has no password sign-up. Use **Sign in with GitHub**
  on the [login page](https://nether.codes/user/login).
- **Questions / chat:** the Matrix space
  [`#nether-voicebridge:nether.im`](https://matrix.to/#/%23nether-voicebridge:nether.im)
  or the [Discord server](https://discord.gg/BQYxEdu7s2), which are bridged to each other.

Before you send code, `cargo test` and `cargo clippy --all-targets` should pass.
[CLAUDE.md](CLAUDE.md) and the [architecture doc](docs/nether-voicebridge-architecture.md)
list the project's hard constraints (pinned `songbird`/`livekit`, echo exclusion,
DAVE-only voice).
