    use super::*;
    use std::time::Duration;

    /// A shared `[puppets]` pool block. `tokens` are emitted verbatim.
    fn puppets_block(tokens: &[&str]) -> String {
        let toks = tokens
            .iter()
            .map(|t| format!("\"{t}\""))
            .collect::<Vec<_>>()
            .join(", ");
        format!("[puppets]\ntokens = [{toks}]\n\n")
    }

    /// One `[[bridge]]` block. `store`/`chan` let each test pick distinct values
    /// so multi-bridge tests pass validation; `name` is emitted only when `Some`.
    /// `[bridge.livekit]` deliberately omits the optional `lk_jwt_url`/`url`
    /// overrides so the default derive (`None`) path is exercised. Puppet tokens
    /// live in the shared `[puppets]` block, not here.
    fn bridge_block(name: Option<&str>, room: &str, store: &str, chan: &str) -> String {
        let name_line = match name {
            Some(n) => format!("name = \"{n}\"\n"),
            None => String::new(),
        };
        format!(
            r#"[[bridge]]
{name_line}[bridge.discord]
guild_id = "111"
channel_id = "{chan}"

[bridge.matrix]
homeserver_url = "https://nether.im"
room_id = "{room}"
bot_user_id = "@bridge:nether.im"
device_id = "DEV{chan}"
appservice_id = "voicebridge"
appservice_token = "hs_tok"
store_path = "{store}"

[bridge.livekit]
media_e2ee = false
"#
        )
    }

    /// Parse a single resolved `BridgeConfig` out of a one-bridge TOML config.
    fn one_bridge(name: Option<&str>, room: &str, store: &str, chan: &str) -> BridgeConfig {
        let toml = format!(
            "{}{}",
            puppets_block(&["tok"]),
            bridge_block(name, room, store, chan)
        );
        let cfg: Config = toml::from_str(&toml).unwrap();
        cfg.bridges.into_iter().next().unwrap()
    }

    #[test]
    fn live_keys_collect_and_admit_a_distinct_bridge() {
        let a = one_bridge(Some("a"), "!a:x", "/s/a", "1");
        let b = one_bridge(Some("b"), "!b:x", "/s/b", "2");
        let live = LiveKeys::from_bridges([&a]);
        assert!(live.names.contains("a"));
        assert!(live.stores.contains("/s/a"));
        assert!(live.channels.contains(&("111".into(), "1".into())));
        let servers = std::collections::HashMap::new();
        validate_bridge_against(&b, &servers, &live).expect("distinct bridge admitted");
    }

    #[test]
    fn validate_against_rejects_device_tuple_collision() {
        let a = one_bridge(Some("a"), "!a:x", "/s/a", "1");
        // Same homeserver/bot and an explicitly identical device_id.
        let mut b = one_bridge(Some("b"), "!b:x", "/s/b", "2");
        b.matrix.device_id = a.matrix.device_id.clone();
        let live = LiveKeys::from_bridges([&a]);
        let servers = std::collections::HashMap::new();
        let err = validate_bridge_against(&b, &servers, &live).unwrap_err();
        assert!(
            err.to_string().contains("duplicate (homeserver_url, bot_user_id, device_id)"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_against_rejects_store_collision() {
        let a = one_bridge(Some("a"), "!a:x", "/s/shared", "1");
        let mut b = one_bridge(Some("b"), "!b:x", "/s/b", "2");
        b.matrix.store_path = "/s/shared".into();
        let live = LiveKeys::from_bridges([&a]);
        let servers = std::collections::HashMap::new();
        let err = validate_bridge_against(&b, &servers, &live).unwrap_err();
        assert!(
            err.to_string().contains("duplicate matrix store_path '/s/shared'"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_against_rejects_unknown_server_ref_and_missing_fields() {
        let live = LiveKeys::default();
        let servers = std::collections::HashMap::new();
        let mut b = one_bridge(Some("b"), "!b:x", "/s/b", "2");
        b.server = Some("nope".into());
        let err = validate_bridge_against(&b, &servers, &live).unwrap_err();
        assert!(
            err.to_string().contains("references unknown server profile 'nope'"),
            "unexpected error: {err}"
        );
        let mut c = one_bridge(Some("c"), "!c:x", "/s/c", "3");
        c.matrix.appservice_token = String::new();
        let err = validate_bridge_against(&c, &servers, &live).unwrap_err();
        assert!(
            err.to_string().contains("missing required matrix field(s): appservice_token"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn parses_pool_and_single_bridge() {
        let raw = format!(
            "{}{}",
            puppets_block(&["Bot abc", "Bot ghi"]),
            bridge_block(Some("general"), "!room:nether.im", "./s1", "222"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert_eq!(cfg.puppets.tokens.len(), 2);
        assert_eq!(cfg.puppets.nickname_prefix, "[Matrix] ");
        assert_eq!(cfg.bridges.len(), 1);
        let b = &cfg.bridges[0];
        assert_eq!(b.name.as_deref(), Some("general"));
        assert_eq!(b.matrix.homeserver_url, "https://nether.im");
        // Optional override URLs absent ⇒ derive-by-default path.
        assert_eq!(b.livekit.lk_jwt_url, None);
        assert_eq!(b.livekit.url, None);
        cfg.validate().expect("should validate");
    }

    #[test]
    fn parses_two_bridges() {
        let raw = format!(
            "{}{}\n{}",
            puppets_block(&["Bot a", "Bot b"]),
            bridge_block(Some("a"), "!a:nether.im", "./sa", "222"),
            bridge_block(Some("b"), "!b:nether.im", "./sb", "333"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert_eq!(cfg.bridges.len(), 2);
        assert_eq!(cfg.bridges[0].matrix.room_id, "!a:nether.im");
        assert_eq!(cfg.bridges[1].matrix.room_id, "!b:nether.im");
        cfg.validate().expect("should validate");
    }

    #[test]
    fn limits_absent_means_unlimited_and_parses_when_present() {
        // Absent [limits] section ⇒ every cap None (unlimited, pre-#47 behavior).
        let raw = format!(
            "{}{}",
            puppets_block(&["Bot a"]),
            bridge_block(Some("a"), "!a:nether.im", "./sa", "222"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert_eq!(cfg.limits.max_tenants, None);
        assert_eq!(cfg.limits.max_ghosts_per_bridge, None);
        assert_eq!(cfg.limits.mgmt_cooldown_secs, None);

        // Present section with a subset of fields: set ones parse, rest stay None.
        let raw = format!(
            "[limits]\nmax_tenants = 25\nmgmt_cooldown_secs = 30\n{}{}",
            puppets_block(&["Bot a"]),
            bridge_block(Some("a"), "!a:nether.im", "./sa", "222"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert_eq!(cfg.limits.max_tenants, Some(25));
        assert_eq!(cfg.limits.max_ghosts_per_bridge, None);
        assert_eq!(cfg.limits.mgmt_cooldown_secs, Some(30));
        cfg.validate().expect("limits never invalidate a config");
    }

    #[test]
    fn name_is_optional() {
        let raw = format!(
            "{}{}",
            puppets_block(&["Bot a"]),
            bridge_block(None, "!room:nether.im", "./s1", "222"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert!(cfg.bridges[0].name.is_none());
        assert_eq!(cfg.bridges[0].label(), "unnamed");
    }

    #[test]
    fn rejects_empty_bridges() {
        // A pool but no `[[bridge]]` → empty Vec → validation error.
        let cfg: Config = toml::from_str(&puppets_block(&["Bot a"])).expect("parses");
        assert!(matches!(cfg.validate(), Err(ConfigError::Validation(_))));
    }

    #[test]
    fn rejects_empty_puppet_pool() {
        // Bridges but no puppet tokens → validation error (no anchors possible).
        let raw = bridge_block(Some("a"), "!a:nether.im", "./sa", "222");
        let cfg: Config = toml::from_str(&raw).expect("parses");
        assert!(matches!(cfg.validate(), Err(ConfigError::Validation(_))));
    }

    #[test]
    fn rejects_duplicate_store_path() {
        let raw = format!(
            "{}{}\n{}",
            puppets_block(&["Bot a"]),
            bridge_block(Some("a"), "!a:nether.im", "./dup", "222"),
            bridge_block(Some("b"), "!b:nether.im", "./dup", "333"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert!(matches!(cfg.validate(), Err(ConfigError::Validation(_))));
    }

    #[test]
    fn duplicate_room_and_token_are_warnings_not_errors() {
        // Same room_id across bridges + a duplicate pool token → Ok (warn only).
        let raw = format!(
            "{}{}\n{}",
            puppets_block(&["Bot dup", "Bot dup"]),
            bridge_block(Some("a"), "!same:nether.im", "./sa", "222"),
            bridge_block(Some("b"), "!same:nether.im", "./sb", "333"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        cfg.validate().expect("dupes are warnings, not errors");
    }

    #[test]
    fn rejects_missing_matrix_fields() {
        // `[bridge.matrix]`/`[bridge.livekit]` are now optional sections (a bridge can
        // inherit everything from a `[server.<name>]` profile), so this PARSES — but
        // with no profile and no inline fields, `validate()` flags the missing
        // required Matrix fields rather than serde failing at parse time.
        let raw = format!(
            "{}{}",
            puppets_block(&["Bot a"]),
            r#"[[bridge]]
[bridge.discord]
guild_id = "111"
channel_id = "222"
"#,
        );
        let cfg: Config = toml::from_str(&raw).expect("parses (sections optional)");
        assert!(matches!(cfg.validate(), Err(ConfigError::Validation(_))));
    }

    /// A `[[bridge]]` block that OMITS `store_path` so the auto-derive path runs.
    /// Mirrors `bridge_block` minus the `store_path = ...` line.
    fn bridge_block_no_store(name: Option<&str>, room: &str, chan: &str) -> String {
        let name_line = match name {
            Some(n) => format!("name = \"{n}\"\n"),
            None => String::new(),
        };
        format!(
            r#"[[bridge]]
{name_line}[bridge.discord]
guild_id = "111"
channel_id = "{chan}"

[bridge.matrix]
homeserver_url = "https://nether.im"
room_id = "{room}"
bot_user_id = "@bridge:nether.im"
device_id = "DEV{chan}"
appservice_id = "voicebridge"
appservice_token = "hs_tok"

[bridge.livekit]
media_e2ee = false
"#
        )
    }

    #[test]
    fn store_path_defaults_empty_and_data_dir_defaults() {
        // Omitted store_path parses to the empty sentinel; data_dir gets its default.
        let raw = format!(
            "{}{}",
            puppets_block(&["Bot a"]),
            bridge_block_no_store(Some("general"), "!r:nether.im", "222"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert_eq!(cfg.data_dir, "./nether-store");
        assert_eq!(cfg.bridges[0].matrix.store_path, "");
    }

    #[test]
    fn resolve_derives_store_path_from_name() {
        let raw = format!(
            "{}{}",
            puppets_block(&["Bot a"]),
            bridge_block_no_store(Some("General Voice"), "!r:nether.im", "222"),
        );
        let mut cfg: Config = toml::from_str(&raw).expect("parse failed");
        cfg.resolve_store_paths("./nether-store");
        // Slugged + lowercased; spaces collapse to a single dash.
        assert_eq!(cfg.bridges[0].matrix.store_path, "./nether-store/general-voice");
        cfg.validate().expect("should validate");
    }

    #[test]
    fn resolve_respects_explicit_store_path() {
        // An explicit path is never overwritten by derivation.
        let raw = format!(
            "{}{}",
            puppets_block(&["Bot a"]),
            bridge_block(Some("general"), "!r:nether.im", "/custom/store", "222"),
        );
        let mut cfg: Config = toml::from_str(&raw).expect("parse failed");
        cfg.resolve_store_paths("/app/data");
        assert_eq!(cfg.bridges[0].matrix.store_path, "/custom/store");
    }

    #[test]
    fn resolve_uses_data_dir_override_and_trims_slash() {
        // The Docker case: NVB_DATA_DIR=/app/data (passed here as the arg). A
        // trailing slash on the base is normalised away.
        let raw = format!(
            "{}{}",
            puppets_block(&["Bot a"]),
            bridge_block_no_store(Some("gaming"), "!r:nether.im", "222"),
        );
        let mut cfg: Config = toml::from_str(&raw).expect("parse failed");
        cfg.resolve_store_paths("/app/data/");
        assert_eq!(cfg.bridges[0].matrix.store_path, "/app/data/gaming");
    }

    #[test]
    fn resolve_unnamed_bridges_get_distinct_indexed_paths() {
        let raw = format!(
            "{}{}\n{}",
            puppets_block(&["Bot a"]),
            bridge_block_no_store(None, "!a:nether.im", "222"),
            bridge_block_no_store(None, "!b:nether.im", "333"),
        );
        let mut cfg: Config = toml::from_str(&raw).expect("parse failed");
        cfg.resolve_store_paths("./nether-store");
        assert_eq!(cfg.bridges[0].matrix.store_path, "./nether-store/bridge-0");
        assert_eq!(cfg.bridges[1].matrix.store_path, "./nether-store/bridge-1");
        // Distinct → passes the uniqueness check.
        cfg.validate().expect("should validate");
    }

    #[test]
    fn resolve_same_named_bridges_collide_and_fail_validation() {
        // Two bridges sharing a name derive the same path → duplicate-store error.
        let raw = format!(
            "{}{}\n{}",
            puppets_block(&["Bot a"]),
            bridge_block_no_store(Some("dup"), "!a:nether.im", "222"),
            bridge_block_no_store(Some("dup"), "!b:nether.im", "333"),
        );
        let mut cfg: Config = toml::from_str(&raw).expect("parse failed");
        cfg.resolve_store_paths("./nether-store");
        assert_eq!(cfg.bridges[0].matrix.store_path, "./nether-store/dup");
        assert_eq!(cfg.bridges[1].matrix.store_path, "./nether-store/dup");
        assert!(matches!(cfg.validate(), Err(ConfigError::Validation(_))));
    }

    fn livekit(media_e2ee: Option<bool>, key_transport: Option<KeyTransport>) -> LiveKitConfig {
        LiveKitConfig {
            lk_jwt_url: None,
            url: None,
            media_e2ee,
            key_transport,
        }
    }

    #[test]
    fn resolved_media_e2ee_override_beats_detection() {
        // None → follow detection; Some → force regardless of detection.
        assert!(livekit(None, None).resolved_media_e2ee(true));
        assert!(!livekit(None, None).resolved_media_e2ee(false));
        assert!(!livekit(Some(false), None).resolved_media_e2ee(true)); // force off in enc room
        assert!(livekit(Some(true), None).resolved_media_e2ee(false)); // force on in unenc room
    }

    #[test]
    fn resolved_key_transport_auto_and_override() {
        // Unset: auto — Olm when E2EE on, Plaintext when off.
        assert_eq!(livekit(None, None).resolved_key_transport(true), KeyTransport::Olm);
        assert_eq!(
            livekit(None, None).resolved_key_transport(false),
            KeyTransport::Plaintext
        );
        // Explicit override is honored regardless of media_e2ee.
        assert_eq!(
            livekit(None, Some(KeyTransport::Plaintext)).resolved_key_transport(true),
            KeyTransport::Plaintext
        );
        assert_eq!(
            livekit(None, Some(KeyTransport::Olm)).resolved_key_transport(true),
            KeyTransport::Olm
        );
    }

    #[test]
    fn media_e2ee_and_key_transport_are_optional() {
        // An EMPTY [bridge.livekit] (no override lines) → None/None (auto path).
        let raw = format!(
            r#"{}[[bridge]]
name = "g"
[bridge.discord]
guild_id = "111"
channel_id = "222"

[bridge.matrix]
homeserver_url = "https://nether.im"
room_id = "!r:nether.im"
bot_user_id = "@bridge:nether.im"
device_id = "BRIDGE01"
appservice_id = "voicebridge"
appservice_token = "hs_tok"
store_path = "./s"

[bridge.livekit]
"#,
            puppets_block(&["Bot a"]),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert_eq!(cfg.bridges[0].livekit.media_e2ee, None);
        assert_eq!(cfg.bridges[0].livekit.key_transport, None);
    }

    #[test]
    fn explicit_media_e2ee_parses_to_some() {
        // The fixtures set `media_e2ee = false` → Some(false), an explicit override.
        let raw = format!(
            "{}{}",
            puppets_block(&["Bot a"]),
            bridge_block(Some("g"), "!r:nether.im", "./s", "222"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert_eq!(cfg.bridges[0].livekit.media_e2ee, Some(false));
    }

    #[test]
    fn slugify_handles_punctuation_and_empties() {
        assert_eq!(slugify("General Voice"), "general-voice");
        assert_eq!(slugify("  weird___name!! "), "weird-name");
        assert_eq!(slugify("a/b\\c"), "a-b-c");
        // All-punctuation → safe fallback rather than an empty leaf.
        assert_eq!(slugify("!!!"), "bridge");
    }

    #[test]
    fn livekit_override_urls_parse_when_present() {
        // When set, the optional override URLs are honored (Some).
        let raw = r#"[puppets]
tokens = ["Bot abc"]

[[bridge]]
[bridge.discord]
guild_id = "111"
channel_id = "222"

[bridge.matrix]
homeserver_url = "https://nether.im"
room_id = "!r:nether.im"
bot_user_id = "@bridge:nether.im"
device_id = "BRIDGE01"
appservice_id = "voicebridge"
appservice_token = "hs_tok"
store_path = "./s1"

[bridge.livekit]
lk_jwt_url = "https://lk-jwt.example.org"
url = "wss://livekit.example.org"
"#;
        let cfg: Config = toml::from_str(raw).expect("parse failed");
        let lk = &cfg.bridges[0].livekit;
        assert_eq!(lk.lk_jwt_url.as_deref(), Some("https://lk-jwt.example.org"));
        assert_eq!(lk.url.as_deref(), Some("wss://livekit.example.org"));
    }

    // ---- server/appservice profiles (named, referenced) ----

    /// A `[server.<name>]` profile block.
    fn server_block(name: &str) -> String {
        format!(
            r#"[server.{name}]
homeserver_url = "https://nether.im"
bot_user_id = "@voicebridge_as:nether.im"
appservice_id = "voicebridge"
appservice_token = "hs_tok"

"#
        )
    }

    /// Parse + run the same resolution chain `Config::load` does (server-merge,
    /// bot_user_id derive, device_id derive, store_path derive) so tests exercise
    /// the real pipeline.
    fn loaded(raw: &str) -> Config {
        let mut cfg: Config = toml::from_str(raw).expect("parse failed");
        cfg.resolve_servers();
        cfg.resolve_bot_user_ids();
        cfg.resolve_device_ids();
        cfg.resolve_store_paths("./nether-store");
        if let Some(m) = cfg.management.as_mut() {
            resolve_management(m, &cfg.servers, "./nether-store");
        }
        cfg
    }

    #[test]
    fn server_profile_inherits_and_autoderives() {
        // A bridge that references a profile and only sets room_id inherits every
        // server field, auto-derives device_id (slug of name) + store_path, validates.
        let raw = format!(
            "{}{}[[bridge]]\nname = \"Dark Voice\"\nserver = \"nether\"\nroom_id = \"!d:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
        );
        let cfg = loaded(&raw);
        let m = &cfg.bridges[0].matrix;
        assert_eq!(m.homeserver_url, "https://nether.im");
        assert_eq!(m.bot_user_id, "@voicebridge_as:nether.im");
        assert_eq!(m.appservice_id, "voicebridge");
        assert_eq!(m.appservice_token, "hs_tok");
        assert_eq!(m.room_id, "!d:nether.im"); // bridge-level room_id → matrix.room_id
        assert_eq!(m.device_id, "dark-voice"); // auto-derived from name
        assert_eq!(m.store_path, "./nether-store/dark-voice");
        assert_eq!(m.prune_stale_devices, None); // unset everywhere → default-on at use site
        cfg.validate().expect("should validate");
    }

    #[test]
    fn prune_stale_devices_inherits_from_profile_and_bridge_wins() {
        // Profile-level prune_stale_devices flows to the bridge; an inline
        // [bridge.matrix] value beats it (the standard profile-merge contract).
        let raw = format!(
            "{}[server.nether]\nhomeserver_url = \"https://nether.im\"\nbot_user_id = \"@voicebridge_as:nether.im\"\nappservice_id = \"voicebridge\"\nappservice_token = \"hs_tok\"\nprune_stale_devices = false\n\n[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n\n[[bridge]]\nname = \"b\"\nserver = \"nether\"\nroom_id = \"!b:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"333\"\n[bridge.matrix]\nprune_stale_devices = true\n",
            puppets_block(&["Bot a"]),
        );
        let cfg = loaded(&raw);
        assert_eq!(cfg.bridges[0].matrix.prune_stale_devices, Some(false)); // inherited
        assert_eq!(cfg.bridges[1].matrix.prune_stale_devices, Some(true)); // override wins
    }

    #[test]
    fn rotate_ghost_sessions_inherits_from_profile_and_bridge_wins() {
        // Same profile-merge contract as prune_stale_devices (#52 knob).
        let raw = format!(
            "{}[server.nether]\nhomeserver_url = \"https://nether.im\"\nbot_user_id = \"@voicebridge_as:nether.im\"\nappservice_id = \"voicebridge\"\nappservice_token = \"hs_tok\"\nrotate_ghost_sessions = false\n\n[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n\n[[bridge]]\nname = \"b\"\nserver = \"nether\"\nroom_id = \"!b:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"333\"\n[bridge.matrix]\nrotate_ghost_sessions = true\n",
            puppets_block(&["Bot a"]),
        );
        let cfg = loaded(&raw);
        assert_eq!(cfg.bridges[0].matrix.rotate_ghost_sessions, Some(false)); // inherited
        assert_eq!(cfg.bridges[1].matrix.rotate_ghost_sessions, Some(true)); // override wins
    }

    #[test]
    fn bridge_override_beats_profile() {
        // Inline [bridge.matrix] values win over the referenced profile.
        let raw = format!(
            "{}{}[[bridge]]\nname = \"b\"\nserver = \"nether\"\nroom_id = \"!b:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n[bridge.matrix]\nbot_user_id = \"@special:nether.im\"\ndevice_id = \"PINNED\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
        );
        let cfg = loaded(&raw);
        let m = &cfg.bridges[0].matrix;
        assert_eq!(m.bot_user_id, "@special:nether.im"); // override
        assert_eq!(m.device_id, "PINNED"); // explicit, not auto-derived
        assert_eq!(m.appservice_token, "hs_tok"); // still inherited
        cfg.validate().expect("should validate");
    }

    #[test]
    fn unknown_server_reference_errors() {
        let raw = format!(
            "{}[[bridge]]\nname = \"b\"\nserver = \"ghost\"\nroom_id = \"!b:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
        );
        let cfg = loaded(&raw);
        assert!(matches!(cfg.validate(), Err(ConfigError::Validation(_))));
    }

    #[test]
    fn missing_room_id_after_merge_errors() {
        // Profile supplies everything except the per-bridge room_id → validate flags it.
        let raw = format!(
            "{}{}[[bridge]]\nname = \"b\"\nserver = \"nether\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
        );
        let cfg = loaded(&raw);
        assert!(matches!(cfg.validate(), Err(ConfigError::Validation(_))));
    }

    #[test]
    fn duplicate_explicit_device_id_errors() {
        // Two bridges on one profile (same bot) hand-set the same device_id → reject.
        let raw = format!(
            "{}{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n[bridge.matrix]\ndevice_id = \"SAME\"\n\n[[bridge]]\nname = \"b\"\nserver = \"nether\"\nroom_id = \"!b:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"333\"\n[bridge.matrix]\ndevice_id = \"SAME\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
        );
        let cfg = loaded(&raw);
        assert!(matches!(cfg.validate(), Err(ConfigError::Validation(_))));
    }

    #[test]
    fn two_profile_bridges_get_distinct_derived_devices() {
        // The real config shape: two bridges, one profile, device_ids auto-derive
        // distinct from their names → no duplicate-device error.
        let raw = format!(
            "{}{}[[bridge]]\nname = \"Light Voice\"\nserver = \"nether\"\nroom_id = \"!l:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n\n[[bridge]]\nname = \"Dark Voice\"\nserver = \"nether\"\nroom_id = \"!d:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"333\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
        );
        let cfg = loaded(&raw);
        assert_eq!(cfg.bridges[0].matrix.device_id, "light-voice");
        assert_eq!(cfg.bridges[1].matrix.device_id, "dark-voice");
        cfg.validate().expect("distinct derived devices → valid");
    }

    // ---- sender_localpart → bot_user_id derivation ----

    /// A `[server.<name>]` profile that uses sender_localpart instead of bot_user_id.
    fn server_block_localpart(name: &str, localpart: &str) -> String {
        format!(
            r#"[server.{name}]
homeserver_url = "https://nether.im"
sender_localpart = "{localpart}"
appservice_id = "voicebridge"
appservice_token = "hs_tok"

"#
        )
    }

    #[test]
    fn server_name_from_url_parses_correctly() {
        assert_eq!(
            server_name_from_url("https://matrix.example.com"),
            Some("matrix.example.com".to_owned())
        );
        assert_eq!(
            server_name_from_url("https://matrix.example.com:8448/"),
            Some("matrix.example.com:8448".to_owned())
        );
        assert_eq!(
            server_name_from_url("http://localhost:8008"),
            Some("localhost:8008".to_owned())
        );
        assert_eq!(server_name_from_url("not-a-url"), None);
        assert_eq!(server_name_from_url("https://"), None);
    }

    #[test]
    fn sender_localpart_inline_derives_bot_user_id() {
        // sender_localpart in [bridge.matrix] → bot_user_id derived as localpart_bot.
        let raw = format!(
            "{}[[bridge]]\nname = \"a\"\nroom_id = \"!r:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n[bridge.matrix]\nhomeserver_url = \"https://nether.im\"\nsender_localpart = \"voicebridge-bot\"\ndevice_id = \"DEV\"\nappservice_id = \"voicebridge\"\nappservice_token = \"hs_tok\"\nstore_path = \"./s\"\n",
            puppets_block(&["Bot a"]),
        );
        let cfg = loaded(&raw);
        let m = &cfg.bridges[0].matrix;
        assert_eq!(m.bot_user_id, "@voicebridge-bot_bot:nether.im");
        assert_eq!(m.sender_localpart.as_deref(), Some("voicebridge-bot"));
        cfg.validate().expect("should validate");
    }

    #[test]
    fn sender_localpart_in_profile_derives_bot_user_id() {
        // Profile with only sender_localpart (no bot_user_id) → bot_user_id derived as localpart_bot.
        let raw = format!(
            "{}{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!r:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            server_block_localpart("nether", "voicebridge-bot"),
        );
        let cfg = loaded(&raw);
        let m = &cfg.bridges[0].matrix;
        assert_eq!(m.bot_user_id, "@voicebridge-bot_bot:nether.im");
        assert_eq!(m.sender_localpart.as_deref(), Some("voicebridge-bot"));
        cfg.validate().expect("should validate");
    }

    #[test]
    fn explicit_bot_user_id_beats_sender_localpart() {
        // Both set in profile: bot_user_id wins; sender_localpart is still propagated.
        let raw = format!(
            "{}[server.nether]\nhomeserver_url = \"https://nether.im\"\nbot_user_id = \"@custom-bot:nether.im\"\nsender_localpart = \"voicebridge-bot\"\nappservice_id = \"voicebridge\"\nappservice_token = \"hs_tok\"\n\n{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!r:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            "",
        );
        let cfg = loaded(&raw);
        let m = &cfg.bridges[0].matrix;
        assert_eq!(m.bot_user_id, "@custom-bot:nether.im");
        assert_eq!(m.sender_localpart.as_deref(), Some("voicebridge-bot"));
        cfg.validate().expect("should validate");
    }

    #[test]
    fn missing_both_bot_user_id_and_sender_localpart_errors() {
        // Neither bot_user_id nor sender_localpart → validate catches the empty bot_user_id.
        let raw = format!(
            "{}[[bridge]]\nname = \"a\"\nroom_id = \"!r:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n[bridge.matrix]\nhomeserver_url = \"https://nether.im\"\ndevice_id = \"DEV\"\nappservice_id = \"voicebridge\"\nappservice_token = \"hs_tok\"\nstore_path = \"./s\"\n",
            puppets_block(&["Bot a"]),
        );
        let cfg = loaded(&raw);
        assert!(matches!(cfg.validate(), Err(ConfigError::Validation(_))));
    }

    #[test]
    fn sender_localpart_independent_of_bot_user_id_in_profile() {
        // bot_user_id and sender_localpart can differ — both are preserved as-is.
        let raw = format!(
            "{}[server.nether]\nhomeserver_url = \"https://nether.im\"\nbot_user_id = \"@bot-account:nether.im\"\nsender_localpart = \"namespace-anchor\"\nappservice_id = \"voicebridge\"\nappservice_token = \"hs_tok\"\n\n{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!r:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            "",
        );
        let cfg = loaded(&raw);
        let m = &cfg.bridges[0].matrix;
        assert_eq!(m.bot_user_id, "@bot-account:nether.im");
        assert_eq!(m.sender_localpart.as_deref(), Some("namespace-anchor"));
        cfg.validate().expect("should validate");
    }

    // ---- matrix_domain overrides homeserver_url host for MXID derivation ----

    #[test]
    fn matrix_domain_inline_overrides_url_host() {
        // homeserver_url is an internal proxy; matrix_domain gives the real server name.
        let raw = format!(
            "{}[[bridge]]\nname = \"a\"\nroom_id = \"!r:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n[bridge.matrix]\nhomeserver_url = \"http://matrix-traefik:8008\"\nmatrix_domain = \"nether.im\"\nsender_localpart = \"voicebridge-bot\"\ndevice_id = \"DEV\"\nappservice_id = \"voicebridge\"\nappservice_token = \"hs_tok\"\nstore_path = \"./s\"\n",
            puppets_block(&["Bot a"]),
        );
        let cfg = loaded(&raw);
        let m = &cfg.bridges[0].matrix;
        assert_eq!(m.matrix_domain.as_deref(), Some("nether.im"));
        assert_eq!(m.bot_user_id, "@voicebridge-bot_bot:nether.im");
        cfg.validate().expect("should validate");
    }

    #[test]
    fn matrix_domain_in_profile_overrides_url_host() {
        // Same but matrix_domain is in the server profile and inherited by the bridge.
        let raw = format!(
            "{}[server.nether]\nhomeserver_url = \"http://matrix-traefik:8008\"\nmatrix_domain = \"nether.im\"\nsender_localpart = \"voicebridge-bot\"\nappservice_id = \"voicebridge\"\nappservice_token = \"hs_tok\"\n\n{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!r:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            "",
        );
        let cfg = loaded(&raw);
        let m = &cfg.bridges[0].matrix;
        assert_eq!(m.matrix_domain.as_deref(), Some("nether.im"));
        assert_eq!(m.bot_user_id, "@voicebridge-bot_bot:nether.im");
        cfg.validate().expect("should validate");
    }

    // ---- from_env() ----

    /// `from_env()` must run the same store-path resolution `load()` does: an omitted
    /// `NETHER_MATRIX_STORE_PATH` is derived from the bridge name, never left empty
    /// (an empty path sends the sqlite + ghost Olm stores off the persistent volume).
    /// Sole reader/writer of `NETHER_*`/`NVB_DATA_DIR` in the suite, so the env
    /// mutation can't race another test.
    #[test]
    fn from_env_derives_store_path_when_unset() {
        let vars = [
            ("NETHER_PUPPET_TOKENS", "tok-a,tok-b"),
            ("NETHER_DISCORD_GUILD_ID", "111"),
            ("NETHER_DISCORD_CHANNEL_ID", "222"),
            ("NETHER_MATRIX_HOMESERVER_URL", "https://nether.im"),
            ("NETHER_MATRIX_ROOM_ID", "!r:nether.im"),
            ("NETHER_MATRIX_SENDER_LOCALPART", "voicebridge-bot"),
            ("NETHER_MATRIX_APPSERVICE_ID", "voicebridge"),
            ("NETHER_MATRIX_APPSERVICE_TOKEN", "hs_tok"),
            ("NETHER_BRIDGE_NAME", "envtest"),
        ];
        for (k, v) in vars {
            std::env::set_var(k, v);
        }
        std::env::set_var("NVB_DATA_DIR", "/app/data");

        let cfg = Config::from_env().expect("from_env should succeed");
        let m = &cfg.bridges[0].matrix;
        // store_path derived under NVB_DATA_DIR, NOT left empty.
        assert_eq!(m.store_path, "/app/data/envtest");
        // device_id auto-derives from the bridge name when unset (file-mode parity).
        assert_eq!(m.device_id, "envtest");
        // sender_localpart still derives the bot MXID (as localpart_bot).
        assert_eq!(m.bot_user_id, "@voicebridge-bot_bot:nether.im");
        cfg.validate().expect("env config should validate");

        for (k, _) in vars {
            std::env::remove_var(k);
        }
        std::env::remove_var("NVB_DATA_DIR");
    }

    // ---- [management] (#4) ----

    fn management_block(server: Option<&str>, allow: &[&str]) -> String {
        let server_line = server.map(|s| format!("server = \"{s}\"\n")).unwrap_or_default();
        let allow_list =
            allow.iter().map(|a| format!("\"{a}\"")).collect::<Vec<_>>().join(", ");
        format!("[management]\n{server_line}allow = [{allow_list}]\n\n")
    }

    #[test]
    fn management_resolves_from_sole_profile() {
        // With exactly one [server.*] profile, `server` is implied; identity
        // fields inherit and device/store derive to the `management` leaf.
        let raw = format!(
            "{}{}{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
            management_block(None, &["@dark:nether.im"]),
        );
        let cfg = loaded(&raw);
        let m = cfg.management.as_ref().expect("management parsed");
        assert_eq!(m.server.as_deref(), Some("nether"));
        assert_eq!(m.homeserver_url, "https://nether.im");
        assert_eq!(m.bot_user_id, "@voicebridge_as:nether.im");
        assert_eq!(m.appservice_token, "hs_tok");
        assert_eq!(m.device_id, "management");
        assert_eq!(m.store_path, "./nether-store/management");
        cfg.validate().expect("should validate");
    }

    #[test]
    fn management_derives_bot_user_id_from_sender_localpart() {
        let raw = format!(
            "{}{}{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            server_block_localpart("nether", "voicebridge"),
            management_block(None, &["@dark:nether.im"]),
        );
        let cfg = loaded(&raw);
        let m = cfg.management.as_ref().unwrap();
        assert_eq!(m.bot_user_id, "@voicebridge_bot:nether.im");
        cfg.validate().expect("should validate");
    }

    #[test]
    fn management_rejects_empty_allowlist() {
        let raw = format!(
            "{}{}[management]\nallow = []\n\n[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
        );
        let err = loaded(&raw).validate().unwrap_err();
        assert!(err.to_string().contains("allow"), "unexpected: {err}");
    }

    #[test]
    fn management_rejects_malformed_mxid() {
        let raw = format!(
            "{}{}{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
            management_block(None, &["dark:nether.im"]),
        );
        let err = loaded(&raw).validate().unwrap_err();
        assert!(err.to_string().contains("not a full MXID"), "unexpected: {err}");
    }

    #[test]
    fn management_requires_server_ref_with_multiple_profiles() {
        let raw = format!(
            "{}{}{}{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
            server_block("other"),
            management_block(None, &["@dark:nether.im"]),
        );
        let err = loaded(&raw).validate().unwrap_err();
        assert!(err.to_string().contains("`server` is required"), "unexpected: {err}");
    }

    #[test]
    fn management_rejects_unknown_server_ref() {
        let raw = format!(
            "{}{}{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
            management_block(Some("rogue"), &["@dark:nether.im"]),
        );
        let err = loaded(&raw).validate().unwrap_err();
        assert!(
            err.to_string().contains("unknown server profile 'rogue'"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn management_store_is_reserved_against_bridges() {
        // A bridge explicitly claiming the management store_path is a hard error
        // (same duplicate-store rule as bridge-vs-bridge).
        let raw = format!(
            "{}{}{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n[bridge.matrix]\nstore_path = \"./nether-store/management\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
            management_block(None, &["@dark:nether.im"]),
        );
        let err = loaded(&raw).validate().unwrap_err();
        assert!(
            err.to_string().contains("duplicate matrix store_path"),
            "unexpected: {err}"
        );
    }

    // --- [timing] / NVB_TIMING resolution (#38 C1). Tests drive the pure
    // `TimingOverrides::resolve` with an explicit preset argument instead of
    // touching the process env (the env test at the bottom of this module is
    // the suite's sole env reader/writer).

    #[test]
    fn timing_defaults_match_historical_consts() {
        // The values the hardcoded constants had before #38 — byte-identical.
        let t = TimingConfig::default();
        assert_eq!(t.leave_grace_ms, 12_000);
        assert_eq!(t.reap_grace_ms, 45_000);
        assert_eq!(t.reap_interval_ms, 20_000);
        assert_eq!(t.anchor_idle_grace_ms, 90_000);
        assert_eq!(t.quarantine_base_ms, 15_000);
        assert_eq!(t.quarantine_max_ms, 120_000);
        assert_eq!(t.voice_health_sweep_ms, 15_000);
        assert!(t.is_default());
        assert!(!TimingConfig::compressed().is_default());
    }

    #[test]
    fn timing_absent_section_resolves_to_default() {
        let cfg: Config = toml::from_str("").expect("parse failed");
        let t = cfg.timing_raw.resolve(None).expect("resolve failed");
        assert!(t.is_default());
        // "default"/empty preset spellings are equivalent to no preset.
        assert!(cfg.timing_raw.resolve(Some("default")).unwrap().is_default());
    }

    #[test]
    fn timing_toml_key_overrides_default() {
        let cfg: Config =
            toml::from_str("[timing]\nleave_grace_ms = 5000\n").expect("parse failed");
        let t = cfg.timing_raw.resolve(None).expect("resolve failed");
        assert_eq!(t.leave_grace_ms, 5_000);
        assert_eq!(t.reap_grace_ms, 45_000); // untouched keys keep defaults
        assert!(!t.is_default());
        assert_eq!(t.leave_grace(), Duration::from_millis(5_000));
    }

    #[test]
    fn timing_explicit_key_beats_compressed_preset() {
        let cfg: Config =
            toml::from_str("[timing]\nreap_grace_ms = 7000\n").expect("parse failed");
        let t = cfg.timing_raw.resolve(Some("compressed")).expect("resolve failed");
        // Preset first, explicit keys on top.
        assert_eq!(t.reap_grace_ms, 7_000);
        assert_eq!(t.leave_grace_ms, TimingConfig::compressed().leave_grace_ms);
        assert_eq!(t.reap_interval_ms, TimingConfig::compressed().reap_interval_ms);
    }

    #[test]
    fn timing_rejects_unknown_preset_and_zero_values() {
        let empty = TimingOverrides::default();
        let err = empty.resolve(Some("fast")).unwrap_err();
        assert!(err.to_string().contains("NVB_TIMING"), "unexpected: {err}");

        // Zero is rejected for every field: a zero-period tokio interval panics.
        let cfg: Config =
            toml::from_str("[timing]\nreap_interval_ms = 0\n").expect("parse failed");
        let err = cfg.timing_raw.resolve(None).unwrap_err();
        assert!(
            err.to_string().contains("reap_interval_ms must be > 0"),
            "unexpected: {err}"
        );
    }
