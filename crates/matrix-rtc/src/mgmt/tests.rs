use super::authority::*;
use super::handlers::*;
use super::parse::*;
use super::pending::*;
use super::*;
use nvb_config::NewTenant;

#[test]
fn cooldown_gates_within_window_and_stamps_attempts() {
    let mut stamps = HashMap::new();
    let cd = Duration::from_secs(30);
    let t0 = Instant::now();

    // First attempt: allowed and stamped.
    assert_eq!(cooldown_check(&mut stamps, "@op:x", t0, cd), None);
    // Immediately again: gated, full window remaining (rounded ≥ 1).
    assert_eq!(cooldown_check(&mut stamps, "@op:x", t0, cd), Some(30));
    // Mid-window: remaining counts down…
    assert_eq!(
        cooldown_check(&mut stamps, "@op:x", t0 + Duration::from_secs(29), cd),
        Some(1)
    );
    // …and sub-second remainders report 1, never a confusing "0s".
    assert_eq!(
        cooldown_check(&mut stamps, "@op:x", t0 + Duration::from_millis(29_500), cd),
        Some(1)
    );
    // A gated attempt did NOT re-stamp: the window still expires at t0+30.
    assert_eq!(
        cooldown_check(&mut stamps, "@op:x", t0 + Duration::from_secs(30), cd),
        None
    );
    // Cooldowns are PER operator — a different MXID is independent.
    assert_eq!(cooldown_check(&mut stamps, "@other:x", t0, cd), None);
}

#[test]
fn parses_ban_unban_and_bans() {
    assert_eq!(
        parse_command("ban owner @evil:x"),
        Parsed::Cmd(Command::Ban { kind: "owner".into(), value: "@evil:x".into() })
    );
    assert_eq!(
        parse_command("UNBAN guild 123"),
        Parsed::Cmd(Command::Unban { kind: "guild".into(), value: "123".into() })
    );
    assert_eq!(parse_command("bans"), Parsed::Cmd(Command::Bans));
    // Wrong arity → usage, not Unknown (these are our verbs).
    assert!(matches!(parse_command("ban owner"), Parsed::Usage(_)));
    assert!(matches!(parse_command("ban a b c"), Parsed::Usage(_)));
    assert!(matches!(parse_command("bans now"), Parsed::Usage(_)));
}

#[test]
fn ban_value_shapes() {
    assert!(ban_value_shape_ok(BanKind::Owner, "@evil:example.org"));
    assert!(!ban_value_shape_ok(BanKind::Owner, "evil"));
    assert!(ban_value_shape_ok(BanKind::Guild, "123456"));
    assert!(!ban_value_shape_ok(BanKind::Guild, "@evil:x"));
    assert!(ban_value_shape_ok(BanKind::Room, "!abc:example.org"));
    assert!(!ban_value_shape_ok(BanKind::Room, "#alias:example.org"));
}

#[test]
fn tokenize_plain_and_quoted() {
    assert_eq!(
        tokenize("link \"Guest Voice\" !r:x 1 2").unwrap(),
        vec!["link", "Guest Voice", "!r:x", "1", "2"]
    );
    assert_eq!(tokenize("  list  ").unwrap(), vec!["list"]);
    assert_eq!(tokenize("").unwrap(), Vec::<String>::new());
    assert!(tokenize("link \"unbalanced").is_none());
}

#[test]
fn parse_verbs_case_insensitive() {
    assert_eq!(parse_command("HELP"), Parsed::Cmd(Command::Help));
    assert_eq!(parse_command("List"), Parsed::Cmd(Command::List));
}

#[test]
fn parse_link_with_and_without_server() {
    assert_eq!(
        parse_command("link \"Guest Voice\" !r:nether.im 111 222"),
        Parsed::Cmd(Command::Link {
            name: "Guest Voice".into(),
            room_id: "!r:nether.im".into(),
            guild_id: "111".into(),
            channel_id: "222".into(),
            server: None,
        })
    );
    assert_eq!(
        parse_command("link g !r:x 1 2 nether"),
        Parsed::Cmd(Command::Link {
            name: "g".into(),
            room_id: "!r:x".into(),
            guild_id: "1".into(),
            channel_id: "2".into(),
            server: Some("nether".into()),
        })
    );
}

#[test]
fn parse_link_wrong_arity_is_usage() {
    assert!(matches!(parse_command("link onlyname"), Parsed::Usage(_)));
    assert!(matches!(parse_command("link a b c d e f"), Parsed::Usage(_)));
    assert!(matches!(parse_command("unlink"), Parsed::Usage(_)));
    assert!(matches!(parse_command("unlink a b"), Parsed::Usage(_)));
}

#[test]
fn parse_chatter_is_unknown() {
    assert_eq!(parse_command("hello there"), Parsed::Unknown);
    assert_eq!(parse_command(""), Parsed::Unknown);
    assert_eq!(parse_command("\"stray quote"), Parsed::Unknown);
}

#[test]
fn numeric_id_check() {
    assert!(is_numeric_id("123456789012345678"));
    assert!(!is_numeric_id(""));
    assert!(!is_numeric_id("12a4"));
    assert!(!is_numeric_id("#general"));
}

fn pl(json: serde_json::Value) -> CallPowerCheck {
    evaluate_call_member_power(Some(&json))
}

#[test]
fn call_power_events_override() {
    // The properly-configured Element Call room.
    let c = pl(serde_json::json!({
        "events": { "org.matrix.msc3401.call.member": 0 },
        "state_default": 50,
    }));
    assert_eq!(c, CallPowerCheck { required: 0, ghost_pl: 0 });
    assert!(c.ok());

    // An explicit blocking override.
    assert!(!pl(serde_json::json!({
        "events": { "org.matrix.msc3401.call.member": 50 },
        "state_default": 0,
    }))
    .ok());
}

#[test]
fn call_power_state_default_fallback() {
    assert!(pl(serde_json::json!({ "state_default": 0 })).ok());
    // No override, no state_default → spec default 50 (the verified
    // 2026-06-16 silent breakage: a room left at default state PL).
    let c = pl(serde_json::json!({ "users_default": 0 }));
    assert_eq!(c, CallPowerCheck { required: 50, ghost_pl: 0 });
    assert!(!c.ok());
}

#[test]
fn call_power_users_default_counts() {
    // Ghosts join at users_default; a room that raises it alongside
    // state_default still works.
    assert!(pl(serde_json::json!({
        "state_default": 50,
        "users_default": 50,
    }))
    .ok());
}

#[test]
fn call_power_accepts_string_encoded_levels() {
    // Legacy rooms carry power levels as numeric strings.
    assert!(pl(serde_json::json!({
        "events": { "org.matrix.msc3401.call.member": "0" },
        "state_default": "50",
    }))
    .ok());
    assert!(!pl(serde_json::json!({ "state_default": "50" })).ok());
}

#[test]
fn call_power_absent_event_fails_safe() {
    let c = evaluate_call_member_power(None);
    assert_eq!(c, CallPowerCheck { required: 50, ghost_pl: 0 });
    assert!(!c.ok());
}

// ---- two-sided link auth (#47) ----

fn joined() -> serde_json::Value {
    serde_json::json!({ "membership": "join" })
}

#[test]
fn sender_authority_users_map_override() {
    let pl = serde_json::json!({
        "state_default": 50,
        "users_default": 0,
        "users": { "@mod:x": 50, "@admin:x": 100 },
    });
    assert!(evaluate_sender_authority(Some(&pl), Some(&joined()), "@mod:x").ok());
    assert!(evaluate_sender_authority(Some(&pl), Some(&joined()), "@admin:x").ok());
    // Not in the users map → users_default 0 < 50.
    let plain = evaluate_sender_authority(Some(&pl), Some(&joined()), "@user:x");
    assert_eq!(plain, SenderAuthority { joined: true, sender_pl: 0, required: 50 });
    assert!(!plain.ok());
}

#[test]
fn sender_authority_requires_joined_membership() {
    let pl = serde_json::json!({ "state_default": 0 });
    // Power is sufficient (0 ≥ 0) but the sender must actually be joined.
    assert!(!evaluate_sender_authority(Some(&pl), None, "@u:x").ok());
    let invited = serde_json::json!({ "membership": "invite" });
    assert!(!evaluate_sender_authority(Some(&pl), Some(&invited), "@u:x").ok());
    assert!(evaluate_sender_authority(Some(&pl), Some(&joined()), "@u:x").ok());
}

#[test]
fn sender_authority_absent_pl_fails_safe() {
    let a = evaluate_sender_authority(None, Some(&joined()), "@u:x");
    assert_eq!(a, SenderAuthority { joined: true, sender_pl: 0, required: 50 });
    assert!(!a.ok());
}

#[test]
fn sender_authority_accepts_string_encoded_levels() {
    let pl = serde_json::json!({
        "state_default": "50",
        "users": { "@mod:x": "50" },
    });
    assert!(evaluate_sender_authority(Some(&pl), Some(&joined()), "@mod:x").ok());
}

#[test]
fn approval_code_shape_and_normalization() {
    let code = mint_approval_code();
    assert!(code.starts_with("NVB-"), "{code}");
    assert_eq!(code.len(), 4 + CODE_LEN);
    assert!(code[4..].bytes().all(|b| CODE_ALPHABET.contains(&b)), "{code}");

    assert_eq!(normalize_code("nvb-abc234"), "NVB-ABC234");
    assert_eq!(normalize_code("  ABC234 "), "NVB-ABC234");
    assert_eq!(normalize_code("NVB-ABC234"), "NVB-ABC234");
}

fn test_pending(guild: &str, created: Instant) -> PendingLink {
    PendingLink {
        tenant: NewTenant {
            name: "t".into(),
            server: "s".into(),
            room_id: "!r:x".into(),
            guild_id: guild.into(),
            channel_id: "2".into(),
            owner: Some("@op:x".into()),
            created: Some(0),
        },
        requester: "@op:x".into(),
        origin_room: "!dm:x".try_into().unwrap(),
        warnings: Vec::new(),
        created,
    }
}

#[test]
fn pending_take_matches_guild_and_burns_only_on_match() {
    let ttl = Duration::from_secs(600);
    let now = Instant::now();
    let mut map = HashMap::new();
    let code = insert_pending(&mut map, test_pending("111", now), now, ttl).unwrap();

    // Wrong guild: refused AND the entry survives for the right server.
    assert!(take_pending(&mut map, &code, 999, now, ttl).is_err());
    assert_eq!(map.len(), 1);

    // Right guild: claimed exactly once.
    let p = take_pending(&mut map, &code, 111, now, ttl).unwrap();
    assert_eq!(p.tenant.guild_id, "111");
    assert!(take_pending(&mut map, &code, 111, now, ttl).is_err());
}

#[test]
fn pending_expires_after_ttl() {
    let ttl = Duration::from_secs(600);
    let t0 = Instant::now();
    let mut map = HashMap::new();
    let code = insert_pending(&mut map, test_pending("111", t0), t0, ttl).unwrap();
    // One second before expiry: still claimable...
    assert!(map.contains_key(&code));
    prune_expired_pending(&mut map, t0 + Duration::from_secs(599), ttl);
    assert!(map.contains_key(&code));
    // ...at/after the TTL: gone.
    assert!(take_pending(&mut map, &code, 111, t0 + Duration::from_secs(600), ttl).is_err());
    assert!(map.is_empty());
}

#[test]
fn self_serve_tier_gates_operator_verbs() {
    assert!(allowed_for_self_serve(&Command::Help));
    assert!(allowed_for_self_serve(&Command::List));
    assert!(allowed_for_self_serve(&Command::Link {
        name: "n".into(),
        room_id: "!r:x".into(),
        guild_id: "1".into(),
        channel_id: "2".into(),
        server: None,
    }));
    assert!(allowed_for_self_serve(&Command::Unlink { name: "n".into() }));
    assert!(!allowed_for_self_serve(&Command::Ban {
        kind: "owner".into(),
        value: "@x:y".into()
    }));
    assert!(!allowed_for_self_serve(&Command::Unban {
        kind: "owner".into(),
        value: "@x:y".into()
    }));
    assert!(!allowed_for_self_serve(&Command::Bans));
}

#[test]
fn pending_cap_refuses_but_expiry_frees() {
    let ttl = Duration::from_secs(600);
    let t0 = Instant::now();
    let mut map = HashMap::new();
    for _ in 0..PENDING_LINK_CAP {
        insert_pending(&mut map, test_pending("111", t0), t0, ttl).unwrap();
    }
    assert!(insert_pending(&mut map, test_pending("111", t0), t0, ttl).is_err());
    // Once the old ones age out, inserts are accepted again.
    let later = t0 + Duration::from_secs(600);
    assert!(insert_pending(&mut map, test_pending("111", later), later, ttl).is_ok());
    assert_eq!(map.len(), 1);
}
