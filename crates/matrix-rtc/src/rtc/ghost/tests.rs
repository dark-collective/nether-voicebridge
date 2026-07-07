    use super::*;

    fn test_pool(max_ghosts: Option<usize>) -> Arc<GhostPool> {
        let matrix_cfg = MatrixConfig {
            homeserver_url: "https://nether.im".into(),
            room_id: "!r:nether.im".into(),
            bot_user_id: "@bridge:nether.im".into(),
            ..MatrixConfig::default()
        };
        GhostPool::new(&matrix_cfg, &LiveKitConfig::default(), false, max_ghosts)
            .expect("test pool")
    }

    fn sink_count(pool: &GhostPool) -> usize {
        pool.sinks.lock().unwrap_or_else(PoisonError::into_inner).len()
    }

    #[test]
    fn assign_enforces_ghost_cap() {
        let pool = test_pool(Some(2));
        // Two speakers fit.
        let _a = pool.assign("a", "Alice", None);
        let _b = pool.assign("b", "Bob", None);
        assert_eq!(sink_count(&pool), 2);
        // A third is capped: null sink, no ghost registered.
        let _c = pool.assign("c", "Carol", None);
        assert_eq!(sink_count(&pool), 2);
        // Idempotent re-assign of an EXISTING speaker is never capped.
        let _a2 = pool.assign("a", "Alice", None);
        assert_eq!(sink_count(&pool), 2);
        // A release frees the slot; the previously-capped speaker now fits.
        pool.release("a");
        assert_eq!(sink_count(&pool), 1);
        let _c2 = pool.assign("c", "Carol", None);
        assert_eq!(sink_count(&pool), 2);
    }

    #[test]
    fn assign_without_cap_is_unlimited() {
        let pool = test_pool(None);
        for i in 0..50 {
            let _ = pool.assign(&format!("u{i}"), "U", None);
        }
        assert_eq!(sink_count(&pool), 50);
    }

    #[test]
    fn ghost_identity_detection() {
        let pfx = "nether-voicebridge_";
        assert!(is_ghost_identity("@nether-voicebridge_12345:nether.im", pfx));
        assert!(is_ghost_identity("@nether-voicebridge_12345:nether.im:DEVICE", pfx));
        assert!(!is_ghost_identity("@dark:nether.im", pfx));
        assert!(!is_ghost_identity("@dark:nether.im:DEVICE", pfx));
        // mautrix-discord's namespace must NOT be detected as a *current* ghost.
        assert!(!is_ghost_identity("@discord_12345:nether.im", pfx));
        assert!(!is_ghost_identity("not-an-mxid", pfx));
        // The bridge bot (`@{sender_localpart}_bot`) shares the ghost prefix but is
        // NOT a ghost: the all-digits suffix check excludes it. Without this the
        // startup sweep would retract the bot's own `m.call.member`.
        assert!(!is_ghost_identity("@nether-voicebridge_bot:nether.im", pfx));
        assert!(!is_ghost_identity("@nether-voicebridge_bot:nether.im:DEVICE", pfx));
        // A human in the namespace with a non-numeric localpart is likewise not a ghost.
        assert!(!is_ghost_identity("@nether-voicebridge_alice:nether.im", pfx));
        assert!(!is_ghost_identity("@nether-voicebridge_:nether.im", pfx));
        // A different deployment prefix works too.
        let test_pfx = "test-nether-voicebridge_";
        assert!(is_ghost_identity("@test-nether-voicebridge_12345:shadowdrake.org", test_pfx));
        assert!(!is_ghost_identity("@nether-voicebridge_12345:shadowdrake.org", test_pfx));
    }

    #[test]
    fn legacy_ghost_identity_detection() {
        // Our pre-rename ghosts were `@discord_<numeric-discord-id>`.
        assert!(is_legacy_ghost_identity("@discord_996161727068643380:nether.im"));
        assert!(is_legacy_ghost_identity("@discord_12345:nether.im:DEVICE"));
        // A human/non-numeric localpart must NOT be caught.
        assert!(!is_legacy_ghost_identity("@discord_alice:nether.im"));
        assert!(!is_legacy_ghost_identity("@discord_:nether.im"));
        assert!(!is_legacy_ghost_identity("@dark:nether.im"));
        // The previous hard-coded default prefix is recognized as legacy too (#37
        // migration): a deployment that moved to a different sender_localpart leaves
        // `@nether-voicebridge_<id>` ghosts behind, and the startup sweep must still
        // clean them up. (When the *current* prefix is still `nether-voicebridge_`,
        // the sweep checks `is_ghost_identity` first, so those take the masquerade
        // path — this legacy match only fires once the prefix has moved on.)
        assert!(is_legacy_ghost_identity("@nether-voicebridge_123:nether.im"));
        assert!(is_legacy_ghost_identity("@nether-voicebridge_12345:nether.im:DEVICE"));
        // ...but a non-numeric localpart under that prefix (the bot, or a human) is not.
        assert!(!is_legacy_ghost_identity("@nether-voicebridge_bot:nether.im"));
        assert!(!is_legacy_ghost_identity("@nether-voicebridge_alice:nether.im"));

        // The combined exclusion helper covers both current and legacy ghosts.
        let pfx = "nether-voicebridge_";
        assert!(is_bridge_excluded_identity("@nether-voicebridge_123:nether.im", pfx));
        assert!(is_bridge_excluded_identity("@discord_996161727068643380:nether.im", pfx));
        assert!(!is_bridge_excluded_identity("@dark:nether.im", pfx));
    }

    #[test]
    fn cs_url_encodes_segments_and_masquerade() {
        let url = cs_url(
            "https://nether.im",
            &["profile", "@nether-voicebridge_1:nether.im", "displayname"],
            Some("@nether-voicebridge_1:nether.im"),
        )
        .unwrap();
        let s = url.as_str();
        assert!(s.starts_with("https://nether.im/_matrix/client/v3/profile/"));
        // Path segment is percent-encoded (@ → %40, : → %3A; - and _ stay literal).
        assert!(s.contains("%40nether-voicebridge_1%3Anether.im"));
        // Masquerade query is present and encoded.
        assert!(s.contains("user_id=%40nether-voicebridge_1%3Anether.im"));
    }

    #[test]
    fn sendtodevice_url_targets_encryption_keys() {
        let url = cs_url(
            "https://nether.im",
            &["sendToDevice", "io.element.call.encryption_keys", "D42-key-3"],
            Some("@nether-voicebridge_42:nether.im"),
        )
        .unwrap();
        let s = url.as_str();
        assert!(s.contains("/_matrix/client/v3/sendToDevice/io.element.call.encryption_keys/D42-key-3"));
        assert!(s.contains("user_id=%40nether-voicebridge_42%3Anether.im"));
    }

    #[test]
    fn ghost_device_id_folds_in_salt() {
        assert_eq!(ghost_device_id("42", "abc"), "D42-abc");
        // Empty salt (no store) → unchanged legacy bare id.
        assert_eq!(ghost_device_id("42", ""), "D42");
    }

    #[test]
    fn salted_device_id_keeps_wire_shape() {
        // A salted device id must still produce the exact claimed_device_id / member.id
        // EC reads — guards the `_m.call` suffix concatenation against separator drift.
        let content =
            keys::build_key_content("!room:nether.im", "D42-abc", "AAAA".to_owned(), 0, 1234);
        let v = serde_json::to_value(&content).unwrap();
        assert_eq!(v["member"]["claimed_device_id"], "D42-abc");
        assert_eq!(v["member"]["id"], "D42-abc_m.call");
    }

    #[test]
    fn key_content_matches_ec_wire_shape() {
        // The to-device content must carry the exact fields Element Call reads to
        // map a key to the ghost's LiveKit identity. A regression here = peers
        // install the key under the wrong identity → noise (constraint #3-adjacent).
        let content = keys::build_key_content("!room:nether.im", "D42", "AAAA".to_owned(), 0, 1234);
        let v = serde_json::to_value(&content).unwrap();
        assert_eq!(v["keys"]["index"], 0);
        assert_eq!(v["keys"]["key"], "AAAA");
        assert_eq!(v["room_id"], "!room:nether.im");
        assert_eq!(v["member"]["claimed_device_id"], "D42");
        assert_eq!(v["member"]["id"], "D42_m.call");
        assert_eq!(v["session"]["application"], "m.call");
        assert_eq!(v["session"]["scope"], "m.room");
        assert_eq!(v["sent_ts"], 1234);
    }

    // ── distribute_key_loop, driven with mock transport + recipient source ──────

    use super::super::key_transport::{FailedRecipient, KeyRecipient, KeySendOutcome, RefreshMode};

    /// Test helper: a call-member recipient with a fixed device id.
    fn rcpt(user: &str) -> KeyRecipient {
        KeyRecipient {
            user_id: user.parse().unwrap(),
            device_id: "DEV1".into(),
        }
    }

    /// Records the recipient lists, `refresh` mode, and session rotations (#52)
    /// passed to the transport.
    #[derive(Default)]
    struct MockTransport {
        sends: StdMutex<Vec<Vec<KeyRecipient>>>,
        refreshes: StdMutex<Vec<RefreshMode>>,
        rotations: StdMutex<Vec<&'static str>>,
    }

    #[async_trait]
    impl KeyTransport for MockTransport {
        async fn send_key(
            &self,
            recipients: &[KeyRecipient],
            _key: &[u8; 16],
            _key_index: u8,
            refresh: RefreshMode,
        ) -> Result<KeySendOutcome> {
            self.sends.lock().unwrap().push(recipients.to_vec());
            self.refreshes.lock().unwrap().push(refresh);
            Ok(KeySendOutcome {
                delivered: recipients.len(),
                failed: Vec::new(),
            })
        }
        fn kind(&self) -> &'static str {
            "mock"
        }
        async fn rotate_sessions(&self, reason: &'static str) -> bool {
            self.rotations.lock().unwrap().push(reason);
            true
        }
    }

    /// Returns a fixed recipient list every poll.
    struct MockSource {
        recipients: Vec<KeyRecipient>,
    }

    #[async_trait]
    impl RecipientSource for MockSource {
        async fn recipients(&self) -> Result<Vec<KeyRecipient>> {
            Ok(self.recipients.clone())
        }
    }

    #[tokio::test]
    async fn key_loop_sends_to_current_recipients_then_stops_on_cancel() {
        let alice = rcpt("@alice:nether.im");
        let source = Arc::new(MockSource {
            recipients: vec![alice.clone()],
        });
        let transport = Arc::new(MockTransport::default());
        let (_gen_tx, gen_rx) = watch::channel(0u64);
        let cancel = CancellationToken::new();

        let handle = tokio::spawn(distribute_key_loop(
            source,
            [7u8; 16],
            transport.clone(),
            false,
            gen_rx,
            cancel.clone(),
        ));

        // The first iteration sends immediately, then waits the 3s burst interval
        // (a new recipient sets burst > 0). Cancel inside that window → exactly one
        // send, to the current recipient set.
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
        handle.await.unwrap();

        let sends = transport.sends.lock().unwrap();
        assert_eq!(sends.len(), 1, "expected one send before cancel");
        assert_eq!(sends[0], vec![alice]);
    }

    #[tokio::test]
    async fn key_loop_skips_send_when_no_recipients() {
        let source = Arc::new(MockSource { recipients: vec![] });
        let transport = Arc::new(MockTransport::default());
        let (_gen_tx, gen_rx) = watch::channel(0u64);
        let cancel = CancellationToken::new();

        let handle = tokio::spawn(distribute_key_loop(
            source,
            [0u8; 16],
            transport.clone(),
            false,
            gen_rx,
            cancel.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
        handle.await.unwrap();

        assert!(
            transport.sends.lock().unwrap().is_empty(),
            "no recipients ⇒ no key send"
        );
    }

    /// Reaches NOBODY for the first `fail_first` sends (simulating a ghost whose
    /// crypto client hasn't tracked the recipients' devices yet), then succeeds.
    struct ColdThenWarmTransport {
        fail_first: usize,
        calls: StdMutex<usize>,
    }

    #[async_trait]
    impl KeyTransport for ColdThenWarmTransport {
        async fn send_key(
            &self,
            recipients: &[KeyRecipient],
            _key: &[u8; 16],
            _key_index: u8,
            _refresh: RefreshMode,
        ) -> Result<KeySendOutcome> {
            let mut n = self.calls.lock().unwrap();
            *n += 1;
            if *n <= self.fail_first {
                // delivered=0 with a non-empty `failed` set ⇒ the "reached NO
                // recipient devices" arm (cold crypto sync), which must fast-retry.
                Ok(KeySendOutcome {
                    delivered: 0,
                    failed: recipients
                        .iter()
                        .map(|r| FailedRecipient {
                            user_id: r.user_id.clone(),
                            device_id: r.device_id.clone(),
                        })
                        .collect(),
                })
            } else {
                Ok(KeySendOutcome {
                    delivered: recipients.len(),
                    failed: Vec::new(),
                })
            }
        }
        fn kind(&self) -> &'static str {
            "cold-then-warm"
        }
    }

    /// Regression: a ghost that keeps reaching nobody (its crypto sync hasn't caught
    /// up) must keep retrying at the FAST 3s burst interval — NOT drain the burst on
    /// the failures and drop to the 60s heartbeat, which left speakers silent for a
    /// minute on a busy VPS. The window (40s) deliberately runs PAST the ~18s it
    /// takes the initial burst (6 × 3s) to drain: with the fix the cadence stays fast
    /// (~14 sends in 40s); the old bug drained the burst and went quiet (~6 sends,
    /// then a 60s gap). `>= 10` cleanly separates the two.
    #[tokio::test(start_paused = true)]
    async fn key_loop_fast_retries_until_crypto_sync_catches_up() {
        let source = Arc::new(MockSource {
            recipients: vec![rcpt("@alice:nether.im")],
        });
        // Stays cold for the whole window so we measure the sustained retry cadence.
        let transport = Arc::new(ColdThenWarmTransport {
            fail_first: usize::MAX,
            calls: StdMutex::new(0),
        });
        let (_gen_tx, gen_rx) = watch::channel(0u64);
        let cancel = CancellationToken::new();

        let handle = tokio::spawn(distribute_key_loop(
            source,
            [1u8; 16],
            transport.clone(),
            false,
            gen_rx,
            cancel.clone(),
        ));

        tokio::time::sleep(Duration::from_secs(40)).await;
        cancel.cancel();
        handle.await.unwrap();

        let calls = *transport.calls.lock().unwrap();
        assert!(
            calls >= 10,
            "expected sustained fast retries while reaching nobody (≈14 sends in 40s \
             at the 3s interval); got {calls} — burst drained on failure → 60s \
             heartbeat (the bug)"
        );
    }

    #[tokio::test]
    async fn plaintext_transport_empty_recipients_is_noop() {
        // The empty guard must short-circuit before any network call.
        let t = PlaintextTransport::new(
            reqwest::Client::new(),
            "https://nether.im".into(),
            "as_tok".into(),
            "!r:nether.im".into(),
            "@nether-voicebridge_1:nether.im".into(),
            "D1".into(),
        );
        let outcome = t
            .send_key(&[], &[0u8; 16], 0, RefreshMode::Steady)
            .await
            .unwrap();
        assert!(outcome.failed.is_empty());
        assert_eq!(outcome.delivered, 0);
        assert_eq!(t.kind(), "plaintext");
    }

    /// Issue #51: the first send forces a fresh device-list query, steady burst ticks
    /// do NOT, and a membership-generation bump re-arms exactly one forced query — so a
    /// same-user rejoin re-fetches the returning device instead of stalling.
    #[tokio::test(start_paused = true)]
    async fn key_loop_forces_refresh_on_first_send_and_after_gen_bump() {
        let source = Arc::new(MockSource {
            recipients: vec![rcpt("@alice:nether.im")],
        });
        let transport = Arc::new(MockTransport::default());
        let (gen_tx, gen_rx) = watch::channel(0u64);
        let cancel = CancellationToken::new();

        let handle = tokio::spawn(distribute_key_loop(
            source,
            [7u8; 16],
            transport.clone(),
            false,
            gen_rx,
            cancel.clone(),
        ));

        // Let the first send fire, then advance one burst interval for a steady tick.
        tokio::time::sleep(Duration::from_millis(10)).await;
        tokio::time::advance(KEY_BURST_INTERVAL).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        // Simulate a listener rejoin: bump the membership generation.
        gen_tx.send_modify(|g| *g = g.wrapping_add(1));
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel.cancel();
        handle.await.unwrap();

        let refreshes = transport.refreshes.lock().unwrap().clone();
        assert!(
            refreshes.len() >= 3,
            "expected first send + a steady burst tick + a post-bump send; got {refreshes:?}"
        );
        assert_eq!(
            refreshes[0],
            RefreshMode::Force,
            "first send must force a fresh /keys/query"
        );
        assert_eq!(
            refreshes[1],
            RefreshMode::Steady,
            "an ordinary burst tick must NOT force a query"
        );
        assert!(
            refreshes[2..].contains(&RefreshMode::Force),
            "a membership-generation bump must re-arm one forced query; got {refreshes:?}"
        );
    }

    /// #52: a membership bump requests a session rotation, but it must wait out the
    /// min-interval (no rotation right at the bump — EC membership flaps would churn
    /// one-time keys) and then fire on a later send tick with reason "membership".
    #[tokio::test(start_paused = true)]
    async fn key_loop_rotates_on_membership_after_min_interval() {
        let source = Arc::new(MockSource {
            recipients: vec![rcpt("@alice:nether.im")],
        });
        let transport = Arc::new(MockTransport::default());
        let (gen_tx, gen_rx) = watch::channel(0u64);
        let cancel = CancellationToken::new();

        let handle = tokio::spawn(distribute_key_loop(
            source,
            [7u8; 16],
            transport.clone(),
            true,
            gen_rx,
            cancel.clone(),
        ));

        // First send fires; a fresh loop must not rotate (bootstrap just built the
        // machine).
        tokio::time::sleep(Duration::from_millis(10)).await;
        gen_tx.send_modify(|g| *g = g.wrapping_add(1));
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            transport.rotations.lock().unwrap().is_empty(),
            "a bump inside the min-interval must NOT rotate yet"
        );

        // Once the min-interval has passed, the still-pending request fires.
        tokio::time::advance(SESSION_ROTATE_MIN_INTERVAL).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel.cancel();
        handle.await.unwrap();

        let rotations = transport.rotations.lock().unwrap().clone();
        assert_eq!(
            rotations,
            vec!["membership"],
            "exactly one deferred membership rotation"
        );
    }

    /// #52: with no membership changes at all, the max-age bound still rotates —
    /// this is what caps a listener-side dead-session wedge in a static call.
    #[tokio::test(start_paused = true)]
    async fn key_loop_rotates_at_max_age_in_static_call() {
        let source = Arc::new(MockSource {
            recipients: vec![rcpt("@alice:nether.im")],
        });
        let transport = Arc::new(MockTransport::default());
        let (_gen_tx, gen_rx) = watch::channel(0u64);
        let cancel = CancellationToken::new();

        let handle = tokio::spawn(distribute_key_loop(
            source,
            [7u8; 16],
            transport.clone(),
            true,
            gen_rx,
            cancel.clone(),
        ));

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(transport.rotations.lock().unwrap().is_empty());

        tokio::time::advance(SESSION_ROTATE_MAX_AGE).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel.cancel();
        handle.await.unwrap();

        let rotations = transport.rotations.lock().unwrap().clone();
        assert_eq!(rotations, vec!["max-age"], "one age-driven rotation");
    }

    /// #52: the config knob off ⇒ no rotation ever, regardless of bumps or age.
    #[tokio::test(start_paused = true)]
    async fn key_loop_never_rotates_when_disabled() {
        let source = Arc::new(MockSource {
            recipients: vec![rcpt("@alice:nether.im")],
        });
        let transport = Arc::new(MockTransport::default());
        let (gen_tx, gen_rx) = watch::channel(0u64);
        let cancel = CancellationToken::new();

        let handle = tokio::spawn(distribute_key_loop(
            source,
            [7u8; 16],
            transport.clone(),
            false,
            gen_rx,
            cancel.clone(),
        ));

        tokio::time::sleep(Duration::from_millis(10)).await;
        gen_tx.send_modify(|g| *g = g.wrapping_add(1));
        tokio::time::advance(SESSION_ROTATE_MAX_AGE + SESSION_ROTATE_MIN_INTERVAL).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel.cancel();
        handle.await.unwrap();

        assert!(
            transport.rotations.lock().unwrap().is_empty(),
            "rotation disabled by config must never fire"
        );
    }
