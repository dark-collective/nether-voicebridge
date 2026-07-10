use super::*;

/// Test-only inspection of allocator state (commands are not consumed in unit
/// tests — `run` is never called — so these assert on the bookkeeping directly).
#[cfg(test)]
impl SharedPool {
    /// Slot index a participant is assigned to, if it has a dedicated puppet.
    fn test_slot_of(&self, bridge: &str, key: &str) -> Option<usize> {
        let g = self.alloc.lock().unwrap();
        match g.channels.get(bridge)?.assignments.get(key)? {
            Assign::Puppet { idx, .. } => Some(*idx),
            Assign::Overflow { .. } => None,
        }
    }
    fn test_is_overflow(&self, bridge: &str, key: &str) -> bool {
        let g = self.alloc.lock().unwrap();
        matches!(
            g.channels.get(bridge).and_then(|c| c.assignments.get(key)),
            Some(Assign::Overflow { .. })
        )
    }
    fn test_anchor(&self, bridge: &str) -> Option<usize> {
        let g = self.alloc.lock().unwrap();
        let a = g.channels.get(bridge)?.anchor.load(Ordering::Relaxed);
        (a != NO_ANCHOR).then_some(a)
    }
    fn test_free_count(&self) -> usize {
        let g = self.alloc.lock().unwrap();
        (0..self.slots.len())
            .filter(|i| !g.sessions.keys().any(|(idx, _)| idx == i))
            .count()
    }
    fn test_is_unhealthy(&self, idx: usize, guild: u64) -> bool {
        let g = self.alloc.lock().unwrap();
        g.health
            .get(&(idx, GuildId::new(guild)))
            .is_some_and(|h| h.unhealthy)
    }
    fn test_set_unhealthy(&self, idx: usize, guild: u64, unhealthy: bool) {
        let mut g = self.alloc.lock().unwrap();
        g.health.entry((idx, GuildId::new(guild))).or_default().unhealthy = unhealthy;
    }
    fn test_is_busy(&self, idx: usize) -> bool {
        self.alloc.lock().unwrap().sessions.keys().any(|(i, _)| *i == idx)
    }
    fn test_is_recovering(&self, idx: usize, guild: u64) -> bool {
        let g = self.alloc.lock().unwrap();
        g.health
            .get(&(idx, GuildId::new(guild)))
            .is_some_and(|h| h.recovering)
    }
    /// The session mixer for `(idx, guild)`, if that session exists.
    fn test_session_mixer(&self, idx: usize, guild: u64) -> Option<Arc<PlaybackMixer>> {
        let g = self.alloc.lock().unwrap();
        g.sessions.get(&(idx, GuildId::new(guild))).map(|s| s.mixer.clone())
    }
    fn test_has_channel(&self, bridge: &str) -> bool {
        self.alloc.lock().unwrap().channels.contains_key(bridge)
    }
    fn is_opted_out(&self, user_id: u64) -> bool {
        self.optout_set.read().unwrap().contains(&user_id)
    }
    /// Arm a fresh `anchor_idle_release` token on the channel and return a clone,
    /// standing in for the grace timer the gateway path would have started.
    fn test_arm_idle_token(&self, bridge: &str) -> CancellationToken {
        let mut g = self.alloc.lock().unwrap();
        let token = CancellationToken::new();
        g.channels.get_mut(bridge).unwrap().anchor_idle_release = Some(token.clone());
        token
    }
}

#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use super::super::router::RoutedSink;
    use super::*;
    use nvb_audio::routing::{CaptureSink as GhostSink, ParticipantSink};

    /// A no-op capture router for allocator tests (no ghosts are minted).
    struct NoopCapture;
    struct NoopCaptureSink;
    impl GhostSink for NoopCaptureSink {
        fn push(&self, _mono: &[i16]) {}
    }
    impl CaptureRouter for NoopCapture {
        fn assign(&self, _k: &str, _n: &str, _a: Option<&str>) -> Arc<dyn GhostSink> {
            Arc::new(NoopCaptureSink)
        }
        fn set_muted(&self, _k: &str, _muted: bool) {}
        fn release(&self, _k: &str) {}
        fn release_all(&self) {}
    }

    /// Build a pool of `n` bots with no channels registered yet.
    fn pool(n: usize) -> Arc<SharedPool> {
        pool_with_timing(n, &TimingConfig::default())
    }

    /// Like [`pool`] but with an explicit timing profile — the paused-time
    /// scenarios shrink the anchor-idle grace through the real `[timing]`
    /// plumbing rather than reaching into pool internals.
    fn pool_with_timing(n: usize, timing: &TimingConfig) -> Arc<SharedPool> {
        let cfg = PuppetsConfig {
            tokens: (0..n).map(|i| format!("Bot {i}")).collect(),
            nickname_prefix: "[Matrix] ".to_string(),
            standby_nickname: "[Matrix: Standby]".to_string(),
        };
        SharedPool::new(&cfg, timing)
    }

    /// Register a channel `label` at guild/channel ids derived from `g`/`c`, and
    /// enroll every pool bot in guild `g` (in production this comes from the
    /// gateway `ready`/`guild_create` events, which don't fire in unit tests).
    fn channel(p: &Arc<SharedPool>, label: &str, g: u64, c: u64) {
        p.register_channel(
            Arc::from(label),
            &g.to_string(),
            &c.to_string(),
            Arc::new(NoopCapture),
            None,
        )
        .expect("register channel");
        for slot in &p.slots {
            slot.guilds
                .write()
                .unwrap()
                .insert(GuildId::new(g));
        }
    }

    #[test]
    fn unregister_releases_bots_and_removes_channel() {
        let p = pool(2);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice");
        p.assign("A", "bob", "Bob");
        assert_eq!(p.test_free_count(), 0);
        p.unregister_channel("A");
        assert_eq!(p.test_free_count(), 2, "all bots freed");
        assert!(!p.test_has_channel("A"), "channel state removed");
        // Double-unregister is a no-op.
        p.unregister_channel("A");
        assert_eq!(p.test_free_count(), 2);
    }

    #[test]
    fn unregister_leaves_same_guild_neighbor_intact() {
        let p = pool(3);
        channel(&p, "A", 1, 10);
        channel(&p, "B", 1, 20);
        p.assign("A", "alice", "Alice");
        p.assign("B", "bob", "Bob");
        let sb = p.test_slot_of("B", "bob").unwrap();
        p.unregister_channel("A");
        // B's per-(bot, guild) session, assignment and anchor are untouched.
        assert_eq!(p.test_slot_of("B", "bob"), Some(sb));
        assert_eq!(p.test_anchor("B"), Some(sb));
        assert!(p.test_session_mixer(sb, 1).is_some(), "B's session survives");
        assert!(p.test_has_channel("B"));
    }

    #[test]
    fn unregister_activates_pending_channel() {
        let p = pool(1);
        channel(&p, "A", 1, 10);
        channel(&p, "B", 1, 20);
        p.assign("A", "alice", "Alice"); // takes the only bot
        p.assign("B", "bob", "Bob"); // pending — no bot free
        assert_eq!(p.test_anchor("B"), None);
        p.unregister_channel("A");
        assert!(p.test_anchor("B").is_some(), "B activated by the freed bot");
        assert!(p.test_slot_of("B", "bob").is_some());
    }

    #[test]
    fn unregister_cancels_armed_idle_anchor_token() {
        let p = pool(1);
        channel(&p, "A", 1, 10);
        let token = p.test_arm_idle_token("A");
        p.unregister_channel("A");
        assert!(token.is_cancelled(), "pending idle-release timer cancelled");
    }

    #[test]
    fn duplicate_register_is_an_error() {
        let p = pool(1);
        channel(&p, "A", 1, 10);
        let Err(err) =
            p.register_channel(Arc::from("A"), "1", "10", Arc::new(NoopCapture), None)
        else {
            panic!("duplicate register_channel must fail");
        };
        assert!(
            err.to_string().contains("already registered"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn dedicated_until_full_then_overflow() {
        let p = pool(2);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice");
        p.assign("A", "bob", "Bob");
        let sa = p.test_slot_of("A", "alice").unwrap();
        let sb = p.test_slot_of("A", "bob").unwrap();
        assert_ne!(sa, sb, "distinct puppets");
        assert_eq!(p.test_anchor("A"), Some(sa), "first assigned is anchor");
        assert_eq!(p.test_free_count(), 0);
        // Pool exhausted → overflow.
        p.assign("A", "carol", "Carol");
        assert!(p.test_is_overflow("A", "carol"));
    }

    #[test]
    fn release_promotes_oldest_overflow_fcfs() {
        let p = pool(2);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice");
        p.assign("A", "bob", "Bob");
        p.assign("A", "carol", "Carol"); // overflow, older
        p.assign("A", "dave", "Dave"); // overflow, newer
        let sa = p.test_slot_of("A", "alice").unwrap();
        p.release("A", "alice");
        // Carol (oldest overflow) promoted into the freed slot; Dave still overflow.
        assert_eq!(p.test_slot_of("A", "carol"), Some(sa));
        assert!(p.test_is_overflow("A", "dave"));
    }

    #[test]
    fn second_channel_waits_then_activates_when_bot_frees() {
        let p = pool(1);
        channel(&p, "A", 1, 10);
        channel(&p, "B", 1, 20);
        p.assign("A", "alice", "Alice"); // takes the only bot (A anchor)
        assert_eq!(p.test_free_count(), 0);
        // B has no anchor and no donor has slack (A has just its anchor) → pending.
        p.assign("B", "bob", "Bob");
        assert_eq!(p.test_anchor("B"), None, "B waiting for a bot");
        assert!(p.test_is_overflow("B", "bob"));
        // Free A's bot → B is activated with it.
        p.release("A", "alice");
        assert!(p.test_anchor("B").is_some(), "B activated");
        assert!(p.test_slot_of("B", "bob").is_some(), "bob now has a puppet");
    }

    #[test]
    fn steals_from_donor_with_slack_to_activate_new_channel() {
        // Same-guild steal: the only kind that still occurs under per-guild
        // allocation (#46) — two channels of ONE guild contend for its bots.
        let p = pool(3);
        channel(&p, "A", 1, 10);
        channel(&p, "B", 1, 20); // same guild, different channel
        p.assign("A", "a1", "A1");
        p.assign("A", "a2", "A2");
        p.assign("A", "a3", "A3"); // A uses all 3 bots (in guild 1)
        assert_eq!(p.test_free_count(), 0);
        let anchor_a = p.test_anchor("A").unwrap();
        // B activates by stealing A's newest non-anchor dedicated speaker (a3).
        p.assign("B", "b1", "B1");
        assert!(p.test_anchor("B").is_some(), "B got an anchor");
        assert!(p.test_slot_of("B", "b1").is_some());
        // a3 (newest) was demoted to A's overflow; A keeps its anchor + a2.
        assert!(p.test_is_overflow("A", "a3"), "newest A speaker demoted");
        assert_eq!(p.test_anchor("A"), Some(anchor_a), "A anchor unchanged");
        assert!(p.test_slot_of("A", "a2").is_some());
    }

    #[test]
    fn freed_bot_repromotes_donor_overflow_after_steal() {
        // Reproduces the live "stranded overflow bot" bug: a steal demotes a donor
        // speaker to overflow; when the stealing channel later frees the bot, the
        // donor's demoted speaker must be re-promoted to a dedicated puppet (it was
        // previously left in overflow forever — no cross-channel re-promotion).
        // Same-guild since #46 (cross-guild contention no longer steals — see
        // guild_not_starved_when_other_guild_holds_all_bots).
        let p = pool(3);
        channel(&p, "A", 1, 10);
        channel(&p, "B", 1, 20); // same guild, different channel
        p.assign("A", "a1", "A1");
        p.assign("A", "a2", "A2");
        p.assign("A", "a3", "A3"); // A uses all 3 bots
        // B activates by stealing A's newest dedicated speaker (a3 → A overflow).
        p.assign("B", "b1", "B1");
        assert!(p.test_is_overflow("A", "a3"), "a3 demoted to overflow by steal");
        assert!(p.test_anchor("B").is_some(), "B got an anchor");
        // B empties → its bot returns to the pool, which must re-promote a3.
        p.release("B", "b1");
        assert!(
            p.test_slot_of("A", "a3").is_some(),
            "a3 re-promoted to a dedicated puppet after the bot freed"
        );
        assert!(!p.test_is_overflow("A", "a3"), "a3 no longer in overflow");
    }

    #[test]
    fn one_bot_serves_two_guilds_concurrently() {
        // The #46 headline capability: one bot holds an independent voice
        // engagement per guild, so a single-bot pool serves two guilds at once.
        let p = pool(1);
        channel(&p, "A", 1, 10);
        channel(&p, "B", 2, 20);
        p.assign("A", "alice", "Alice");
        p.assign("B", "bob", "Bob");
        assert_eq!(p.test_slot_of("A", "alice"), Some(0));
        assert_eq!(p.test_slot_of("B", "bob"), Some(0), "same bot, second guild");
        assert_eq!(p.test_anchor("A"), Some(0));
        assert_eq!(p.test_anchor("B"), Some(0));
        assert!(!p.test_is_overflow("A", "alice"));
        assert!(!p.test_is_overflow("B", "bob"));
    }

    #[test]
    fn guild_not_starved_when_other_guild_holds_all_bots() {
        // The #46 headline regression: guild 1 claiming every bot must not starve
        // guild 2 — the bots are still free PER GUILD 2, no steal, no overflow.
        let p = pool(2);
        channel(&p, "A", 1, 10);
        channel(&p, "B", 2, 20);
        p.assign("A", "a1", "A1");
        p.assign("A", "a2", "A2"); // both bots engaged in guild 1
        p.assign("B", "b1", "B1");
        assert!(p.test_slot_of("B", "b1").is_some(), "B got a dedicated puppet");
        assert!(p.test_anchor("B").is_some(), "B anchored immediately");
        // A is completely undisturbed — nothing was stolen or demoted.
        assert!(p.test_slot_of("A", "a1").is_some());
        assert!(p.test_slot_of("A", "a2").is_some());
        assert!(!p.test_is_overflow("A", "a1"));
        assert!(!p.test_is_overflow("A", "a2"));
    }

    #[test]
    fn same_guild_channels_still_need_distinct_bots() {
        // One voice Call per guild per bot: two bridged channels in the SAME
        // guild can never share a bot (constraint pinned).
        let p = pool(2);
        channel(&p, "A", 1, 10);
        channel(&p, "B", 1, 20);
        p.assign("A", "a", "A");
        p.assign("B", "b", "B");
        let sa = p.test_slot_of("A", "a").unwrap();
        let sb = p.test_slot_of("B", "b").unwrap();
        assert_ne!(sa, sb, "same-guild channels use distinct bots");
    }

    #[test]
    fn release_in_one_guild_preserves_other_guild_session() {
        let p = pool(1);
        channel(&p, "A", 1, 10);
        channel(&p, "B", 2, 20);
        p.assign("A", "alice", "Alice");
        p.assign("B", "bob", "Bob"); // bot 0 multiplexes guilds 1 + 2
        p.release("A", "alice");
        // Guild 1 engagement gone; guild 2 untouched.
        assert!(p.test_session_mixer(0, 1).is_none(), "guild-1 session released");
        assert_eq!(p.test_slot_of("B", "bob"), Some(0), "guild-2 puppet intact");
        assert_eq!(p.test_anchor("B"), Some(0), "guild-2 anchor intact");
        assert_eq!(p.test_anchor("A"), None);
        // The bot is claimable again in guild 1 without disturbing guild 2.
        p.assign("A", "carol", "Carol");
        assert_eq!(p.test_slot_of("A", "carol"), Some(0));
        assert_eq!(p.test_slot_of("B", "bob"), Some(0));
    }

    #[test]
    fn steal_skips_bot_already_serving_target_guild() {
        // A cross-guild donor's spare bot that is already engaged in the target
        // guild must not be stolen (it would clobber that engagement); the steal
        // takes a same-guild donor's spare instead.
        let p = pool(2);
        channel(&p, "A", 1, 10); // guild 1
        channel(&p, "B", 2, 20); // guild 2
        channel(&p, "C", 2, 30); // guild 2, second channel
        p.assign("A", "a1", "A1");
        p.assign("A", "a2", "A2"); // A: both bots engaged in guild 1 (has slack)
        p.assign("B", "b1", "B1");
        p.assign("B", "b2", "B2"); // B: both bots ALSO engaged in guild 2
        // C (guild 2) finds no free bot in guild 2 → must steal. A has slack but
        // its spare is busy in guild 2 (via B) → only B is a valid donor.
        p.assign("C", "c1", "C1");
        assert!(p.test_anchor("C").is_some(), "C activated via same-guild steal");
        assert!(p.test_is_overflow("B", "b2"), "B's newest speaker demoted");
        // A (guild 1) is completely untouched.
        assert!(p.test_slot_of("A", "a1").is_some());
        assert!(p.test_slot_of("A", "a2").is_some());
        assert!(!p.test_is_overflow("A", "a1"));
        assert!(!p.test_is_overflow("A", "a2"));
    }

    #[test]
    fn cross_guild_release_does_not_promote_where_bot_still_busy() {
        // Bot 0 serves guild 1 (a1) and guild 2 (b1); a2 waits in guild 1's
        // overflow. Releasing the guild-2 engagement frees the bot FOR GUILD 2
        // only — it must not "promote" a2, whose guild still has the bot busy.
        let p = pool(1);
        channel(&p, "A", 1, 10);
        channel(&p, "B", 2, 20);
        p.assign("A", "a1", "A1");
        p.assign("B", "b1", "B1");
        p.assign("A", "a2", "A2");
        assert!(p.test_is_overflow("A", "a2"), "no second guild-1 bot → overflow");
        p.release("B", "b1");
        assert!(p.test_is_overflow("A", "a2"), "a2 stays overflow — bot still busy in guild 1");
        assert_eq!(p.test_slot_of("A", "a1"), Some(0), "a1 undisturbed");
        // Releasing a1 promotes a2 within the channel as usual.
        p.release("A", "a1");
        assert_eq!(p.test_slot_of("A", "a2"), Some(0), "a2 promoted once guild 1 frees");
    }

    #[test]
    fn freed_bot_only_repromotes_guild_capable_channel() {
        // A freed bot in guild 1 must NOT be pulled into a guild-2 channel's overflow
        // it can't serve. Partitioned pool: bot 0 ∈ guild 1, bot 1 ∈ guild 2.
        let p = pool(2);
        p.register_channel(Arc::from("A"), "1", "10", Arc::new(NoopCapture), None).unwrap();
        p.register_channel(Arc::from("B"), "2", "20", Arc::new(NoopCapture), None).unwrap();
        p.slots[0].guilds.write().unwrap().insert(GuildId::new(1));
        p.slots[1].guilds.write().unwrap().insert(GuildId::new(2));
        // B (guild 2) anchored on bot 1, plus an overflow speaker it can't promote
        // (no guild-2 bot free).
        p.assign("B", "b1", "B1");
        p.assign("B", "b2", "B2");
        assert!(p.test_is_overflow("B", "b2"), "b2 overflow (no guild-2 spare)");
        // A (guild 1) takes bot 0, then releases it → bot 0 frees.
        p.assign("A", "a1", "A1");
        p.release("A", "a1");
        // The freed guild-1 bot can't serve B (guild 2); b2 stays overflow.
        assert!(p.test_is_overflow("B", "b2"), "b2 still overflow — bot wrong guild");
    }

    #[test]
    fn any_bot_in_guild_reflects_learned_membership() {
        let p = pool(2);
        assert!(!p.any_bot_in_guild(7), "nothing learned yet");
        p.slots[1].guilds.write().unwrap().insert(GuildId::new(7));
        assert!(p.any_bot_in_guild(7), "one bot knowing the guild suffices");
        assert!(!p.any_bot_in_guild(8), "unknown guild");
        assert!(!p.any_bot_in_guild(0), "guild 0 is invalid input, not a panic");
        assert!(!pool(0).any_bot_in_guild(7), "empty pool covers nothing");
    }

    #[test]
    fn join_failure_quarantines_and_fails_over_anchor() {
        let p = pool(2);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice");
        let sa = p.test_slot_of("A", "alice").unwrap();
        assert_eq!(p.test_anchor("A"), Some(sa));
        // The anchor bot's join times out → fail over to the other pool bot.
        p.on_join_failed(sa, GuildId::new(1));
        assert!(p.test_is_unhealthy(sa, 1), "stuck bot quarantined in guild 1");
        let sb = p.test_slot_of("A", "alice").expect("alice re-assigned");
        assert_ne!(sb, sa, "moved to a different bot");
        assert_eq!(p.test_anchor("A"), Some(sb), "anchor handed to replacement");
        assert!(!p.test_is_unhealthy(sb, 1));
    }

    #[test]
    fn join_failure_with_no_spare_degrades_then_recovers() {
        let p = pool(1);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice");
        let sa = p.test_slot_of("A", "alice").unwrap();
        // Sole bot's join times out, no spare → degrade to overflow + no anchor.
        p.on_join_failed(sa, GuildId::new(1));
        assert!(p.test_is_unhealthy(sa, 1));
        assert!(p.test_is_overflow("A", "alice"), "dropped to overflow");
        assert_eq!(p.test_anchor("A"), None, "channel has no anchor (degraded)");
        // Quarantine clears → service_pending re-activates the channel on that bot.
        p.recover_bot(sa, GuildId::new(1));
        assert!(!p.test_is_unhealthy(sa, 1));
        assert_eq!(p.test_anchor("A"), Some(sa), "re-anchored after recovery");
        assert_eq!(p.test_slot_of("A", "alice"), Some(sa));
    }

    #[test]
    fn driver_disconnect_recovery_gate() {
        let p = pool(2);
        channel(&p, "A", 1, 10);
        let sink = p.assign("A", "alice", "Alice"); // alice → dedicated puppet + anchor
        let sa = p.test_slot_of("A", "alice").unwrap();
        let free = (0..2).find(|i| !p.test_is_busy(*i)).expect("a spare bot");

        let g = GuildId::new(1);
        // An active, healthy bot claims recovery once; repeats are debounced until
        // the in-flight attempt finishes (so concurrent DriverDisconnect + sweep
        // can't launch two rejoins).
        assert!(p.should_recover_disconnect(sa, g), "active bot: claim recovery");
        assert!(p.test_is_recovering(sa, 1));
        assert!(!p.should_recover_disconnect(sa, g), "debounced while recovering");
        p.finish_recovery(sa, g);
        assert!(!p.test_is_recovering(sa, 1));
        assert!(p.should_recover_disconnect(sa, g), "claimable again after finish");
        p.finish_recovery(sa, g);

        // An idle bot has nothing to recover.
        assert!(!p.should_recover_disconnect(free, g), "idle bot: skip");

        // A quarantined pair is left to the join-timeout/backoff machinery.
        p.test_set_unhealthy(sa, 1, true);
        assert!(!p.should_recover_disconnect(sa, g), "unhealthy pair: skip");
        p.test_set_unhealthy(sa, 1, false);

        // The crux: a deliberate release drops the session BEFORE its
        // DriverDisconnect fires, so a late disconnect for that bot must NOT
        // trigger a spurious rejoin.
        drop(sink);
        p.release("A", "alice");
        assert!(!p.test_is_busy(sa), "released → not busy");
        assert!(
            !p.should_recover_disconnect(sa, g),
            "deliberate leave must not be mistaken for a fault"
        );
    }

    #[test]
    fn quarantine_in_one_guild_leaves_bot_usable_in_another() {
        // Health is per-(bot, guild) (#46): a broken voice connection in guild 1
        // must not sideline the bot's engagements (or claimability) in guild 2.
        let p = pool(1);
        channel(&p, "A", 1, 10);
        channel(&p, "B", 2, 20);
        p.assign("A", "alice", "Alice");
        p.on_join_failed(0, GuildId::new(1)); // guild-1 join timed out
        assert!(p.test_is_unhealthy(0, 1), "quarantined in guild 1");
        assert!(!p.test_is_unhealthy(0, 2), "guild 2 unaffected");
        assert_eq!(p.test_anchor("A"), None, "A degraded (sole bot)");
        // The bot is still claimable — and recoverable — in guild 2.
        p.assign("B", "bob", "Bob");
        assert_eq!(p.test_slot_of("B", "bob"), Some(0), "claimed in guild 2 while quarantined in guild 1");
        assert!(p.should_recover_disconnect(0, GuildId::new(2)), "guild-2 recovery unaffected");
        p.finish_recovery(0, GuildId::new(2));
        // Guild 1's quarantine clears → its pending channel re-anchors on the bot.
        p.recover_bot(0, GuildId::new(1));
        assert!(!p.test_is_unhealthy(0, 1));
        assert_eq!(p.test_anchor("A"), Some(0), "A re-anchored after guild-1 recovery");
        assert_eq!(p.test_slot_of("A", "alice"), Some(0));
        assert_eq!(p.test_slot_of("B", "bob"), Some(0), "guild-2 engagement undisturbed throughout");
    }

    #[test]
    fn quarantine_backoff_grows_then_resets() {
        let p = pool(1);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice");
        let sa = p.test_slot_of("A", "alice").unwrap();
        // Consecutive failures back off exponentially, capped at quarantine_max
        // (the default [timing] profile: base 15 s, cap 120 s).
        let g = GuildId::new(1);
        assert_eq!(p.on_join_failed(sa, g), Duration::from_secs(15));
        assert_eq!(p.on_join_failed(sa, g), Duration::from_secs(30));
        assert_eq!(p.on_join_failed(sa, g), Duration::from_secs(60));
        assert_eq!(p.on_join_failed(sa, g), Duration::from_secs(120));
        assert_eq!(p.on_join_failed(sa, g), Duration::from_secs(120), "cap holds");
        // A successful join proves the bot healthy → backoff resets to base.
        p.on_join_succeeded(sa, g);
        assert_eq!(p.on_join_failed(sa, g), Duration::from_secs(15), "reset after success");
    }

    #[test]
    fn allocates_only_guild_capable_bots() {
        // Partitioned pool: bot 0 only in guild 1, bot 1 only in guild 2.
        let p = pool(2);
        p.register_channel(Arc::from("A"), "1", "10", Arc::new(NoopCapture), None).unwrap();
        p.register_channel(Arc::from("B"), "2", "20", Arc::new(NoopCapture), None).unwrap();
        p.slots[0].guilds.write().unwrap().insert(GuildId::new(1));
        p.slots[1].guilds.write().unwrap().insert(GuildId::new(2));

        // Each channel can only use the bot that's in its guild.
        p.assign("A", "a", "A");
        p.assign("B", "b", "B");
        assert_eq!(p.test_slot_of("A", "a"), Some(0), "A uses the guild-1 bot");
        assert_eq!(p.test_slot_of("B", "b"), Some(1), "B uses the guild-2 bot");

        // A second A speaker can't use bot 1 (wrong guild) and B has no slack to
        // steal a guild-1 bot from → overflow.
        p.assign("A", "a2", "A2");
        assert!(p.test_is_overflow("A", "a2"));
    }

    #[test]
    fn nickname_prefix_applied() {
        assert_eq!(build_nickname("[Matrix] ", "Dark Collective"), "[Matrix] Dark Collective");
    }

    #[test]
    fn nickname_empty_prefix_is_just_name() {
        assert_eq!(build_nickname("", "Victor"), "Victor");
    }

    #[test]
    fn nickname_truncated_to_32_chars() {
        let nick = build_nickname("[Matrix] ", "A Very Long Display Name That Exceeds The Limit");
        assert_eq!(nick.chars().count(), 32);
        assert!(nick.starts_with("[Matrix] A Very Long"));
    }

    #[test]
    fn nickname_truncation_respects_char_boundaries() {
        let nick = build_nickname("[Matrix] ", &"é".repeat(40));
        assert!(nick.chars().count() <= 32);
        assert!(nick.starts_with("[Matrix] "));
    }

    // Samples a buffer must accumulate before the mixer releases it (priming).
    const PRIME: usize = 150 * 48;

    #[test]
    fn routed_sink_follows_swapped_mixer() {
        // A RoutedSink pushes to whatever mixer its route points at; swapping the
        // route (as promotion/demotion does) redirects subsequent frames with no
        // change on the Matrix side.
        let a = Arc::new(PlaybackMixer::new());
        let b = Arc::new(PlaybackMixer::new());
        let route: Route = Arc::new(StdMutex::new(a.clone()));
        let sink = RoutedSink { route: route.clone(), key: "k".to_owned() };

        sink.push(&vec![100i16; PRIME]);
        assert!(a.pop_10ms_mixed().iter().all(|&s| s == 100), "frames went to A");

        *route.lock().unwrap() = b.clone();
        sink.push(&vec![80i16; PRIME]);
        assert!(b.pop_10ms_mixed().iter().all(|&s| s == 80), "frames follow to B after swap");
    }

    #[test]
    fn note_presence_reports_edges_once() {
        let p = pool(2);
        channel(&p, "A", 1, 10);
        // Voice-state events reach every pool bot; only the first observer of a
        // join sees the 0→1 edge (the one that should mint the ghost).
        assert!(p.note_presence("A", 1001, true), "first join is an edge");
        assert!(!p.note_presence("A", 1001, true), "duplicate join (2nd bot) is not");
        assert!(!p.note_presence("A", 1001, true), "still not");
        // Likewise only the first observer of the matching leave sees the 1→0 edge.
        assert!(p.note_presence("A", 1001, false), "first leave is an edge");
        assert!(!p.note_presence("A", 1001, false), "duplicate leave is not");
        // A leave for a user that was never present is never an edge.
        assert!(!p.note_presence("A", 9999, false));
    }

    #[test]
    fn note_presence_is_per_user_and_per_channel() {
        let p = pool(2);
        channel(&p, "A", 1, 10);
        channel(&p, "B", 1, 20);
        assert!(p.note_presence("A", 1, true));
        assert!(p.note_presence("A", 2, true), "a distinct user is its own edge");
        assert!(p.note_presence("B", 1, true), "same user in another channel edges once");
        // Leaving A leaves the user's B presence untouched.
        assert!(p.note_presence("A", 1, false));
        assert!(!p.note_presence("A", 1, false));
        assert!(p.note_presence("B", 1, false), "B still tracked the user");
        // An unknown bridge is a no-op, never an edge.
        assert!(!p.note_presence("nope", 1, true));
    }

    #[test]
    fn bridge_bots_excluded_from_presence() {
        let p = pool(1);
        // A puppet bot's user id, as recorded on the gateway `ready` event.
        p.bot_ids.write().unwrap().insert(4242);
        assert!(p.is_bridge_bot(4242), "our own bot is recognised");
        assert!(!p.is_bridge_bot(1001), "a human is not");
    }

    #[test]
    fn releasing_anchor_consolidates_a_participant_onto_it() {
        // When the anchor's own participant leaves while OTHER participants remain,
        // the oldest moves onto the anchor (route-swap, SSRC map preserved) and its
        // old bot is freed — no nameless "[Matrix] " capture-only orphan.
        let p = pool(3);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice"); // anchor
        p.assign("A", "bob", "Bob"); // oldest non-anchor
        p.assign("A", "carol", "Carol");
        let anchor = p.test_anchor("A").unwrap();
        let bob_old = p.test_slot_of("A", "bob").unwrap();
        assert_ne!(bob_old, anchor);

        p.release("A", "alice");

        assert_eq!(p.test_anchor("A"), Some(anchor), "anchor bot is unchanged");
        assert_eq!(p.test_slot_of("A", "bob"), Some(anchor), "bob consolidated onto the anchor");
        assert!(p.test_slot_of("A", "carol").is_some(), "carol still served");
        assert_eq!(p.test_free_count(), 1, "bob's old bot was freed");
        assert!(p.test_slot_of("A", "alice").is_none(), "alice released");
    }

    #[tokio::test]
    async fn assign_reuses_a_capture_only_anchor() {
        // A lone participant leaving while a Discord speaker remains retains a
        // capture-only anchor; the next participant must REUSE it, not grab a fresh
        // bot (which would leave the anchor as a nameless "[Matrix] " bot).
        let p = pool(2);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice");
        let anchor = p.test_anchor("A").unwrap();
        // Mark a Discord speaker present so the anchor is retained on release.
        {
            let guard = p.alloc.lock().unwrap();
            let ch = guard.channels.get("A").unwrap();
            ch.user_sinks
                .write()
                .unwrap()
                .insert(999, Arc::new(NoopCaptureSink));
        }
        p.release("A", "alice"); // no other puppet → capture-only anchor (grace armed)
        assert_eq!(p.test_anchor("A"), Some(anchor), "capture-only anchor retained");
        assert!(p.test_slot_of("A", "alice").is_none());

        p.assign("A", "dave", "Dave");
        assert_eq!(p.test_slot_of("A", "dave"), Some(anchor), "dave reused the anchor bot");
        assert_eq!(p.test_free_count(), 1, "the spare bot stayed free (no fresh allocation)");
    }

    /// Drain and return the command names queued on bot `idx`'s command channel
    /// (test-only: the async command loop that would consume them isn't running).
    fn drain_cmd_names(p: &Arc<SharedPool>, idx: usize) -> Vec<&'static str> {
        let mut slot_rx = p.slots[idx].cmd_rx.lock().unwrap();
        let mut rx = slot_rx.take().expect("cmd_rx present");
        let mut names = Vec::new();
        while let Ok(cmd) = rx.try_recv() {
            names.push(cmd.label());
        }
        *slot_rx = Some(rx); // restore for later drains / the real command loop
        names
    }

    #[tokio::test]
    async fn join_failure_on_capture_only_anchor_clears_stale_anchor() {
        // review F1-1/F2-1: a capture-only anchor (kept after its participant left
        // with Discord speakers present) whose rejoin then FAILS used to leave the
        // channel's `anchor` pointing at a session-less bot — the next assign()
        // panicked at the reuse-capture-only-anchor path. on_join_failed's
        // assignment-gone branch must clear that stale election.
        let p = pool(2);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice");
        let anchor = p.test_anchor("A").unwrap();
        mark_speaker(&p, "A", 999);
        p.release("A", "alice"); // capture-only anchor retained, grace armed
        assert_eq!(p.test_anchor("A"), Some(anchor));
        assert!(p.test_slot_of("A", "alice").is_none());

        // The kept anchor's rejoin fails (voice server still down).
        p.on_join_failed(anchor, GuildId::new(1));
        assert_eq!(p.test_anchor("A"), None, "stale capture-only anchor cleared");

        // The formerly-panicking assign must now succeed and land dave consistently.
        p.assign("A", "dave", "Dave");
        // bot `anchor` is quarantined, so dave overflows until it recovers.
        assert!(p.test_is_overflow("A", "dave") || p.test_slot_of("A", "dave").is_some());
        p.recover_bot(anchor, GuildId::new(1));
        assert!(p.test_anchor("A").is_some(), "channel re-anchored after recovery");
        assert!(p.test_slot_of("A", "dave").is_some(), "dave served on a live bot");
    }

    #[tokio::test]
    async fn demoted_mid_join_anchor_still_activates_capture() {
        // review F1-2: an elected anchor demoted to capture-only while its join was
        // in flight (its own participant left) must still receive BecomeAnchor when
        // the join completes — otherwise the anchor sits in voice with no capture
        // handler and Discord→Matrix audio is silent.
        let p = pool(2);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice");
        let anchor = p.test_anchor("A").unwrap();
        let _ = drain_cmd_names(&p, anchor); // clear the Assign queued so far
        mark_speaker(&p, "A", 999);
        p.release("A", "alice"); // demote to capture-only (keep anchor), grace armed
        assert_eq!(p.test_anchor("A"), Some(anchor));

        // The join (started before the demotion) now completes.
        p.on_join_succeeded(anchor, GuildId::new(1));
        let names = drain_cmd_names(&p, anchor);
        assert!(
            names.contains(&"BecomeAnchor"),
            "demoted capture-only anchor must still activate capture, got {names:?}"
        );
    }

    #[test]
    fn recovery_reelects_surviving_puppet_without_a_free_bot() {
        // review F1-3: a channel left anchor-less but still served by a dedicated
        // puppet (the failed anchor's participant left, a sibling puppet remains)
        // must re-elect that surviving puppet as capture anchor — even with no free
        // bot to hand out. Previously service_pending could only anchor a channel by
        // promoting an overflow speaker, so the channel stayed capture-less.
        let p = pool(2);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice"); // bot 0, anchor
        p.assign("A", "bob", "Bob"); // bot 1, dedicated
        let anchor = p.test_anchor("A").unwrap();
        let survivor = p.test_slot_of("A", "bob").unwrap();
        assert_ne!(anchor, survivor);

        // The anchor bot's join fails; no spare (bot 1 is busy serving bob).
        p.on_join_failed(anchor, GuildId::new(1));
        // With the fix, the surviving puppet is re-elected immediately.
        assert_eq!(
            p.test_anchor("A"),
            Some(survivor),
            "surviving puppet re-elected as anchor with no free bot"
        );
        assert!(p.test_slot_of("A", "bob").is_some(), "bob still served");
    }

    /// Mark a Discord speaker present in channel `label` (in production this
    /// comes from the anchor's capture wiring).
    fn mark_speaker(p: &Arc<SharedPool>, label: &str, user: u64) {
        let guard = p.alloc.lock().unwrap();
        let ch = guard.channels.get(label).unwrap();
        ch.user_sinks.write().unwrap().insert(user, Arc::new(NoopCaptureSink));
    }

    #[tokio::test]
    async fn call_end_release_all_keeps_capture_only_anchor() {
        // #59: the prod sequence — the per-member release keeps a capture-only
        // anchor (Discord speakers remain), then the call-end `release_all`
        // fires ~0.5s later. It used to free the kept anchor unconditionally,
        // leaving the bridge dormant with humans still in the channel.
        let p = pool(2);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice");
        let anchor = p.test_anchor("A").unwrap();
        mark_speaker(&p, "A", 999);
        p.release("A", "alice"); // keep decision (grace armed)
        assert_eq!(p.test_anchor("A"), Some(anchor));
        p.release_all("A"); // the call-end sweep must respect it
        assert_eq!(
            p.test_anchor("A"),
            Some(anchor),
            "call-end keeps the capture-only anchor while Discord speakers remain"
        );
        // A Matrix user returning within the grace reuses the live anchor.
        p.assign("A", "dave", "Dave");
        assert_eq!(p.test_slot_of("A", "dave"), Some(anchor), "returning user reused the anchor");
        assert_eq!(p.test_free_count(), 1);
    }

    #[tokio::test]
    async fn call_end_release_all_demotes_active_anchor_frees_the_rest() {
        // #59: a call-end that still holds live assignments — the anchor (with
        // speakers present) is demoted to capture-only, every other puppet is
        // freed, routes/overflow are cleared.
        let p = pool(2);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice");
        p.assign("A", "bob", "Bob");
        let anchor = p.test_anchor("A").unwrap();
        mark_speaker(&p, "A", 999);
        p.release_all("A");
        assert_eq!(p.test_anchor("A"), Some(anchor), "anchor demoted, not freed");
        assert!(p.test_slot_of("A", "alice").is_none());
        assert!(p.test_slot_of("A", "bob").is_none());
        assert_eq!(p.test_free_count(), 1, "the non-anchor bot returned to the pool");
    }

    #[test]
    fn bot_identity_map_routes_ids_to_slots() {
        // #62: the id→slot map lets the gateway handler route a bot's own
        // voice-state event to that bot's command loop. Test tokens don't
        // decode, so seed via the `ready`-path insert.
        let p = pool(2);
        p.bot_index_by_id.write().unwrap().insert(111, 0);
        p.bot_index_by_id.write().unwrap().insert(222, 1);
        assert_eq!(p.bot_index_of(111), Some(0));
        assert_eq!(p.bot_index_of(222), Some(1));
        assert_eq!(p.bot_index_of(333), None);
    }

    #[test]
    fn optout_retires_ghost_and_gates_lookup() {
        // #63: opting out retires the user's existing ghost sink everywhere
        // (their audio then has no demux target, and ensure_ghost won't mint
        // a new one while they stay in the set).
        let p = pool(1);
        channel(&p, "A", 1, 10);
        mark_speaker(&p, "A", 555); // user 555 has a live ghost sink
        assert!(!p.is_opted_out(555));
        p.optout_set.write().unwrap().insert(555);
        assert!(p.is_opted_out(555));
        p.apply_optout(555);
        let guard = p.alloc.lock().unwrap();
        let sinks = guard.channels.get("A").unwrap().user_sinks.read().unwrap();
        assert!(!sinks.contains_key(&555), "opted-out user's ghost sink retired");
    }

    #[test]
    fn call_end_release_all_drains_fully_without_speakers() {
        // #59 boundary: with no Discord speakers there is nothing to capture —
        // the call-end sweep drains to baseline exactly as before.
        let p = pool(2);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice");
        p.release_all("A");
        assert!(p.test_anchor("A").is_none(), "no speakers → anchor freed");
        assert_eq!(p.test_free_count(), 2);
    }

    #[test]
    fn reclaimed_bot_gets_a_fresh_session_mixer() {
        // A released bot's session (and its mixer) is dropped; a later claim mints
        // a fresh session with a NEW mixer, so a reclaimed bot can never replay a
        // previous participant's buffered audio.
        let p = pool(1);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice");
        let idx = p.test_slot_of("A", "alice").unwrap();
        let first = p.test_session_mixer(idx, 1).expect("live session has a mixer");
        p.release("A", "alice");
        assert!(p.test_session_mixer(idx, 1).is_none(), "session gone after release");
        p.assign("A", "bob", "Bob");
        assert_eq!(p.test_slot_of("A", "bob"), Some(idx));
        let second = p.test_session_mixer(idx, 1).expect("new session has a mixer");
        assert!(!Arc::ptr_eq(&first, &second), "fresh mixer per session");
    }

    #[test]
    fn decode_bot_user_id_from_token() {
        use base64::Engine;
        let uid = 1515382284960796774u64;
        let seg = base64::engine::general_purpose::STANDARD_NO_PAD.encode(uid.to_string());
        let token = format!("{seg}.AbCdEf.xyz123-hmac");
        assert_eq!(decode_bot_user_id(&token), Some(uid), "decodes a token-shaped string");
        assert_eq!(
            decode_bot_user_id(&format!("Bot {token}")),
            Some(uid),
            "tolerates a 'Bot ' prefix"
        );
        assert_eq!(decode_bot_user_id("not-base64.x.y"), None, "rejects garbage");
    }

    #[test]
    fn retract_self_ghost_clears_presence_and_sink() {
        // A startup race can mint a ghost for our own puppet bot before its id is in
        // `bot_ids`; retract_self_ghost (called from `ready`) must clean it up.
        let p = pool(1);
        channel(&p, "A", 1, 10);
        {
            let mut guard = p.alloc.lock().unwrap();
            let ch = guard.channels.get_mut("A").unwrap();
            ch.presence.insert(4242);
            ch.user_sinks
                .write()
                .unwrap()
                .insert(4242, Arc::new(NoopCaptureSink));
        }
        p.retract_self_ghost(4242);
        let guard = p.alloc.lock().unwrap();
        let ch = guard.channels.get("A").unwrap();
        assert!(!ch.presence.contains(&4242), "presence entry cleared");
        assert!(
            !ch.user_sinks.read().unwrap().contains_key(&4242),
            "ghost sink cleared"
        );
    }

    // --- Tier-2 named scenarios (#38 Phase C2): multi-step walks through the
    // allocation lifecycle, composing the seams the single-step tests above
    // verify in isolation. Each maps to an entry in the scenario catalogue
    // (docs/nether-voicebridge-testing-design.md).

    /// Catalogue: FCFS promotion order. Overflow speakers must be promoted in
    /// strict arrival order as slots free up, across releases of dedicated
    /// speakers AND of the anchor's own participant (which consolidates and
    /// frees a second bot).
    #[test]
    fn scenario_overflow_fcfs_promotion_order() {
        let p = pool(3);
        channel(&p, "A", 1, 10);
        p.assign("A", "u1", "U1"); // anchor
        p.assign("A", "u2", "U2");
        p.assign("A", "u3", "U3"); // pool exhausted
        p.assign("A", "o1", "O1"); // overflow, oldest
        p.assign("A", "o2", "O2");
        p.assign("A", "o3", "O3"); // overflow, newest
        for o in ["o1", "o2", "o3"] {
            assert!(p.test_is_overflow("A", o), "{o} starts in overflow");
        }

        // First freed slot goes to o1 — and exactly the freed slot.
        let s2 = p.test_slot_of("A", "u2").unwrap();
        p.release("A", "u2");
        assert_eq!(p.test_slot_of("A", "o1"), Some(s2), "o1 promoted first (oldest)");
        assert!(p.test_is_overflow("A", "o2") && p.test_is_overflow("A", "o3"));

        // Second freed slot goes to o2, never o3.
        p.release("A", "u3");
        assert!(p.test_slot_of("A", "o2").is_some(), "o2 promoted second");
        assert!(p.test_is_overflow("A", "o3"), "o3 still waiting");

        // Releasing the anchor's own participant consolidates the oldest
        // non-anchor speaker onto the anchor and frees that bot → o3 promoted.
        p.release("A", "u1");
        assert!(p.test_slot_of("A", "o3").is_some(), "o3 promoted by consolidation");
        for o in ["o1", "o2", "o3"] {
            assert!(!p.test_is_overflow("A", o), "{o} ends dedicated");
        }
        assert_eq!(p.test_free_count(), 0, "all three bots serving o1-o3");
    }

    /// Catalogue: #24 steal → release → re-promotion, run for TWO rounds. The
    /// live "stranded overflow bot" bug was a one-shot fix; this guards that
    /// the demote/re-promote bookkeeping is re-entrant under channel churn.
    #[test]
    fn scenario_steal_release_repromotion_two_rounds() {
        let p = pool(3);
        channel(&p, "A", 1, 10);
        channel(&p, "B", 1, 20); // same guild: the only steal case since #46
        p.assign("A", "a1", "A1");
        p.assign("A", "a2", "A2");
        p.assign("A", "a3", "A3"); // A holds all 3 bots
        let anchor_a = p.test_anchor("A").unwrap();

        for round in 1..=2 {
            // B activates by stealing A's newest dedicated speaker.
            p.assign("B", "b1", "B1");
            assert!(p.test_anchor("B").is_some(), "round {round}: B anchored");
            assert!(
                p.test_is_overflow("A", "a3"),
                "round {round}: a3 demoted to overflow by the steal"
            );
            assert_eq!(p.test_anchor("A"), Some(anchor_a), "round {round}: A anchor stable");
            // B empties → freed bot must re-promote a3 (not strand it).
            p.release("B", "b1");
            assert!(
                p.test_slot_of("A", "a3").is_some() && !p.test_is_overflow("A", "a3"),
                "round {round}: a3 re-promoted after B freed the bot"
            );
            assert_eq!(p.test_free_count(), 0, "round {round}: all bots back on A");
        }
    }

    /// Catalogue: anchor idle release. A capture-only anchor (Discord speakers
    /// present, no Matrix participant) must actually leave voice once the
    /// anchor-idle grace elapses — driven end-to-end through the `[timing]`
    /// profile (#38 C1) under tokio paused time.
    #[tokio::test(start_paused = true)]
    async fn scenario_anchor_idle_release_after_grace() {
        let timing = TimingConfig { anchor_idle_grace_ms: 5_000, ..TimingConfig::default() };
        let p = pool_with_timing(2, &timing);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice");
        let anchor = p.test_anchor("A").unwrap();
        // A Discord speaker is present, so alice's release keeps a capture-only
        // anchor and arms the idle grace.
        {
            let guard = p.alloc.lock().unwrap();
            let ch = guard.channels.get("A").unwrap();
            ch.user_sinks.write().unwrap().insert(999, Arc::new(NoopCaptureSink));
        }
        p.release("A", "alice");
        assert_eq!(p.test_anchor("A"), Some(anchor), "capture-only anchor retained");

        // Just short of the grace: still holding voice.
        tokio::time::sleep(Duration::from_millis(4_900)).await;
        assert_eq!(p.test_anchor("A"), Some(anchor), "still anchored before grace");

        // Past the grace: the timer fires and the bot leaves.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(p.test_anchor("A"), None, "anchor released after idle grace");
        assert_eq!(p.test_free_count(), 2, "connection count back to baseline");
    }

    /// Catalogue: anchor idle release must be CANCELLED by a Matrix participant
    /// returning within the grace — and stay cancelled (no late release firing
    /// after the original deadline passes).
    #[tokio::test(start_paused = true)]
    async fn scenario_anchor_idle_release_cancelled_by_returning_user() {
        let timing = TimingConfig { anchor_idle_grace_ms: 5_000, ..TimingConfig::default() };
        let p = pool_with_timing(2, &timing);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice");
        let anchor = p.test_anchor("A").unwrap();
        {
            let guard = p.alloc.lock().unwrap();
            let ch = guard.channels.get("A").unwrap();
            ch.user_sinks.write().unwrap().insert(999, Arc::new(NoopCaptureSink));
        }
        p.release("A", "alice"); // grace armed

        // A participant returns well within the grace and reuses the anchor.
        tokio::time::sleep(Duration::from_millis(2_000)).await;
        p.assign("A", "dave", "Dave");
        assert_eq!(p.test_slot_of("A", "dave"), Some(anchor), "dave reused the anchor");

        // Long past the original deadline: the cancelled timer must not fire.
        tokio::time::sleep(Duration::from_millis(10_000)).await;
        assert_eq!(p.test_anchor("A"), Some(anchor), "no late idle release");
        assert_eq!(p.test_slot_of("A", "dave"), Some(anchor), "dave undisturbed");
    }
}
