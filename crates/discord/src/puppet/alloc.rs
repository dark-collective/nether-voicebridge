//! Synchronous allocation core for [`SharedPool`] — the per-(bot, guild)
//! allocator discipline (all under `SharedPool::alloc`): assign/release,
//! steal-for-activation, overflow promotion, rebalance, join-timeout
//! quarantine + disconnect recovery, the anchor-idle-release machinery, and
//! the channel/bot lookup helpers. Pure `std::sync::Mutex` code — no `await`.

use super::*;
use super::router::{NullSink, RoutedSink};
use nvb_audio::routing::ParticipantSink;

impl SharedPool {
    /// Build the per-channel command context (clones the channel's `Arc` handles
    /// plus the global bot-id set).
    pub(super) fn ctx_for(&self, ch: &ChannelState) -> ChannelCtx {
        ChannelCtx {
            bridge: ch.bridge.clone(),
            guild_id: ch.guild_id,
            channel_id: ch.channel_id,
            exclude: ch.exclude.clone(),
            ssrc_map: ch.ssrc_map.clone(),
            user_sinks: ch.user_sinks.clone(),
            capture_router: ch.capture_router.clone(),
            capture_sink: ch.capture_sink.clone(),
            bot_ids: self.bot_ids.clone(),
            optout: self.optout_set.clone(),
            anchor: ch.anchor.clone(),
            overflow_mixer: ch.overflow_mixer.clone(),
        }
    }

    /// Look up the bridged channel at `guild_id`/`channel_id` (regardless of which
    /// bot, if any, anchors it), returning its [`ChannelCtx`]. Used to attribute a
    /// voice member to its room without holding the anchor.
    pub(super) fn by_channel(&self, guild_id: GuildId, channel_id: ChannelId) -> Option<ChannelCtx> {
        let guard = self.alloc();
        let ch = guard
            .channels
            .values()
            .find(|ch| ch.guild_id == guild_id && ch.channel_id == channel_id)?;
        Some(self.ctx_for(ch))
    }

    /// Every bridged channel in `guild_id`, as [`ChannelCtx`]es — regardless of
    /// which bot (if any) anchors them. Lets ANY bot reconcile a voice-state event
    /// against all the guild's channels, so presence detection is not gated on
    /// holding the anchor.
    pub(super) fn channels_in_guild(&self, guild_id: GuildId) -> Vec<ChannelCtx> {
        let guard = self.alloc();
        guard
            .channels
            .values()
            .filter(|ch| ch.guild_id == guild_id)
            .map(|ch| self.ctx_for(ch))
            .collect()
    }

    /// Whether `user_id` is one of our own puppet bots (echo-exclusion: a bridge
    /// bot is never a ghost and never counts as Discord presence).
    pub(super) fn is_bridge_bot(&self, user_id: u64) -> bool {
        self.bot_ids
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&user_id)
    }

    /// Slot index of one of our OWN puppet bots, by Discord user id (#62).
    pub(super) fn bot_index_of(&self, user_id: u64) -> Option<usize> {
        self.bot_index_by_id
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&user_id)
            .copied()
    }

    /// Retract any ghost wrongly minted for one of our OWN puppet bots. A puppet is
    /// never a Discord speaker, but a startup race can mint one: if the bot was
    /// already in a voice channel (e.g. a session lingering from a previous crash)
    /// when another bot's `guild_create` seeding scanned voice members, this bot's
    /// id was not yet in `bot_ids`, so it slipped past echo-exclusion (constraint
    /// #5). Called from `ready` the moment the id is recorded, so the echo
    /// self-corrects — typically before the ghost ever publishes or posts
    /// membership. Mirrors `drop_ghost`'s teardown.
    pub(super) fn retract_self_ghost(&self, user_id: u64) {
        let mut guard = self.alloc();
        for ch in guard.channels.values_mut() {
            ch.presence.remove(&user_id);
            let removed = ch
                .user_sinks
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&user_id)
                .is_some();
            if removed {
                ch.capture_router.release(&user_id.to_string());
                tracing::warn!(user_id, bridge = %ch.bridge, "retracted echo ghost minted for our own puppet bot before its ready (startup race)");
            }
        }
    }

    /// Record that `user_id` is now `present` (or gone) in `bridge`'s channel,
    /// returning `true` iff this call observed the **edge** — the user was newly
    /// inserted (0→1) or actually removed (1→0). Voice-state events reach every bot
    /// in the guild; the shared `presence` set under the alloc lock makes duplicates
    /// idempotent, so exactly one observer drives the ghost create/retire.
    pub(super) fn note_presence(&self, bridge: &str, user_id: u64, present: bool) -> bool {
        let mut guard = self.alloc();
        let Some(ch) = guard.channels.get_mut(bridge) else {
            return false;
        };
        if present {
            ch.presence.insert(user_id)
        } else {
            ch.presence.remove(&user_id)
        }
    }

    // ── Allocation (all under `self.alloc`) ──────────────────────────────────

    /// Claim a bot for `guild`: the first bot that is a member, healthy, and has
    /// no session in **that guild**, inserting a fresh [`PuppetSession`] for
    /// `(bot, guild)` — session presence IS the claim. Engagements in other
    /// guilds don't block a claim (#46): a bot holds one independent voice Call
    /// per guild, so per-(bot, guild) is the true allocation unit. `None` if no
    /// capable bot is free in this guild.
    pub(super) fn take_free_bot(&self, guard: &mut AllocState, guild: GuildId) -> Option<usize> {
        for (i, slot) in self.slots.iter().enumerate() {
            if guard.sessions.contains_key(&(i, guild)) {
                continue;
            }
            // Skip pairs quarantined after a join timeout in THIS guild — picking
            // one would just fail again. A recovery timer clears the flag.
            if guard.health.get(&(i, guild)).is_some_and(|h| h.unhealthy) {
                continue;
            }
            if !slot
                .guilds
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .contains(&guild)
            {
                continue;
            }
            guard.sessions.insert((i, guild), PuppetSession::fresh());
            return Some(i);
        }
        None
    }

    /// Send a command to bot `idx`'s loop, logging when the bounded channel is
    /// full/closed instead of dropping silently — a lost `Release` leaks a voice
    /// presence, a lost `Assign` a silent participant.
    pub(super) fn send_cmd(&self, idx: usize, cmd: PuppetCommand) {
        let label = cmd.label();
        if let Err(e) = self.slots[idx].cmd_tx.try_send(cmd) {
            tracing::warn!(idx, cmd = label, error = %e, "puppet command dropped");
        }
    }

    /// Whether bot `idx` is a member of `guild`.
    pub(super) fn bot_in_guild(&self, idx: usize, guild: GuildId) -> bool {
        self.slots[idx]
            .guilds
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&guild)
    }

    /// Get or create the route handle for `key`, re-pointing it at `mixer`.
    pub(super) fn route_to(routes: &mut HashMap<String, Route>, key: &str, mixer: Arc<PlaybackMixer>) -> Route {
        match routes.get(key) {
            Some(r) => {
                *r.lock().unwrap_or_else(PoisonError::into_inner) = mixer;
                r.clone()
            }
            None => {
                let r: Route = Arc::new(StdMutex::new(mixer));
                routes.insert(key.to_owned(), r.clone());
                r
            }
        }
    }

    /// Set the anchor's nickname to reflect overflow state: the overflow label
    /// while it hosts ≥1 overflow speaker, else its own participant's name.
    pub(super) fn reconcile_anchor_label(&self, sessions: &SessionMap, ch: &ChannelState) {
        let anchor = ch.anchor.load(Ordering::Relaxed);
        if anchor == NO_ANCHOR {
            return;
        }
        let name = if ch.overflow_count() > 0 {
            OVERFLOW_LABEL.to_string()
        } else {
            sessions
                .get(&(anchor, ch.guild_id))
                .and_then(|s| s.name.clone())
                .unwrap_or_default()
        };
        self.send_cmd(anchor, PuppetCommand::SetNickname { guild: ch.guild_id, name });
    }

    /// Apply the anchor's mute: forced **unmuted** while it hosts the overflow mix
    /// (so muting its own participant never silences the others), else its
    /// participant's intended mute.
    pub(super) fn reconcile_anchor_mute(&self, sessions: &SessionMap, ch: &ChannelState) {
        let anchor = ch.anchor.load(Ordering::Relaxed);
        if anchor == NO_ANCHOR {
            return;
        }
        let applied = ch.overflow_count() == 0
            && sessions
                .get(&(anchor, ch.guild_id))
                .is_some_and(|s| s.muted);
        self.send_cmd(anchor, PuppetCommand::SetMuted { guild: ch.guild_id, muted: applied });
    }

    /// Steal a spare bot for **activation** of a channel in `target_guild`: find
    /// the channel with the most slack (most dedicated puppets) that has a
    /// non-anchor dedicated bot usable in `target_guild`, demote that bot's
    /// (most-recently-assigned) speaker into the donor's overflow, and return
    /// the freed bot (still claimed — the caller repurposes it). The bot is sent
    /// a `Release` so its loop leaves the donor channel first. `None` if no
    /// donor has a usable spare bot.
    ///
    /// Under per-guild allocation (#46) steals effectively happen only BETWEEN
    /// channels of the same guild: a bot free-in-target-guild would have been
    /// claimed by `take_free_bot` directly, so a cross-guild donor's victims are
    /// (almost) always already engaged in the target guild and filtered out —
    /// exactly right, since (bot, guild) is the scarce resource.
    pub(super) fn steal_for_activation(
        &self,
        guard: &mut AllocState,
        exclude_key: &str,
        target_guild: GuildId,
    ) -> Option<usize> {
        // For each candidate donor, the newest non-anchor dedicated speaker whose
        // bot can join the target guild. Pick the donor with the most slack.
        let pick = guard
            .channels
            .iter()
            .filter(|(k, ch)| k.as_str() != exclude_key && ch.puppet_count() > 1)
            .filter_map(|(k, ch)| {
                let donor_anchor = ch.anchor.load(Ordering::Relaxed);
                let victim = ch
                    .assignments
                    .iter()
                    .filter_map(|(vk, a)| match a {
                        Assign::Puppet { idx, seq }
                            if *idx != donor_anchor
                                && self.bot_in_guild(*idx, target_guild)
                                && !guard
                                    .health
                                    .get(&(*idx, target_guild))
                                    .is_some_and(|h| h.unhealthy)
                                // A cross-guild victim must be idle in the target
                                // guild — stealing it there would clobber its
                                // existing engagement. (A same-guild steal hands
                                // over the donor session itself.)
                                && (ch.guild_id == target_guild
                                    || !guard.sessions.contains_key(&(*idx, target_guild))) =>
                        {
                            Some((vk.clone(), *idx, *seq))
                        }
                        _ => None,
                    })
                    .max_by_key(|(_, _, seq)| *seq)?;
                Some((k.clone(), ch.puppet_count(), victim))
            })
            .max_by_key(|(_, slack, _)| *slack);

        let (donor_key, _slack, (vkey, vidx, vseq)) = pick?;
        let donor = guard.channels.get_mut(&donor_key)?;
        let donor_guild = donor.guild_id;

        // Demote the victim into the donor's overflow (route-swap = promotion in
        // reverse). The Matrix side keeps pushing to the same RoutedSink. The
        // victim bot's donor-guild session ends here; a fresh one is minted below
        // under the target guild (the caller repurposes the still-claimed bot).
        let (name, muted) = guard
            .sessions
            .remove(&(vidx, donor_guild))
            .map(|s| (s.name.unwrap_or_default(), s.muted))
            .unwrap_or_default();
        donor
            .assignments
            .insert(vkey.clone(), Assign::Overflow { name, muted, seq: vseq });
        Self::route_to(&mut donor.routes, &vkey, donor.overflow_mixer.clone());
        guard.sessions.insert((vidx, target_guild), PuppetSession::fresh());

        // The donor anchor now hosts ≥1 overflow speaker → relabel + unmute it.
        let donor_ref = &guard.channels[&donor_key];
        self.reconcile_anchor_label(&guard.sessions, donor_ref);
        self.reconcile_anchor_mute(&guard.sessions, donor_ref);

        // Send the bot away from the donor; the caller will Assign it elsewhere.
        self.send_cmd(vidx, PuppetCommand::Release { guild: donor_guild });
        tracing::info!(
            donor = %donor_key,
            for_channel = exclude_key,
            idx = vidx,
            demoted = %vkey,
            "stole bot from donor to activate channel"
        );
        Some(vidx)
    }

    /// After a bot frees, activate any channel that has waiting speakers but no
    /// anchor (was left pending because the pool was exhausted): give it the bot
    /// as its anchor, promoting the oldest waiting speaker.
    pub(super) fn service_pending(&self, guard: &mut AllocState) {
        // Each pending channel needs a free bot *in its own guild*; try each once
        // (activating one channel consumes one bot but creates no new pending).
        let pending: Vec<(String, GuildId)> = guard
            .channels
            .iter()
            .filter(|(_, ch)| {
                ch.anchor.load(Ordering::Relaxed) == NO_ANCHOR && !ch.assignments.is_empty()
            })
            .map(|(k, ch)| (k.clone(), ch.guild_id))
            .collect();
        for (key, guild) in pending {
            // A channel left anchor-less but still served by a dedicated puppet
            // (its former anchor's participant left, or the anchor's join failed,
            // leaving a sibling puppet behind) just needs that live puppet promoted
            // to capture anchor — it is already in voice, so elect it directly with
            // no fresh bot (review F1-3). Only a channel with solely overflow
            // speakers waiting needs a new bot to anchor.
            let existing = guard.channels.get(&key).and_then(|ch| {
                ch.assignments.values().find_map(|a| match a {
                    Assign::Puppet { idx, .. } => Some(*idx),
                    Assign::Overflow { .. } => None,
                })
            });
            if let Some(idx) = existing {
                guard
                    .channels
                    .get_mut(&key)
                    .unwrap()
                    .anchor
                    .store(idx, Ordering::Relaxed);
                self.send_cmd(idx, PuppetCommand::BecomeAnchor { guild });
                tracing::info!(channel = %key, idx, "re-elected surviving puppet as capture anchor");
                continue;
            }
            if let Some(idx) = self.take_free_bot(guard, guild) {
                self.activate_channel_with(guard, &key, idx);
            }
        }
    }

    /// After a bot frees, upgrade overflow speakers to dedicated puppets across ALL
    /// already-anchored channels — not just one whose own slot freed. A steal demotes
    /// a donor's speaker into overflow without ever freeing a slot *in that channel*,
    /// so without this cross-channel sweep the demoted speaker stays stranded in the
    /// mix even after the stealing channel returns the bot to the pool. FCFS: promote
    /// the globally-oldest waiting overflow speaker first; stop when no guild-capable
    /// free bot remains. Each iteration consumes a free bot or marks a guild
    /// exhausted, so it terminates.
    pub(super) fn promote_overflow_into_free_bots(&self, guard: &mut AllocState) {
        let mut exhausted: HashSet<GuildId> = HashSet::new();
        loop {
            // The anchored channel (guild not yet exhausted) whose oldest overflow
            // speaker has the lowest seq — global FCFS across channels.
            let pick = guard
                .channels
                .iter()
                .filter(|(_, ch)| {
                    ch.anchor.load(Ordering::Relaxed) != NO_ANCHOR
                        && !exhausted.contains(&ch.guild_id)
                })
                .filter_map(|(k, ch)| {
                    ch.assignments
                        .values()
                        .filter_map(|a| match a {
                            Assign::Overflow { seq, .. } => Some(*seq),
                            _ => None,
                        })
                        .min()
                        .map(|seq| (k.clone(), ch.guild_id, seq))
                })
                .min_by_key(|(_, _, seq)| *seq);
            let Some((key, guild, _)) = pick else { break };
            let Some(idx) = self.take_free_bot(guard, guild) else {
                exhausted.insert(guild);
                continue;
            };
            // Promote that channel's oldest overflow speaker into the freed bot as a
            // dedicated (non-anchor) puppet — mirrors the free-bot path in `assign`.
            let promoted = {
                let Some(ch) = guard.channels.get_mut(&key) else {
                    guard.sessions.remove(&(idx, guild));
                    continue;
                };
                let promote = ch
                    .assignments
                    .iter()
                    .filter_map(|(k, a)| match a {
                        Assign::Overflow { name, muted, seq } => {
                            Some((k.clone(), name.clone(), *muted, *seq))
                        }
                        _ => None,
                    })
                    .min_by_key(|(_, _, _, seq)| *seq);
                let Some((pkey, pname, pmuted, pseq)) = promote else {
                    // Raced away (speaker left); return the bot and re-evaluate.
                    guard.sessions.remove(&(idx, guild));
                    continue;
                };
                ch.overflow_mixer.remove(&pkey);
                ch.assignments
                    .insert(pkey.clone(), Assign::Puppet { idx, seq: pseq });
                let mixer = {
                    let sess = guard
                        .sessions
                        .get_mut(&(idx, guild))
                        .expect("session inserted by take_free_bot");
                    sess.name = Some(pname.clone());
                    sess.muted = pmuted;
                    sess.mixer.clone()
                };
                Self::route_to(&mut ch.routes, &pkey, mixer.clone());
                tracing::info!(channel = %key, promoted = %pkey, idx, "promoted overflow speaker into freed pool bot (cross-channel)");
                (self.ctx_for(ch), pname, pmuted, mixer)
            };
            let (ctx, name, pmuted, mixer) = promoted;
            self.send_cmd(idx, PuppetCommand::Assign {
                ctx: Box::new(ctx),
                display_name: name,
                mixer,
            });
            // A fresh Assign joins unmuted; restore the speaker's mute if it was
            // muted while demoted (the join applies the nickname, not the mute).
            if pmuted {
                self.send_cmd(idx, PuppetCommand::SetMuted { guild, muted: true });
            }
            // Overflow count dropped → the anchor may no longer host overflow.
            let ch = &guard.channels[&key];
            self.reconcile_anchor_label(&guard.sessions, ch);
            self.reconcile_anchor_mute(&guard.sessions, ch);
        }
    }

    /// Re-home freed pool capacity after a bot returns to the pool: first give
    /// anchor-less channels a bot (a fully-unbridged channel recovers before we
    /// upgrade an already-working one), then promote overflow speakers into any
    /// remaining free bots. Called wherever a bot frees.
    pub(super) fn rebalance(&self, guard: &mut AllocState) {
        self.service_pending(guard);
        self.promote_overflow_into_free_bots(guard);
    }

    /// Make bot `idx` the anchor of channel `key`, promoting that channel's oldest
    /// waiting (overflow) speaker into it. Sends the bot Assign + BecomeAnchor.
    pub(super) fn activate_channel_with(&self, guard: &mut AllocState, key: &str, idx: usize) {
        let (ctx, name, mixer) = {
            let ch = match guard.channels.get_mut(key) {
                Some(c) => c,
                None => return,
            };
            let guild = ch.guild_id;
            // Oldest waiting speaker (lowest seq) becomes the anchor's participant.
            let promote = ch
                .assignments
                .iter()
                .filter_map(|(k, a)| match a {
                    Assign::Overflow { name, muted, seq } => {
                        Some((k.clone(), name.clone(), *muted, *seq))
                    }
                    _ => None,
                })
                .min_by_key(|(_, _, _, seq)| *seq);
            let Some((pkey, pname, pmuted, pseq)) = promote else {
                // No waiting speaker after all — return the bot to the pool.
                guard.sessions.remove(&(idx, guild));
                return;
            };
            ch.overflow_mixer.remove(&pkey);
            let mixer = {
                let sess = guard
                    .sessions
                    .get_mut(&(idx, guild))
                    .expect("session inserted by take_free_bot");
                sess.name = Some(pname.clone());
                sess.muted = pmuted;
                sess.mixer.clone()
            };
            Self::route_to(&mut ch.routes, &pkey, mixer.clone());
            ch.assignments
                .insert(pkey.clone(), Assign::Puppet { idx, seq: pseq });
            ch.anchor.store(idx, Ordering::Relaxed);
            (self.ctx_for(ch), pname, mixer)
        };
        self.send_cmd(idx, PuppetCommand::Assign {
            ctx: Box::new(ctx),
            display_name: name,
            mixer,
        });
        // Anchor activation is deferred to `on_join_succeeded` (sent once the bot's
        // `manager.join` actually completes), so a join timeout can fail over
        // instead of falsely reporting the anchor live.
        // The promoted speaker is the anchor's own participant now; remaining
        // overflow (if any) is reconciled by the caller's normal path.
        let ch = &guard.channels[key];
        self.reconcile_anchor_label(&guard.sessions, ch);
        self.reconcile_anchor_mute(&guard.sessions, ch);
        tracing::info!(channel = key, idx, "channel activated — anchor assigned");
    }

    pub(super) fn assign(&self, bridge: &str, key: &str, display_name: &str) -> Arc<dyn ParticipantSink> {
        let mut guard = self.alloc();
        // Reborrow as `&mut AllocState` so `channels`/`sessions` split-borrow.
        let guard = &mut *guard;

        // Idempotent: a re-subscribe for the same participant reuses its route.
        if let Some(ch) = guard.channels.get(bridge) {
            if ch.assignments.contains_key(key) {
                if let Some(r) = ch.routes.get(key) {
                    return Arc::new(RoutedSink { route: r.clone(), key: key.to_owned() });
                }
            }
        }

        // The channel's guild scopes which bots can serve it.
        let guild = match guard.channels.get(bridge) {
            Some(ch) => ch.guild_id,
            None => return Arc::new(NullSink),
        };

        let seq = {
            guard.next_seq += 1;
            guard.next_seq
        };

        // A Matrix participant is (re)joining → cancel any pending idle-release grace
        // on this channel's capture-only anchor.
        Self::cancel_anchor_idle_release(guard, bridge);

        // (Fix 2c) Reuse a capture-only anchor — one held in voice with no
        // participant of its own — for this participant instead of taking a fresh
        // bot. It's already in voice, so relabel + re-point the route; don't send a
        // fresh Assign (which would re-join and churn its SSRC map). Keeps it named
        // and avoids a nameless "[Matrix] " bot beside a freshly-allocated puppet.
        let reuse_anchor = guard.channels.get(bridge).and_then(|ch| {
            let a = ch.anchor.load(Ordering::Relaxed);
            let serves = ch
                .assignments
                .values()
                .any(|x| matches!(x, Assign::Puppet { idx, .. } if *idx == a));
            (a != NO_ANCHOR && !serves).then_some(a)
        });
        if let Some(idx) = reuse_anchor {
            let route = {
                let ch = guard.channels.get_mut(bridge).unwrap();
                ch.assignments
                    .insert(key.to_owned(), Assign::Puppet { idx, seq });
                // A live session swaps participants: KEEP its mixer (the bot's
                // playback track reads it) — just flush the old audio.
                let mixer = {
                    let sess = guard
                        .sessions
                        .get_mut(&(idx, guild))
                        .expect("capture-only anchor has a session");
                    sess.name = Some(display_name.to_owned());
                    sess.muted = false;
                    sess.mixer.clear();
                    sess.mixer.clone()
                };
                Self::route_to(&mut ch.routes, key, mixer)
            };
            let ch = &guard.channels[bridge];
            self.reconcile_anchor_label(&guard.sessions, ch);
            self.reconcile_anchor_mute(&guard.sessions, ch);
            tracing::info!(bridge, key, display_name, idx, "assigned participant to capture-only anchor (reused, no new bot)");
            return Arc::new(RoutedSink { route, key: key.to_owned() });
        }

        // Try to claim a free bot (in this guild) for a dedicated puppet.
        if let Some(idx) = self.take_free_bot(guard, guild) {
            let ctx = {
                let Some(ch) = guard.channels.get_mut(bridge) else {
                    guard.sessions.remove(&(idx, guild));
                    return Arc::new(NullSink);
                };
                ch.assignments
                    .insert(key.to_owned(), Assign::Puppet { idx, seq });
                let mixer = {
                    let sess = guard
                        .sessions
                        .get_mut(&(idx, guild))
                        .expect("session inserted by take_free_bot");
                    sess.name = Some(display_name.to_owned());
                    sess.mixer.clone()
                };
                Self::route_to(&mut ch.routes, key, mixer.clone());
                let elect = ch.anchor.load(Ordering::Relaxed) == NO_ANCHOR;
                if elect {
                    ch.anchor.store(idx, Ordering::Relaxed);
                }
                let ctx = self.ctx_for(ch);
                (ctx, elect, mixer)
            };
            let (ctx, elect, mixer) = ctx;
            let route = guard.channels[bridge].routes[key].clone();
            self.send_cmd(idx, PuppetCommand::Assign {
                ctx: Box::new(ctx),
                display_name: display_name.to_owned(),
                mixer,
            });
            tracing::info!(bridge, key, display_name, idx, "assigned participant to puppet");
            if elect {
                // Elected, but `BecomeAnchor` is deferred to `on_join_succeeded` so
                // the anchor only goes live after the join actually completes.
                tracing::info!(bridge, idx, "puppet elected capture anchor (activates after join)");
            }
            return Arc::new(RoutedSink { route, key: key.to_owned() });
        }

        // Pool exhausted. If this channel has no anchor yet, it MUST get a bot to
        // function — try to steal one. Otherwise the speaker overflows.
        let has_anchor = guard
            .channels
            .get(bridge)
            .map(|ch| ch.anchor.load(Ordering::Relaxed) != NO_ANCHOR)
            .unwrap_or(false);

        if !has_anchor {
            if let Some(idx) = self.steal_for_activation(guard, bridge, guild) {
                // Bot freed from a donor (Release already sent; the steal minted a
                // fresh session for it under THIS guild). Assign it here as the
                // anchor for this participant.
                let (ctx, mixer) = {
                    let Some(ch) = guard.channels.get_mut(bridge) else {
                        guard.sessions.remove(&(idx, guild));
                        return Arc::new(NullSink);
                    };
                    ch.assignments
                        .insert(key.to_owned(), Assign::Puppet { idx, seq });
                    let mixer = {
                        let sess = guard
                            .sessions
                            .get_mut(&(idx, guild))
                            .expect("session inserted by steal_for_activation");
                        sess.name = Some(display_name.to_owned());
                        sess.mixer.clone()
                    };
                    Self::route_to(&mut ch.routes, key, mixer.clone());
                    ch.anchor.store(idx, Ordering::Relaxed);
                    (self.ctx_for(ch), mixer)
                };
                let route = guard.channels[bridge].routes[key].clone();
                self.send_cmd(idx, PuppetCommand::Assign {
                    ctx: Box::new(ctx),
                    display_name: display_name.to_owned(),
                    mixer,
                });
                // `BecomeAnchor` deferred to `on_join_succeeded` (see assign above).
                tracing::info!(bridge, key, idx, "stole bot to activate channel anchor");
                return Arc::new(RoutedSink { route, key: key.to_owned() });
            }
            // No guild-capable spare bot anywhere — the channel can't be bridged
            // yet. Record the speaker as overflow (no audio plays until a bot in
            // this guild frees up; `service_pending` will then activate it).
            tracing::warn!(
                bridge,
                key,
                guild = %guild,
                "no puppet available for this guild (pool exhausted / no bot in guild) — \
                 channel waiting for a free bot (Matrix audio not yet playing)"
            );
        }

        // Overflow into this channel's overflow mix (played by its anchor when it
        // has one). On the first overflow speaker of an anchored channel, relabel
        // the anchor and force it unmuted.
        let Some(ch) = guard.channels.get_mut(bridge) else {
            return Arc::new(NullSink);
        };
        let was_overflowing = ch.overflow_count() > 0;
        ch.assignments.insert(
            key.to_owned(),
            Assign::Overflow { name: display_name.to_owned(), muted: false, seq },
        );
        let route = Self::route_to(&mut ch.routes, key, ch.overflow_mixer.clone());
        tracing::info!(bridge, key, display_name, "overflow to anchor mix");
        if !was_overflowing {
            let ch = &guard.channels[bridge];
            self.reconcile_anchor_label(&guard.sessions, ch);
            self.reconcile_anchor_mute(&guard.sessions, ch);
        }
        Arc::new(RoutedSink { route, key: key.to_owned() })
    }

    /// Called by a puppet's command loop after `manager.join` SUCCEEDS. If this bot
    /// is the elected anchor of the channel it now serves, activate capture — this
    /// is deferred from the assign paths so a timed-out join never falsely reports
    /// the anchor live.
    pub(super) fn on_join_succeeded(&self, idx: usize, guild: GuildId) {
        let mut guard = self.alloc();
        // Activate capture on whichever channel elected this bot as its anchor —
        // even a capture-only anchor whose own participant left mid-join (#59): the
        // elected anchor still owns the channel's capture path and must register it,
        // else Discord→Matrix stays silent until the channel fully re-elects (review
        // F1-2). A bot holds one session per guild, so at most one channel matches.
        for ch in guard.channels.values().filter(|ch| ch.guild_id == guild) {
            if ch.anchor.load(Ordering::Relaxed) == idx {
                self.send_cmd(idx, PuppetCommand::BecomeAnchor { guild });
                tracing::info!(bridge = %ch.bridge, idx, "puppet joined — activating capture anchor");
                break;
            }
        }
        // A successful join proves the pair healthy → drop its health record
        // entirely (quarantine, backoff count, and recovery latch all reset).
        guard.health.remove(&(idx, guild));
    }

    /// Called by a puppet's command loop after `manager.join` FAILS (e.g. the voice
    /// gateway timed out after a channel hop). Quarantine the stuck bot so the
    /// allocator skips it, then fail the participant over to a healthy free bot in
    /// the same guild — re-pointing its EXISTING route so the Matrix side follows
    /// with no re-subscribe, and re-electing the anchor if the failed bot was it.
    /// If no healthy bot is free, the participant drops to overflow and the channel
    /// degrades to no-anchor with a loud error — an honest state, not a silent
    /// fake-live anchor. Returns the quarantine backoff the caller should wait
    /// before scheduling a recovery (`recover_bot`).
    pub(super) fn on_join_failed(&self, idx: usize, guild: GuildId) -> Duration {
        let mut guard = self.alloc();
        let guard = &mut *guard;

        // Quarantine the stuck (bot, guild) regardless (cleared after the backoff
        // cooldown). Exponential backoff: quarantine_base * 2^n, capped at
        // quarantine_max. `n` = prior consecutive failures here; clamp the shift
        // so it can't overflow.
        let backoff = {
            let h = guard.health.entry((idx, guild)).or_default();
            h.unhealthy = true;
            let shift = h.fail_count.min(6);
            h.fail_count += 1;
            self.quarantine_base
                .saturating_mul(1u32 << shift)
                .min(self.quarantine_max)
        };

        // Locate the (bridge, key) this bot was serving in the failed guild.
        let found = guard
            .channels
            .iter()
            .filter(|(_, ch)| ch.guild_id == guild)
            .find_map(|(bk, ch)| {
                ch.assignments.iter().find_map(|(k, a)| match a {
                    Assign::Puppet { idx: i, .. } if *i == idx => Some((bk.clone(), k.clone())),
                    _ => None,
                })
            });
        let Some((bridge, key)) = found else {
            // No live Assign::Puppet for this bot in the guild. Either the
            // participant left during the join, OR this bot is a capture-only
            // anchor (kept after the last participant left with Discord speakers
            // still present, #59) whose rejoin failed. Free the bot; and if it was
            // still the elected anchor of a channel, clear that election and cancel
            // its idle-release grace — otherwise the channel keeps a session-less
            // anchor that panics the next assign() at the reuse-capture-only-anchor
            // path (review F1-1/F2-1), and a stale grace timer would later tear down
            // whichever channel next reuses this (idx, guild).
            guard.sessions.remove(&(idx, guild));
            let stale = guard
                .channels
                .iter()
                .find(|(_, ch)| {
                    ch.guild_id == guild && ch.anchor.load(Ordering::Relaxed) == idx
                })
                .map(|(bk, _)| bk.clone());
            if let Some(bk) = stale {
                Self::cancel_anchor_idle_release(guard, &bk);
                guard
                    .channels
                    .get_mut(&bk)
                    .unwrap()
                    .anchor
                    .store(NO_ANCHOR, Ordering::Relaxed);
                tracing::warn!(idx, bridge = %bk, "puppet join failed for a capture-only anchor — cleared stale anchor election");
            }
            tracing::warn!(idx, "puppet join failed; assignment already gone — quarantined");
            return backoff;
        };

        let was_anchor = guard.channels[&bridge].anchor.load(Ordering::Relaxed) == idx;

        // Free the stuck bot (its route entry is kept — we re-point it below) and
        // tell it to leave voice so its songbird Call is reset.
        let display_name = guard
            .sessions
            .remove(&(idx, guild))
            .and_then(|s| s.name)
            .unwrap_or_default();
        self.send_cmd(idx, PuppetCommand::Release { guild });
        if was_anchor {
            guard
                .channels
                .get_mut(&bridge)
                .unwrap()
                .anchor
                .store(NO_ANCHOR, Ordering::Relaxed);
        }
        tracing::warn!(bridge = %bridge, key = %key, idx, was_anchor, "puppet join failed — bot quarantined; failing over");

        let seq = {
            guard.next_seq += 1;
            guard.next_seq
        };

        // Fail over to a healthy free bot in the same guild.
        if let Some(new_idx) = self.take_free_bot(guard, guild) {
            let (ctx, mixer) = {
                let ch = guard.channels.get_mut(&bridge).unwrap();
                ch.assignments
                    .insert(key.clone(), Assign::Puppet { idx: new_idx, seq });
                let mixer = {
                    let sess = guard
                        .sessions
                        .get_mut(&(new_idx, guild))
                        .expect("session inserted by take_free_bot");
                    sess.name = Some(display_name.clone());
                    sess.mixer.clone()
                };
                Self::route_to(&mut ch.routes, &key, mixer.clone());
                if was_anchor {
                    ch.anchor.store(new_idx, Ordering::Relaxed);
                }
                (self.ctx_for(ch), mixer)
            };
            self.send_cmd(new_idx, PuppetCommand::Assign {
                ctx: Box::new(ctx),
                display_name,
                mixer,
            });
            // BecomeAnchor (if new_idx is the anchor) is deferred to its own
            // on_join_succeeded — same as the normal assign paths.
            tracing::info!(bridge = %bridge, key = %key, old = idx, new = new_idx, "failed over to replacement puppet");
        } else {
            // No healthy bot free: drop the participant to overflow (its anchor, if
            // any, plays the overflow mix) and let service_pending re-anchor once a
            // bot recovers/frees. Degraded but honest.
            let ch = guard.channels.get_mut(&bridge).unwrap();
            ch.assignments.insert(
                key.clone(),
                Assign::Overflow { name: display_name, muted: false, seq },
            );
            Self::route_to(&mut ch.routes, &key, ch.overflow_mixer.clone());
            let ch = &guard.channels[&bridge];
            self.reconcile_anchor_label(&guard.sessions, ch);
            self.reconcile_anchor_mute(&guard.sessions, ch);
            self.rebalance(guard);
            tracing::error!(
                bridge = %bridge,
                key = %key,
                guild = %guild,
                "puppet join failed and no healthy pool bot free — channel DEGRADED \
                 (capture anchor down / audio via overflow until a bot recovers)"
            );
        }
        backoff
    }

    /// Clear a (bot, guild) join-timeout quarantine (after the backoff cooldown)
    /// and try to activate any channel left without an anchor while it was
    /// unavailable. `fail_count` deliberately survives — the backoff keeps
    /// growing across repeat failures and resets only on a successful join.
    pub(super) fn recover_bot(&self, idx: usize, guild: GuildId) {
        let mut guard = self.alloc();
        let guard = &mut *guard;
        let clear = match guard.health.get_mut(&(idx, guild)) {
            Some(h) => {
                h.unhealthy = false;
                !h.recovering && h.fail_count == 0
            }
            None => false,
        };
        if clear {
            guard.health.remove(&(idx, guild));
        }
        self.rebalance(guard);
        tracing::info!(idx, guild = guild.get(), "puppet join-timeout quarantine cleared — eligible again");
    }

    /// Decide whether an unexpected voice disconnect for `idx` warrants a rejoin,
    /// and claim the recovery latch if so. Returns `true` (and sets `recovering`)
    /// only when the allocator still believes this bot is live (it has a session),
    /// it is not already quarantined (`unhealthy`), and no recovery is already in
    /// flight. A deliberate leave removes the session *before* `manager.remove`
    /// fires its `DriverDisconnect`, so that path returns `false` here — no
    /// spurious rejoin. Caller MUST pair a `true` with exactly one
    /// [`finish_recovery`] (directly or via the rejoin path / `on_join_succeeded`).
    /// Scoped to `guild`: a deliberate leave there must not be masked by the bot
    /// still being live in another guild.
    pub(super) fn should_recover_disconnect(&self, idx: usize, guild: GuildId) -> bool {
        // Serialize against the allocator's own session/anchor mutations.
        let mut guard = self.alloc();
        let guard = &mut *guard;
        if !guard.sessions.contains_key(&(idx, guild)) {
            return false;
        }
        let h = guard.health.entry((idx, guild)).or_default();
        if h.unhealthy || h.recovering {
            return false;
        }
        // Claim the latch (it was clear).
        h.recovering = true;
        true
    }

    /// Release the recovery latch claimed by [`should_recover_disconnect`],
    /// pruning the health record if nothing else keeps it alive.
    pub(super) fn finish_recovery(&self, idx: usize, guild: GuildId) {
        let mut guard = self.alloc();
        let guard = &mut *guard;
        let clear = match guard.health.get_mut(&(idx, guild)) {
            Some(h) => {
                h.recovering = false;
                !h.unhealthy && h.fail_count == 0
            }
            None => false,
        };
        if clear {
            guard.health.remove(&(idx, guild));
        }
    }

    /// Reconcile sweep: ask every bot the allocator believes is live to verify it
    /// is actually in voice (handled in the command loop, which has the songbird
    /// `Call`). A backstop for a silent connection death that fired no
    /// `DriverDisconnect`. Skips bots already quarantined or mid-recovery.
    pub(super) fn sweep_voice_health(&self) {
        let probes: Vec<(usize, GuildId)> = {
            let guard = self.alloc();
            guard
                .sessions
                .keys()
                .filter(|k| {
                    !guard
                        .health
                        .get(k)
                        .is_some_and(|h| h.unhealthy || h.recovering)
                })
                .copied()
                .collect()
        };
        for (i, guild) in probes {
            self.send_cmd(i, PuppetCommand::VerifyConnected { guild });
        }
    }

    pub(super) fn set_muted(&self, bridge: &str, key: &str, muted: bool) {
        let mut guard = self.alloc();
        let guard = &mut *guard;
        let Some(ch) = guard.channels.get_mut(bridge) else { return };
        let guild = ch.guild_id;
        match ch.assignments.get_mut(key) {
            Some(Assign::Puppet { idx, .. }) => {
                let idx = *idx;
                if let Some(sess) = guard.sessions.get_mut(&(idx, guild)) {
                    sess.muted = muted;
                }
                if idx == ch.anchor.load(Ordering::Relaxed) {
                    let ch = &guard.channels[bridge];
                    self.reconcile_anchor_mute(&guard.sessions, ch);
                } else {
                    self.send_cmd(idx, PuppetCommand::SetMuted { guild, muted });
                }
            }
            Some(Assign::Overflow { muted: m, .. }) => *m = muted,
            None => {}
        }
    }

    pub(super) fn release(self: &Arc<Self>, bridge: &str, key: &str) {
        let mut guard = self.alloc();
        let guard = &mut *guard;
        // Remove the assignment; act on what it was.
        let (removed, guild) = match guard.channels.get_mut(bridge) {
            Some(ch) => (ch.assignments.remove(key), ch.guild_id),
            None => return,
        };
        match removed {
            Some(Assign::Puppet { idx, .. }) => {
                {
                    let ch = guard.channels.get_mut(bridge).unwrap();
                    ch.routes.remove(key);
                }
                // Promote the oldest overflow speaker (FCFS) into the freed slot.
                let promote = {
                    let ch = &guard.channels[bridge];
                    ch.assignments
                        .iter()
                        .filter_map(|(k, a)| match a {
                            Assign::Overflow { name, muted, seq } => {
                                Some((k.clone(), name.clone(), *muted, *seq))
                            }
                            _ => None,
                        })
                        .min_by_key(|(_, _, _, seq)| *seq)
                };
                match promote {
                    Some((yk, yname, ymuted, yseq)) => {
                        let ch = guard.channels.get_mut(bridge).unwrap();
                        ch.assignments
                            .insert(yk.clone(), Assign::Puppet { idx, seq: yseq });
                        ch.overflow_mixer.remove(&yk);
                        // The bot's session stays live (it remains in voice); the
                        // participant swaps → keep the mixer, flush the old audio.
                        let mixer = {
                            let sess = guard
                                .sessions
                                .get_mut(&(idx, guild))
                                .expect("promoted-into bot has a live session");
                            sess.name = Some(yname.clone());
                            sess.muted = ymuted;
                            sess.mixer.clear();
                            sess.mixer.clone()
                        };
                        Self::route_to(&mut ch.routes, &yk, mixer);
                        let is_anchor = idx == ch.anchor.load(Ordering::Relaxed);
                        let overflow_cleared = ch.overflow_count() == 0;
                        tracing::info!(bridge, key, promoted = %yk, idx, "promoted overflow speaker into freed slot");
                        if !is_anchor {
                            self.send_cmd(idx, PuppetCommand::SetNickname { guild, name: yname });
                            self.send_cmd(idx, PuppetCommand::SetMuted { guild, muted: ymuted });
                        }
                        if overflow_cleared {
                            let ch = &guard.channels[bridge];
                            self.reconcile_anchor_label(&guard.sessions, ch);
                            self.reconcile_anchor_mute(&guard.sessions, ch);
                        }
                    }
                    None => {
                        let is_anchor = guard.channels[bridge].anchor.load(Ordering::Relaxed) == idx;

                        // (Fix 2a) If this freed bot was the anchor but OTHER puppets
                        // still serve Matrix participants, move the oldest such
                        // participant onto this anchor (route-swap — the anchor keeps
                        // its songbird session / SSRC map, so no audio churn) and free
                        // the now-spare bot. Otherwise the anchor would linger as a
                        // nameless "[Matrix] " capture-only bot beside the real
                        // puppets, wasting a slot.
                        if is_anchor {
                            let victim = guard.channels[bridge]
                                .assignments
                                .iter()
                                .filter_map(|(k, a)| match a {
                                    Assign::Puppet { idx: vidx, seq } if *vidx != idx => {
                                        Some((k.clone(), *vidx, *seq))
                                    }
                                    _ => None,
                                })
                                .min_by_key(|(_, _, seq)| *seq);
                            if let Some((vkey, vidx, vseq)) = victim {
                                // Free the now-spare bot's session; its participant
                                // moves onto the anchor's (live) session.
                                let (vname, vmuted) = guard
                                    .sessions
                                    .remove(&(vidx, guild))
                                    .map(|s| (s.name.unwrap_or_default(), s.muted))
                                    .unwrap_or_default();
                                {
                                    let ch = guard.channels.get_mut(bridge).unwrap();
                                    ch.assignments
                                        .insert(vkey.clone(), Assign::Puppet { idx, seq: vseq });
                                    let mixer = {
                                        let sess = guard
                                            .sessions
                                            .get_mut(&(idx, guild))
                                            .expect("anchor has a live session");
                                        sess.name = Some(vname);
                                        sess.muted = vmuted;
                                        sess.mixer.clear();
                                        sess.mixer.clone()
                                    };
                                    Self::route_to(&mut ch.routes, &vkey, mixer);
                                }
                                self.send_cmd(vidx, PuppetCommand::Release { guild });
                                let ch = &guard.channels[bridge];
                                self.reconcile_anchor_label(&guard.sessions, ch);
                                self.reconcile_anchor_mute(&guard.sessions, ch);
                                tracing::info!(bridge, moved = %vkey, freed = vidx, onto_anchor = idx, "consolidated participant onto capture anchor; freed spare bot");
                                self.rebalance(guard);
                                return;
                            }
                        }

                        // If this freed bot is the capture anchor and Discord
                        // speakers are still present, KEEP it in the voice channel
                        // as a capture-only anchor — briefly. Leaving immediately would
                        // drop the songbird session and reset SSRC→user mappings,
                        // cutting Discord→Matrix audio until each speaker re-onsets —
                        // the churn a spurious Matrix membership flap would cause. But
                        // bound it with a grace timer so no nameless bot idles in voice
                        // once no Matrix user returns (Fix 2b).
                        let speakers_present = !guard.channels[bridge]
                            .user_sinks
                            .read()
                            .unwrap_or_else(PoisonError::into_inner)
                            .is_empty();
                        if is_anchor && speakers_present {
                            // Drop only its playback identity; keep the session +
                            // anchor so it stays un-allocatable and still the
                            // elected anchor.
                            if let Some(sess) = guard.sessions.get_mut(&(idx, guild)) {
                                sess.mixer.clear();
                                sess.name = None;
                            }
                            self.send_cmd(idx, PuppetCommand::SetStandbyNickname { guild });
                            self.send_cmd(idx, PuppetCommand::SetMuted { guild, muted: false });
                            tracing::info!(bridge, key, idx, "last puppet released but Discord speakers remain — keeping capture-only anchor (grace before release)");
                            self.schedule_anchor_idle_release(guard, bridge);
                            return;
                        }
                        // No overflow waiting, no capture need → free the bot.
                        guard.sessions.remove(&(idx, guild));
                        self.send_cmd(idx, PuppetCommand::Release { guild });
                        tracing::info!(bridge, key, idx, "released puppet");
                        // Hand the anchor off to another puppet in this channel.
                        let ch = guard.channels.get_mut(bridge).unwrap();
                        if ch.anchor.load(Ordering::Relaxed) == idx {
                            let next = ch.assignments.values().find_map(|a| match a {
                                Assign::Puppet { idx, .. } => Some(*idx),
                                Assign::Overflow { .. } => None,
                            });
                            match next {
                                Some(new_idx) => {
                                    ch.anchor.store(new_idx, Ordering::Relaxed);
                                    self.send_cmd(new_idx, PuppetCommand::BecomeAnchor { guild });
                                    tracing::info!(bridge, old = idx, new = new_idx, "capture anchor handed off");
                                }
                                None => {
                                    ch.anchor.store(NO_ANCHOR, Ordering::Relaxed);
                                    tracing::info!(bridge, idx, "capture anchor released — no puppets left");
                                }
                            }
                        }
                        // A freed bot can activate a channel waiting for an anchor.
                        self.rebalance(guard);
                    }
                }
            }
            Some(Assign::Overflow { .. }) => {
                let ch = guard.channels.get_mut(bridge).unwrap();
                ch.overflow_mixer.remove(key);
                ch.routes.remove(key);
                tracing::info!(bridge, key, "released overflow participant");
                if ch.overflow_count() == 0 {
                    let ch = &guard.channels[bridge];
                    self.reconcile_anchor_label(&guard.sessions, ch);
                    self.reconcile_anchor_mute(&guard.sessions, ch);
                }
            }
            None => {}
        }
    }

    pub(super) fn release_all(self: &Arc<Self>, bridge: &str) {
        let mut guard = self.alloc();
        let guard = &mut *guard;
        // Call-end teardown (#59): KEEP a capture-only anchor when Discord
        // speakers remain, mirroring the single-participant `release` path.
        // This seam used to clobber the keep decision `release` had made ~0.5s
        // earlier, leaving the bridge dormant with humans still in the channel.
        // Process/tenant drains go through `unregister_channel` instead, which
        // drains unconditionally.
        self.release_all_locked(guard, bridge, true);
        // The bots just freed can activate channels waiting for an anchor.
        self.rebalance(guard);
    }

    /// Body of [`release_all`](Self::release_all), shared with
    /// [`unregister_channel`](Self::unregister_channel): release every bot serving
    /// this channel. Only this channel's own `(bot, guild)` sessions are removed —
    /// a bot serving another channel in the same guild holds a different entry and
    /// is untouched (#46). Idempotent: a second pass finds nothing to drain.
    /// Caller rebalances.
    ///
    /// `keep_capture_anchor` (#59): on a call-end (`release_all`) a channel whose
    /// Discord speakers are still present keeps its anchor as capture-only under
    /// the anchor-idle grace timer — a Matrix user returning within the grace
    /// (e.g. after an Element Call membership flap) reuses the live session with
    /// no join latency and no SSRC-map reset. `false` (`unregister_channel`)
    /// drains unconditionally: teardown must return the connection count to
    /// baseline.
    pub(super) fn release_all_locked(
        self: &Arc<Self>,
        guard: &mut AllocState,
        bridge: &str,
        keep_capture_anchor: bool,
    ) {
        let Some(ch) = guard.channels.get_mut(bridge) else { return };
        let guild = ch.guild_id;
        let anchor = ch.anchor.load(Ordering::Relaxed);
        let speakers_present = !ch
            .user_sinks
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty();
        let keep_anchor = keep_capture_anchor && anchor != NO_ANCHOR && speakers_present;
        let mut anchor_freed = false;
        for (_key, assign) in ch.assignments.drain() {
            if let Assign::Puppet { idx, .. } = assign {
                if keep_anchor && idx == anchor {
                    // Demote to capture-only instead of releasing: drop the
                    // playback identity, keep the session + anchor election.
                    if let Some(sess) = guard.sessions.get_mut(&(idx, guild)) {
                        sess.mixer.clear();
                        sess.name = None;
                    }
                    self.send_cmd(idx, PuppetCommand::SetStandbyNickname { guild });
                    self.send_cmd(idx, PuppetCommand::SetMuted { guild, muted: false });
                    continue;
                }
                guard.sessions.remove(&(idx, guild));
                self.send_cmd(idx, PuppetCommand::Release { guild });
                if idx == anchor {
                    anchor_freed = true;
                }
            }
        }
        // A capture-only-held anchor has no Assign::Puppet of its own, so the
        // drain above won't free it — release it explicitly so teardown returns
        // to baseline (Phase 9 guarantee), unless it is being deliberately kept.
        if anchor != NO_ANCHOR && !anchor_freed && !keep_anchor {
            guard.sessions.remove(&(anchor, guild));
            self.send_cmd(anchor, PuppetCommand::Release { guild });
        }
        ch.routes.clear();
        ch.overflow_mixer.clear();
        if !keep_anchor {
            ch.anchor.store(NO_ANCHOR, Ordering::Relaxed);
        }
        if keep_anchor {
            tracing::info!(
                bridge,
                idx = anchor,
                "call ended but Discord speakers remain — keeping capture-only anchor (grace before release)"
            );
            self.schedule_anchor_idle_release(guard, bridge);
        }
    }

    /// Release a capture-only-held anchor once its channel is idle on BOTH sides:
    /// no playback puppet assigned AND no Discord speakers left. Called when the
    /// last Discord speaker leaves (see `drop_ghost`). No-op if there is no held
    /// anchor or either side is still active.
    pub(super) fn maybe_release_idle_anchor(&self, bridge: &str) {
        let mut guard = self.alloc();
        let (idx, guild) = {
            let Some(ch) = guard.channels.get(bridge) else { return };
            let anchor = ch.anchor.load(Ordering::Relaxed);
            if anchor == NO_ANCHOR {
                return;
            }
            let has_puppet = ch
                .assignments
                .values()
                .any(|a| matches!(a, Assign::Puppet { .. }));
            let speakers = !ch.user_sinks.read().unwrap_or_else(PoisonError::into_inner).is_empty();
            if has_puppet || speakers {
                return; // still in use on one side — keep the anchor
            }
            (anchor, ch.guild_id)
        };
        guard.sessions.remove(&(idx, guild));
        self.send_cmd(idx, PuppetCommand::Release { guild });
        {
            let ch = guard.channels.get_mut(bridge).unwrap();
            ch.anchor.store(NO_ANCHOR, Ordering::Relaxed);
            ch.overflow_mixer.clear();
        }
        tracing::info!(bridge, idx, "channel idle (no puppets, no Discord speakers) — releasing capture-only anchor");
        self.rebalance(&mut guard);
    }

    /// Arm (or re-arm) the grace timer that releases a capture-only anchor if no
    /// Matrix participant returns within the anchor-idle grace. Cancels any prior
    /// timer for the channel. Must be called with the `alloc` guard already held
    /// (it stores the token under it); the spawned task re-locks on expiry.
    pub(super) fn schedule_anchor_idle_release(self: &Arc<Self>, guard: &mut AllocState, bridge: &str) {
        let token = CancellationToken::new();
        let Some(ch) = guard.channels.get_mut(bridge) else { return };
        if let Some(old) = ch.anchor_idle_release.replace(token.clone()) {
            old.cancel();
        }
        let pool = self.clone();
        let bridge = bridge.to_owned();
        tokio::spawn(async move {
            tokio::select! {
                _ = token.cancelled() => {}
                _ = tokio::time::sleep(pool.anchor_idle_grace) => {
                    pool.release_capture_only_anchor_if_idle(&bridge);
                }
            }
        });
    }

    /// Cancel a pending capture-only-anchor idle-release timer (a Matrix
    /// participant is (re)joining, so the anchor is no longer idle).
    pub(super) fn cancel_anchor_idle_release(guard: &mut AllocState, bridge: &str) {
        if let Some(ch) = guard.channels.get_mut(bridge) {
            if let Some(tok) = ch.anchor_idle_release.take() {
                tok.cancel();
            }
        }
    }

    /// Grace-timer callback: release the capture-only anchor (leave Discord voice)
    /// iff the channel still has an anchor and **no** Matrix participant — i.e. the
    /// grace elapsed with nobody returning. A no-op if a participant reused the
    /// anchor in the meantime (then it serves someone and must stay).
    pub(super) fn release_capture_only_anchor_if_idle(&self, bridge: &str) {
        let mut guard = self.alloc();
        let (idx, guild) = {
            let Some(ch) = guard.channels.get(bridge) else { return };
            let anchor = ch.anchor.load(Ordering::Relaxed);
            if anchor == NO_ANCHOR {
                return;
            }
            let any_puppet = ch
                .assignments
                .values()
                .any(|a| matches!(a, Assign::Puppet { .. }));
            if any_puppet {
                return; // a Matrix participant is present — keep the capture path
            }
            (anchor, ch.guild_id)
        };
        guard.sessions.remove(&(idx, guild));
        self.send_cmd(idx, PuppetCommand::Release { guild });
        {
            let ch = guard.channels.get_mut(bridge).unwrap();
            ch.anchor.store(NO_ANCHOR, Ordering::Relaxed);
            ch.overflow_mixer.clear();
            ch.anchor_idle_release = None;
        }
        tracing::info!(bridge, idx, "capture-only anchor idle grace elapsed — leaving Discord voice (no Matrix user present)");
        self.rebalance(&mut guard);
    }

    /// Record that bot `idx` is now a member of `guild` (from `ready`/
    /// `guild_create`), then service any channel in that guild waiting for a bot
    /// (covers the startup race where an assign arrives before membership is known).
    pub(super) fn note_bot_guild(&self, idx: usize, guild: GuildId) {
        let added = self.slots[idx]
            .guilds
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(guild);
        if added {
            let mut guard = self.alloc();
            self.rebalance(&mut guard);
        }
    }

    /// Record that bot `idx` left `guild` (kicked / guild unavailable) so it is no
    /// longer assigned there.
    pub(super) fn forget_bot_guild(&self, idx: usize, guild: GuildId) {
        self.slots[idx]
            .guilds
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&guild);
    }

    /// Whether bot `idx` currently holds a session in `guild`.
    pub(super) fn bot_has_session_in(&self, idx: usize, guild: GuildId) -> bool {
        let guard = self.alloc();
        guard.sessions.contains_key(&(idx, guild))
    }

    /// Display name of the participant bot `idx` voices in `guild` (empty if none —
    /// e.g. a capture-only anchor). Used by the rejoin path, which runs in the
    /// bot's command loop and has no live reference into the alloc state.
    pub(super) fn session_display_name(&self, idx: usize, guild: GuildId) -> String {
        let guard = self.alloc();
        guard
            .sessions
            .get(&(idx, guild))
            .and_then(|s| s.name.clone())
            .unwrap_or_default()
    }
}
