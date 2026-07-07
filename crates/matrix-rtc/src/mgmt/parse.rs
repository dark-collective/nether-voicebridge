//! Command grammar for the management bot: the `Command`/`Parsed` types, the
//! usage/help text, and the quote-aware tokenizer + parser.

#[derive(Debug, PartialEq)]
pub(super) enum Command {
    Help,
    List,
    Link {
        name: String,
        room_id: String,
        guild_id: String,
        channel_id: String,
        server: Option<String>,
    },
    Unlink {
        name: String,
    },
    Ban {
        kind: String,
        value: String,
    },
    Unban {
        kind: String,
        value: String,
    },
    Bans,
}

#[derive(Debug, PartialEq)]
pub(super) enum Parsed {
    Cmd(Command),
    /// A known verb with wrong arguments → the usage string to send back.
    Usage(String),
    /// Not one of our verbs — stay silent outside DMs.
    Unknown,
}

const LINK_USAGE: &str = "Usage: `link <name> <matrix-room-id> <discord-guild-id> <discord-channel-id> [server]`\nQuote names containing spaces: `link \"Guest Voice\" !abc:example.org 123456 789012`";
const UNLINK_USAGE: &str = "Usage: `unlink <name>`";
pub(super) const BAN_USAGE: &str =
    "Usage: `ban <owner|guild|room> <value>` — e.g. `ban owner @evil:example.org`, `ban guild 123456`, `ban room !abc:example.org`. Banning also unlinks every matching tenant.";
pub(super) const UNBAN_USAGE: &str = "Usage: `unban <owner|guild|room> <value>`";

pub(super) const HELP_TEXT: &str = "Voice bridge management commands:\n  list — all bridges (static + tenants) and whether they are running\n  link <name> <matrix-room-id> <discord-guild-id> <discord-channel-id> [server] — bridge a Discord voice channel to a Matrix room\n  unlink <name> — remove a bridge created with link\n  ban <owner|guild|room> <value> — deny-list an owner MXID / Discord guild / Matrix room AND unlink every matching tenant\n  unban <owner|guild|room> <value> — remove a ban\n  bans — show the deny-lists\n  help — this text\nQuote names containing spaces: link \"Guest Voice\" !abc:example.org 123456 789012";

/// Help for self-serve users (#43): only the verbs they may run, plus the
/// consent flow they will hit.
pub(super) const SELF_SERVE_HELP_TEXT: &str = "Voice bridge commands:\n  link <name> <matrix-room-id> <discord-guild-id> <discord-channel-id> — bridge a Discord voice channel to a Matrix room. You must be a moderator of the Matrix room, and someone with Manage Server on the Discord side must approve the link with the `/bridge approve` command and the code you'll receive.\n  unlink <name> — remove a bridge you created\n  list — your bridges\n  help — this text\nQuote names containing spaces: link \"Guest Voice\" !abc:example.org 123456 789012";

/// The self-serve command tier (#43): which verbs a non-operator may run when
/// `open_registration` is on. Ownership scoping inside `list`/`unlink` narrows
/// further; `ban`/`unban`/`bans` are operator-only.
pub(super) fn allowed_for_self_serve(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Help | Command::List | Command::Link { .. } | Command::Unlink { .. }
    )
}

/// Parse one message body. The verb is case-insensitive; arguments support
/// double-quoting for names with spaces.
pub(super) fn parse_command(body: &str) -> Parsed {
    let Some(tokens) = tokenize(body.trim()) else {
        return Parsed::Unknown; // unbalanced quote in chatter — not for us
    };
    let Some((verb, args)) = tokens.split_first() else {
        return Parsed::Unknown;
    };
    match verb.to_ascii_lowercase().as_str() {
        "help" => Parsed::Cmd(Command::Help),
        "list" => Parsed::Cmd(Command::List),
        "link" => match args {
            [name, room_id, guild_id, channel_id, rest @ ..] if rest.len() <= 1 => {
                Parsed::Cmd(Command::Link {
                    name: name.clone(),
                    room_id: room_id.clone(),
                    guild_id: guild_id.clone(),
                    channel_id: channel_id.clone(),
                    server: rest.first().cloned(),
                })
            }
            _ => Parsed::Usage(LINK_USAGE.to_string()),
        },
        "unlink" => match args {
            [name] => Parsed::Cmd(Command::Unlink { name: name.clone() }),
            _ => Parsed::Usage(UNLINK_USAGE.to_string()),
        },
        "ban" => match args {
            [kind, value] => Parsed::Cmd(Command::Ban { kind: kind.clone(), value: value.clone() }),
            _ => Parsed::Usage(BAN_USAGE.to_string()),
        },
        "unban" => match args {
            [kind, value] => {
                Parsed::Cmd(Command::Unban { kind: kind.clone(), value: value.clone() })
            }
            _ => Parsed::Usage(UNBAN_USAGE.to_string()),
        },
        "bans" => match args {
            [] => Parsed::Cmd(Command::Bans),
            _ => Parsed::Usage("Usage: `bans`".to_string()),
        },
        _ => Parsed::Unknown,
    }
}

/// Whitespace tokenizer with double-quote grouping. `None` on an unbalanced
/// quote.
pub(super) fn tokenize(s: &str) -> Option<Vec<String>> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut had_token = false;
    for c in s.chars() {
        match c {
            '"' => {
                in_quotes = !in_quotes;
                had_token = true; // `""` is an (empty) token
            }
            c if c.is_whitespace() && !in_quotes => {
                if had_token {
                    out.push(std::mem::take(&mut cur));
                    had_token = false;
                }
            }
            c => {
                cur.push(c);
                had_token = true;
            }
        }
    }
    if in_quotes {
        return None;
    }
    if had_token {
        out.push(cur);
    }
    Some(out)
}
