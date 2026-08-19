//! Is a failed Matrix request a *fatal* auth failure, or something transient?
//!
//! This exists because of the 2026-08-11 outage (#130). The sync loops used to
//! decide by substring-matching the *stringified* error:
//!
//! ```ignore
//! let msg = e.to_string();
//! if msg.contains("401") || msg.contains("403") || msg.contains("M_UNKNOWN_TOKEN") { /* fatal */ }
//! ```
//!
//! A `matrix_sdk` error stringifies to include the request URL — and `/sync`
//! carries `?since=<token>`. When the homeserver's sync stream sat at position
//! `5401xxxx`, every token contained the substring `401`, so a plain network
//! blip ("error sending request for url …") was read as an auth failure. Five
//! bridges were killed in ten minutes; three stayed dead for three days. The
//! landmine re-armed every time the stream crossed a position containing `401`
//! or `403`.
//!
//! So: decide on the **structure** of the error, never on its text. The
//! decision itself is a pure function over `(status, errcode)` so it can be
//! unit-tested without constructing SDK errors, with a thin adapter over
//! `matrix_sdk::Error` above it.

use matrix_sdk::ruma::api::error::ErrorKind;

/// Is this `(HTTP status, Matrix errcode)` pair a fatal auth failure — one
/// where the credentials will never work again and retrying is pointless?
///
/// Deliberately conservative: anything not positively identified as an auth
/// failure is transient, because the cost of the two mistakes is wildly
/// asymmetric. Treating a transient error as fatal kills a bridge until an
/// operator notices (the #130 outage); treating a fatal error as transient
/// just means a retry loop that logs, and the supervisor's restart backoff
/// bounds that.
///
/// `status: None` means the request never got an HTTP response at all — a
/// transport error, DNS failure or timeout. Never an auth failure.
pub(crate) fn is_fatal_auth(status: Option<u16>, errcode: Option<&str>) -> bool {
    match errcode {
        // The token is gone/invalid — no amount of retrying revives it.
        Some("M_UNKNOWN_TOKEN" | "M_MISSING_TOKEN" | "M_UNAUTHORIZED") => return true,
        // The account itself is finished. A locked account needs an operator.
        Some("M_USER_DEACTIVATED" | "M_USER_LOCKED") => return true,
        // M_FORBIDDEN is only fatal paired with a 403 — as an errcode alone it
        // is also used for ordinary per-room permission denials.
        Some("M_FORBIDDEN") if status == Some(403) => return true,
        _ => {}
    }
    // A bare 401 with no (or an unrecognised) errcode is still an auth failure.
    status == Some(401)
}

/// Extract `(status, errcode)` from an SDK error and run [`is_fatal_auth`].
///
/// Everything that is not a structured client-API error — `HttpError::Reqwest`
/// (transport), timeouts, deserialization failures — yields `(None, None)` and
/// is therefore transient, which is exactly the #130 case.
pub(crate) fn sdk_error_is_fatal_auth(e: &matrix_sdk::Error) -> bool {
    let Some(api_err) = e.as_client_api_error() else {
        // No HTTP response to inspect ⇒ not an auth failure.
        return false;
    };
    is_fatal_auth(
        Some(api_err.status_code.as_u16()),
        api_err.error_kind().map(errcode_str),
    )
}

/// Is an HTTP status from the raw appservice **login** a terminal (config-
/// shaped) failure, or worth retrying (#126)?
///
/// The asymmetry argument is the same as [`is_fatal_auth`]'s but the polarity
/// differs by status class: at login time a 4xx means the *request itself* is
/// wrong (401/403 bad `as_token`, 404 wrong `homeserver_url` path) and will
/// never succeed by waiting — fail fast and loudly so the operator sees a
/// config error, not an infinite silent retry. 5xx / no-response mean the
/// homeserver (or its front proxy) is not ready — the boot-race class that
/// used to permanently kill the management bot until a manual restart.
/// 408/429 are the transient members of the 4xx family.
pub(crate) fn login_status_is_terminal(status: u16) -> bool {
    match status {
        408 | 429 => false,
        400..=499 => true,
        _ => false,
    }
}

/// [`login_status_is_terminal`] over an [`anyhow::Error`] from
/// `appservice_login_as`: terminal only when the chain carries a typed
/// [`AppserviceHttpError`](crate::matrix::AppserviceHttpError) with a terminal
/// status. Transport errors, timeouts and response-parse failures have no
/// status and are always retryable.
pub(crate) fn appservice_login_error_is_terminal(e: &anyhow::Error) -> bool {
    e.downcast_ref::<crate::matrix::AppserviceHttpError>()
        .is_some_and(|http| login_status_is_terminal(http.status.as_u16()))
}

/// Map the ruma error kind back to its wire errcode, for the few codes the
/// decision above cares about. `ErrorKind` is `non_exhaustive` and carries
/// per-variant data, so matching the handful we act on (and letting everything
/// else fall through as "not an auth failure") is both simpler and more
/// version-stable than trying to render every variant.
fn errcode_str(kind: &ErrorKind) -> &'static str {
    match kind {
        ErrorKind::UnknownToken(_) => "M_UNKNOWN_TOKEN",
        ErrorKind::MissingToken => "M_MISSING_TOKEN",
        ErrorKind::Unauthorized => "M_UNAUTHORIZED",
        ErrorKind::UserDeactivated => "M_USER_DEACTIVATED",
        ErrorKind::UserLocked => "M_USER_LOCKED",
        ErrorKind::Forbidden => "M_FORBIDDEN",
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_error_is_never_fatal() {
        // THE #130 REGRESSION. The killer error had no HTTP response at all —
        // only a message that happened to contain "401" inside the sync token:
        //   error sending request for url
        //   (https://nether.im/_matrix/client/v3/sync?since=54015213&timeout=30000…)
        // Structurally there is no status and no errcode, so: transient.
        assert!(!is_fatal_auth(None, None));
    }

    #[test]
    fn old_substring_check_misfires_where_the_typed_one_does_not() {
        // The four sync tokens that actually killed bridges on 2026-08-11, plus
        // a 403-shaped one. For each, the retired substring predicate says
        // "fatal auth" and the typed one says "transient" — which is the whole
        // bug in one assertion pair.
        for token in ["54013070", "54014591", "54015213", "54015517", "54403111"] {
            let msg = format!(
                "error sending request for url \
                 (https://nether.im/_matrix/client/v3/sync?since={token}&timeout=30000)"
            );
            let old_verdict = msg.contains("401") || msg.contains("403");
            assert!(old_verdict, "token {token} no longer reproduces the old misfire");
            // A transport error carries no status and no errcode.
            assert!(
                !is_fatal_auth(None, None),
                "token {token} must be transient under the typed check"
            );
        }
    }

    #[test]
    fn real_auth_failures_are_fatal() {
        assert!(is_fatal_auth(Some(401), Some("M_UNKNOWN_TOKEN")));
        assert!(is_fatal_auth(Some(401), None));
        assert!(is_fatal_auth(Some(403), Some("M_FORBIDDEN")));
        assert!(is_fatal_auth(Some(401), Some("M_MISSING_TOKEN")));
        assert!(is_fatal_auth(Some(403), Some("M_USER_DEACTIVATED")));
        // An unknown-token verdict stands whatever status carried it.
        assert!(is_fatal_auth(Some(200), Some("M_UNKNOWN_TOKEN")));
    }

    #[test]
    fn server_and_rate_limit_errors_are_transient() {
        // The reboot boot-race: Tuwunel not up yet ⇒ 502 through Cloudflare.
        assert!(!is_fatal_auth(Some(502), None));
        assert!(!is_fatal_auth(Some(500), None));
        assert!(!is_fatal_auth(Some(504), None));
        assert!(!is_fatal_auth(Some(429), Some("M_LIMIT_EXCEEDED")));
    }

    #[test]
    fn forbidden_without_403_is_not_fatal() {
        // A per-room permission denial (e.g. posting a call membership into a
        // room whose PL forbids it) must not take the whole bridge down.
        assert!(!is_fatal_auth(Some(404), Some("M_FORBIDDEN")));
        assert!(!is_fatal_auth(None, Some("M_FORBIDDEN")));
    }

    #[test]
    fn login_retryability_splits_on_status_class() {
        // The #126 boot race: homeserver/proxy not up yet — always retry.
        for s in [500, 502, 503, 504] {
            assert!(!login_status_is_terminal(s), "{s} must be retryable");
        }
        // Config errors: bad as_token, wrong URL — fail fast.
        for s in [400, 401, 403, 404] {
            assert!(login_status_is_terminal(s), "{s} must be terminal");
        }
        // Transient members of the 4xx family.
        assert!(!login_status_is_terminal(408));
        assert!(!login_status_is_terminal(429));
    }

    #[test]
    fn login_error_terminal_only_with_typed_status() {
        use crate::matrix::AppserviceHttpError;
        let terminal = anyhow::Error::new(AppserviceHttpError {
            status: reqwest::StatusCode::UNAUTHORIZED,
            body: "{\"errcode\":\"M_UNKNOWN_TOKEN\"}".into(),
        })
        .context("appservice login failed");
        assert!(appservice_login_error_is_terminal(&terminal));

        let boot_race = anyhow::Error::new(AppserviceHttpError {
            status: reqwest::StatusCode::BAD_GATEWAY,
            body: "<html>cloudflare</html>".into(),
        })
        .context("appservice login failed");
        assert!(!appservice_login_error_is_terminal(&boot_race));

        // A bare transport-shaped error has no typed status: retryable.
        let transport = anyhow::anyhow!("error sending request for url (…)")
            .context("appservice login request");
        assert!(!appservice_login_error_is_terminal(&transport));
    }
}
