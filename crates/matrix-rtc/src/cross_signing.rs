//! Cross-sign the bot account's devices (#150).
//!
//! Without a cross-signing identity none of the bot's devices are signed by
//! the account itself, so every client shows a persistent warning in a DM
//! with the bot ("We can't confirm … devices are really theirs"). The bot runs
//! one device per crypto store — the management bot plus one per bridge, each
//! minted fresh for a new store (#30) — so the identity must be shared by all
//! of them and survive store wipes:
//!
//! - The FIRST device to start with no identity on the account bootstraps one
//!   (`/keys/device_signing/upload`; tried without user-interactive auth, then
//!   with `m.login.dummy` — an appservice login has no password to offer),
//!   which also signs that device.
//! - The private keys go into the account's secret storage (SSSS, encrypted
//!   account data), and the secret-storage key is written to
//!   `<data_dir>/cross-signing/<mxid>.key` (0600).
//! - Every other device — another bridge, a re-provisioned store — opens the
//!   secret storage with that key, imports the private keys into its own
//!   store and signs itself.
//!
//! An account that already HAS an identity but no key file here (it was
//! bootstrapped elsewhere, or the file was lost) is left alone with a WARN:
//! resetting would invalidate whatever else trusts it, and two deployments
//! sharing one bot account would keep resetting each other. Best-effort
//! throughout — a failure leaves devices unverified, never stops the bridge.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Context, Result};
use matrix_sdk::Client;
use matrix_sdk::ruma::api::client::uiaa;

/// `<data_dir>`, set once by the binary before any client starts.
static KEY_DIR: OnceLock<PathBuf> = OnceLock::new();

/// Serializes the check-then-bootstrap across this process's clients (the
/// management bot and every bridge start concurrently on the same account).
static BOOTSTRAP_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Bound on the whole pass, so a wedged homeserver can't hold startup.
const PASS_TIMEOUT: Duration = Duration::from_secs(60);

/// Tell the crate where the cross-signing key files live (`data_dir`). Call
/// once at startup; later calls are ignored. Unset ⇒ cross-signing is skipped.
pub fn set_key_dir(data_dir: impl Into<PathBuf>) {
    let _ = KEY_DIR.set(data_dir.into());
}

/// `<dir>/cross-signing/<mxid>.key`, with the MXID made filename-safe.
fn key_path(dir: &Path, user_id: &str) -> PathBuf {
    let safe: String = user_id
        .trim_start_matches('@')
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') { c } else { '_' })
        .collect();
    dir.join("cross-signing").join(format!("{safe}.key"))
}

/// Make sure `client`'s device is signed by the account's cross-signing
/// identity, creating the identity if the account has none. Run after the
/// first sync (the device keys must be uploaded). Logs and returns on failure.
pub(crate) async fn ensure_device_cross_signed(client: &Client) {
    let Some(dir) = KEY_DIR.get() else {
        tracing::debug!("no data dir registered — skipping cross-signing (#150)");
        return;
    };
    match tokio::time::timeout(PASS_TIMEOUT, ensure_inner(client, dir)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(
            error = %format!("{e:#}"),
            "could not cross-sign this device — clients will show the bot's devices as unverified (#150)"
        ),
        Err(_) => tracing::warn!("cross-signing timed out after 60 s — device left unverified (#150)"),
    }
}

async fn ensure_inner(client: &Client, dir: &Path) -> Result<()> {
    let _guard = BOOTSTRAP_LOCK.lock().await;
    let enc = client.encryption();
    let user_id = client.user_id().context("client has no user id")?.to_owned();
    let path = key_path(dir, user_id.as_str());

    if let Some(s) = enc.cross_signing_status().await
        && s.has_master
        && s.has_self_signing
    {
        // This store already holds the private keys; just make sure the
        // device itself carries the signature.
        return sign_own_device(client).await;
    }

    // Ask the server: does the account already have an identity?
    let identity = enc.request_user_identity(&user_id).await.context("querying own identity")?;
    if identity.is_some() {
        let key = match std::fs::read_to_string(&path) {
            Ok(k) => k,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!(
                    path = %path.display(),
                    "the bot account has a cross-signing identity but no key file here — \
                     this device stays unverified. Copy the key file from the deployment that \
                     created the identity (#150)"
                );
                return Ok(());
            }
            Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
        };
        let store = enc
            .secret_storage()
            .open_secret_store(key.trim())
            .await
            .context("opening secret storage with the stored key")?;
        // Imports the private keys AND signs this device.
        store.import_secrets().await.context("importing the cross-signing keys")?;
        tracing::info!("imported the bot's cross-signing keys and signed this device (#150)");
        return Ok(());
    }

    // No identity yet: create it (this signs the current device too).
    if let Err(e) = enc.bootstrap_cross_signing(None).await {
        let Some(uiaa) = e.as_uiaa_response() else {
            return Err(e).context("uploading cross-signing keys");
        };
        let mut dummy = uiaa::Dummy::new();
        dummy.session = uiaa.session.clone();
        enc.bootstrap_cross_signing(Some(uiaa::AuthData::Dummy(dummy)))
            .await
            .context("uploading cross-signing keys (the homeserver wants interactive auth)")?;
    }
    tracing::info!(user_id = %user_id, "created the bot's cross-signing identity (#150)");

    // Back the private keys up to secret storage so other devices can import
    // them, and keep its key where they will look.
    let store = enc
        .secret_storage()
        .create_secret_store()
        .await
        .context("creating secret storage for the cross-signing keys")?;
    write_key_file(&path, &store.secret_storage_key())
        .with_context(|| format!("writing {}", path.display()))?;
    tracing::info!(path = %path.display(), "stored the cross-signing secret-storage key (#150)");
    sign_own_device(client).await
}

/// Sign this device with the self-signing key if it isn't already.
async fn sign_own_device(client: &Client) -> Result<()> {
    let device = client
        .encryption()
        .get_own_device()
        .await
        .context("reading own device")?
        .context("own device missing from the crypto store")?;
    if !device.is_cross_signed_by_owner() {
        device.verify().await.context("signing own device")?;
        tracing::info!(device_id = %device.device_id(), "signed this device with the bot's cross-signing identity (#150)");
    }
    Ok(())
}

/// Write `key` to `path` (0600), via a temp file + rename so a crash never
/// leaves a truncated key behind.
fn write_key_file(path: &Path, key: &str) -> std::io::Result<()> {
    use std::io::Write;
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("key.tmp");
    // A leftover temp file would keep its old mode through `truncate`.
    let _ = std::fs::remove_file(&tmp);
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    opts.mode(0o600);
    let mut f = opts.open(&tmp)?;
    f.write_all(key.as_bytes())?;
    f.sync_all()?;
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_path_is_filename_safe_per_account() {
        let p = key_path(Path::new("/data"), "@nvb_bot:nether.im");
        assert_eq!(p, PathBuf::from("/data/cross-signing/nvb_bot_nether.im.key"));
        let p = key_path(Path::new("/data"), "@a/../b:x");
        assert_eq!(p.parent(), Some(Path::new("/data/cross-signing")));
    }

    #[test]
    fn key_file_is_private_and_replaced_atomically() {
        let dir = std::env::temp_dir().join(format!("nvb-xsign-{}", std::process::id()));
        let path = key_path(&dir, "@bot:example.org");
        write_key_file(&path, "first").unwrap();
        write_key_file(&path, "second").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "second");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
