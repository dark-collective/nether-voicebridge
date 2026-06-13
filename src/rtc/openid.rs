//! Obtain a Matrix OpenID token for the bridge bot.
//!
//! The token is used to authenticate to lk-jwt-service, which mints a
//! LiveKit JWT in exchange.

use anyhow::Result;
use matrix_sdk::Client;
use matrix_sdk::ruma::api::client::account::request_openid_token;

/// A Matrix OpenID token ready to forward to lk-jwt-service.
///
/// Field names and types match the Matrix spec wire format so the struct
/// can be serialised directly into the lk-jwt-service request JSON.
#[derive(Debug, Clone, serde::Serialize)]
pub struct OpenIdToken {
    pub access_token: String,
    pub token_type: String,
    pub matrix_server_name: String,
    /// Expiry in seconds (matching the Matrix spec `expires_in` field).
    pub expires_in: u64,
}

/// Request a Matrix OpenID token for the authenticated client.
pub async fn get_openid_token(client: &Client) -> Result<OpenIdToken> {
    let user_id = client
        .user_id()
        .ok_or_else(|| anyhow::anyhow!("Matrix client not authenticated"))?
        .to_owned();

    let request = request_openid_token::v3::Request::new(user_id);
    let resp = client.send(request).await?;

    let token = OpenIdToken {
        access_token: resp.access_token,
        // TokenType is a StringEnum; as_str() gives the canonical string (e.g. "Bearer").
        token_type: resp.token_type.as_str().to_owned(),
        matrix_server_name: resp.matrix_server_name.as_str().to_owned(),
        // Matrix spec uses seconds; lk-jwt-service expects `expires_in` in seconds.
        expires_in: resp.expires_in.as_secs(),
    };

    tracing::info!(
        matrix_server_name = %token.matrix_server_name,
        token_type = %token.token_type,
        "Matrix OpenID token obtained"
    );
    Ok(token)
}
