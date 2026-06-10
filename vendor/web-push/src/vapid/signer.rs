use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use ct_codecs::{Base64UrlSafeNoPadding, Encoder};
use http::uri::Uri;
use p256::ecdsa::{signature::Signer, Signature};
use serde_json::Value;

use crate::{error::WebPushError, vapid::VapidKey};

/// A struct representing a VAPID signature. Should be generated using the
/// [VapidSignatureBuilder](struct.VapidSignatureBuilder.html).
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VapidSignature {
    /// The signed JWT, base64-encoded
    pub auth_t: String,
    /// The public key bytes
    pub auth_k: Vec<u8>,
}

/// JWT claims object. Custom claims are implemented as a map.
#[derive(Clone, Debug)]
pub struct Claims {
    pub custom: BTreeMap<String, Value>,
}

impl Claims {
    pub fn with_custom_claims(custom: BTreeMap<String, Value>) -> Self {
        Self { custom }
    }
}

pub struct VapidSigner {}

impl VapidSigner {
    /// Create a signature with a given key. Sets the default audience from the
    /// endpoint host and sets the expiry in twelve hours. Values can be
    /// overwritten by adding the `aud` and `exp` claims.
    pub fn sign(key: VapidKey, endpoint: &Uri, mut claims: Claims) -> Result<VapidSignature, WebPushError> {
        let endpoint_scheme = endpoint.scheme_str().ok_or(WebPushError::InvalidUri)?;
        let endpoint_host = endpoint.host().ok_or(WebPushError::InvalidUri)?;

        if !claims.custom.contains_key("aud") {
            let audience = format!("{}://{}", endpoint_scheme, endpoint_host);
            claims.custom.insert("aud".to_string(), Value::String(audience));
        } else {
            let aud = claims.custom.get("aud").unwrap().clone();
            if aud.as_str().is_none() {
                return Err(WebPushError::InvalidClaims);
            }
        }

        if let Some(exp) = claims.custom.get("exp") {
            if exp.as_u64().is_none() {
                return Err(WebPushError::InvalidClaims);
            }
        } else {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| WebPushError::InvalidClaims)?
                .as_secs();
            claims
                .custom
                .insert("exp".to_string(), Value::from(now.saturating_add(12 * 60 * 60)));
        }

        // Some push services require a contact subject even though the VAPID
        // specification does not make it mandatory for every deployment.
        if !claims.custom.contains_key("sub") {
            claims.custom.insert(
                "sub".to_string(),
                Value::String("mailto:example@example.com".to_string()),
            );
        }

        log::trace!("Using jwt: {:?}", claims);

        let auth_k = key.public_key();

        let header = serde_json::json!({
            "typ": "JWT",
            "alg": "ES256",
        });
        let header_json = serde_json::to_vec(&header).map_err(|_| WebPushError::InvalidClaims)?;
        let claims_json = serde_json::to_vec(&claims.custom).map_err(|_| WebPushError::InvalidClaims)?;
        let encoded_header =
            Base64UrlSafeNoPadding::encode_to_string(&header_json).map_err(|_| WebPushError::InvalidClaims)?;
        let encoded_claims =
            Base64UrlSafeNoPadding::encode_to_string(&claims_json).map_err(|_| WebPushError::InvalidClaims)?;
        let signing_input = format!("{}.{}", encoded_header, encoded_claims);
        let signature: Signature = key.0.sign(signing_input.as_bytes());
        let signature_bytes = signature.to_bytes();
        let encoded_signature =
            Base64UrlSafeNoPadding::encode_to_string(signature_bytes).map_err(|_| WebPushError::InvalidClaims)?;
        let auth_t = format!("{}.{}", signing_input, encoded_signature);

        Ok(VapidSignature { auth_t, auth_k })
    }
}

#[cfg(test)]
mod tests {}
