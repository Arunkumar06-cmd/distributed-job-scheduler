use chrono::Utc;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, TokenData, Validation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use common::Config;

/// Access tokens are short-lived and statelessly validated; refresh tokens
/// (30d) rotate them. This is what makes logout/compromise response possible
/// without a per-request DB hit.
pub const ACCESS_TTL_SECS: usize = 3600;
pub const REFRESH_TTL_SECS: usize = 30 * 24 * 3600;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    Access,
    Refresh,
}

impl TokenKind {
    fn as_str(self) -> &'static str {
        match self {
            TokenKind::Access => "access",
            TokenKind::Refresh => "refresh",
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub sub: Uuid, // user_id
    pub email: String,
    /// "access" | "refresh" — a refresh token must never authenticate a request.
    pub typ: String,
    pub exp: usize,
    pub iat: usize,
}

impl Claims {
    pub fn new(user_id: Uuid, email: String, kind: TokenKind) -> Self {
        let now = Utc::now().timestamp() as usize;
        let ttl = match kind {
            TokenKind::Access => ACCESS_TTL_SECS,
            TokenKind::Refresh => REFRESH_TTL_SECS,
        };
        Self {
            sub: user_id,
            email,
            typ: kind.as_str().to_string(),
            exp: now + ttl,
            iat: now,
        }
    }
}

fn encode_claims(claims: &Claims, config: &Config) -> Result<String, jsonwebtoken::errors::Error> {
    encode(
        &Header::default(),
        claims,
        &EncodingKey::from_secret(config.jwt_secret.as_bytes()),
    )
}

pub fn create_token(
    user_id: Uuid,
    email: &str,
    kind: TokenKind,
    config: &Config,
) -> Result<String, jsonwebtoken::errors::Error> {
    encode_claims(&Claims::new(user_id, email.to_string(), kind), config)
}

/// Verify a token of the exact expected kind; a refresh token presented where
/// an access token is required (and vice versa) is rejected.
pub fn verify_kind(
    token: &str,
    kind: TokenKind,
    config: &Config,
) -> Result<TokenData<Claims>, jsonwebtoken::errors::Error> {
    let data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(config.jwt_secret.as_bytes()),
        &Validation::default(),
    )?;
    if data.claims.typ != kind.as_str() {
        return Err(jsonwebtoken::errors::ErrorKind::InvalidToken.into());
    }
    Ok(data)
}

pub fn hash_password(password: &str) -> Result<String, argon2::password_hash::Error> {
    use argon2::{
        password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
        Argon2,
    };
    let salt = SaltString::generate(&mut OsRng);
    let hash = Argon2::default().hash_password(password.as_bytes(), &salt)?;
    Ok(hash.to_string())
}

pub fn verify_password(hash: &str, password: &str) -> Result<bool, argon2::password_hash::Error> {
    use argon2::{
        password_hash::{PasswordHash, PasswordVerifier},
        Argon2,
    };
    let parsed = PasswordHash::new(hash)?;
    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::Config;

    fn test_config() -> Config {
        std::env::set_var("JWT_SECRET", "test-secret-for-unit-tests-0123456789");
        let cfg = Config::from_env();
        std::env::remove_var("JWT_SECRET");
        cfg
    }

    #[test]
    fn access_roundtrip_and_refresh_rejected_as_access() {
        let cfg = test_config();
        let uid = Uuid::new_v4();
        let access = create_token(uid, "u@x.com", TokenKind::Access, &cfg).unwrap();
        assert_eq!(verify_kind(&access, TokenKind::Access, &cfg).unwrap().claims.sub, uid);

        let refresh = create_token(uid, "u@x.com", TokenKind::Refresh, &cfg).unwrap();
        assert!(verify_kind(&refresh, TokenKind::Access, &cfg).is_err(),
                "refresh token must not authenticate API requests");
        assert!(verify_kind(&access, TokenKind::Refresh, &cfg).is_err());

        // Refresh roundtrip for the rotation endpoint.
        assert_eq!(verify_kind(&refresh, TokenKind::Refresh, &cfg).unwrap().claims.sub, uid);
    }

    #[test]
    fn tampered_and_wrong_secret_rejected() {
        let cfg = test_config();
        let token = create_token(Uuid::new_v4(), "u@x.com", TokenKind::Access, &cfg).unwrap();
        let tampered = format!("{}x", &token[..token.len() - 2]);
        assert!(verify_kind(&tampered, TokenKind::Access, &cfg).is_err());
        let mut other = test_config();
        other.jwt_secret = "another-secret-entirely-0123456789abc".to_string();
        assert!(verify_kind(&token, TokenKind::Access, &other).is_err());
    }

    #[test]
    fn expired_access_rejected() {
        let cfg = test_config();
        let claims = Claims {
            sub: Uuid::new_v4(),
            email: "u@x.com".into(),
            typ: "access".into(),
            iat: 0,
            exp: 1,
        };
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(cfg.jwt_secret.as_bytes()),
        )
        .unwrap();
        assert!(verify_kind(&token, TokenKind::Access, &cfg).is_err());
    }

    #[test]
    fn password_hash_and_verify() {
        let hash = hash_password("correct horse battery staple").unwrap();
        assert!(verify_password(&hash, "correct horse battery staple").unwrap());
        assert!(!verify_password(&hash, "wrong").unwrap());
        assert_ne!(hash, hash_password("correct horse battery staple").unwrap());
    }
}
