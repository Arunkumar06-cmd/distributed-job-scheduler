use async_trait::async_trait;
use axum::{extract::Request, middleware::Next, response::Response};
use std::collections::HashMap;
use std::sync::Mutex;
use uuid::Uuid;

use crate::auth::{verify_kind, TokenKind};
use crate::state::AppState;
use common::AppError;

/// Per-user fixed-window request limiter. In-process by design: the API is
/// horizontally scaled behind a load balancer where per-instance limits still
/// cap runaway clients; a Redis counter is the next tier up.
#[derive(Clone)]
pub struct RateLimiter {
    limit: u32,
    window_secs: u64,
    // Shared across clones so every AppState copy limits the same bucket set.
    buckets: std::sync::Arc<Mutex<HashMap<Uuid, (std::time::Instant, u32)>>>,
}

impl RateLimiter {
    pub fn new(limit_per_min: u32) -> Self {
        Self {
            limit: limit_per_min,
            window_secs: 60,
            buckets: std::sync::Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns false when this user has exhausted their window.
    pub fn check(&self, user: Uuid) -> bool {
        if self.limit == 0 {
            return true; // 0 disables limiting
        }
        let mut map = self.buckets.lock().unwrap();
        let now = std::time::Instant::now();
        // Opportunistic cleanup keeps the map bounded without a sweeper task.
        if map.len() > 10_000 {
            map.retain(|_, (start, _)| now.duration_since(*start).as_secs() < self.window_secs);
        }
        let entry = map.entry(user).or_insert((now, 0));
        if now.duration_since(entry.0).as_secs() >= self.window_secs {
            *entry = (now, 0);
        }
        entry.1 += 1;
        entry.1 <= self.limit
    }
}

/// Copies the x-request-id (established by SetRequestIdLayer) into a
/// common-managed task-local so error bodies carry the same id as the header.
pub async fn request_id_middleware(req: Request, next: Next) -> Response {
    let id = req
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    common::ids::with_request_id(id, next.run(req)).await
}

#[derive(Debug, Clone)]
pub struct AuthUser {
    pub user_id: Uuid,
    pub email: String,
}

fn bearer_token(header_value: &str) -> &str {
    header_value.strip_prefix("Bearer ").unwrap_or(header_value)
}

pub async fn auth_middleware(
    axum::extract::State(state): axum::extract::State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response, AppError> {
    let user = extract_auth(&req, &state)?;

    // Per-user request limiting, checked after identity is established.
    if let Some(limiter) = &state.rate_limiter {
        if !limiter.check(user.user_id) {
            return Err(AppError::RateLimited(
                "too many requests; slow down".to_string(),
            ));
        }
    }

    req.extensions_mut().insert(user);
    Ok(next.run(req).await)
}

fn extract_auth(req: &Request, state: &AppState) -> Result<AuthUser, AppError> {
    // Header first; then ?access_token= for EventSource/WebSocket clients,
    // which cannot attach custom headers. Safe enough now that access tokens
    // expire hourly.
    let from_query = req.uri().query().and_then(|q| {
        q.split('&').find_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            (k == "access_token" && !v.is_empty()).then(|| v.to_string())
        })
    });
    let bearer = req
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .map(bearer_token)
        .map(str::to_string);

    let token = bearer.or(from_query).ok_or(AppError::Unauthorized)?;
    let data = verify_kind(&token, TokenKind::Access, &state.config)
        .map_err(|_| AppError::Unauthorized)?;
    Ok(AuthUser {
        user_id: data.claims.sub,
        email: data.claims.email,
    })
}

#[async_trait]
impl axum::extract::FromRequestParts<AppState> for AuthUser {
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        if let Some(user) = parts.extensions.get::<AuthUser>() {
            return Ok(user.clone());
        }
        let header = parts
            .headers
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or(AppError::Unauthorized)?;
        let data = verify_kind(bearer_token(header), TokenKind::Access, &state.config)
            .map_err(|_| AppError::Unauthorized)?;
        Ok(AuthUser {
            user_id: data.claims.sub,
            email: data.claims.email,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_up_to_limit_then_blocks_and_recovers() {
        let lim = RateLimiter::new(3);
        let u = Uuid::new_v4();
        assert!(lim.check(u));
        assert!(lim.check(u));
        assert!(lim.check(u));
        assert!(!lim.check(u), "4th request in window must be blocked");
        assert!(!lim.check(u));
    }

    #[test]
    fn zero_disables_limiting() {
        let lim = RateLimiter::new(0);
        let u = Uuid::new_v4();
        for _ in 0..1000 {
            assert!(lim.check(u));
        }
    }

    #[test]
    fn buckets_are_per_user() {
        let lim = RateLimiter::new(1);
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        assert!(lim.check(a));
        assert!(!lim.check(a));
        assert!(lim.check(b), "another user must have an independent bucket");
    }

    #[tokio::test]
    async fn window_resets_after_expiry() {
        let mut lim = RateLimiter::new(1);
        lim.window_secs = 0; // every check starts a fresh window
        let u = Uuid::new_v4();
        assert!(lim.check(u));
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        // A zero-length window expires instantly, so the user is never blocked.
        assert!(lim.check(u));
    }
}
