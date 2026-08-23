use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use db::models::Job;

/// Result of executing a job handler.
#[derive(Debug, Clone)]
pub enum HandlerResult {
    /// Job completed successfully. The optional value is stored as the job result.
    Ok(Option<Value>),
    /// Job failed and should be retried (if attempts remain).
    Retry { message: String, kind: String },
    /// Job failed permanently (goes straight to DLQ, no retries).
    Permanent { message: String, kind: String },
    /// External result unknown (e.g., timeout after sending request, spec §25)
    Unknown { message: String, kind: String },
}

/// A handler for a specific job type.
#[async_trait]
pub trait JobHandler: Send + Sync {
    /// The job type string this handler matches (e.g. "send_email").
    fn job_type(&self) -> &str;

    /// Execute the job. Must be idempotent.
    async fn handle(&self, job: &Job) -> HandlerResult;
}

/// Registry of job handlers, keyed by job type.
#[derive(Default)]
pub struct HandlerRegistry {
    handlers: RwLock<HashMap<String, Arc<dyn JobHandler>>>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(&self, handler: Arc<dyn JobHandler>) {
        let key = handler.job_type().to_string();
        self.handlers.write().await.insert(key, handler);
    }

    pub async fn get(&self, job_type: &str) -> Option<Arc<dyn JobHandler>> {
        self.handlers.read().await.get(job_type).cloned()
    }
}

/// The standard demo/test handler set. Shared by the supervisor startup path
/// and the new-queue watcher so every consumer sees identical routing.
pub async fn with_default_handlers() -> Arc<HandlerRegistry> {
    let registry = Arc::new(HandlerRegistry::new());
    registry.register(Arc::new(EchoHandler)).await;
    registry.register(Arc::new(SleepHandler)).await;
    registry.register(Arc::new(ExternalPaymentHandler)).await;
    registry
        .register(Arc::new(AlwaysFailHandler {
            message: "intentional test failure".to_string(),
        }))
        .await;
    registry
}

/// Runs a handler under a panic guard and a hard timeout.
///
/// - A panicking handler must not take down the consumer task that serves the
///   subject; the panic is converted into a retryable failure.
/// - A handler that awaits forever would otherwise pin its consumer forever.
///   Handlers are contractually idempotent, so timeout => retry is safe.
pub async fn run_protected<F>(
    timeout_secs: u64,
    fut: F,
) -> HandlerResult
where
    F: std::future::Future<Output = HandlerResult>,
{
    let timed = tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs.max(1)),
        futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(fut)),
    )
    .await;

    match timed {
        Err(_) => HandlerResult::Retry {
            message: format!("handler exceeded {}s timeout", timeout_secs),
            kind: "handler_timeout".to_string(),
        },
        Ok(Err(panic_payload)) => {
            let detail = panic_payload
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| panic_payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "non-string panic payload".to_string());
            HandlerResult::Retry {
                message: format!("handler panicked: {detail}"),
                kind: "handler_panicked".to_string(),
            }
        }
        Ok(Ok(result)) => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn panic_becomes_retryable_failure() {
        let result = run_protected(
            5,
            async {
                panic!("boom inside handler");
                #[allow(unreachable_code)]
                HandlerResult::Ok(None)
            },
        )
        .await;
        match result {
            HandlerResult::Retry { message, kind } => {
                assert!(message.contains("boom inside handler"));
                assert_eq!(kind, "handler_panicked");
            }
            other => panic!("expected Retry, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn timeout_becomes_retryable_failure() {
        let result = run_protected(
            1,
            async {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                HandlerResult::Ok(None)
            },
        )
        .await;
        match result {
            HandlerResult::Retry { kind, .. } => assert_eq!(kind, "handler_timeout"),
            other => panic!("expected Retry, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn healthy_handler_passes_through() {
        let result = run_protected(5, async { HandlerResult::Ok(Some(serde_json::json!(1))) }).await;
        assert!(matches!(result, HandlerResult::Ok(_)));
    }
}

/// A built-in echo handler for testing/demos.
pub struct EchoHandler;

#[async_trait]
impl JobHandler for EchoHandler {
    fn job_type(&self) -> &str {
        "echo"
    }

    async fn handle(&self, job: &Job) -> HandlerResult {
        tracing::info!(job_id = %job.id, payload = %job.payload, "echo handler executing");
        HandlerResult::Ok(Some(serde_json::json!({
            "echoed": true,
            "attempt": job.attempt,
        })))
    }
}

/// A handler that always fails — useful for testing retries and DLQ.
pub struct AlwaysFailHandler {
    pub message: String,
}

#[async_trait]
impl JobHandler for AlwaysFailHandler {
    fn job_type(&self) -> &str {
        "always_fail"
    }

    async fn handle(&self, _job: &Job) -> HandlerResult {
        HandlerResult::Retry {
            message: self.message.clone(),
            kind: "test_failure".to_string(),
        }
    }
}

/// A handler that simulates work by sleeping.
pub struct SleepHandler;

#[async_trait]
impl JobHandler for SleepHandler {
    fn job_type(&self) -> &str {
        "sleep"
    }

    async fn handle(&self, job: &Job) -> HandlerResult {
        let secs = job
            .payload
            .get("secs")
            .and_then(|v| v.as_u64())
            .unwrap_or(1);
        tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
        HandlerResult::Ok(Some(serde_json::json!({ "slept_secs": secs })))
    }
}

/// External payment handler that simulates UNKNOWN (spec §25: request sent, connection lost)
pub struct ExternalPaymentHandler;

#[async_trait]
impl JobHandler for ExternalPaymentHandler {
    fn job_type(&self) -> &str {
        "external_payment"
    }

    async fn handle(&self, job: &Job) -> HandlerResult {
        let should_unknown = job
            .payload
            .get("force_unknown")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if should_unknown || job.attempt == 1 {
            // First attempt simulates network timeout after sending request
            return HandlerResult::Unknown {
                message: "payment request sent but response timeout - unknown if succeeded"
                    .to_string(),
                kind: "external_timeout".to_string(),
            };
        }
        HandlerResult::Ok(Some(
            serde_json::json!({"payment": "confirmed", "idempotency": job.id}),
        ))
    }
}
