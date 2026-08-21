use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use common::AppResult;
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
        let should_unknown = job.payload.get("force_unknown").and_then(|v| v.as_bool()).unwrap_or(false);
        if should_unknown || job.attempt == 1 {
            // First attempt simulates network timeout after sending request
            return HandlerResult::Unknown {
                message: "payment request sent but response timeout - unknown if succeeded".to_string(),
                kind: "external_timeout".to_string(),
            };
        }
        HandlerResult::Ok(Some(serde_json::json!({"payment": "confirmed", "idempotency": job.id})))
    }
}
