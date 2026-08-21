use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "job_status", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobStatus {
    Scheduled,
    Queued,
    Claimed,
    Running,
    RetryWait,
    Completed,
    Failed,
    Cancelled,
    Waiting,
    UnknownExternalResult,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Scheduled => "SCHEDULED",
            JobStatus::Queued => "QUEUED",
            JobStatus::Claimed => "CLAIMED",
            JobStatus::Running => "RUNNING",
            JobStatus::RetryWait => "RETRY_WAIT",
            JobStatus::Completed => "COMPLETED",
            JobStatus::Failed => "FAILED",
            JobStatus::Cancelled => "CANCELLED",
            JobStatus::Waiting => "WAITING",
            JobStatus::UnknownExternalResult => "UNKNOWN_EXTERNAL_RESULT",
        }
    }
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "job_type_enum", rename_all = "lowercase")]
pub enum JobKind {
    Immediate,
    Delayed,
    Scheduled,
    Recurring,
    Batch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "execution_status", rename_all = "UPPERCASE")]
pub enum ExecutionStatus {
    Started,
    Completed,
    Failed,
    Abandoned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "batch_status", rename_all = "UPPERCASE")]
pub enum BatchStatus {
    Queued,
    Running,
    PartiallyCompleted,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "dlq_reason", rename_all = "snake_case")]
pub enum DlqReason {
    MaxAttemptsExceeded,
    PermanentFailure,
    Cancelled,
}

impl DlqReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            DlqReason::MaxAttemptsExceeded => "max_attempts_exceeded",
            DlqReason::PermanentFailure => "permanent_failure",
            DlqReason::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StateTransitionError {
    #[error("invalid transition: {from} -> {to}")]
    Invalid { from: &'static str, to: &'static str },
}

pub fn validate_transition(from: JobStatus, to: JobStatus) -> Result<(), StateTransitionError> {
    use JobStatus::*;
    let allowed = matches!(
        (from, to),
        (Scheduled, Queued)
            | (Waiting, Queued)
            | (Queued, Claimed)
            | (Claimed, Running)
            | (Running, Completed)
            | (Running, RetryWait)
            | (Running, Failed)
            | (Running, UnknownExternalResult)
            | (RetryWait, Queued)
            | (RetryWait, Failed)
            | (Queued, Cancelled)
            | (Scheduled, Cancelled)
            | (Waiting, Cancelled)
            | (RetryWait, Cancelled)
    );
    if allowed {
        Ok(())
    } else {
        Err(StateTransitionError::Invalid {
            from: from.as_str(),
            to: to.as_str(),
        })
    }
}
