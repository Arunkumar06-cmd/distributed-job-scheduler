use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "job_status", rename_all = "SCREAMING_SNAKE_CASE")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
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

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled
        )
    }
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "job_type_enum", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum JobKind {
    Immediate,
    Delayed,
    Scheduled,
    Recurring,
    Batch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "execution_status", rename_all = "UPPERCASE")]
#[serde(rename_all = "UPPERCASE")]
pub enum ExecutionStatus {
    Started,
    Completed,
    Failed,
    Abandoned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "batch_status", rename_all = "UPPERCASE")]
#[serde(rename_all = "UPPERCASE")]
pub enum BatchStatus {
    Queued,
    Running,
    PartiallyCompleted,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "dlq_reason", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
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
    Invalid {
        from: &'static str,
        to: &'static str,
    },
}

/// The lifecycle state machine. Every DB-level status update must be checked
/// against this table so jobs can never enter an undefined state.
///
/// Terminal states: Completed, Failed, Cancelled.
/// `UnknownExternalResult` covers the crash window where a worker lost its
/// lease after dispatching work but before observing the outcome; it must
/// always resolve to a terminal or retryable state.
pub fn validate_transition(from: JobStatus, to: JobStatus) -> Result<(), StateTransitionError> {
    use JobStatus::*;
    let allowed = matches!(
        (from, to),
        (Scheduled, Queued)
            | (Waiting, Queued)
            | (Queued, Claimed)
            | (Claimed, Running)
            | (Claimed, Queued)
            | (Claimed, RetryWait)
            | (Claimed, Failed)
            | (Claimed, Cancelled)
            | (Running, Completed)
            | (Running, RetryWait)
            | (Running, Failed)
            | (Running, Queued)
            | (Running, UnknownExternalResult)
            | (UnknownExternalResult, Queued)
            | (UnknownExternalResult, RetryWait)
            | (UnknownExternalResult, Failed)
            | (UnknownExternalResult, Completed)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(from: JobStatus, to: JobStatus) {
        assert!(validate_transition(from, to).is_ok(), "{from:?} -> {to:?}");
    }

    fn bad(from: JobStatus, to: JobStatus) {
        assert!(validate_transition(from, to).is_err(), "{from:?} -> {to:?}");
    }

    #[test]
    fn happy_path() {
        ok(JobStatus::Scheduled, JobStatus::Queued);
        ok(JobStatus::Queued, JobStatus::Claimed);
        ok(JobStatus::Claimed, JobStatus::Running);
        ok(JobStatus::Running, JobStatus::Completed);
    }

    #[test]
    fn no_skipping_claim_or_run() {
        bad(JobStatus::Queued, JobStatus::Running);
        bad(JobStatus::Queued, JobStatus::Completed);
        bad(JobStatus::Scheduled, JobStatus::Running);
        bad(JobStatus::Claimed, JobStatus::Completed);
    }

    #[test]
    fn terminal_states_are_absorbing() {
        for from in [JobStatus::Completed, JobStatus::Failed, JobStatus::Cancelled] {
            assert!(from.is_terminal());
            for to in [
                JobStatus::Queued,
                JobStatus::Running,
                JobStatus::Claimed,
                JobStatus::Scheduled,
                JobStatus::RetryWait,
                JobStatus::Waiting,
                JobStatus::UnknownExternalResult,
            ] {
                bad(from, to);
            }
        }
    }

    #[test]
    fn lease_loss_paths_exist() {
        // Reaper requeues jobs whose worker died between claim and run.
        ok(JobStatus::Claimed, JobStatus::Queued);
        // Worker loses lease mid-run; job is redriven by another worker.
        ok(JobStatus::Running, JobStatus::Queued);
        // Attempts exhausted while claimed.
        ok(JobStatus::Claimed, JobStatus::Failed);
        ok(JobStatus::Claimed, JobStatus::RetryWait);
    }

    #[test]
    fn unknown_external_result_is_never_a_dead_end() {
        ok(JobStatus::UnknownExternalResult, JobStatus::Queued);
        ok(JobStatus::UnknownExternalResult, JobStatus::RetryWait);
        ok(JobStatus::UnknownExternalResult, JobStatus::Failed);
        ok(JobStatus::UnknownExternalResult, JobStatus::Completed);
        bad(JobStatus::UnknownExternalResult, JobStatus::Running);
        bad(JobStatus::UnknownExternalResult, JobStatus::Claimed);
    }

    #[test]
    fn serde_matches_sql_labels() {
        assert_eq!(serde_json::to_string(&JobStatus::RetryWait).unwrap(), "\"RETRY_WAIT\"");
        assert_eq!(serde_json::to_string(&JobKind::Recurring).unwrap(), "\"recurring\"");
        assert_eq!(
            serde_json::to_string(&ExecutionStatus::Abandoned).unwrap(),
            "\"ABANDONED\""
        );
        assert_eq!(
            serde_json::to_string(&DlqReason::MaxAttemptsExceeded).unwrap(),
            "\"max_attempts_exceeded\""
        );
    }
}
