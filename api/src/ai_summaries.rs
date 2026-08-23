//! AI failure summaries against any OpenAI-compatible chat/completions
//! endpoint (OpenAI, NVIDIA NIM, vLLM, …) — configured entirely via env:
//!   AI_LLM_BASE_URL (default https://api.openai.com/v1)
//!   OPENAI_API_KEY   (bearer key for that endpoint)
//!   OPENAI_MODEL     (e.g. gpt-4o-mini | nvidia/nemotron-3.5-lightning-30b-a3b)

use common::Config;
use serde::Deserialize;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::warn;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
struct SummaryOutput {
    summary: String,
    root_cause: String,
    remediation: String,
}

#[derive(sqlx::FromRow)]
struct PendingSummary {
    id: Uuid,
    job_id: Uuid,
    final_error: Option<String>,
    error_kind: Option<String>,
    attempt: i32,
}

pub fn spawn(pool: PgPool, config: Arc<Config>, shutdown: tokio_util::sync::CancellationToken) {
    if !config.ai_summaries_enabled {
        tracing::info!("AI failure summaries disabled");
        return;
    }
    let Some(api_key) = config.openai_api_key.clone() else {
        tracing::warn!("AI summaries enabled but OPENAI_API_KEY is not configured");
        return;
    };
    tokio::spawn(async move {
        let client = reqwest::Client::builder()
            // No timeout means one hung upstream call stalls the whole loop.
            .timeout(std::time::Duration::from_secs(30))
            .build();
        let Ok(client) = client else {
            tracing::error!("AI summaries: could not build HTTP client");
            return;
        };
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {}
            }
            let pending: Vec<PendingSummary> = match sqlx::query_as(
                r#"SELECT dlq.id, dlq.job_id, dlq.final_error, dlq.error_kind, dlq.attempt
                   FROM dead_letter_entries dlq LEFT JOIN failure_summaries fs ON fs.dlq_id = dlq.id
                   WHERE fs.id IS NULL ORDER BY dlq.moved_at ASC LIMIT 5"#,
            )
            .fetch_all(&pool)
            .await
            {
                Ok(rows) => rows,
                Err(error) => {
                    warn!(%error, "AI summaries: failed to query pending entries");
                    continue;
                }
            };

            for entry in &pending {
                // Primary model first; configured fallbacks (e.g.
                // stepfun-ai/step-3.7-flash) are tried in order on failure.
                let mut models = vec![config.openai_model.clone()];
                models.extend(config.llm_model_fallbacks.iter().cloned());

                let mut outcome: anyhow::Result<SummaryOutput> =
                    Err(anyhow::anyhow!("no model attempted"));
                for model in &models {
                    outcome = summarize(
                        &client,
                        config.llm_base_url.trim_end_matches('/'),
                        &api_key,
                        model,
                        entry,
                    )
                    .await;
                    if outcome.is_ok() {
                        break;
                    }
                }
                match outcome {
                    Ok(summary) => {
                        let result = sqlx::query(
                            r#"INSERT INTO failure_summaries (dlq_id, job_id, summary, root_cause, remediation, model)
                               VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (dlq_id) DO NOTHING"#,
                        )
                        .bind(entry.id)
                        .bind(entry.job_id)
                        .bind(&summary.summary)
                        .bind(&summary.root_cause)
                        .bind(&summary.remediation)
                        .bind(&config.openai_model)
                        .execute(&pool)
                        .await;
                        if let Err(error) = result {
                            warn!(dlq_id = %entry.id, %error, "failed to store AI failure summary");
                        } else {
                            info_summary_stored(entry.id);
                        }
                    }
                    Err(error) => {
                        warn!(dlq_id = %entry.id, %error, "AI failure summary deferred (will retry next tick)")
                    }
                }
            }
        }
    });
}

fn info_summary_stored(dlq_id: Uuid) {
    tracing::info!(%dlq_id, "AI failure summary stored");
}

/// Truncate operator-supplied error text: unbounded payloads bloat the
/// request and can smuggle instructions into the prompt.
fn truncate(s: &str) -> String {
    if s.len() > 2000 {
        format!("{}…[truncated]", &s[..2000])
    } else {
        s.to_string()
    }
}

async fn summarize(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    model: &str,
    entry: &PendingSummary,
) -> anyhow::Result<SummaryOutput> {
    let prompt = format!(
        "Summarize this failed background job for an on-call operator. Use only the facts given; do not invent details. Reply with STRICT JSON only (no markdown fences) with keys summary, root_cause, remediation. Error kind: {}. Final error: {}. Attempts used: {}.",
        truncate(entry.error_kind.as_deref().unwrap_or("unknown")),
        truncate(entry.final_error.as_deref().unwrap_or("no error message recorded")),
        entry.attempt
    );

    let body = serde_json::json!({
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a precise SRE assistant. Output strict JSON only."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 1500,
        // Reasoning models (e.g. nvidia/nemotron) emit long thinking traces;
        // disable where supported and keep the budget for the answer itself.
        "chat_template_kwargs": {"enable_thinking": false},
    });

    let response: serde_json::Value = client
        .post(format!("{base_url}/chat/completions"))
        .bearer_auth(api_key)
        .json(&body)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let text = response
        .pointer("/choices/0/message/content")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("chat completion returned no content"))?;

    // Models sometimes wrap JSON in fences despite instructions.
    let cleaned = text
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim();
    let (start, _) = cleaned
        .char_indices()
        .find(|(_, c)| *c == '{')
        .ok_or_else(|| anyhow::anyhow!("no JSON object in model output: {cleaned:.200}"))?;
    let end = cleaned.rfind('}').map(|i| i + 1).unwrap_or(cleaned.len());
    Ok(serde_json::from_str(&cleaned[start..end])?)
}

// keep PendingSummary fields referenced in doc builds
impl PendingSummary {
    #[allow(dead_code)]
    fn kind_label(&self) -> &str {
        self.error_kind.as_deref().unwrap_or("unknown")
    }
}
