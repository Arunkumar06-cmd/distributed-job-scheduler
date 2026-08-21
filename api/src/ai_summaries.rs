use common::Config;
use serde::Deserialize;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, warn};
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
        let client = reqwest::Client::new();
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            if shutdown.is_cancelled() {
                break;
            }
            let pending: Vec<PendingSummary> = sqlx::query_as(
                r#"SELECT dlq.id, dlq.job_id, dlq.final_error, dlq.error_kind, dlq.attempt
                   FROM dead_letter_entries dlq LEFT JOIN failure_summaries fs ON fs.dlq_id = dlq.id
                   WHERE fs.id IS NULL ORDER BY dlq.moved_at ASC LIMIT 5"#,
            )
            .fetch_all(&pool)
            .await
            .unwrap_or_default();
            for pending in pending {
                let dlq_id = pending.id;
                let job_id = pending.job_id;
                match summarize(
                    &client,
                    &api_key,
                    &config.openai_model,
                    pending.final_error,
                    pending.error_kind,
                    pending.attempt,
                )
                .await
                {
                    Ok(summary) => {
                        let result = sqlx::query(
                            r#"INSERT INTO failure_summaries (dlq_id, job_id, summary, root_cause, remediation, model)
                               VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (dlq_id) DO NOTHING"#,
                        ).bind(dlq_id).bind(job_id).bind(summary.summary).bind(summary.root_cause)
                         .bind(summary.remediation).bind(&config.openai_model).execute(&pool).await;
                        if let Err(error) = result {
                            warn!(%dlq_id, %error, "failed to store AI failure summary");
                        }
                    }
                    Err(error) => debug!(%dlq_id, %error, "AI failure summary deferred"),
                }
            }
        }
    });
}

async fn summarize(
    client: &reqwest::Client,
    api_key: &str,
    model: &str,
    final_error: Option<String>,
    error_kind: Option<String>,
    attempt: i32,
) -> anyhow::Result<SummaryOutput> {
    let prompt = format!("Summarize this failed background job for an operator. Do not invent facts. Return JSON only with summary, root_cause, remediation. Error kind: {}. Final error: {}. Attempts: {}.", error_kind.as_deref().unwrap_or("unknown"), final_error.as_deref().unwrap_or("no error message recorded"), attempt);
    let response: serde_json::Value = client.post("https://api.openai.com/v1/responses").bearer_auth(api_key)
        .json(&serde_json::json!({"model": model, "input": prompt, "max_output_tokens": 300, "store": false, "text": {"format": {"type": "json_object"}}}))
        .send().await?.error_for_status()?.json().await?;
    let text = response
        .get("output_text")
        .and_then(|value| value.as_str())
        .or_else(|| {
            response
                .get("output")
                .and_then(|output| output.as_array())
                .and_then(|items| {
                    items.iter().find_map(|item| {
                        item.get("content")
                            .and_then(|content| content.as_array())
                            .and_then(|content| {
                                content.iter().find_map(|part| {
                                    part.get("text").and_then(|text| text.as_str())
                                })
                            })
                    })
                })
        })
        .ok_or_else(|| anyhow::anyhow!("Responses API returned no output text"))?;
    Ok(serde_json::from_str(text)?)
}
