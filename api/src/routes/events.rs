use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    response::{sse::Event, IntoResponse, Response, Sse},
};
use futures::Stream;
use serde::Deserialize;
use std::convert::Infallible;
use tokio_stream::{wrappers::IntervalStream, StreamExt};
use uuid::Uuid;

use crate::middleware::AuthUser;
use crate::state::AppState;
use common::{AppError, AppResult};
use db::queries;

#[derive(Debug, Deserialize)]
pub struct ProjectEventsQuery {
    pub project_id: Uuid,
}

async fn authorize_project(state: &AppState, user_id: Uuid, project_id: Uuid) -> AppResult<()> {
    let project = queries::get_project(&state.pool, project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    if !queries::user_in_org(&state.pool, user_id, project.org_id).await? {
        return Err(AppError::Forbidden("forbidden".to_string()));
    }
    Ok(())
}

async fn project_snapshot(state: &AppState, project_id: Uuid) -> serde_json::Value {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        r#"SELECT j.status::text, COUNT(*)::bigint
           FROM jobs j JOIN queues q ON q.id = j.queue_id
           WHERE q.project_id = $1
           GROUP BY j.status
           UNION ALL
           SELECT 'DLQ', COUNT(*)::bigint
           FROM dead_letter_entries d JOIN queues q ON q.id = d.queue_id
           WHERE q.project_id = $1"#,
    )
    .bind(project_id)
    .fetch_all(&state.pool)
    .await
    .unwrap_or_default();
    let mut counts = serde_json::Map::new();
    for (status, count) in rows {
        counts.insert(status, serde_json::json!(count));
    }
    let recent = db::queries::recent_activity(&state.pool, 8)
        .await
        .unwrap_or_default();
    serde_json::json!({"type":"project.snapshot", "project_id":project_id, "counts":counts, "recent":recent})
}

/// Authenticated, tenant-scoped server-sent snapshots. The stream deliberately
/// does not use the process-wide broadcast channel because that channel carries
/// unscoped identifiers from other organizations.
pub async fn sse_handler(
    auth: AuthUser,
    State(state): State<AppState>,
    Query(query): Query<ProjectEventsQuery>,
) -> AppResult<Sse<impl Stream<Item = Result<Event, Infallible>>>> {
    authorize_project(&state, auth.user_id, query.project_id).await?;
    let state_for_stream = state.clone();
    let project_id = query.project_id;
    let interval = tokio::time::interval(std::time::Duration::from_secs(2));
    let stream = IntervalStream::new(interval).then(move |_| {
        let state = state_for_stream.clone();
        async move {
            let snapshot = project_snapshot(&state, project_id).await;
            Ok(Event::default()
                .event("project.snapshot")
                .data(snapshot.to_string()))
        }
    });
    Ok(Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

/// Authenticated WebSocket snapshots for non-browser and browser clients that
/// can attach the existing Bearer token during the handshake.
pub async fn ws_handler(
    auth: AuthUser,
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(query): Query<ProjectEventsQuery>,
) -> AppResult<Response> {
    authorize_project(&state, auth.user_id, query.project_id).await?;
    Ok(ws
        .on_upgrade(move |socket| handle_ws(socket, state, query.project_id))
        .into_response())
}

async fn handle_ws(mut socket: WebSocket, state: AppState, project_id: Uuid) {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(2));
    loop {
        // Watch the inbound half so client disconnects tear this down
        // immediately instead of waiting for a failed send.
        tokio::select! {
            _ = ticker.tick() => {
                let snapshot = project_snapshot(&state, project_id).await;
                if socket
                    .send(Message::Text(snapshot.to_string()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    None | Some(Err(_)) => break,
                    Some(Ok(Message::Close(_))) => break,
                    Some(Ok(_)) => continue,
                }
            }
        }
    }
}
