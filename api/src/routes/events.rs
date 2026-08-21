use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::{IntoResponse, Sse, sse::Event},
};
use futures::{SinkExt, Stream};
use std::convert::Infallible;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::middleware::AuthUser;
use crate::state::AppState;

pub async fn sse_handler(
    _auth: AuthUser,
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.broadcast.subscribe();
    let stream = BroadcastStream::new(rx)
        .filter_map(|msg| msg.ok())
        .map(|msg| {
            let (event, data) = if let Some((e, d)) = msg.split_once(':') {
                (e, d)
            } else {
                ("message", msg.as_str())
            };
            Ok(Event::default().event(event).data(data))
        });
    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("keep-alive"),
    )
}

pub async fn public_sse(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.broadcast.subscribe();
    let stream = BroadcastStream::new(rx)
        .filter_map(|msg| msg.ok())
        .map(|msg| Ok(Event::default().data(msg)));
    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("keep-alive"),
    )
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_ws(socket, state))
}

async fn handle_ws(mut socket: WebSocket, state: AppState) {
    use tokio::time::{interval, Duration};
    let mut rx = state.broadcast.subscribe();
    let mut ticker = interval(Duration::from_secs(2));
    // Send initial snapshot
    if let Ok(metrics) = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM jobs WHERE status='QUEUED'")
        .fetch_one(&state.pool)
        .await
    {
        let _ = socket
            .send(Message::Text(format!(r#"{{"type":"snapshot","queued":{}}}"#, metrics).into()))
            .await;
    }
    loop {
        tokio::select! {
            msg = rx.recv() => {
                if let Ok(m) = msg {
                    let _ = socket.send(Message::Text(m.into())).await;
                } else { break; }
            }
            _ = ticker.tick() => {
                // Periodic metrics push (WebSocket live updates per spec §38)
                let queued: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE status='QUEUED'").fetch_one(&state.pool).await.unwrap_or(0);
                let running: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE status='RUNNING'").fetch_one(&state.pool).await.unwrap_or(0);
                let dlq: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dead_letter_entries").fetch_one(&state.pool).await.unwrap_or(0);
                let payload = serde_json::json!({"type":"metrics","queued":queued,"running":running,"dlq":dlq});
                if socket.send(Message::Text(payload.to_string().into())).await.is_err() { break; }
            }
        }
    }
}
