use async_nats::jetstream;
use async_nats::HeaderMap;
use bytes::Bytes;
use tracing::{debug, error, info};

use db::models::OutboxEvent;

/// Publishes a single outbox event to NATS JetStream.
/// Uses Nats-Msg-Id header for broker-side dedup within the duplicate window.
pub struct Publisher {
    js: jetstream::Context,
}

impl Publisher {
    pub fn new(client: async_nats::Client) -> Self {
        let js = jetstream::new(client);
        Self { js }
    }

    pub async fn new_async(client: async_nats::Client) -> anyhow::Result<Self> {
        Ok(Self::new(client))
    }

    pub async fn ensure_stream(
        &self,
        stream_name: &str,
        subject_filter: &str,
    ) -> anyhow::Result<()> {
        match self.js.get_stream(stream_name).await {
            Ok(_) => {
                debug!(stream = stream_name, "stream exists");
                Ok(())
            }
            Err(_) => {
                info!(
                    stream = stream_name,
                    subject = subject_filter,
                    "creating stream"
                );
                self.js
                    .create_stream(jetstream::stream::Config {
                        name: stream_name.to_string(),
                        subjects: vec![subject_filter.to_string()],
                        max_messages: 100_000,
                        ..Default::default()
                    })
                    .await
                    .map(|_| ())
                    .map_err(Into::into)
            }
        }
    }

    pub async fn publish(&self, event: &OutboxEvent) -> anyhow::Result<()> {
        let payload: Bytes = serde_json::to_vec(&event.payload)?.into();
        let mut headers = HeaderMap::new();
        headers.insert("Nats-Msg-Id", event.nats_msg_id.as_str());
        headers.insert("Job-Id", event.job_id.to_string());
        headers.insert("Queue-Id", event.queue_id.to_string());
        headers.insert("Priority", event.priority.to_string());

        let ack_future = self
            .js
            .publish_with_headers(event.subject.clone(), headers, payload)
            .await
            .map_err(|e| {
                error!(subject = %event.subject, error = %e, "nats publish failed");
                anyhow::anyhow!(e)
            })?;

        let ack = ack_future.await.map_err(|e| {
            error!(subject = %event.subject, error = %e, "nats puback failed");
            anyhow::anyhow!(e)
        })?;

        debug!(subject = %event.subject, stream = ?ack.stream, "published to jetstream");
        Ok(())
    }
}
