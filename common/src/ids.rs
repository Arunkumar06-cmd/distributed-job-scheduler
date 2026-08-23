use uuid::Uuid;

tokio::task_local! {
    static REQUEST_ID: String;
}

/// Runs `f` with a request id installed for the current task; error responses
/// created anywhere inside `f` will carry it.
pub async fn with_request_id<F>(request_id: String, f: F) -> F::Output
where
    F: std::future::Future,
{
    REQUEST_ID.scope(request_id, f).await
}

/// The request id installed by `with_request_id` on this task, if any.
pub fn current_request_id() -> Option<String> {
    REQUEST_ID.try_with(|id| id.clone()).ok().filter(|s| !s.is_empty())
}

pub fn new_id() -> Uuid {
    Uuid::new_v4()
}

/// URL-safe slug: lowercase, whitespace/underscores collapse to hyphens,
/// anything outside [a-z0-9-] is dropped, edges trimmed. Empty input maps to
/// "item" so callers can rely on a non-empty result.
pub fn normalize_slug(input: &str) -> String {
    let mut slug = String::with_capacity(input.len());
    let mut last_dash = false;
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash && (ch == ' ' || ch == '_' || ch == '-' || ch == '.') {
            slug.push('-');
            last_dash = true;
        }
    }
    let trimmed = slug.trim_matches('-').to_string();
    if trimmed.is_empty() {
        "item".to_string()
    } else {
        trimmed
    }
}

/// Priority tiers bound the number of NATS subjects per queue so worker
/// subscriptions stay finite regardless of the [0,100] priority range.
pub fn priority_tier(priority: i32) -> &'static str {
    if priority >= 10 {
        "high"
    } else if priority > 0 {
        "standard"
    } else {
        "low"
    }
}

pub fn nats_subject(org_id: &Uuid, project_id: &Uuid, queue_id: &Uuid, priority: i32) -> String {
    let tier = priority_tier(priority);
    format!("org.{org_id}.proj.{project_id}.queue.{queue_id}.{tier}")
}

pub fn nats_shard_subject(
    org_id: &Uuid,
    project_id: &Uuid,
    queue_id: &Uuid,
    shard_id: i32,
    priority: i32,
) -> String {
    let tier = priority_tier(priority);
    format!("org.{org_id}.proj.{project_id}.queue.{queue_id}.shard.{shard_id}.{tier}")
}

pub fn nats_subject_for_shard(
    org_id: &Uuid,
    project_id: &Uuid,
    queue_id: &Uuid,
    shard_count: i32,
    shard_id: i32,
    priority: i32,
) -> String {
    if shard_count <= 1 {
        nats_subject(org_id, project_id, queue_id, priority)
    } else {
        nats_shard_subject(org_id, project_id, queue_id, shard_id, priority)
    }
}

/// Stable FNV-1a routing avoids a new dependency while ensuring retries with
/// the same key reach the same shard.
pub fn shard_for_key(key: &str, shard_count: i32) -> i32 {
    let shards = shard_count.max(1) as u64;
    let hash = key.bytes().fold(0xcbf29ce484222325_u64, |acc, byte| {
        (acc ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    });
    (hash % shards) as i32
}

pub fn nats_subject_prefix(org_id: &Uuid, project_id: &Uuid, queue_id: &Uuid) -> String {
    format!("org.{org_id}.proj.{project_id}.queue.{queue_id}")
}

/// NATS stream names must match `[a-zA-Z0-9_-]+`; UUID hyphens are stripped.
pub fn nats_stream_name(org_id: &Uuid, project_id: &Uuid, queue_id: &Uuid) -> String {
    format!("JOBS_{org_id}_{project_id}_{queue_id}").replace('-', "_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_routing_is_stable_and_bounded() {
        for key in ["order-123", "", "x"] {
            let first = shard_for_key(key, 8);
            assert_eq!(first, shard_for_key(key, 8));
            assert!((0..8).contains(&first));
        }
    }

    #[test]
    fn shard_count_of_one_or_less_maps_to_shard_zero() {
        assert_eq!(shard_for_key("k", 1), 0);
        assert_eq!(shard_for_key("k", 0), 0);
        assert_eq!(shard_for_key("k", -3), 0);
    }

    #[test]
    fn slugs_are_url_safe_and_stable() {
        assert_eq!(normalize_slug("My Cool Org"), "my-cool-org");
        assert_eq!(normalize_slug("  A__B--C  "), "a-b-c");
        assert_eq!(normalize_slug("Ünïcödé!!"), "ncd");
        assert_eq!(normalize_slug("!!!"), "item");
        assert_eq!(normalize_slug("-x-"), "x");
    }

    #[test]
    fn tier_boundaries() {
        assert_eq!(priority_tier(0), "low");
        assert_eq!(priority_tier(1), "standard");
        assert_eq!(priority_tier(9), "standard");
        assert_eq!(priority_tier(10), "high");
        assert_eq!(priority_tier(100), "high");
    }

    #[test]
    fn subject_shape() {
        let org = Uuid::nil();
        let proj = Uuid::from_u128(1);
        let q = Uuid::from_u128(2);
        assert_eq!(
            nats_subject(&org, &proj, &q, 50),
            format!("org.{org}.proj.{proj}.queue.{q}.high")
        );
        assert_eq!(
            nats_subject_for_shard(&org, &proj, &q, 4, 2, 5),
            format!("org.{org}.proj.{proj}.queue.{q}.shard.2.standard")
        );
        // Unsharded queues never expose a shard segment.
        assert_eq!(
            nats_subject_for_shard(&org, &proj, &q, 1, 0, 0),
            nats_subject(&org, &proj, &q, 0)
        );
    }

    #[test]
    fn stream_name_has_no_hyphens() {
        let name = nats_stream_name(&Uuid::new_v4(), &Uuid::new_v4(), &Uuid::new_v4());
        assert!(!name.contains('-'));
        assert!(name.starts_with("JOBS_"));
    }
}
