use uuid::Uuid;

pub fn new_id() -> Uuid {
    Uuid::new_v4()
}

pub fn nats_subject(org_id: &Uuid, project_id: &Uuid, queue_id: &Uuid, priority: i32) -> String {
    let tier = if priority >= 10 {
        "high"
    } else if priority > 0 {
        "standard"
    } else {
        "low"
    };
    format!("org.{org_id}.proj.{project_id}.queue.{queue_id}.{tier}")
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

pub fn nats_shard_subject(
    org_id: &Uuid,
    project_id: &Uuid,
    queue_id: &Uuid,
    shard_id: i32,
    priority: i32,
) -> String {
    let tier = if priority >= 10 {
        "high"
    } else if priority > 0 {
        "standard"
    } else {
        "low"
    };
    format!("org.{org_id}.proj.{project_id}.queue.{queue_id}.shard.{shard_id}.{tier}")
}

/// Stable FNV-1a routing avoids a new dependency while ensuring retries with
/// the same key reach the same shard.
pub fn shard_for_key(key: &str, shard_count: i32) -> i32 {
    let hash = key.bytes().fold(0xcbf29ce484222325_u64, |acc, byte| {
        (acc ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    });
    (hash % shard_count.max(1) as u64) as i32
}

pub fn nats_subject_prefix(org_id: &Uuid, project_id: &Uuid, queue_id: &Uuid) -> String {
    format!("org.{org_id}.proj.{project_id}.queue.{queue_id}")
}

pub fn nats_stream_name(org_id: &Uuid, project_id: &Uuid, queue_id: &Uuid) -> String {
    format!("JOBS_{org_id}_{project_id}_{queue_id}").replace('-', "_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_routing_is_stable_and_bounded() {
        let first = shard_for_key("order-123", 8);
        assert_eq!(first, shard_for_key("order-123", 8));
        assert!((0..8).contains(&first));
    }
}
