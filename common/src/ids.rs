use uuid::Uuid;

pub fn new_id() -> Uuid {
    Uuid::new_v4()
}

pub fn nats_subject(org_id: &Uuid, project_id: &Uuid, queue_id: &Uuid, priority: i32) -> String {
    let tier = if priority >= 10 { "high" } else if priority > 0 { "standard" } else { "low" };
    format!("org.{org_id}.proj.{project_id}.queue.{queue_id}.{tier}")
}

pub fn nats_subject_prefix(org_id: &Uuid, project_id: &Uuid, queue_id: &Uuid) -> String {
    format!("org.{org_id}.proj.{project_id}.queue.{queue_id}")
}

pub fn nats_stream_name(org_id: &Uuid, project_id: &Uuid, queue_id: &Uuid) -> String {
    format!("JOBS_{org_id}_{project_id}_{queue_id}").replace('-', "_")
}
