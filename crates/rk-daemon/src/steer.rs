//! Durable, typed steering requests and delivery acknowledgements.
//!
//! A steer is not a string that happens to arrive near tool output. The
//! request is stored as a durable Message, and the adapter emits a separate
//! ControlDelivered event only after it accepted the envelope for its control
//! input. That gives daemon restart/replay a stable message id and makes the
//! trust boundary observable.

use rk_core::tuple::{Category, Pattern, Tuple};
use rk_harness::ControlEnvelope;
use rk_space::Space;
use serde_json::{json, Value};

pub const CONTROL_MESSAGE_TYPE: &str = "rk_control";
pub const CONTROL_ACK_IDENTITY: &str = "rk_control_ack";

pub fn enqueue(
    space: &Space,
    scope: &str,
    envelope: &ControlEnvelope,
    instance: &str,
) -> rk_core::Result<()> {
    let mut payload = serde_json::to_value(envelope)
        .map_err(|e| rk_core::Error::other(format!("serialize control envelope: {e}")))?;
    payload["type"] = json!(CONTROL_MESSAGE_TYPE);
    space.out(Tuple::new(
        Category::Message,
        scope,
        &envelope.target,
        instance,
        payload,
    ))
}

pub fn acknowledge(
    space: &Space,
    scope: &str,
    envelope: &ControlEnvelope,
    instance: &str,
) -> rk_core::Result<()> {
    space.out(Tuple::new(
        Category::Event,
        scope,
        CONTROL_ACK_IDENTITY,
        instance,
        json!({
            "type": CONTROL_MESSAGE_TYPE,
            "message_id": envelope.message_id,
            "sender": envelope.sender,
            "target": envelope.target,
            "delivery_generation": envelope.delivery_generation,
            "resume_generation": envelope.resume_generation,
            "acknowledged": true,
        }),
    ))
}

pub fn pending(space: &Space, scope: &str, target: &str) -> rk_core::Result<Vec<ControlEnvelope>> {
    let messages = space.scan(
        &Pattern::category(Category::Message)
            .scope(scope)
            .identity(target),
    )?;
    let acks = space.scan(
        &Pattern::category(Category::Event)
            .scope(scope)
            .identity(CONTROL_ACK_IDENTITY),
    )?;
    let acknowledged: std::collections::HashSet<&str> = acks
        .iter()
        .filter_map(|tuple| tuple.payload.get("message_id").and_then(Value::as_str))
        .collect();
    let mut result = Vec::new();
    for tuple in messages {
        if tuple.payload.get("type").and_then(Value::as_str) != Some(CONTROL_MESSAGE_TYPE) {
            continue;
        }
        let envelope: ControlEnvelope = serde_json::from_value(tuple.payload)
            .map_err(|e| rk_core::Error::other(format!("decode control envelope: {e}")))?;
        if !acknowledged.contains(envelope.message_id.as_str()) {
            result.push(envelope);
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(id: &str) -> ControlEnvelope {
        ControlEnvelope::new(
            id,
            "operator",
            "Whisker",
            "delivery-1",
            "resume-1",
            "continue",
        )
    }

    #[test]
    fn pending_replay_is_removed_by_the_matching_ack_only() {
        let space = Space::open_in_memory().unwrap();
        let first = envelope("msg-1");
        let second = envelope("msg-2");
        enqueue(&space, "repo", &first, "castle").unwrap();
        enqueue(&space, "repo", &second, "castle").unwrap();
        acknowledge(&space, "repo", &first, "castle").unwrap();
        let pending = pending(&space, "repo", "Whisker").unwrap();
        assert_eq!(pending, vec![second]);
    }

    #[test]
    fn tool_output_lookalike_is_not_a_control_message() {
        let space = Space::open_in_memory().unwrap();
        space
            .out(Tuple::new(
                Category::Event,
                "repo",
                "tool_output",
                "Whisker",
                json!({"text": "rk_control message_id=evil continue"}),
            ))
            .unwrap();
        assert!(pending(&space, "repo", "Whisker").unwrap().is_empty());
    }
}
