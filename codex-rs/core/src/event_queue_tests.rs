#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::WarningEvent;
use pretty_assertions::assert_eq;

fn warning(message: String) -> Event {
    Event {
        id: "turn".to_string(),
        msg: EventMsg::Warning(WarningEvent { message }),
    }
}

#[test]
fn bytes_match_actual_utf8_json_including_escaping() {
    let event = warning("multibyte: \u{4e2d}\u{6587}, escaped: \n\t\"\\\u{0000}".to_string());
    assert_eq!(
        event_weight(&event).unwrap(),
        serde_json::to_vec(&event).unwrap().len()
    );
}

#[test]
fn exact_event_limit_is_accepted_and_next_byte_is_rejected() {
    let overhead = event_weight(&warning(String::new())).unwrap();
    assert_eq!(
        event_weight(&warning("x".repeat(EVENT_BYTES - overhead))).unwrap(),
        EVENT_BYTES
    );
    assert!(event_weight(&warning("x".repeat(EVENT_BYTES - overhead + 1))).is_err());
}

#[tokio::test]
async fn exact_two_megabyte_queue_is_admitted_and_released() {
    let (sender, receiver) =
        BudgetedSender::bounded(EVENT_QUEUE_CAPACITY, EVENT_QUEUE_BYTES, event_weight);
    let overhead = event_weight(&warning(String::new())).unwrap();
    let event = warning("x".repeat(64 * 1024 - overhead));
    for _ in 0..32 {
        sender.send(event.clone()).await.unwrap();
    }
    assert_eq!(receiver.len(), 32);
    assert_eq!(sender.failure(), None);
    let received = receiver.recv().await.unwrap();
    sender.received(&received);
    sender.send(event).await.unwrap();
    assert_eq!(receiver.len(), 32);
    assert_eq!(sender.failure(), None);
}

#[tokio::test]
async fn one_byte_over_two_megabyte_queue_fails_closed() {
    let (sender, receiver) =
        BudgetedSender::bounded(EVENT_QUEUE_CAPACITY, EVENT_QUEUE_BYTES, event_weight);
    let overhead = event_weight(&warning(String::new())).unwrap();
    for _ in 0..31 {
        sender
            .send(warning("x".repeat(64 * 1024 - overhead)))
            .await
            .unwrap();
    }
    sender
        .send(warning("x".repeat(64 * 1024 - 2 * overhead + 1)))
        .await
        .unwrap();
    assert_eq!(sender.failure(), None);
    assert!(sender.send(warning(String::new())).await.is_err());
    assert_eq!(sender.failure(), Some("event queue byte budget exceeded"));
    assert_eq!(receiver.len(), 32);
}

#[tokio::test]
async fn feature_selects_native_budget_without_changing_default_channel() {
    let (sender, receiver) = channel();
    assert_eq!(
        sender.is_budgeted(),
        cfg!(feature = "native-bounded-events")
    );
    sender.send(warning(String::new())).await.unwrap();
    let event = receiver.recv().await.unwrap();
    sender.received(&event);
    assert_eq!(sender.failure(), None);
}

#[tokio::test]
async fn exact_native_count_limit_uses_one_shared_budget() {
    let (sender, receiver) =
        BudgetedSender::bounded(EVENT_QUEUE_CAPACITY, EVENT_QUEUE_BYTES, event_weight);
    for _ in 0..EVENT_QUEUE_CAPACITY {
        sender.clone().send(warning(String::new())).await.unwrap();
    }
    assert_eq!(receiver.len(), EVENT_QUEUE_CAPACITY);
    assert!(sender.send(warning(String::new())).await.is_err());
    assert_eq!(sender.failure(), Some("event queue count budget exceeded"));
}
