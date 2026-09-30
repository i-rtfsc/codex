#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;
use pretty_assertions::assert_eq;
use std::time::Duration;

fn weight(value: &String) -> Result<usize, &'static str> {
    if value.len() > 8 {
        Err("individual value budget exceeded")
    } else {
        Ok(value.len())
    }
}

#[tokio::test]
async fn exact_count_and_byte_limits_are_admitted_then_released() {
    let (sender, receiver) =
        BudgetedSender::bounded(/*capacity*/ 2, /*max_bytes*/ 8, weight);
    sender.send("1234".to_string()).await.unwrap();
    sender.send("5678".to_string()).await.unwrap();
    let value = receiver.recv().await.unwrap();
    sender.received(&value);
    sender.send("abcd".to_string()).await.unwrap();
    assert_eq!(receiver.len(), 2);
    assert_eq!(sender.failure(), None);
}

#[tokio::test]
async fn count_overflow_fails_without_waiting_and_wakes_shutdown() {
    let (sender, receiver) =
        BudgetedSender::bounded(/*capacity*/ 1, /*max_bytes*/ 8, weight);
    sender.send("a".to_string()).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), sender.clone().send("b".to_string()))
        .await
        .unwrap();
    assert!(result.is_err());
    assert_eq!(sender.failure(), Some("event queue count budget exceeded"));
    tokio::time::timeout(Duration::from_secs(1), sender.failed())
        .await
        .unwrap();
    assert!(sender.send("c".to_string()).await.is_err());
    assert_eq!(receiver.len(), 1);
}

#[tokio::test]
async fn byte_and_individual_overflow_are_distinct_and_latched() {
    for (first, second, reason) in [
        ("1234", "567", "event queue byte budget exceeded"),
        ("", "123456789", "individual value budget exceeded"),
    ] {
        let (sender, receiver) =
            BudgetedSender::bounded(/*capacity*/ 3, /*max_bytes*/ 6, weight);
        sender.send(first.to_string()).await.unwrap();
        assert!(sender.send(second.to_string()).await.is_err());
        assert_eq!(sender.failure(), Some(reason));
        assert_eq!(receiver.len(), 1);
    }
}

#[tokio::test]
async fn disconnected_consumer_does_not_invent_overflow() {
    let (sender, receiver) =
        BudgetedSender::bounded(/*capacity*/ 1, /*max_bytes*/ 8, weight);
    drop(receiver);
    assert!(sender.send("a".to_string()).await.is_err());
    assert_eq!(sender.failure(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_clones_share_one_admission_budget() {
    let (sender, receiver) =
        BudgetedSender::bounded(/*capacity*/ 8, /*max_bytes*/ 8, weight);
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let sender = sender.clone();
        tasks.spawn(async move { sender.send("x".to_string()).await.is_ok() });
    }
    let mut admitted = 0;
    while let Some(result) = tasks.join_next().await {
        admitted += usize::from(result.unwrap());
    }
    assert_eq!(admitted, 8);
    assert_eq!(receiver.len(), 8);
    assert_eq!(sender.failure(), Some("event queue count budget exceeded"));
    while let Ok(value) = receiver.try_recv() {
        sender.received(&value);
    }
    let usage = sender.budget.as_ref().unwrap().usage.lock().unwrap();
    assert_eq!((usage.count, usage.bytes), (0, 0));
}

#[tokio::test]
async fn unexpected_physical_full_fails_closed_instead_of_silent_send_error() {
    let (sender, receiver) =
        BudgetedSender::bounded(/*capacity*/ 1, /*max_bytes*/ 8, weight);
    sender.sender.try_send("a".to_string()).unwrap();
    assert!(sender.send("b".to_string()).await.is_err());
    assert_eq!(
        sender.failure(),
        Some("event queue admission accounting failed")
    );
    tokio::time::timeout(Duration::from_secs(1), sender.failed())
        .await
        .unwrap();
    assert_eq!(receiver.len(), 1);
}

#[tokio::test]
async fn legacy_bounded_sender_still_waits_for_capacity() {
    let (raw, receiver) = async_channel::bounded(1);
    let sender = BudgetedSender::from(raw);
    sender.send("a".to_string()).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), sender.send("b".to_string()))
            .await
            .is_err()
    );
    assert_eq!(receiver.recv().await.unwrap(), "a");
    sender.send("c".to_string()).await.unwrap();
    assert_eq!(receiver.recv().await.unwrap(), "c");
    assert_eq!(sender.failure(), None);
}

#[tokio::test]
async fn poisoned_owner_explicitly_fails_and_wakes_shutdown() {
    let (sender, receiver) =
        BudgetedSender::bounded(/*capacity*/ 1, /*max_bytes*/ 8, weight);
    let budget = sender.budget.as_ref().unwrap().clone();
    assert!(
        std::thread::spawn(move || {
            let _usage = budget.usage.lock().unwrap();
            panic!("private budget owner fault");
        })
        .join()
        .is_err()
    );
    assert_eq!(sender.failure(), Some("event queue budget owner poisoned"));
    tokio::time::timeout(Duration::from_secs(1), sender.failed())
        .await
        .unwrap();
    assert!(sender.send("a".to_string()).await.is_err());
    assert!(receiver.is_closed());
}

#[tokio::test]
async fn invalid_consumer_release_fails_closed_instead_of_underflowing() {
    let (sender, receiver) =
        BudgetedSender::bounded(/*capacity*/ 1, /*max_bytes*/ 8, weight);
    sender.received(&"a".to_string());
    assert_eq!(
        sender.failure(),
        Some("event queue admission accounting failed")
    );
    assert!(receiver.is_closed());
    tokio::time::timeout(Duration::from_secs(1), sender.failed())
        .await
        .unwrap();
}
