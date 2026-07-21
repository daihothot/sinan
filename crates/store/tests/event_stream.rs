use serde_json::json;
use sinan_store::{
    AuthorizedAccountScope, CanonicalJson, EventReplayError, EventRetentionPolicy,
    EventStreamFilter, NewEventStreamRecord, StoreError, WriteOutcome,
};
use sinan_types::{AccountId, EventStreamTopic};

mod common;

use common::test_store;

fn event(
    event_id: &str,
    topic: EventStreamTopic,
    account_id: Option<&str>,
    value: i64,
    created_at: i64,
) -> NewEventStreamRecord {
    NewEventStreamRecord {
        event_id: event_id.to_owned(),
        topic,
        account_id: account_id.map(AccountId::from),
        event_type: format!("test.{value}"),
        payload: CanonicalJson::from_value(json!({"value": value})).unwrap(),
        created_at,
    }
}

#[tokio::test]
async fn append_is_ordered_idempotent_and_rejects_identity_drift() {
    let (_database, store, _raw_pool) = test_store().await;
    let first = event("event-1", EventStreamTopic::SystemEvent, None, 1, 10);
    let second = event("event-2", EventStreamTopic::SystemEvent, None, 2, 10);

    let inserted_first = store
        .append_event_stream_record(first.clone())
        .await
        .expect("first event should append");
    let inserted_second = store
        .append_event_stream_record(second)
        .await
        .expect("second event should append");
    let duplicate = store
        .append_event_stream_record(first.clone())
        .await
        .expect("identical event should be idempotent");

    assert!(matches!(inserted_first, WriteOutcome::Inserted(_)));
    assert!(matches!(inserted_second, WriteOutcome::Inserted(_)));
    assert!(matches!(duplicate, WriteOutcome::Duplicate(_)));
    assert!(
        inserted_first.record().stream_sequence < inserted_second.record().stream_sequence,
        "created_at ties must retain append order"
    );
    assert_eq!(
        duplicate.record().stream_sequence,
        inserted_first.record().stream_sequence
    );

    let mut drifted = first;
    drifted.payload = CanonicalJson::from_value(json!({"value": 999})).unwrap();
    let error = store
        .append_event_stream_record(drifted)
        .await
        .expect_err("same event id with another payload must conflict");
    assert!(matches!(
        error,
        StoreError::IdentityConflict {
            entity: "event_stream",
            ..
        }
    ));
}

#[tokio::test]
async fn concurrent_appends_receive_unique_monotonic_sequences() {
    let (_database, store, _raw_pool) = test_store().await;
    let mut tasks = Vec::new();
    for index in 0..24 {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            store
                .append_event_stream_record(event(
                    &format!("event-{index}"),
                    EventStreamTopic::SystemEvent,
                    None,
                    index,
                    100,
                ))
                .await
                .expect("concurrent append should succeed")
                .into_record()
                .stream_sequence
        }));
    }

    let mut sequences = Vec::new();
    for task in tasks {
        sequences.push(task.await.expect("append task should complete"));
    }
    sequences.sort_unstable();
    assert_eq!(sequences, (1_u64..=24).collect::<Vec<_>>());
}

#[tokio::test]
async fn replay_is_cursor_exclusive_topic_filtered_and_account_authorized() {
    let (_database, store, _raw_pool) = test_store().await;
    let fixtures = [
        event(
            "cursor",
            EventStreamTopic::SystemEvent,
            Some("account-a"),
            0,
            100,
        ),
        event(
            "account-a",
            EventStreamTopic::ExecutionSummary,
            Some("account-a"),
            1,
            100,
        ),
        event(
            "account-b",
            EventStreamTopic::ExecutionSummary,
            Some("account-b"),
            2,
            100,
        ),
        event("global", EventStreamTopic::SystemEvent, None, 3, 99),
        event(
            "other-topic",
            EventStreamTopic::RiskSummary,
            Some("account-a"),
            4,
            101,
        ),
    ];
    for fixture in fixtures {
        store
            .append_event_stream_record(fixture)
            .await
            .expect("fixture should append");
    }

    let filter = EventStreamFilter::new(
        [
            EventStreamTopic::ExecutionSummary,
            EventStreamTopic::SystemEvent,
        ],
        AuthorizedAccountScope::new([AccountId::from("account-a")]),
    );
    let replay = store
        .replay_event_stream(Some("cursor"), &filter, 10)
        .await
        .expect("authorized replay should succeed");

    assert_eq!(
        replay
            .records
            .iter()
            .map(|record| record.event_id.as_str())
            .collect::<Vec<_>>(),
        ["account-a", "global"]
    );
    assert!(!replay.has_more);
    assert_eq!(
        replay.high_water,
        store.event_stream_high_water().await.unwrap()
    );

    let global_only = EventStreamFilter::new(
        [EventStreamTopic::SystemEvent],
        AuthorizedAccountScope::empty(),
    );
    let replay = store
        .replay_event_stream(Some("cursor"), &global_only, 10)
        .await
        .expect_err("an account-bound cursor outside scope must not disclose its existence");
    assert!(matches!(replay, EventReplayError::CursorExpired { .. }));

    let replay = store
        .replay_event_stream(Some("account-b"), &filter, 10)
        .await
        .expect_err("a cross-account cursor must not disclose its existence");
    assert!(matches!(replay, EventReplayError::CursorExpired { .. }));

    let replay = store
        .replay_event_stream(Some("global"), &global_only, 10)
        .await
        .expect("a global cursor is authorized even with an empty account scope");
    assert!(replay.records.is_empty());
}

#[tokio::test]
async fn only_system_and_deadletter_topics_may_be_global() {
    let (_database, store, raw_pool) = test_store().await;

    for (event_id, topic) in [
        ("market", EventStreamTopic::MarketSnapshot),
        ("risk", EventStreamTopic::RiskSummary),
        ("execution", EventStreamTopic::ExecutionSummary),
    ] {
        let error = store
            .append_event_stream_record(event(event_id, topic, None, 1, 1))
            .await
            .expect_err("account-bound topics must reject a missing account_id");
        assert!(matches!(error, StoreError::InvalidRecord { .. }));
    }

    for (event_id, topic) in [
        ("system", EventStreamTopic::SystemEvent),
        ("deadletter", EventStreamTopic::DeadletterSummary),
    ] {
        store
            .append_event_stream_record(event(event_id, topic, None, 1, 1))
            .await
            .expect("global topic should accept a missing account_id");
    }

    let hash = "0".repeat(64);
    let result = sqlx::query(
        "INSERT INTO event_stream_log (\
             event_id, topic, event_type, payload_json, payload_hash, created_at\
         ) VALUES ('invalid-global', 'execution.summary', 'test', '{}', ?, 1)",
    )
    .bind(hash)
    .execute(&raw_pool)
    .await;
    assert!(
        result.is_err(),
        "the SQL constraint must enforce the same scope rule"
    );
}

#[tokio::test]
async fn replay_reports_limit_without_crossing_the_captured_high_water() {
    let (_database, store, _raw_pool) = test_store().await;
    for index in 0..4 {
        store
            .append_event_stream_record(event(
                &format!("event-{index}"),
                EventStreamTopic::SystemEvent,
                None,
                index,
                index,
            ))
            .await
            .unwrap();
    }
    let filter = EventStreamFilter::all_topics(AuthorizedAccountScope::empty());
    let replay = store
        .replay_event_stream(Some("event-0"), &filter, 2)
        .await
        .unwrap();

    assert_eq!(replay.records.len(), 2);
    assert_eq!(replay.records[0].event_id, "event-1");
    assert_eq!(replay.records[1].event_id, "event-2");
    assert!(replay.has_more);
    assert_eq!(
        replay.high_water,
        Some(
            store
                .get_event_stream_record("event-3")
                .await
                .unwrap()
                .unwrap()
                .stream_sequence
        )
    );
}

#[tokio::test]
async fn retention_prunes_by_count_or_age_and_expires_removed_cursors() {
    let (_database, store, _raw_pool) = test_store().await;
    for (index, created_at) in [10, 20, 30, 40, 50].into_iter().enumerate() {
        store
            .append_event_stream_record(event(
                &format!("event-{index}"),
                EventStreamTopic::SystemEvent,
                None,
                index as i64,
                created_at,
            ))
            .await
            .unwrap();
    }

    let outcome = store
        .prune_event_stream(EventRetentionPolicy {
            retain_latest: Some(3),
            created_at_or_after: Some(40),
        })
        .await
        .expect("retention should prune records outside either bound");
    assert_eq!(outcome.deleted, 3);
    assert_eq!(
        outcome.oldest_remaining_event_id.as_deref(),
        Some("event-3")
    );

    let filter = EventStreamFilter::all_topics(AuthorizedAccountScope::empty());
    let error = store
        .replay_event_stream(Some("event-2"), &filter, 10)
        .await
        .expect_err("a pruned cursor must expire");
    assert!(matches!(error, EventReplayError::CursorExpired { .. }));

    let replay = store
        .replay_event_stream(Some("event-3"), &filter, 10)
        .await
        .expect("retained cursor should replay");
    assert_eq!(replay.records[0].event_id, "event-4");

    let previous_high_water = replay.high_water.unwrap();
    store
        .prune_event_stream(EventRetentionPolicy {
            retain_latest: None,
            created_at_or_after: Some(100),
        })
        .await
        .expect("age retention should be able to empty the replay window");
    let appended = store
        .append_event_stream_record(event(
            "after-empty-retention",
            EventStreamTopic::SystemEvent,
            None,
            6,
            100,
        ))
        .await
        .unwrap();
    assert!(appended.record().stream_sequence > previous_high_water);
}

#[tokio::test]
async fn replay_detects_corrupt_payloads() {
    let (_database, store, raw_pool) = test_store().await;
    store
        .append_event_stream_record(event("cursor", EventStreamTopic::SystemEvent, None, 0, 1))
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO event_stream_log (\
             event_id, topic, event_type, payload_json, payload_hash, created_at\
         ) VALUES ('corrupt', 'system.event', 'test', '{\"b\":2,\"a\":1}',\
                   '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a', 2)",
    )
    .execute(&raw_pool)
    .await
    .expect("schema-valid corrupt fixture should insert");

    let filter = EventStreamFilter::all_topics(AuthorizedAccountScope::empty());
    let error = store
        .replay_event_stream(Some("cursor"), &filter, 10)
        .await
        .expect_err("non-canonical stored payload must be rejected");
    assert!(matches!(
        error,
        EventReplayError::Store(StoreError::CorruptData {
            entity: "event_stream",
            ..
        })
    ));
}
