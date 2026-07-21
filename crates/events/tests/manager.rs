use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::json;
use sinan_events::{
    EventStreamManager, EventStreamManagerConfig, EventStreamManagerError,
    EventSubscriptionRequest, SubscriptionCloseReason, SubscriptionOutcome,
};
use sinan_store::{
    AuthorizedAccountScope, CanonicalJson, NewEventStreamRecord, SqliteStateStore, StoreOptions,
    WriteOutcome,
};
use sinan_types::{AccountId, EventStreamTopic};
use tokio::time::timeout;

static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);

struct TestDatabase(PathBuf);

impl TestDatabase {
    async fn manager(config: EventStreamManagerConfig) -> (Self, EventStreamManager) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let sequence = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
        let database = Self(std::env::temp_dir().join(format!(
            "sinan-events-{}-{timestamp}-{sequence}.sqlite",
            std::process::id()
        )));
        let store = SqliteStateStore::connect(StoreOptions::new(format!(
            "sqlite://{}",
            database.0.display()
        )))
        .await
        .expect("test store should connect");
        let manager =
            EventStreamManager::new(store, config).expect("manager config should be valid");
        (database, manager)
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
        let _ = fs::remove_file(format!("{}-wal", self.0.display()));
        let _ = fs::remove_file(format!("{}-shm", self.0.display()));
    }
}

fn event(
    event_id: &str,
    topic: EventStreamTopic,
    account_id: Option<&str>,
    value: i64,
) -> NewEventStreamRecord {
    NewEventStreamRecord {
        event_id: event_id.to_owned(),
        topic,
        account_id: account_id.map(AccountId::from),
        event_type: format!("test.{value}"),
        payload: CanonicalJson::from_value(json!({"value": value})).unwrap(),
        created_at: value,
    }
}

fn request(
    topics: Vec<EventStreamTopic>,
    account_id: Option<&str>,
    last_event_id: Option<&str>,
) -> EventSubscriptionRequest {
    EventSubscriptionRequest {
        topics,
        account_id: account_id.map(AccountId::from),
        last_event_id: last_event_id.map(str::to_owned),
    }
}

async fn next_event(subscription: &mut sinan_events::EventSubscription) -> String {
    match timeout(Duration::from_secs(2), subscription.recv())
        .await
        .expect("subscription should make progress")
    {
        SubscriptionOutcome::Event(record) => record.event_id,
        outcome => panic!("expected event, got {outcome:?}"),
    }
}

#[tokio::test]
async fn cursor_resume_transitions_to_live_without_duplicates() {
    let (_database, manager) = TestDatabase::manager(EventStreamManagerConfig::default()).await;
    manager
        .publish(event(
            "cursor",
            EventStreamTopic::ExecutionSummary,
            Some("account-a"),
            1,
        ))
        .await
        .unwrap();
    manager
        .publish(event(
            "replayed",
            EventStreamTopic::ExecutionSummary,
            Some("account-a"),
            2,
        ))
        .await
        .unwrap();

    let scope = AuthorizedAccountScope::new([AccountId::from("account-a")]);
    let mut subscription = manager
        .subscribe(
            request(
                vec![EventStreamTopic::ExecutionSummary],
                None,
                Some("cursor"),
            ),
            &scope,
        )
        .await
        .unwrap();
    manager
        .publish(event(
            "live",
            EventStreamTopic::ExecutionSummary,
            Some("account-a"),
            3,
        ))
        .await
        .unwrap();

    assert_eq!(next_event(&mut subscription).await, "replayed");
    assert_eq!(next_event(&mut subscription).await, "live");
}

#[tokio::test]
async fn concurrent_subscribe_and_publish_delivers_the_boundary_event_once() {
    let (_database, manager) = TestDatabase::manager(EventStreamManagerConfig::default()).await;
    manager
        .publish(event("cursor", EventStreamTopic::SystemEvent, None, 1))
        .await
        .unwrap();
    let scope = AuthorizedAccountScope::empty();
    let subscribing = {
        let manager = manager.clone();
        let scope = scope.clone();
        tokio::spawn(async move {
            manager
                .subscribe(
                    request(vec![EventStreamTopic::SystemEvent], None, Some("cursor")),
                    &scope,
                )
                .await
        })
    };
    tokio::task::yield_now().await;
    manager
        .publish(event("boundary", EventStreamTopic::SystemEvent, None, 2))
        .await
        .unwrap();
    let mut subscription = subscribing.await.unwrap().unwrap();

    assert_eq!(next_event(&mut subscription).await, "boundary");
    assert!(
        timeout(Duration::from_millis(50), subscription.recv())
            .await
            .is_err(),
        "the replay/live overlap must not duplicate the boundary event"
    );
}

#[tokio::test]
async fn topic_and_account_filters_include_global_events_but_not_other_accounts() {
    let (_database, manager) = TestDatabase::manager(EventStreamManagerConfig::default()).await;
    let scope =
        AuthorizedAccountScope::new([AccountId::from("account-a"), AccountId::from("account-b")]);
    let mut subscription = manager
        .subscribe(
            request(
                vec![
                    EventStreamTopic::ExecutionSummary,
                    EventStreamTopic::SystemEvent,
                ],
                Some("account-a"),
                None,
            ),
            &scope,
        )
        .await
        .unwrap();

    manager
        .publish(event(
            "other-account",
            EventStreamTopic::ExecutionSummary,
            Some("account-b"),
            1,
        ))
        .await
        .unwrap();
    manager
        .publish(event(
            "other-topic",
            EventStreamTopic::RiskSummary,
            Some("account-a"),
            2,
        ))
        .await
        .unwrap();
    manager
        .publish(event("global", EventStreamTopic::SystemEvent, None, 3))
        .await
        .unwrap();
    manager
        .publish(event(
            "account-a",
            EventStreamTopic::ExecutionSummary,
            Some("account-a"),
            4,
        ))
        .await
        .unwrap();

    assert_eq!(next_event(&mut subscription).await, "global");
    assert_eq!(next_event(&mut subscription).await, "account-a");
}

#[tokio::test]
async fn account_filter_cannot_expand_the_principal_scope() {
    let (_database, manager) = TestDatabase::manager(EventStreamManagerConfig::default()).await;
    let error = manager
        .subscribe(
            request(vec![EventStreamTopic::SystemEvent], Some("account-b"), None),
            &AuthorizedAccountScope::new([AccountId::from("account-a")]),
        )
        .await
        .expect_err("requested account must be authorized");

    assert!(matches!(
        error,
        EventStreamManagerError::UnauthorizedAccount { account_id }
            if account_id == AccountId::from("account-b")
    ));
}

#[tokio::test]
async fn replay_over_limit_fails_closed() {
    let (_database, manager) = TestDatabase::manager(EventStreamManagerConfig {
        live_capacity: 8,
        replay_limit: 2,
    })
    .await;
    for index in 0..4 {
        manager
            .publish(event(
                &format!("event-{index}"),
                EventStreamTopic::SystemEvent,
                None,
                index,
            ))
            .await
            .unwrap();
    }

    let error = manager
        .subscribe(
            request(vec![EventStreamTopic::SystemEvent], None, Some("event-0")),
            &AuthorizedAccountScope::empty(),
        )
        .await
        .expect_err("an incomplete replay must not switch to live");
    assert!(matches!(
        error,
        EventStreamManagerError::ReplayLimitExceeded { limit: 2 }
    ));
}

#[tokio::test]
async fn slow_consumer_closes_on_lag_without_blocking_publishers() {
    let (_database, manager) = TestDatabase::manager(EventStreamManagerConfig {
        live_capacity: 2,
        replay_limit: 10,
    })
    .await;
    let mut subscription = manager
        .subscribe(
            request(vec![EventStreamTopic::SystemEvent], None, None),
            &AuthorizedAccountScope::empty(),
        )
        .await
        .unwrap();

    for index in 0..3 {
        let outcome = manager
            .publish(event(
                &format!("event-{index}"),
                EventStreamTopic::SystemEvent,
                None,
                index,
            ))
            .await
            .expect("publisher must not wait for the subscriber");
        assert!(matches!(outcome, WriteOutcome::Inserted(_)));
    }

    let outcome = subscription.recv().await;
    assert!(matches!(
        outcome,
        SubscriptionOutcome::Closed(SubscriptionCloseReason::SlowConsumer { skipped: 1 })
    ));
    assert_eq!(subscription.recv().await, outcome);
    assert!(manager
        .store()
        .get_event_stream_record("event-2")
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn duplicate_publish_is_not_fanned_out_twice() {
    let (_database, manager) = TestDatabase::manager(EventStreamManagerConfig::default()).await;
    let mut subscription = manager
        .subscribe(
            request(vec![EventStreamTopic::SystemEvent], None, None),
            &AuthorizedAccountScope::empty(),
        )
        .await
        .unwrap();
    let item = event("event", EventStreamTopic::SystemEvent, None, 1);
    manager.publish(item.clone()).await.unwrap();
    assert_eq!(next_event(&mut subscription).await, "event");
    assert!(matches!(
        manager.publish(item).await.unwrap(),
        WriteOutcome::Duplicate(_)
    ));
    assert!(
        timeout(Duration::from_millis(50), subscription.recv())
            .await
            .is_err(),
        "duplicate append must not create a second live delivery"
    );
}
