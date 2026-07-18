use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::Value;
use sinan_core::{
    restore_durable_circuit_breaker, DurableCircuitBreakerRestoreDisposition,
    DurableCircuitBreakerRestoreError,
};
use sinan_risk::{
    fail_closed_circuit_breaker_restore_after_epoch, restore_circuit_breaker_snapshot,
    CircuitBreakerError, CircuitBreakerRestoreFailure, CircuitBreakerState, CircuitBreakerStatus,
};
use sinan_store::{
    CanonicalJson, NewCircuitBreakerSnapshot, SqliteStateStore, StoreOptions, WriteOutcome,
};
use sqlx::SqlitePool;

const NOW: i64 = 1_700_000_000_000;
static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);

struct TestDatabase(PathBuf);

impl TestDatabase {
    fn unique() -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("test clock should be after the Unix epoch")
            .as_nanos();
        let sequence = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "sinan-core-breaker-{}-{timestamp}-{sequence}.sqlite",
            std::process::id()
        )))
    }

    fn path(&self) -> &Path {
        &self.0
    }

    fn url(&self) -> String {
        format!("sqlite://{}", self.path().display())
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
        let _ = fs::remove_file(format!("{}-wal", self.0.display()));
        let _ = fs::remove_file(format!("{}-shm", self.0.display()));
    }
}

async fn test_store() -> (TestDatabase, SqliteStateStore) {
    let database = TestDatabase::unique();
    let mut options = StoreOptions::new(database.url());
    options.max_connections = 4;
    let store = SqliteStateStore::connect(options)
        .await
        .expect("test store should connect and migrate");
    (database, store)
}

async fn raw_pool(database: &TestDatabase) -> SqlitePool {
    SqlitePool::connect(&database.url())
        .await
        .expect("raw test pool should connect")
}

fn open_state(epoch: u64, triggered_at: i64) -> CircuitBreakerState {
    assert!(epoch > 0);
    fail_closed_circuit_breaker_restore_after_epoch(
        CircuitBreakerRestoreFailure::StoreUnavailable,
        triggered_at,
        epoch - 1,
    )
    .expect("fixture epoch should have a successor")
    .state
}

fn status_name(status: CircuitBreakerStatus) -> &'static str {
    match status {
        CircuitBreakerStatus::Closed => "CLOSED",
        CircuitBreakerStatus::Open => "OPEN",
        CircuitBreakerStatus::HalfOpen => "HALF_OPEN",
    }
}

async fn write_state(
    store: &SqliteStateStore,
    state: &CircuitBreakerState,
    expected_head_revision: Option<u64>,
    updated_at: i64,
) -> u64 {
    let snapshot = state.durable_snapshot_v1();
    let payload = CanonicalJson::parse(&snapshot.to_json().expect("snapshot should serialize"))
        .expect("snapshot should canonicalize");
    store
        .write_circuit_breaker_snapshot(NewCircuitBreakerSnapshot {
            expected_head_revision,
            schema_version: snapshot.schema_version().to_owned(),
            status: status_name(snapshot.status()).to_owned(),
            recovery_epoch: snapshot.recovery_epoch(),
            updated_at,
            payload,
        })
        .await
        .expect("snapshot should persist")
        .into_record()
        .state_revision
}

async fn write_mutated_snapshot(
    store: &SqliteStateStore,
    state: &CircuitBreakerState,
    schema_version: &str,
    status: &str,
    mutate: impl FnOnce(&mut Value),
) {
    let snapshot = state.durable_snapshot_v1();
    let mut value: Value = serde_json::from_str(
        &snapshot
            .to_json()
            .expect("snapshot fixture should serialize"),
    )
    .expect("snapshot fixture should be JSON");
    mutate(&mut value);
    let payload = CanonicalJson::from_value(value).expect("mutated fixture should canonicalize");
    let write = store
        .write_circuit_breaker_snapshot(NewCircuitBreakerSnapshot {
            expected_head_revision: None,
            schema_version: schema_version.to_owned(),
            status: status.to_owned(),
            recovery_epoch: state.recovery_epoch(),
            updated_at: NOW - 100,
            payload,
        })
        .await
        .expect("alias-consistent raw snapshot should persist");
    assert!(matches!(write, WriteOutcome::Inserted(_)));
}

#[tokio::test]
async fn missing_snapshot_persists_open_revision_one() {
    let (_database, store) = test_store().await;

    let restored = restore_durable_circuit_breaker(&store, NOW)
        .await
        .expect("missing state should fail closed and persist");

    assert_eq!(
        restored.disposition,
        DurableCircuitBreakerRestoreDisposition::FailClosedPersisted
    );
    assert_eq!(restored.state_revision, 1);
    assert_eq!(restored.outcome.state.status(), CircuitBreakerStatus::Open);
    assert_eq!(restored.outcome.state.recovery_epoch(), 1);
    assert_eq!(
        restored.outcome.error,
        Some(CircuitBreakerError::DurableRestoreFailed {
            failure: CircuitBreakerRestoreFailure::MissingSnapshot,
        })
    );

    let head = store
        .get_circuit_breaker_head_metadata()
        .await
        .expect("head should load")
        .expect("head should exist");
    assert_eq!(head.state_revision, 1);
    assert_eq!(head.recovery_epoch, 1);
}

#[tokio::test]
async fn valid_snapshot_round_trips_without_appending() {
    let (_database, store) = test_store().await;
    let state = open_state(7, NOW - 100);
    assert_eq!(write_state(&store, &state, None, NOW - 100).await, 1);

    let restored = restore_durable_circuit_breaker(&store, NOW)
        .await
        .expect("valid state should restore");

    assert_eq!(
        restored.disposition,
        DurableCircuitBreakerRestoreDisposition::Restored
    );
    assert_eq!(restored.state_revision, 1);
    assert_eq!(restored.outcome.state, state);
    assert!(restored.outcome.error.is_none());
    assert_eq!(
        store
            .get_circuit_breaker_head_metadata()
            .await
            .unwrap()
            .unwrap()
            .state_revision,
        1
    );
}

#[tokio::test]
async fn unknown_high_epoch_snapshot_appends_a_new_open_epoch() {
    let (_database, store) = test_store().await;
    let state = open_state(41, NOW - 100);
    write_mutated_snapshot(
        &store,
        &state,
        "circuit-breaker-state.v999",
        "OPEN",
        |value| value["schema_version"] = Value::String("circuit-breaker-state.v999".into()),
    )
    .await;

    let restored = restore_durable_circuit_breaker(&store, NOW)
        .await
        .expect("unknown version should append a safety snapshot");

    assert_eq!(restored.state_revision, 2);
    assert_eq!(restored.outcome.state.status(), CircuitBreakerStatus::Open);
    assert_eq!(restored.outcome.state.recovery_epoch(), 42);
    assert!(matches!(
        restored.outcome.error,
        Some(CircuitBreakerError::DurableRestoreFailed {
            failure: CircuitBreakerRestoreFailure::UnsupportedSchemaVersion { .. }
        })
    ));
    assert_latest_restores(&store, 2, 42).await;
}

#[tokio::test]
async fn semantically_invalid_high_epoch_snapshot_appends_a_new_open_epoch() {
    let (_database, store) = test_store().await;
    let state = open_state(91, NOW - 100);
    write_mutated_snapshot(
        &store,
        &state,
        "circuit-breaker-state.v1",
        "CLOSED",
        |value| {
            value["status"] = Value::String("CLOSED".into());
        },
    )
    .await;

    let restored = restore_durable_circuit_breaker(&store, NOW)
        .await
        .expect("invalid semantics should append a safety snapshot");

    assert_eq!(restored.state_revision, 2);
    assert_eq!(restored.outcome.state.status(), CircuitBreakerStatus::Open);
    assert_eq!(restored.outcome.state.recovery_epoch(), 92);
    assert!(matches!(
        restored.outcome.error,
        Some(CircuitBreakerError::DurableRestoreFailed {
            failure: CircuitBreakerRestoreFailure::InvalidSnapshot { .. }
        })
    ));
    assert_latest_restores(&store, 2, 92).await;
}

#[tokio::test]
async fn corrupt_high_epoch_payload_appends_a_new_open_epoch() {
    let (database, store) = test_store().await;
    let state = open_state(51, NOW - 100);
    assert_eq!(write_state(&store, &state, None, NOW - 100).await, 1);
    let raw = raw_pool(&database).await;
    sqlx::query("DROP TRIGGER trg_circuit_breaker_snapshots_no_update")
        .execute(&raw)
        .await
        .expect("test should disable the immutable-update guard");
    sqlx::query(
        "UPDATE circuit_breaker_snapshots SET payload_hash = ? \
         WHERE scope = 'GLOBAL' AND state_revision = 1",
    )
    .bind("0".repeat(64))
    .execute(&raw)
    .await
    .expect("test should inject payload corruption");

    let restored = restore_durable_circuit_breaker(&store, NOW)
        .await
        .expect("corrupt payload should append a safety snapshot");

    assert_eq!(restored.state_revision, 2);
    assert_eq!(restored.outcome.state.status(), CircuitBreakerStatus::Open);
    assert_eq!(restored.outcome.state.recovery_epoch(), 52);
    assert_eq!(
        restored.outcome.error,
        Some(CircuitBreakerError::DurableRestoreFailed {
            failure: CircuitBreakerRestoreFailure::CorruptPayload,
        })
    );
    assert_latest_restores(&store, 2, 52).await;
    raw.close().await;
}

#[tokio::test]
async fn unavailable_store_error_retains_an_open_safety_outcome() {
    let (database, store) = test_store().await;
    let raw = raw_pool(&database).await;
    sqlx::query(
        "ALTER TABLE circuit_breaker_snapshots \
         RENAME TO unavailable_circuit_breaker_snapshots",
    )
    .execute(&raw)
    .await
    .expect("test should make the breaker repository unavailable");

    let error = restore_durable_circuit_breaker(&store, NOW)
        .await
        .expect_err("an unavailable store cannot complete durable restore");

    assert!(matches!(
        &error,
        DurableCircuitBreakerRestoreError::StoreRead { .. }
    ));
    assert_eq!(error.outcome().state.status(), CircuitBreakerStatus::Open);
    assert_eq!(error.outcome().state.recovery_epoch(), 1);
    assert_eq!(
        error.outcome().error,
        Some(CircuitBreakerError::DurableRestoreFailed {
            failure: CircuitBreakerRestoreFailure::StoreUnavailable,
        })
    );
    raw.close().await;
}

#[tokio::test]
async fn repeated_startup_restores_the_existing_safety_snapshot() {
    let (_database, store) = test_store().await;
    let first = restore_durable_circuit_breaker(&store, NOW)
        .await
        .expect("first startup should persist OPEN");
    let second = restore_durable_circuit_breaker(&store, NOW + 1)
        .await
        .expect("second startup should restore existing OPEN");

    assert_eq!(first.state_revision, 1);
    assert_eq!(second.state_revision, 1);
    assert_eq!(
        second.disposition,
        DurableCircuitBreakerRestoreDisposition::Restored
    );
    assert_eq!(second.outcome.state, first.outcome.state);
    assert!(second.outcome.error.is_none());
}

async fn assert_latest_restores(
    store: &SqliteStateStore,
    expected_revision: u64,
    expected_epoch: u64,
) {
    let latest = store
        .get_latest_circuit_breaker_snapshot()
        .await
        .expect("latest snapshot should load")
        .expect("latest snapshot should exist");
    assert_eq!(latest.state_revision, expected_revision);
    assert_eq!(latest.recovery_epoch, expected_epoch);
    let round_trip = restore_circuit_breaker_snapshot(Some(latest.payload.as_str()), NOW);
    assert!(round_trip.error.is_none());
    assert_eq!(round_trip.state.status(), CircuitBreakerStatus::Open);
    assert_eq!(round_trip.state.recovery_epoch(), expected_epoch);
}
