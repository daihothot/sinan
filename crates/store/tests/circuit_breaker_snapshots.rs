mod common;

use common::test_store;
use serde_json::json;
use sinan_store::{
    CanonicalJson, NewCircuitBreakerSnapshot, StoreError, WriteOutcome,
    GLOBAL_CIRCUIT_BREAKER_SCOPE,
};

const RISK_CLOSED_SNAPSHOT_V1_JSON: &str = r#"{
  "schema_version": "circuit-breaker-state.v1",
  "recovery_epoch": 0,
  "status": "CLOSED",
  "reason": "OK",
  "triggered_at_ms": null,
  "triggered_by": null,
  "last_incident_fingerprint": null,
  "incident_evidence_cleared_at_ms": null,
  "half_opened_at_ms": null,
  "half_open_daily_loss_baseline_bps": null,
  "half_open_drawdown_baseline_bps": null,
  "reset_at_ms": null,
  "reset_by": null,
  "blocked_intent_count": 0
}"#;

fn payload(status: &str, recovery_epoch: u64, blocked_intent_count: u64) -> CanonicalJson {
    CanonicalJson::from_value(json!({
        "schema_version": "circuit-breaker-state.v1",
        "recovery_epoch": recovery_epoch,
        "status": status,
        "reason": if status == "CLOSED" { "OK" } else { "STORE_RECOVERY_RECONCILIATION_PENDING" },
        "triggered_at_ms": if status == "CLOSED" { None } else { Some(2_000_i64) },
        "triggered_by": if status == "CLOSED" {
            None
        } else {
            Some(json!({"kind": "STORE_RECOVERY"}))
        },
        "last_incident_fingerprint": null,
        "incident_evidence_cleared_at_ms": null,
        "half_opened_at_ms": null,
        "half_open_daily_loss_baseline_bps": null,
        "half_open_drawdown_baseline_bps": null,
        "reset_at_ms": null,
        "reset_by": null,
        "blocked_intent_count": blocked_intent_count
    }))
    .expect("snapshot fixture should canonicalize")
}

fn snapshot(
    expected_head_revision: Option<u64>,
    status: &str,
    recovery_epoch: u64,
    updated_at: i64,
    blocked_intent_count: u64,
) -> NewCircuitBreakerSnapshot {
    NewCircuitBreakerSnapshot {
        expected_head_revision,
        schema_version: "circuit-breaker-state.v1".to_owned(),
        status: status.to_owned(),
        recovery_epoch,
        updated_at,
        payload: payload(status, recovery_epoch, blocked_intent_count),
    }
}

#[tokio::test]
async fn persists_the_real_risk_v1_json_contract_as_canonical_payload() {
    let (_database, store, _pool) = test_store().await;
    let payload = CanonicalJson::parse(RISK_CLOSED_SNAPSHOT_V1_JSON)
        .expect("Risk snapshot fixture should canonicalize");
    let new_snapshot = NewCircuitBreakerSnapshot {
        expected_head_revision: None,
        schema_version: "circuit-breaker-state.v1".to_owned(),
        status: "CLOSED".to_owned(),
        recovery_epoch: 0,
        updated_at: 1_000,
        payload: payload.clone(),
    };

    let stored = store
        .write_circuit_breaker_snapshot(new_snapshot)
        .await
        .expect("valid Risk snapshot should persist")
        .into_record();
    assert_eq!(stored.scope, GLOBAL_CIRCUIT_BREAKER_SCOPE);
    assert_eq!(stored.state_revision, 1);
    assert_eq!(stored.payload, payload);
    assert_eq!(
        store
            .get_latest_circuit_breaker_snapshot()
            .await
            .expect("latest snapshot should load"),
        Some(stored)
    );
}

#[tokio::test]
async fn revision_cas_is_append_only_idempotent_and_rejects_stale_writers() {
    let (_database, store, pool) = test_store().await;
    let initial = snapshot(None, "CLOSED", 0, 1_000, 0);
    let first = store
        .write_circuit_breaker_snapshot(initial.clone())
        .await
        .expect("initial snapshot should insert");
    assert!(matches!(first, WriteOutcome::Inserted(_)));
    assert!(matches!(
        store
            .write_circuit_breaker_snapshot(initial.clone())
            .await
            .expect("exact initial retry should be idempotent"),
        WriteOutcome::Duplicate(_)
    ));

    let second = snapshot(Some(1), "OPEN", 1, 2_000, 1);
    let inserted = store
        .write_circuit_breaker_snapshot(second.clone())
        .await
        .expect("matching head should append")
        .into_record();
    assert_eq!(inserted.state_revision, 2);
    assert!(matches!(
        store
            .write_circuit_breaker_snapshot(second.clone())
            .await
            .expect("exact update retry should be idempotent"),
        WriteOutcome::Duplicate(_)
    ));

    let stale = snapshot(Some(1), "OPEN", 2, 3_000, 2);
    assert!(matches!(
        store.write_circuit_breaker_snapshot(stale).await,
        Err(StoreError::StaleWrite { .. })
    ));
    let conflicting_initial = snapshot(None, "OPEN", 1, 2_000, 99);
    assert!(matches!(
        store
            .write_circuit_breaker_snapshot(conflicting_initial)
            .await,
        Err(StoreError::StaleWrite { .. })
    ));

    let revisions: Vec<i64> = sqlx::query_scalar(
        "SELECT state_revision FROM circuit_breaker_snapshots ORDER BY state_revision",
    )
    .fetch_all(&pool)
    .await
    .expect("snapshot revisions should be readable");
    assert_eq!(revisions, [1, 2]);
}

#[tokio::test]
async fn rejects_invalid_or_drifting_payload_aliases_before_writing() {
    let (_database, store, _pool) = test_store().await;
    let mut wrong_schema = snapshot(None, "CLOSED", 0, 1_000, 0);
    wrong_schema.schema_version = "circuit-breaker-state.v2".to_owned();
    assert!(matches!(
        store.write_circuit_breaker_snapshot(wrong_schema).await,
        Err(StoreError::InvalidRecord { .. })
    ));

    let mut wrong_status = snapshot(None, "CLOSED", 0, 1_000, 0);
    wrong_status.status = "OPEN".to_owned();
    assert!(matches!(
        store.write_circuit_breaker_snapshot(wrong_status).await,
        Err(StoreError::InvalidRecord { .. })
    ));

    let mut wrong_epoch = snapshot(None, "CLOSED", 0, 1_000, 0);
    wrong_epoch.recovery_epoch = 1;
    assert!(matches!(
        store.write_circuit_breaker_snapshot(wrong_epoch).await,
        Err(StoreError::InvalidRecord { .. })
    ));
    assert_eq!(
        store
            .get_circuit_breaker_head_revision()
            .await
            .expect("head query should succeed"),
        None
    );
}

#[tokio::test]
async fn corrupt_latest_payload_does_not_block_a_fail_closed_append() {
    let (_database, store, pool) = test_store().await;
    store
        .write_circuit_breaker_snapshot(snapshot(None, "CLOSED", 0, 1_000, 0))
        .await
        .expect("initial snapshot should insert");

    sqlx::query("DROP TRIGGER trg_circuit_breaker_snapshots_no_update")
        .execute(&pool)
        .await
        .expect("test fixture should disable immutable protection");
    sqlx::query(
        "UPDATE circuit_breaker_snapshots SET payload_hash = ? \
         WHERE scope = 'GLOBAL' AND state_revision = 1",
    )
    .bind("b".repeat(64))
    .execute(&pool)
    .await
    .expect("test fixture should corrupt the latest payload hash");

    assert!(matches!(
        store.get_latest_circuit_breaker_snapshot().await,
        Err(StoreError::CorruptData { .. })
    ));
    let head = store
        .get_circuit_breaker_head_revision()
        .await
        .expect("head must remain readable without parsing payload");
    assert_eq!(head, Some(1));

    let recovered = store
        .write_circuit_breaker_snapshot(snapshot(head, "OPEN", 1, 2_000, 0))
        .await
        .expect("fail-closed snapshot should append after corrupt payload")
        .into_record();
    assert_eq!(recovered.state_revision, 2);
    assert_eq!(recovered.status, "OPEN");
    assert_eq!(
        store
            .get_latest_circuit_breaker_snapshot()
            .await
            .expect("new latest snapshot should be healthy"),
        Some(recovered)
    );
}

#[tokio::test]
async fn concurrent_writers_with_the_same_expected_head_cannot_both_append() {
    let (_database, store, _pool) = test_store().await;
    store
        .write_circuit_breaker_snapshot(snapshot(None, "CLOSED", 0, 1_000, 0))
        .await
        .expect("initial snapshot should insert");

    let left_store = store.clone();
    let right_store = store.clone();
    let (left, right) = tokio::join!(
        left_store.write_circuit_breaker_snapshot(snapshot(Some(1), "OPEN", 1, 2_000, 1)),
        right_store.write_circuit_breaker_snapshot(snapshot(Some(1), "OPEN", 1, 2_001, 2)),
    );
    let results = [left, right];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(StoreError::StaleWrite { .. })))
            .count(),
        1
    );
    assert_eq!(
        store
            .get_circuit_breaker_head_revision()
            .await
            .expect("head should load"),
        Some(2)
    );
}

#[tokio::test]
async fn write_transaction_can_roll_back_a_snapshot_append() {
    let (_database, store, _pool) = test_store().await;
    let mut transaction = store.begin_write().await.expect("transaction should begin");
    transaction
        .write_circuit_breaker_snapshot(snapshot(None, "CLOSED", 0, 1_000, 0))
        .await
        .expect("snapshot should insert inside the transaction");
    assert_eq!(
        transaction
            .get_circuit_breaker_head_revision()
            .await
            .expect("transaction should see its own append"),
        Some(1)
    );
    transaction
        .rollback()
        .await
        .expect("rollback should succeed");

    assert_eq!(
        store
            .get_circuit_breaker_head_revision()
            .await
            .expect("rolled-back head should load"),
        None
    );
}
