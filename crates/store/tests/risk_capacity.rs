mod common;

use sinan_store::{NewRiskCapacitySnapshot, ProjectionWriteOutcome, StoreError};
use sinan_types::{AccountId, RiskCapacity, StrategyId};

use common::test_store;

fn capacity(observed_at: i64) -> RiskCapacity {
    RiskCapacity {
        account_id: AccountId::new("account-1"),
        strategy_id: StrategyId::new("strategy-1"),
        observed_at,
        daily_realized_loss_pct: 0.5,
        equity_drawdown_pct: 1.0,
        remaining_account_risk_pct: 4.0,
        remaining_portfolio_risk_pct: 8.0,
        remaining_strategy_legs: 3,
    }
}

#[tokio::test]
async fn capacity_facts_are_append_only_and_latest_is_monotonic() {
    let (_database, store, pool) = test_store().await;
    let first = NewRiskCapacitySnapshot {
        capacity: capacity(100),
        recorded_at: 101,
    };
    assert_eq!(
        store
            .record_risk_capacity_snapshot(first.clone())
            .await
            .unwrap(),
        ProjectionWriteOutcome::Applied
    );
    assert_eq!(
        store
            .record_risk_capacity_snapshot(first.clone())
            .await
            .unwrap(),
        ProjectionWriteOutcome::Duplicate
    );

    let older = NewRiskCapacitySnapshot {
        capacity: capacity(90),
        recorded_at: 102,
    };
    assert_eq!(
        store.record_risk_capacity_snapshot(older).await.unwrap(),
        ProjectionWriteOutcome::FactAppendedProjectionIgnored
    );
    let latest = store
        .get_latest_risk_capacity_snapshot(
            &AccountId::new("account-1"),
            &StrategyId::new("strategy-1"),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.capacity, first.capacity);
    assert_eq!(latest.recorded_at, first.recorded_at);

    assert!(sqlx::query(
        "UPDATE risk_capacity_snapshots SET recorded_at = 103 WHERE observed_at = 100"
    )
    .execute(&pool)
    .await
    .is_err());
    assert!(
        sqlx::query("DELETE FROM risk_capacity_snapshots WHERE observed_at = 100")
            .execute(&pool)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn same_capacity_watermark_with_payload_or_recording_time_drift_conflicts() {
    let (_database, store, _pool) = test_store().await;
    let original = NewRiskCapacitySnapshot {
        capacity: capacity(100),
        recorded_at: 101,
    };
    store
        .record_risk_capacity_snapshot(original.clone())
        .await
        .unwrap();

    let mut drifted = original.clone();
    drifted.capacity.remaining_account_risk_pct = 3.0;
    assert!(matches!(
        store.record_risk_capacity_snapshot(drifted).await,
        Err(StoreError::ObservationConflict { .. })
    ));
    let mut time_drifted = original;
    time_drifted.recorded_at = 102;
    assert!(matches!(
        store.record_risk_capacity_snapshot(time_drifted).await,
        Err(StoreError::ObservationConflict { .. })
    ));
}

#[tokio::test]
async fn invalid_or_corrupt_capacity_fails_closed() {
    let (_database, store, pool) = test_store().await;
    let mut invalid = capacity(100);
    invalid.remaining_portfolio_risk_pct = f64::NAN;
    assert!(matches!(
        store
            .record_risk_capacity_snapshot(NewRiskCapacitySnapshot {
                capacity: invalid,
                recorded_at: 101,
            })
            .await,
        Err(StoreError::InvalidRecord { .. })
    ));
    assert!(matches!(
        store
            .record_risk_capacity_snapshot(NewRiskCapacitySnapshot {
                capacity: capacity(100),
                recorded_at: 99,
            })
            .await,
        Err(StoreError::InvalidRecord { .. })
    ));

    store
        .record_risk_capacity_snapshot(NewRiskCapacitySnapshot {
            capacity: capacity(100),
            recorded_at: 101,
        })
        .await
        .unwrap();
    sqlx::query(
        "UPDATE risk_capacity_snapshots_latest SET payload_hash = ?\
         WHERE account_id = 'account-1' AND strategy_id = 'strategy-1'",
    )
    .bind("0".repeat(64))
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        store
            .get_latest_risk_capacity_snapshot(
                &AccountId::new("account-1"),
                &StrategyId::new("strategy-1"),
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));
}
