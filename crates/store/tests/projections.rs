use sinan_store::{
    AuthorizedAccountScope, CanonicalJson, CoreEventMetadata, NewCoreEvent, ProjectionWriteOutcome,
    StoreError,
};
use sinan_types::{
    AccountId, AccountSnapshot, BrokerOrderId, MarketBar, MarketSnapshot, MessageId, OrderSnapshot,
    OrderSnapshotStatus, OrderType, PositionId, PositionSide, PositionSnapshot, SymbolCode,
    SymbolMetadataSnapshot, SymbolTradeMode, TimeframeCode,
};
mod common;

use common::test_store;

fn metadata(sequence: u64, event_type: &str, account_id: &str, event_at: i64) -> CoreEventMetadata {
    CoreEventMetadata {
        event_id: format!("event_{sequence}"),
        event_type: event_type.to_owned(),
        aggregate_type: event_type.to_owned(),
        aggregate_id: account_id.to_owned(),
        message_id: Some(MessageId::from(format!("message_{sequence}"))),
        schema_version: "ecp.v1.0".to_owned(),
        correlation_id: None,
        causation_id: None,
        account_id: Some(AccountId::from(account_id)),
        client_id: None,
        terminal_id: None,
        strategy_id: None,
        intent_id: None,
        plan_id: None,
        leg_id: None,
        command_id: None,
        idempotency_key: None,
        event_at,
        received_at: event_at + 10,
        created_at: event_at + 10,
        source: "projection-test".to_owned(),
    }
}

fn account(account_id: &str, equity: f64, observed_at: i64) -> AccountSnapshot {
    AccountSnapshot {
        account_id: AccountId::from(account_id),
        balance: equity - 100.0,
        equity,
        margin: 50.0,
        free_margin: equity - 50.0,
        currency: "USD".to_owned(),
        observed_at,
    }
}

fn position(account_id: &str, position_id: &str, observed_at: i64) -> PositionSnapshot {
    PositionSnapshot {
        account_id: AccountId::from(account_id),
        symbol: SymbolCode::from("EURUSD"),
        position_id: PositionId::from(position_id),
        side: PositionSide::Buy,
        lots: 0.25,
        open_price: 1.1,
        sl: Some(1.09),
        tp: Some(1.12),
        floating_pnl: 12.0,
        observed_at,
    }
}

fn order(account_id: &str, order_id: &str, observed_at: i64) -> OrderSnapshot {
    OrderSnapshot {
        account_id: AccountId::from(account_id),
        terminal_id: None,
        client_id: None,
        symbol: SymbolCode::from("EURUSD"),
        broker_symbol: Some("EURUSD.a".to_owned()),
        broker_order_id: BrokerOrderId::from(order_id),
        position_ticket: None,
        command_id: None,
        plan_id: None,
        leg_id: None,
        idempotency_key: None,
        side: PositionSide::Buy,
        order_type: OrderType::Limit,
        status: OrderSnapshotStatus::Placed,
        requested_lots: 0.25,
        filled_lots: 0.0,
        remaining_lots: 0.25,
        price: Some(1.1),
        sl: Some(1.09),
        tp: Some(1.12),
        created_at: Some(observed_at - 100),
        updated_at: Some(observed_at),
        observed_at,
    }
}

fn symbol(account_id: &str, observed_at: i64) -> SymbolMetadataSnapshot {
    SymbolMetadataSnapshot {
        account_id: AccountId::from(account_id),
        symbol: SymbolCode::from("EURUSD"),
        broker_symbol: "EURUSD.a".to_owned(),
        digits: 5,
        point: 0.00001,
        tick_size: 0.00001,
        tick_value_loss: 1.0,
        contract_size: 100_000.0,
        volume_min: 0.01,
        volume_max: 100.0,
        volume_step: 0.01,
        stops_level_points: 10,
        freeze_level_points: 5,
        margin_initial: Some(1_000.0),
        margin_maintenance: Some(500.0),
        trade_mode: SymbolTradeMode::Full,
        observed_at,
    }
}

fn bar(timestamp: i64) -> MarketBar {
    MarketBar {
        symbol: SymbolCode::from("EURUSD"),
        timeframe: TimeframeCode::from("M1"),
        timestamp,
        open: 1.1,
        high: 1.11,
        low: 1.09,
        close: 1.105,
        volume: 42.0,
    }
}

#[tokio::test]
async fn durable_ingest_rolls_back_fact_when_projection_write_fails() {
    let (_database, store, pool) = test_store().await;
    sqlx::query("DROP TABLE account_snapshots_latest")
        .execute(&pool)
        .await
        .expect("test fixture should remove projection table");

    let error = store
        .ingest_account_snapshot(
            metadata(1, "account.snapshot", "account_a", 100),
            &account("account_a", 1_000.0, 100),
        )
        .await
        .expect_err("projection failure should fail the whole handler transaction");
    assert!(matches!(error, StoreError::Database(_)));

    let fact_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM core_events")
        .fetch_one(&pool)
        .await
        .expect("fact count should be readable");
    assert_eq!(fact_count, 0);
}

#[tokio::test]
async fn duplicate_fact_retry_restores_a_missing_projection_from_the_stored_fact() {
    let (_database, store, pool) = test_store().await;
    let snapshot = account("account_a", 1_000.0, 100);
    let original_metadata = metadata(1, "account.snapshot", "account_a", 100);
    store
        .append_core_event(NewCoreEvent {
            metadata: original_metadata.clone(),
            payload: CanonicalJson::from_serializable(&snapshot).unwrap(),
        })
        .await
        .unwrap();

    let projection_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_snapshots_latest")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(projection_count, 0);

    let mut retry_metadata = original_metadata.clone();
    retry_metadata.received_at += 1_000;
    retry_metadata.created_at += 1_000;
    assert_eq!(
        store
            .ingest_account_snapshot(retry_metadata, &snapshot)
            .await
            .unwrap(),
        ProjectionWriteOutcome::Duplicate
    );

    let state = store
        .load_latest_state(&AuthorizedAccountScope::new([AccountId::from("account_a")]))
        .await
        .unwrap();
    assert_eq!(state.accounts, vec![snapshot]);
    let updated_at: i64 = sqlx::query_scalar(
        "SELECT updated_at FROM account_snapshots_latest WHERE account_id = 'account_a'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(updated_at, original_metadata.received_at);
}

#[tokio::test]
async fn account_projection_handles_newer_older_duplicate_and_equal_observations() {
    let (_database, store, pool) = test_store().await;
    let first_metadata = metadata(1, "account.snapshot", "account_a", 100);
    let first = account("account_a", 1_000.0, 100);
    assert_eq!(
        store
            .ingest_account_snapshot(first_metadata.clone(), &first)
            .await
            .unwrap(),
        ProjectionWriteOutcome::Applied
    );
    assert_eq!(
        store
            .ingest_account_snapshot(first_metadata, &first)
            .await
            .unwrap(),
        ProjectionWriteOutcome::Duplicate
    );

    let newest = account("account_a", 1_200.0, 300);
    assert_eq!(
        store
            .ingest_account_snapshot(metadata(2, "account.snapshot", "account_a", 300), &newest,)
            .await
            .unwrap(),
        ProjectionWriteOutcome::Applied
    );

    let older = account("account_a", 900.0, 200);
    assert_eq!(
        store
            .ingest_account_snapshot(metadata(3, "account.snapshot", "account_a", 200), &older,)
            .await
            .unwrap(),
        ProjectionWriteOutcome::FactAppendedProjectionIgnored
    );
    assert_eq!(
        store
            .ingest_account_snapshot(metadata(4, "account.snapshot", "account_a", 300), &newest,)
            .await
            .unwrap(),
        ProjectionWriteOutcome::FactAppendedProjectionUnchanged
    );

    let conflicting = account("account_a", 1_201.0, 300);
    let error = store
        .ingest_account_snapshot(
            metadata(5, "account.snapshot", "account_a", 300),
            &conflicting,
        )
        .await
        .expect_err("same observation time with different data must conflict");
    assert!(matches!(
        error,
        StoreError::ObservationConflict {
            entity: "account_snapshot",
            observed_at: 300,
            ..
        }
    ));

    let state = store
        .load_latest_state(&AuthorizedAccountScope::new([AccountId::from("account_a")]))
        .await
        .unwrap();
    assert_eq!(state.accounts, vec![newest]);

    let fact_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM core_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(fact_count, 4, "duplicate and conflict must not add facts");
}

#[tokio::test]
async fn state_read_is_account_scoped_and_empty_scope_authorizes_nothing() {
    let (_database, store, _) = test_store().await;

    for (offset, account_id) in [(0, "account_a"), (10, "account_b")] {
        store
            .ingest_account_snapshot(
                metadata(1 + offset, "account.snapshot", account_id, 100),
                &account(account_id, 1_000.0 + offset as f64, 100),
            )
            .await
            .unwrap();
        store
            .ingest_position_snapshot(
                metadata(2 + offset, "position.snapshot", account_id, 101),
                &position(account_id, &format!("position_{account_id}"), 101),
            )
            .await
            .unwrap();
        store
            .ingest_order_snapshot(
                metadata(3 + offset, "order.snapshot", account_id, 102),
                &order(account_id, &format!("order_{account_id}"), 102),
            )
            .await
            .unwrap();
        store
            .ingest_symbol_metadata(
                metadata(4 + offset, "symbol.metadata", account_id, 103),
                &symbol(account_id, 103),
            )
            .await
            .unwrap();
        store
            .update_market_snapshot(
                &AccountId::from(account_id),
                &MarketSnapshot {
                    symbol: SymbolCode::from("EURUSD"),
                    broker_symbol: Some("EURUSD.a".to_owned()),
                    bid: 1.1,
                    ask: 1.1002,
                    spread: 0.0002,
                    observed_at: 104,
                },
                105,
            )
            .await
            .unwrap();
    }

    let empty = store
        .load_latest_state(&AuthorizedAccountScope::empty())
        .await
        .unwrap();
    assert!(empty.accounts.is_empty());
    assert!(empty.positions.is_empty());
    assert!(empty.orders.is_empty());
    assert!(empty.symbols.is_empty());
    assert!(empty.markets.is_empty());

    let account_a = AccountId::from("account_a");
    let state = store
        .load_latest_state(&AuthorizedAccountScope::new([account_a.clone()]))
        .await
        .unwrap();
    assert_eq!(state.accounts.len(), 1);
    assert_eq!(state.positions.len(), 1);
    assert_eq!(state.orders.len(), 1);
    assert_eq!(state.symbols.len(), 1);
    assert_eq!(state.markets.len(), 1);
    assert_eq!(state.accounts[0].account_id, account_a);
    assert!(state
        .positions
        .iter()
        .all(|value| value.account_id.as_str() == "account_a"));
    assert!(state
        .orders
        .iter()
        .all(|value| value.account_id.as_str() == "account_a"));
    assert!(state
        .symbols
        .iter()
        .all(|value| value.account_id.as_str() == "account_a"));
    assert!(state
        .markets
        .iter()
        .all(|value| value.account_id.as_str() == "account_a"));

    let both = store
        .load_latest_state(&AuthorizedAccountScope::new([
            AccountId::from("account_b"),
            AccountId::from("account_a"),
        ]))
        .await
        .unwrap();
    assert_eq!(both.markets.len(), 2);
    assert_eq!(both.markets[0].account_id.as_str(), "account_a");
    assert_eq!(both.markets[1].account_id.as_str(), "account_b");
    assert_eq!(both.markets[0].snapshot.symbol.as_str(), "EURUSD");
    assert_eq!(both.markets[1].snapshot.symbol.as_str(), "EURUSD");
}

#[tokio::test]
async fn state_read_rejects_payload_identity_that_disagrees_with_projection_key() {
    let (_database, store, pool) = test_store().await;
    store
        .ingest_account_snapshot(
            metadata(1, "account.snapshot", "account_a", 100),
            &account("account_a", 1_000.0, 100),
        )
        .await
        .unwrap();

    let wrong_payload =
        CanonicalJson::from_serializable(&account("account_b", 1_000.0, 100)).unwrap();
    sqlx::query(
        "UPDATE account_snapshots_latest SET payload_json = ?, payload_hash = ? \
         WHERE account_id = 'account_a'",
    )
    .bind(wrong_payload.as_str())
    .bind(wrong_payload.sha256_hex())
    .execute(&pool)
    .await
    .unwrap();

    let error = store
        .load_latest_state(&AuthorizedAccountScope::new([AccountId::from("account_a")]))
        .await
        .expect_err("projection key mismatch must be reported as corruption");
    assert!(matches!(
        error,
        StoreError::CorruptData {
            entity: "account_snapshot",
            ..
        }
    ));
}

#[tokio::test]
async fn state_read_rejects_observed_at_that_disagrees_with_payload() {
    let (_database, store, pool) = test_store().await;
    store
        .ingest_account_snapshot(
            metadata(1, "account.snapshot", "account_a", 100),
            &account("account_a", 1_000.0, 100),
        )
        .await
        .unwrap();

    sqlx::query(
        "UPDATE account_snapshots_latest SET observed_at = 101 WHERE account_id = 'account_a'",
    )
    .execute(&pool)
    .await
    .unwrap();

    let error = store
        .load_latest_state(&AuthorizedAccountScope::new([AccountId::from("account_a")]))
        .await
        .expect_err("projection observation time mismatch must be reported as corruption");
    assert!(matches!(
        error,
        StoreError::CorruptData {
            entity: "account_snapshot",
            ..
        }
    ));
}

#[tokio::test]
async fn rebuild_restores_durable_projections_and_leaves_latest_only_markets() {
    let (_database, store, pool) = test_store().await;
    let account_id = AccountId::from("account_a");

    store
        .ingest_account_snapshot(
            metadata(1, "account.snapshot", "account_a", 100),
            &account("account_a", 1_000.0, 100),
        )
        .await
        .unwrap();
    store
        .ingest_account_snapshot(
            metadata(2, "account.snapshot", "account_a", 200),
            &account("account_a", 1_100.0, 200),
        )
        .await
        .unwrap();
    let mut stale_metadata = metadata(3, "account.snapshot", "account_a", 150);
    stale_metadata.received_at = 300;
    stale_metadata.created_at = 300;
    store
        .ingest_account_snapshot(stale_metadata, &account("account_a", 900.0, 150))
        .await
        .unwrap();
    store
        .ingest_position_snapshot(
            metadata(4, "position.snapshot", "account_a", 200),
            &position("account_a", "position_1", 200),
        )
        .await
        .unwrap();
    store
        .ingest_order_snapshot(
            metadata(5, "order.snapshot", "account_a", 200),
            &order("account_a", "order_1", 200),
        )
        .await
        .unwrap();
    store
        .ingest_symbol_metadata(
            metadata(6, "symbol.metadata", "account_a", 200),
            &symbol("account_a", 200),
        )
        .await
        .unwrap();
    store
        .ingest_market_bar(metadata(7, "market.bar", "account_a", 200), &bar(200))
        .await
        .unwrap();
    store
        .update_market_snapshot(
            &account_id,
            &MarketSnapshot {
                symbol: SymbolCode::from("EURUSD"),
                broker_symbol: Some("EURUSD.a".to_owned()),
                bid: 1.1,
                ask: 1.1002,
                spread: 0.0002,
                observed_at: 210,
            },
            211,
        )
        .await
        .unwrap();

    let expected = store
        .load_latest_state(&AuthorizedAccountScope::new([account_id.clone()]))
        .await
        .unwrap();
    for table in [
        "account_snapshots_latest",
        "symbol_metadata_latest",
        "position_snapshots_latest",
        "order_snapshots_latest",
        "market_bars",
    ] {
        sqlx::query(&format!("DELETE FROM {table}"))
            .execute(&pool)
            .await
            .unwrap();
    }

    let report = store.rebuild_ingest_projections().await.unwrap();
    assert_eq!(report.replayed_facts, 7);
    assert_eq!(report.ignored_older, 1);
    let rebuilt_bar_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM market_bars \
         WHERE account_id = 'account_a' AND symbol = 'EURUSD' AND timeframe = 'M1' \
           AND timestamp = 200",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(rebuilt_bar_count, 1);
    let rebuilt = store
        .load_latest_state(&AuthorizedAccountScope::new([account_id.clone()]))
        .await
        .unwrap();
    assert_eq!(rebuilt, expected);
    assert_eq!(
        rebuilt.markets.len(),
        1,
        "tick-only projection is untouched"
    );

    let second_report = store.rebuild_ingest_projections().await.unwrap();
    assert_eq!(second_report, report);
    let rebuilt_twice = store
        .load_latest_state(&AuthorizedAccountScope::new([account_id]))
        .await
        .unwrap();
    assert_eq!(rebuilt_twice, expected);
}

#[tokio::test]
async fn failed_rebuild_rolls_back_and_preserves_existing_projection() {
    let (_database, store, _) = test_store().await;
    let account_id = AccountId::from("account_a");
    let expected = account("account_a", 1_000.0, 100);
    store
        .ingest_account_snapshot(metadata(1, "account.snapshot", "account_a", 100), &expected)
        .await
        .unwrap();

    let malformed = CanonicalJson::from_serializable(&serde_json::json!({
        "account_id": "account_a",
        "observed_at": 200,
        "not_an_account_snapshot": true
    }))
    .unwrap();
    store
        .append_core_event(NewCoreEvent {
            metadata: metadata(2, "account.snapshot", "account_a", 200),
            payload: malformed,
        })
        .await
        .unwrap();

    let error = store
        .rebuild_ingest_projections()
        .await
        .expect_err("malformed durable fact must abort rebuild");
    assert!(matches!(error, StoreError::CorruptData { .. }));

    let state = store
        .load_latest_state(&AuthorizedAccountScope::new([account_id]))
        .await
        .unwrap();
    assert_eq!(state.accounts, vec![expected]);
}
