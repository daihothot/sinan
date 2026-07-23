use sinan_store::migrate;
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};

const HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

async fn migrated_pool() -> SqlitePool {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("in-memory SQLite should open");
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&pool)
        .await
        .expect("foreign keys should be enabled");
    migrate(&pool).await.expect("schema should migrate");
    pool
}

async fn insert_intent_and_risk(pool: &SqlitePool) {
    sqlx::query(
        "INSERT INTO trade_intents (\
             intent_id, decision_id, strategy_id, account_id, symbol, action, status,\
             decision_timestamp, requested_at, signal_expires_at, idempotency_key, payload_json, payload_hash,\
             created_at, updated_at\
         ) VALUES ('intent-1', 'decision-1', 'strategy-1', 'account-1', 'EURUSD',\
                   'BUY', 'ACCEPTED', 0, 1, 10, 'intent-idem-1', '{}', ?, 1, 1)",
    )
    .bind(HASH)
    .execute(pool)
    .await
    .expect("intent fixture should insert");

    sqlx::query(
        "INSERT INTO risk_results (\
             risk_id, intent_id, account_id, approved, reason, snapshot_age_ms,\
             symbol_metadata_age_ms, evaluated_at, valid_until, payload_json, payload_hash\
         ) VALUES ('risk-1', 'intent-1', 'account-1', 1, 'OK', 0, 0, 2, 9, '{}', ?)",
    )
    .bind(HASH)
    .execute(pool)
    .await
    .expect("risk fixture should insert");
}

async fn insert_plan_leg_and_command(pool: &SqlitePool) {
    sqlx::query(
        "INSERT INTO execution_plans (\
             plan_id, risk_id, intent_id, account_id, strategy_id, status, mode,\
             failure_policy, payload_json, payload_hash, created_at, updated_at\
         ) VALUES ('plan-1', 'risk-1', 'intent-1', 'account-1', 'strategy-1',\
                   'PENDING', 'sequential', 'cancel_all', '{}', ?, 3, 3)",
    )
    .bind(HASH)
    .execute(pool)
    .await
    .expect("plan fixture should insert");

    sqlx::query(
        "INSERT INTO execution_legs (\
             leg_id, plan_id, symbol, action, status, payload_json, payload_hash, updated_at\
         ) VALUES ('leg-1', 'plan-1', 'EURUSD', 'BUY', 'PENDING', '{}', ?, 3)",
    )
    .bind(HASH)
    .execute(pool)
    .await
    .expect("leg fixture should insert");

    sqlx::query(
        "INSERT INTO execution_commands (\
             command_id, risk_id, plan_id, leg_id, account_id, symbol, action, expires_at,\
             idempotency_key, payload_json, payload_hash, hmac, created_at\
         ) VALUES ('command-1', 'risk-1', 'plan-1', 'leg-1', 'account-1', 'EURUSD',\
                   'BUY', 9, 'command-idem-1', '{}', ?, ?, 3)",
    )
    .bind(HASH)
    .bind(HASH)
    .execute(pool)
    .await
    .expect("command fixture should insert");
}

#[tokio::test]
async fn migration_creates_all_state_store_tables_at_version_ten() {
    let pool = migrated_pool().await;
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_schema \
         WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name != 'schema_migrations' \
         ORDER BY name",
    )
    .fetch_all(&pool)
    .await
    .expect("table names should be readable");
    let user_version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&pool)
        .await
        .expect("user_version should be readable");
    let migration_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM schema_migrations")
        .fetch_one(&pool)
        .await
        .expect("migration count should be readable");

    assert_eq!(tables.len(), 33);
    assert_eq!(
        tables,
        [
            "account_reconciliation_checkpoints",
            "account_snapshots_latest",
            "circuit_breaker_snapshots",
            "command_delivery_attempts",
            "core_events",
            "deadletter_events",
            "event_stream_log",
            "execution_client_sessions",
            "execution_command_states",
            "execution_commands",
            "execution_events",
            "execution_legs",
            "execution_plans",
            "inbound_admissions",
            "inbound_rejections",
            "market_bars",
            "market_snapshots",
            "order_snapshots_latest",
            "outbound_delivery_work",
            "outbound_spool",
            "position_snapshots_latest",
            "reconciliation_order_set_members",
            "reconciliation_position_set_members",
            "reconciliation_runs",
            "risk_capacity_snapshots",
            "risk_capacity_snapshots_latest",
            "risk_results",
            "session_resume_admissions",
            "symbol_metadata_latest",
            "system_events",
            "trade_intents",
            "wire_inbox",
            "wire_outbox",
        ]
    );
    assert_eq!(migration_count, 10);
    assert_eq!(user_version, 10);
}

#[tokio::test]
async fn inbound_raw_payload_length_is_nullable_non_negative_and_immutable() {
    let pool = migrated_pool().await;
    let column: (String, String, i64) = sqlx::query_as(
        "SELECT name, type, \"notnull\" FROM pragma_table_info('inbound_admissions') \
         WHERE name = 'raw_payload_length'",
    )
    .fetch_one(&pool)
    .await
    .expect("raw payload length column should exist");
    let trigger_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'trigger' \
         AND name = 'trg_inbound_admissions_raw_payload_length_immutable')",
    )
    .fetch_one(&pool)
    .await
    .expect("raw payload length trigger should be readable");

    assert_eq!(
        column,
        ("raw_payload_length".to_owned(), "INTEGER".to_owned(), 0)
    );
    assert!(trigger_exists);
    sqlx::query(
        "INSERT INTO execution_client_sessions (\
             session_id, client_id, account_id, platform, status, capabilities_json, connected_at, \
             last_heartbeat_at, last_time_sync_at, clock_sync_status, updated_at\
         ) VALUES ('session-raw-length', 'client-1', 'account-1', 'MT5', 'ACTIVE', '[]', \
                   10, 10, 10, 'SYNCED', 10)",
    )
    .execute(&pool)
    .await
    .expect("session fixture should insert");
    assert!(sqlx::query(
        "INSERT INTO inbound_admissions (\
             message_id, session_id, client_id, account_id, message_type, schema_version, \
             sequence, envelope_json, envelope_hash, raw_payload_length, received_at, status, \
             created_at, updated_at\
         ) VALUES ('message-invalid-length', 'session-raw-length', 'client-1', 'account-1', \
                   'market.tick', 'ecp.v1.0', 1, '{}', ?, -1, 10, 'PENDING', 10, 10)",
    )
    .bind(HASH)
    .execute(&pool)
    .await
    .is_err());
}

#[tokio::test]
async fn every_payload_json_column_has_a_payload_hash() {
    let pool = migrated_pool().await;
    let payload_tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_schema \
         WHERE type = 'table' AND sql LIKE '%payload_json%' ORDER BY name",
    )
    .fetch_all(&pool)
    .await
    .expect("payload table names should be readable");

    assert_eq!(
        payload_tables,
        [
            "account_snapshots_latest",
            "circuit_breaker_snapshots",
            "command_delivery_attempts",
            "core_events",
            "event_stream_log",
            "execution_commands",
            "execution_events",
            "execution_legs",
            "execution_plans",
            "market_bars",
            "market_snapshots",
            "order_snapshots_latest",
            "outbound_spool",
            "position_snapshots_latest",
            "reconciliation_order_set_members",
            "reconciliation_position_set_members",
            "reconciliation_runs",
            "risk_capacity_snapshots",
            "risk_capacity_snapshots_latest",
            "risk_results",
            "symbol_metadata_latest",
            "trade_intents",
            "wire_outbox",
        ]
    );

    for table in payload_tables {
        let columns: Vec<String> =
            sqlx::query_scalar(&format!("SELECT name FROM pragma_table_info('{table}')"))
                .fetch_all(&pool)
                .await
                .expect("payload table columns should be readable");
        if table == "command_delivery_attempts" {
            assert!(columns
                .iter()
                .any(|column| column == "request_payload_hash"));
        } else if table == "reconciliation_runs" {
            for hash in ["request_payload_hash", "result_payload_hash"] {
                assert!(columns.iter().any(|column| column == hash));
            }
        } else {
            assert!(columns.iter().any(|column| column == "payload_hash"));
        }
    }
}

#[tokio::test]
async fn schema_contains_required_query_indexes() {
    let pool = migrated_pool().await;
    let indexes: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_schema \
         WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex_%' ORDER BY name",
    )
    .fetch_all(&pool)
    .await
    .expect("index names should be readable");

    for required in [
        "idx_active_session_identity",
        "idx_command_delivery_attempts_command",
        "idx_core_events_account_time",
        "idx_core_events_aggregate_time",
        "idx_core_events_command",
        "idx_core_events_intent",
        "idx_core_events_message_id",
        "idx_core_events_type_time",
        "idx_event_stream_topic_time",
        "idx_event_stream_topic_sequence",
        "idx_event_stream_account_sequence",
        "idx_event_stream_created_sequence",
        "idx_execution_commands_idempotency",
        "idx_execution_events_command_time",
        "idx_outbound_delivery_work_due",
        "idx_outbound_spool_due",
        "idx_reconciliation_runs_account_status_time",
        "idx_risk_capacity_snapshots_account_strategy_time",
        "idx_trade_intents_idempotency",
        "idx_wire_inbox_session_sequence",
        "idx_wire_outbox_session_sequence",
    ] {
        assert!(
            indexes.iter().any(|index| index == required),
            "missing {required}"
        );
    }
}

#[tokio::test]
async fn status_action_mode_json_and_hash_checks_reject_invalid_values() {
    let pool = migrated_pool().await;

    let missing_decision_timestamp = sqlx::query(
        "INSERT INTO trade_intents (\
             intent_id, decision_id, strategy_id, account_id, symbol, action, status,\
             requested_at, signal_expires_at, idempotency_key, payload_json, payload_hash,\
             created_at, updated_at\
         ) VALUES ('intent-missing-decision-time', 'decision-1', 'strategy-1', 'account-1',\
                   'EURUSD', 'BUY', 'ACCEPTED', 1, 10, 'idem-missing-decision-time', '{}', ?, 1, 1)",
    )
    .bind(HASH)
    .execute(&pool)
    .await;
    assert!(missing_decision_timestamp.is_err());

    let decision_after_request = sqlx::query(
        "INSERT INTO trade_intents (\
             intent_id, decision_id, strategy_id, account_id, symbol, action, status,\
             decision_timestamp, requested_at, signal_expires_at, idempotency_key, payload_json,\
             payload_hash, created_at, updated_at\
         ) VALUES ('intent-late-decision', 'decision-1', 'strategy-1', 'account-1', 'EURUSD',\
                   'BUY', 'ACCEPTED', 2, 1, 10, 'idem-late-decision', '{}', ?, 1, 1)",
    )
    .bind(HASH)
    .execute(&pool)
    .await;
    assert!(decision_after_request.is_err());

    let invalid_action = sqlx::query(
        "INSERT INTO trade_intents (\
             intent_id, decision_id, strategy_id, account_id, symbol, action, status,\
             decision_timestamp, requested_at, signal_expires_at, idempotency_key, payload_json, payload_hash,\
             created_at, updated_at\
         ) VALUES ('intent-bad-action', 'decision-1', 'strategy-1', 'account-1', 'EURUSD',\
                   'SHORT', 'ACCEPTED', 0, 1, 10, 'idem-bad-action', '{}', ?, 1, 1)",
    )
    .bind(HASH)
    .execute(&pool)
    .await;
    assert!(invalid_action.is_err());

    let invalid_status = sqlx::query(
        "INSERT INTO execution_client_sessions (\
             session_id, client_id, account_id, platform, status, capabilities_json, connected_at\
         ) VALUES ('session-bad-status', 'client-1', 'account-1', 'MT5', 'READY', '[]', 1)",
    )
    .execute(&pool)
    .await;
    assert!(invalid_status.is_err());

    let invalid_platform = sqlx::query(
        "INSERT INTO execution_client_sessions (\
             session_id, client_id, account_id, platform, status, capabilities_json, connected_at\
         ) VALUES ('session-bad-platform', 'client-2', 'account-1', 'UNKNOWN', 'ACTIVE', '[]', 1)",
    )
    .execute(&pool)
    .await;
    assert!(invalid_platform.is_err());

    let zero_sequence = sqlx::query(
        "INSERT INTO wire_inbox (\
             message_id, message_type, sequence, received_at, status, payload_hash\
         ) VALUES ('message-zero-sequence', 'session.hello', 0, 1, 'RECEIVED', ?)",
    )
    .bind(HASH)
    .execute(&pool)
    .await;
    assert!(zero_sequence.is_err());

    let invalid_json = sqlx::query(
        "INSERT INTO trade_intents (\
             intent_id, decision_id, strategy_id, account_id, symbol, action, status,\
             decision_timestamp, requested_at, signal_expires_at, idempotency_key, payload_json, payload_hash,\
             created_at, updated_at\
         ) VALUES ('intent-bad-json', 'decision-1', 'strategy-1', 'account-1', 'EURUSD',\
                   'BUY', 'ACCEPTED', 0, 1, 10, 'idem-bad-json', '{', ?, 1, 1)",
    )
    .bind(HASH)
    .execute(&pool)
    .await;
    assert!(invalid_json.is_err());

    let invalid_hash = sqlx::query(
        "INSERT INTO trade_intents (\
             intent_id, decision_id, strategy_id, account_id, symbol, action, status,\
             decision_timestamp, requested_at, signal_expires_at, idempotency_key, payload_json, payload_hash,\
             created_at, updated_at\
         ) VALUES ('intent-bad-hash', 'decision-1', 'strategy-1', 'account-1', 'EURUSD',\
                   'BUY', 'ACCEPTED', 0, 1, 10, 'idem-bad-hash', '{}', 'ABC', 1, 1)",
    )
    .execute(&pool)
    .await;
    assert!(invalid_hash.is_err());

    let invalid_breaker_status = sqlx::query(
        "INSERT INTO circuit_breaker_snapshots (\
             scope, state_revision, schema_version, status, recovery_epoch, updated_at, \
             payload_json, payload_hash\
         ) VALUES ('GLOBAL', 1, 'circuit-breaker-state.v1', 'ACTIVE', 0, 1, '{}', ?)",
    )
    .bind(HASH)
    .execute(&pool)
    .await;
    assert!(invalid_breaker_status.is_err());

    insert_intent_and_risk(&pool).await;
    let invalid_mode = sqlx::query(
        "INSERT INTO execution_plans (\
             plan_id, risk_id, intent_id, account_id, strategy_id, status, mode,\
             failure_policy, payload_json, payload_hash, created_at, updated_at\
         ) VALUES ('plan-bad-mode', 'risk-1', 'intent-1', 'account-1', 'strategy-1',\
                   'PENDING', 'atomic', 'cancel_all', '{}', ?, 3, 3)",
    )
    .bind(HASH)
    .execute(&pool)
    .await;
    assert!(invalid_mode.is_err());
}

#[tokio::test]
async fn execution_definitions_are_immutable_but_status_projections_can_advance() {
    let pool = migrated_pool().await;
    insert_intent_and_risk(&pool).await;
    insert_plan_leg_and_command(&pool).await;

    sqlx::query(
        "UPDATE execution_plans SET status = 'RECONCILING', updated_at = 4 \
         WHERE plan_id = 'plan-1'",
    )
    .execute(&pool)
    .await
    .expect("plan materialized status should be mutable");
    sqlx::query("UPDATE execution_legs SET status = 'SENT', updated_at = 4 WHERE leg_id = 'leg-1'")
        .execute(&pool)
        .await
        .expect("leg materialized status should be mutable");

    let plan_definition_update = sqlx::query(
        "UPDATE execution_plans SET strategy_id = strategy_id WHERE plan_id = 'plan-1'",
    )
    .execute(&pool)
    .await;
    assert!(plan_definition_update.is_err());
    let leg_definition_update =
        sqlx::query("UPDATE execution_legs SET payload_json = payload_json WHERE leg_id = 'leg-1'")
            .execute(&pool)
            .await;
    assert!(leg_definition_update.is_err());

    assert!(
        sqlx::query("DELETE FROM execution_legs WHERE leg_id = 'leg-1'")
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM execution_plans WHERE plan_id = 'plan-1'")
            .execute(&pool)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn foreign_keys_protect_projections_without_blocking_execution_facts() {
    let pool = migrated_pool().await;
    let foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(&pool)
        .await
        .expect("foreign key mode should be readable");
    assert_eq!(foreign_keys, 1);

    let missing_session = sqlx::query(
        "INSERT INTO wire_inbox (\
             message_id, session_id, message_type, sequence, received_at, status, payload_hash\
         ) VALUES ('message-1', 'missing-session', 'heartbeat', 1, 1, 'RECEIVED', ?)",
    )
    .bind(HASH)
    .execute(&pool)
    .await;
    assert!(missing_session.is_err());

    let missing_intent = sqlx::query(
        "INSERT INTO risk_results (\
             risk_id, intent_id, account_id, approved, reason, snapshot_age_ms,\
             symbol_metadata_age_ms, evaluated_at, valid_until, payload_json, payload_hash\
         ) VALUES ('risk-orphan', 'intent-missing', 'account-1', 0, 'BAD_REQUEST',\
                   0, 0, 1, 2, '{}', ?)",
    )
    .bind(HASH)
    .execute(&pool)
    .await;
    assert!(missing_intent.is_err());

    sqlx::query(
        "INSERT INTO execution_events (\
             execution_id, command_id, account_id, status, event_at, payload_json,\
             payload_hash, created_at\
         ) VALUES ('execution-orphan', 'command-missing', 'account-1', 'FAILED', 1, '{}', ?, 1)",
    )
    .bind(HASH)
    .execute(&pool)
    .await
    .expect("append-only execution facts must not depend on command projections");
}

#[tokio::test]
async fn active_session_identity_treats_null_terminal_as_a_real_identity_component() {
    let pool = migrated_pool().await;

    sqlx::query(
        "INSERT INTO execution_client_sessions (\
             session_id, client_id, account_id, terminal_id, platform, status,\
             capabilities_json, connected_at\
         ) VALUES ('session-1', 'client-1', 'account-1', NULL, 'MT5', 'ACTIVE', '[]', 1)",
    )
    .execute(&pool)
    .await
    .expect("first active session should insert");

    let duplicate_active = sqlx::query(
        "INSERT INTO execution_client_sessions (\
             session_id, client_id, account_id, terminal_id, platform, status,\
             capabilities_json, connected_at\
         ) VALUES ('session-2', 'client-1', 'account-1', NULL, 'MT5', 'ACTIVE', '[]', 2)",
    )
    .execute(&pool)
    .await;
    assert!(duplicate_active.is_err());

    sqlx::query(
        "INSERT INTO execution_client_sessions (\
             session_id, client_id, account_id, terminal_id, platform, status,\
             capabilities_json, connected_at\
         ) VALUES ('session-3', 'client-1', 'account-1', NULL, 'MT5', 'DISCONNECTED', '[]', 3)",
    )
    .execute(&pool)
    .await
    .expect("inactive duplicate identity should insert");
}

#[tokio::test]
async fn append_only_facts_reject_mutation_while_replay_log_allows_retention() {
    let pool = migrated_pool().await;
    insert_intent_and_risk(&pool).await;
    insert_plan_leg_and_command(&pool).await;

    sqlx::query(
        "INSERT INTO circuit_breaker_snapshots (\
             scope, state_revision, schema_version, status, recovery_epoch, updated_at, \
             payload_json, payload_hash\
         ) VALUES ('GLOBAL', 1, 'circuit-breaker-state.v1', 'CLOSED', 0, 1, '{}', ?)",
    )
    .bind(HASH)
    .execute(&pool)
    .await
    .expect("circuit-breaker snapshot fixture should insert");

    sqlx::query(
        "INSERT INTO core_events (\
             event_id, event_type, aggregate_type, aggregate_id, schema_version, event_at,\
             received_at, created_at, source, payload_json, payload_hash\
         ) VALUES ('event-1', 'market.bar', 'market', 'EURUSD', 'ecp.v1.0', 1, 1, 1,\
                   'test', '{}', ?)",
    )
    .bind(HASH)
    .execute(&pool)
    .await
    .expect("core event should insert");
    sqlx::query(
        "INSERT INTO deadletter_events (deadletter_id, reason, received_at, created_at)\
         VALUES ('deadletter-1', 'DECODE_FAILED', 1, 1)",
    )
    .execute(&pool)
    .await
    .expect("deadletter event should insert");
    sqlx::query(
        "INSERT INTO system_events (\
             system_event_id, type, severity, component, message, timestamp, created_at\
         ) VALUES ('system-1', 'STATE_STORE_RESTORED', 'INFO', 'store', 'restored', 1, 1)",
    )
    .execute(&pool)
    .await
    .expect("system event should insert");
    sqlx::query(
        "INSERT INTO execution_events (\
             execution_id, command_id, account_id, status, event_at, payload_json,\
             payload_hash, created_at\
         ) VALUES ('execution-1', 'command-1', 'account-1', 'ACCEPTED', 4, '{}', ?, 4)",
    )
    .bind(HASH)
    .execute(&pool)
    .await
    .expect("execution event should insert");
    sqlx::query(
        "INSERT INTO event_stream_log (\
             event_id, topic, event_type, payload_json, payload_hash, created_at\
         ) VALUES ('stream-1', 'system.event', 'STATE_STORE_RESTORED', '{}', ?, 1)",
    )
    .bind(HASH)
    .execute(&pool)
    .await
    .expect("stream event should insert");

    for table in [
        "core_events",
        "circuit_breaker_snapshots",
        "deadletter_events",
        "system_events",
        "risk_results",
        "execution_commands",
        "execution_events",
    ] {
        let update = sqlx::query(&format!("UPDATE {table} SET rowid = rowid"))
            .execute(&pool)
            .await;
        assert!(update.is_err(), "{table} unexpectedly allowed UPDATE");

        let delete = sqlx::query(&format!("DELETE FROM {table}"))
            .execute(&pool)
            .await;
        assert!(delete.is_err(), "{table} unexpectedly allowed DELETE");
    }

    let stream_update = sqlx::query("UPDATE event_stream_log SET rowid = rowid")
        .execute(&pool)
        .await;
    assert!(stream_update.is_err());

    sqlx::query("DELETE FROM event_stream_log WHERE event_id = 'stream-1'")
        .execute(&pool)
        .await
        .expect("bounded replay retention should be able to delete old entries");
}
