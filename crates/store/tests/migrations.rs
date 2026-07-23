use sinan_store::{migrate, Migration, MigrationError, Migrator};
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};

const INIT_SQL: &str = include_str!("../migrations/V0001__init.sql");
const STATE_STORE_SQL: &str = include_str!("../migrations/V0002__state_store_schema.sql");
const EXECUTION_DURABILITY_SQL: &str =
    include_str!("../migrations/V0003__execution_durability.sql");
const RECONCILIATION_DURABILITY_SQL: &str =
    include_str!("../migrations/V0004__reconciliation_durability.sql");
const GATEWAY_DELIVERY_DURABILITY_SQL: &str =
    include_str!("../migrations/V0005__gateway_delivery_durability.sql");
const EVENT_STREAM_SEQUENCE_SQL: &str =
    include_str!("../migrations/V0006__event_stream_sequence.sql");
const INBOUND_DURABILITY_SQL: &str = include_str!("../migrations/V0007__inbound_durability.sql");
const RISK_WORKFLOW_INPUTS_SQL: &str =
    include_str!("../migrations/V0008__risk_workflow_inputs.sql");
const OUTBOUND_DELIVERY_WORK_SQL: &str =
    include_str!("../migrations/V0009__outbound_delivery_work.sql");
const INBOUND_RAW_PAYLOAD_LENGTH_SQL: &str =
    include_str!("../migrations/V0010__inbound_raw_payload_length.sql");

fn embedded_migrations() -> [Migration; 10] {
    [
        Migration::new(1, "init", INIT_SQL),
        Migration::new(2, "state_store_schema", STATE_STORE_SQL),
        Migration::new(3, "execution_durability", EXECUTION_DURABILITY_SQL),
        Migration::new(
            4,
            "reconciliation_durability",
            RECONCILIATION_DURABILITY_SQL,
        ),
        Migration::new(
            5,
            "gateway_delivery_durability",
            GATEWAY_DELIVERY_DURABILITY_SQL,
        ),
        Migration::new(6, "event_stream_sequence", EVENT_STREAM_SEQUENCE_SQL),
        Migration::new(7, "inbound_durability", INBOUND_DURABILITY_SQL),
        Migration::new(8, "risk_workflow_inputs", RISK_WORKFLOW_INPUTS_SQL),
        Migration::new(9, "outbound_delivery_work", OUTBOUND_DELIVERY_WORK_SQL),
        Migration::new(
            10,
            "inbound_raw_payload_length",
            INBOUND_RAW_PAYLOAD_LENGTH_SQL,
        ),
    ]
}

async fn memory_pool() -> SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("in-memory SQLite should open")
}

#[tokio::test]
async fn first_run_applies_all_embedded_migrations() {
    let pool = memory_pool().await;

    migrate(&pool).await.expect("migration should succeed");

    let rows: Vec<(i64, String, String, i64)> = sqlx::query_as(
        "SELECT version, name, checksum, applied_at FROM schema_migrations ORDER BY version",
    )
    .fetch_all(&pool)
    .await
    .expect("migration records should exist");
    let user_version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&pool)
        .await
        .expect("user_version should be readable");

    assert_eq!(rows.len(), 10);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1, "init");
    assert_eq!(rows[0].2, Migration::new(1, "init", INIT_SQL).checksum());
    assert!(rows[0].3 > 0);
    assert_eq!(rows[1].0, 2);
    assert_eq!(rows[1].1, "state_store_schema");
    assert_eq!(
        rows[1].2,
        Migration::new(2, "state_store_schema", STATE_STORE_SQL).checksum()
    );
    assert!(rows[1].3 > 0);
    assert_eq!(rows[2].0, 3);
    assert_eq!(rows[2].1, "execution_durability");
    assert_eq!(
        rows[2].2,
        Migration::new(3, "execution_durability", EXECUTION_DURABILITY_SQL).checksum()
    );
    assert!(rows[2].3 > 0);
    assert_eq!(rows[3].0, 4);
    assert_eq!(rows[3].1, "reconciliation_durability");
    assert_eq!(
        rows[3].2,
        Migration::new(
            4,
            "reconciliation_durability",
            RECONCILIATION_DURABILITY_SQL
        )
        .checksum()
    );
    assert!(rows[3].3 > 0);
    assert_eq!(rows[4].0, 5);
    assert_eq!(rows[4].1, "gateway_delivery_durability");
    assert_eq!(
        rows[4].2,
        Migration::new(
            5,
            "gateway_delivery_durability",
            GATEWAY_DELIVERY_DURABILITY_SQL
        )
        .checksum()
    );
    assert!(rows[4].3 > 0);
    assert_eq!(rows[5].0, 6);
    assert_eq!(rows[5].1, "event_stream_sequence");
    assert_eq!(
        rows[5].2,
        Migration::new(6, "event_stream_sequence", EVENT_STREAM_SEQUENCE_SQL).checksum()
    );
    assert!(rows[5].3 > 0);
    assert_eq!(rows[6].0, 7);
    assert_eq!(rows[6].1, "inbound_durability");
    assert_eq!(
        rows[6].2,
        Migration::new(7, "inbound_durability", INBOUND_DURABILITY_SQL).checksum()
    );
    assert!(rows[6].3 > 0);
    assert_eq!(rows[7].0, 8);
    assert_eq!(rows[7].1, "risk_workflow_inputs");
    assert_eq!(
        rows[7].2,
        Migration::new(8, "risk_workflow_inputs", RISK_WORKFLOW_INPUTS_SQL).checksum()
    );
    assert!(rows[7].3 > 0);
    assert_eq!(rows[8].0, 9);
    assert_eq!(rows[8].1, "outbound_delivery_work");
    assert_eq!(
        rows[8].2,
        Migration::new(9, "outbound_delivery_work", OUTBOUND_DELIVERY_WORK_SQL).checksum()
    );
    assert!(rows[8].3 > 0);
    assert_eq!(rows[9].0, 10);
    assert_eq!(rows[9].1, "inbound_raw_payload_length");
    assert_eq!(
        rows[9].2,
        Migration::new(
            10,
            "inbound_raw_payload_length",
            INBOUND_RAW_PAYLOAD_LENGTH_SQL
        )
        .checksum()
    );
    assert!(rows[9].3 > 0);
    assert_eq!(user_version, 10);
}

#[tokio::test]
async fn repeated_run_is_idempotent() {
    let pool = memory_pool().await;

    migrate(&pool).await.expect("first run should succeed");
    migrate(&pool).await.expect("second run should succeed");

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM schema_migrations")
        .fetch_one(&pool)
        .await
        .expect("migration count should be readable");

    assert_eq!(count, 10);
}

#[tokio::test]
async fn version_one_database_upgrades_to_latest_schema() {
    let pool = memory_pool().await;
    Migrator::new([Migration::new(1, "init", INIT_SQL)])
        .expect("version one migration should be valid")
        .run(&pool)
        .await
        .expect("version one should apply");

    migrate(&pool)
        .await
        .expect("remaining migrations should apply");

    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM schema_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .expect("migration versions should be readable");
    let user_version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&pool)
        .await
        .expect("user_version should be readable");

    assert_eq!(versions, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    assert_eq!(user_version, 10);
}

#[tokio::test]
async fn version_two_database_upgrades_to_execution_durability_schema() {
    let pool = memory_pool().await;
    Migrator::new([
        Migration::new(1, "init", INIT_SQL),
        Migration::new(2, "state_store_schema", STATE_STORE_SQL),
    ])
    .expect("version two migrations should be valid")
    .run(&pool)
    .await
    .expect("version two should apply");

    migrate(&pool)
        .await
        .expect("remaining migrations should apply");

    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM schema_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .expect("migration versions should be readable");
    assert_eq!(versions, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
}

#[tokio::test]
async fn version_nine_database_adds_nullable_inbound_raw_payload_length() {
    let pool = memory_pool().await;
    Migrator::new(embedded_migrations()[..9].iter().copied())
        .expect("version nine migrations should be valid")
        .run(&pool)
        .await
        .expect("version nine should apply");
    sqlx::query(
        "INSERT INTO execution_client_sessions (\
             session_id, client_id, account_id, platform, status, capabilities_json, connected_at, \
             last_heartbeat_at, last_time_sync_at, clock_sync_status, updated_at\
         ) VALUES ('session-1', 'client-1', 'account-1', 'MT5', 'ACTIVE', '[]', 10, 10, 10, \
                   'SYNCED', 10)",
    )
    .execute(&pool)
    .await
    .expect("legacy session fixture should insert");
    sqlx::query(
        "INSERT INTO inbound_admissions (\
             message_id, session_id, client_id, account_id, message_type, schema_version, \
             sequence, envelope_json, envelope_hash, received_at, status, created_at, updated_at\
         ) VALUES ('message-1', 'session-1', 'client-1', 'account-1', 'market.tick', \
                   'ecp.v1.0', 1, '{}', ?, 10, 'PENDING', 10, 10)",
    )
    .bind("0".repeat(64))
    .execute(&pool)
    .await
    .expect("legacy inbound fixture should insert");

    migrate(&pool).await.expect("version ten should apply");

    let raw_payload_length: Option<i64> = sqlx::query_scalar(
        "SELECT raw_payload_length FROM inbound_admissions WHERE message_id = 'message-1'",
    )
    .fetch_one(&pool)
    .await
    .expect("new inbound evidence column should be readable");
    let user_version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&pool)
        .await
        .expect("user_version should be readable");
    assert_eq!(raw_payload_length, None);
    assert_eq!(user_version, 10);
}

#[tokio::test]
async fn version_seven_upgrade_preserves_legacy_intents_without_forging_decision_time() {
    let pool = memory_pool().await;
    Migrator::new(embedded_migrations()[..7].iter().copied())
        .expect("version seven migrations should be valid")
        .run(&pool)
        .await
        .expect("version seven should apply");
    sqlx::query(
        "INSERT INTO trade_intents (\
             intent_id, decision_id, strategy_id, account_id, symbol, action, status,\
             requested_at, signal_expires_at, idempotency_key, payload_json, payload_hash,\
             created_at, updated_at\
         ) VALUES ('legacy-intent', 'decision-1', 'strategy-1', 'account-1', 'EURUSD',\
                   'BUY', 'ACCEPTED', 10, 20, 'legacy-idem', '{}', ?, 10, 10)",
    )
    .bind("0".repeat(64))
    .execute(&pool)
    .await
    .expect("legacy intent should insert before V8");

    migrate(&pool).await.expect("V8 should apply");

    let decision_timestamp: Option<i64> = sqlx::query_scalar(
        "SELECT decision_timestamp FROM trade_intents WHERE intent_id = 'legacy-intent'",
    )
    .fetch_one(&pool)
    .await
    .expect("new column should be readable");
    assert_eq!(decision_timestamp, None);
    assert!(sqlx::query(
        "INSERT INTO trade_intents (\
                 intent_id, decision_id, strategy_id, account_id, symbol, action, status,\
                 requested_at, signal_expires_at, idempotency_key, payload_json, payload_hash,\
                 created_at, updated_at\
             ) VALUES ('new-intent', 'decision-2', 'strategy-1', 'account-1', 'EURUSD',\
                       'BUY', 'ACCEPTED', 10, 20, 'new-idem', '{}', ?, 10, 10)",
    )
    .bind("0".repeat(64))
    .execute(&pool)
    .await
    .is_err());
}

#[tokio::test]
async fn version_four_upgrade_backfills_outbound_sequence_high_water_marks() {
    let pool = memory_pool().await;
    Migrator::new([
        embedded_migrations()[0],
        embedded_migrations()[1],
        embedded_migrations()[2],
        embedded_migrations()[3],
    ])
    .expect("version four migrations should be valid")
    .run(&pool)
    .await
    .expect("version four should apply");

    for (session_id, client_id) in [
        ("session-with-history", "client-with-history"),
        ("session-without-history", "client-without-history"),
    ] {
        sqlx::query(
            "INSERT INTO execution_client_sessions (\
                 session_id, client_id, account_id, platform, status, capabilities_json, \
                 connected_at, last_heartbeat_at, last_time_sync_at, clock_sync_status\
             ) VALUES (?, ?, 'account-1', 'MT5', 'ACTIVE', '[]', 100, 110, 105, 'SYNCED')",
        )
        .bind(session_id)
        .bind(client_id)
        .execute(&pool)
        .await
        .expect("version four session fixture should insert");
    }

    for (message_id, sequence) in [("message-7", 7_i64), ("message-42", 42_i64)] {
        sqlx::query(
            "INSERT INTO wire_outbox (\
                 message_id, session_id, message_type, sequence, payload_json, payload_hash, \
                 status, created_at\
             ) VALUES (?, 'session-with-history', 'heartbeat', ?, '{}', ?, 'PENDING', 111)",
        )
        .bind(message_id)
        .bind(sequence)
        .bind("0".repeat(64))
        .execute(&pool)
        .await
        .expect("version four outbox fixture should insert");
    }

    migrate(&pool)
        .await
        .expect("gateway delivery migration should apply");

    let with_history: (i64, i64, i64) = sqlx::query_as(
        "SELECT last_outbound_sequence, revision, updated_at \
         FROM execution_client_sessions WHERE session_id = 'session-with-history'",
    )
    .fetch_one(&pool)
    .await
    .expect("upgraded session should be readable");
    let without_history: (i64, i64, i64) = sqlx::query_as(
        "SELECT last_outbound_sequence, revision, updated_at \
         FROM execution_client_sessions WHERE session_id = 'session-without-history'",
    )
    .fetch_one(&pool)
    .await
    .expect("upgraded session without outbox should be readable");

    assert_eq!(with_history, (42, 1, 110));
    assert_eq!(without_history, (1, 0, 110));

    let next_reservation: (i64, i64) = sqlx::query_as(
        "UPDATE execution_client_sessions \
         SET last_outbound_sequence = last_outbound_sequence + 1, \
             revision = revision + 1, updated_at = 111 \
         WHERE session_id = 'session-with-history' AND status = 'ACTIVE' \
           AND revision = 1 AND last_outbound_sequence = 42 \
         RETURNING last_outbound_sequence, revision",
    )
    .fetch_one(&pool)
    .await
    .expect("first post-upgrade reservation should pass the session CAS");
    assert_eq!(next_reservation, (43, 2));
}

#[tokio::test]
async fn version_five_upgrade_backfills_event_order_and_preserves_spool_foreign_keys() {
    let pool = memory_pool().await;
    Migrator::new(embedded_migrations()[..5].iter().copied())
        .expect("version five migrations should be valid")
        .run(&pool)
        .await
        .expect("version five should apply");
    let hash = "0".repeat(64);
    for event_id in ["event-a", "event-b"] {
        sqlx::query(
            "INSERT INTO event_stream_log (\
                 event_id, topic, event_type, payload_json, payload_hash, created_at\
             ) VALUES (?, 'system.event', 'test', '{}', ?, 100)",
        )
        .bind(event_id)
        .bind(&hash)
        .execute(&pool)
        .await
        .expect("version five stream fixture should insert");
    }
    sqlx::query(
        "INSERT INTO outbound_spool (\
             spool_id, target, event_id, payload_json, payload_hash, status, created_at, updated_at\
         ) VALUES ('spool-a', 'audit', 'event-a', '{}', ?, 'PENDING', 100, 100)",
    )
    .bind(&hash)
    .execute(&pool)
    .await
    .expect("version five spool fixture should insert");

    migrate(&pool)
        .await
        .expect("event stream sequence migration should apply");

    let ordered: Vec<(i64, String)> = sqlx::query_as(
        "SELECT stream_sequence, event_id FROM event_stream_log ORDER BY stream_sequence",
    )
    .fetch_all(&pool)
    .await
    .expect("upgraded event order should be readable");
    assert_eq!(
        ordered,
        [(1, "event-a".to_owned()), (2, "event-b".to_owned())]
    );
    let spool_event: Option<String> =
        sqlx::query_scalar("SELECT event_id FROM outbound_spool WHERE spool_id = 'spool-a'")
            .fetch_one(&pool)
            .await
            .expect("upgraded spool should be readable");
    assert_eq!(spool_event.as_deref(), Some("event-a"));

    sqlx::query(
        "INSERT INTO event_stream_log (\
             event_id, topic, event_type, payload_json, payload_hash, created_at\
         ) VALUES ('event-c', 'system.event', 'test', '{}', ?, 100)",
    )
    .bind(&hash)
    .execute(&pool)
    .await
    .expect("post-upgrade event should insert");
    let next_sequence: i64 = sqlx::query_scalar(
        "SELECT stream_sequence FROM event_stream_log WHERE event_id = 'event-c'",
    )
    .fetch_one(&pool)
    .await
    .expect("post-upgrade sequence should be readable");
    assert_eq!(next_sequence, 3);

    sqlx::query("DELETE FROM event_stream_log WHERE event_id = 'event-a'")
        .execute(&pool)
        .await
        .expect("retention delete should succeed");
    let cleared_spool_event: Option<String> =
        sqlx::query_scalar("SELECT event_id FROM outbound_spool WHERE spool_id = 'spool-a'")
            .fetch_one(&pool)
            .await
            .expect("spool foreign-key action should be readable");
    assert_eq!(cleared_spool_event, None);
}

#[tokio::test]
async fn preexisting_untracked_migration_table_is_rejected() {
    let pool = memory_pool().await;
    sqlx::query(
        "CREATE TABLE schema_migrations (\
             version INTEGER NOT NULL,\
             name TEXT NOT NULL,\
             checksum TEXT NOT NULL,\
             applied_at INTEGER NOT NULL\
         )",
    )
    .execute(&pool)
    .await
    .expect("test fixture should create an incompatible migration table");

    let error = migrate(&pool)
        .await
        .expect_err("an untracked migration table must not be adopted");

    assert!(matches!(error, MigrationError::Database(_)));
}

#[tokio::test]
async fn changed_migration_contents_are_rejected() {
    let pool = memory_pool().await;
    migrate(&pool)
        .await
        .expect("initial migration should succeed");
    let changed = Migrator::new([
        Migration::new(1, "init", INIT_SQL),
        Migration::new(2, "state_store_schema", "SELECT 2;"),
    ])
    .expect("migration list should be valid");

    let error = changed
        .run(&pool)
        .await
        .expect_err("checksum drift must be rejected");

    assert!(matches!(
        error,
        MigrationError::ChecksumMismatch {
            version: 2,
            name: "state_store_schema"
        }
    ));
}

#[tokio::test]
async fn changed_migration_name_is_rejected() {
    let pool = memory_pool().await;
    migrate(&pool)
        .await
        .expect("initial migration should succeed");
    let renamed = Migrator::new([
        Migration::new(1, "init", INIT_SQL),
        Migration::new(2, "renamed", STATE_STORE_SQL),
    ])
    .expect("migration list should be valid");

    let error = renamed
        .run(&pool)
        .await
        .expect_err("name drift must be rejected");

    assert!(matches!(
        error,
        MigrationError::NameMismatch {
            version: 2,
            expected: "renamed",
            actual
        } if actual == "state_store_schema"
    ));
}

#[tokio::test]
async fn gaps_in_applied_versions_are_rejected() {
    let pool = memory_pool().await;
    migrate(&pool)
        .await
        .expect("initial migration should succeed");
    sqlx::query("DELETE FROM schema_migrations WHERE version = 1")
        .execute(&pool)
        .await
        .expect("test fixture should remove the first stored version");

    let error = migrate(&pool)
        .await
        .expect_err("an applied version gap must be rejected");

    assert!(matches!(
        error,
        MigrationError::InvalidAppliedSequence {
            expected: 1,
            found: 2
        }
    ));
}

#[tokio::test]
async fn database_versions_newer_than_the_binary_are_rejected() {
    let pool = memory_pool().await;
    migrate(&pool)
        .await
        .expect("initial migration should succeed");
    sqlx::query(
        "INSERT INTO schema_migrations (version, name, checksum, applied_at) \
         VALUES (11, 'unknown', 'unknown', 1)",
    )
    .execute(&pool)
    .await
    .expect("test fixture should insert an unknown migration");

    let error = migrate(&pool)
        .await
        .expect_err("a database newer than the binary must be rejected");

    assert!(matches!(
        error,
        MigrationError::DatabaseAhead {
            applied: 11,
            available: 10
        }
    ));
}

#[tokio::test]
async fn lower_user_version_than_migration_ledger_is_rejected() {
    let pool = memory_pool().await;
    migrate(&pool)
        .await
        .expect("initial migration should succeed");
    sqlx::query("PRAGMA user_version = 0")
        .execute(&pool)
        .await
        .expect("test fixture should lower user_version");

    let error = migrate(&pool)
        .await
        .expect_err("lower user_version drift must be rejected");

    assert!(matches!(
        error,
        MigrationError::UserVersionMismatch {
            expected: 10,
            actual: 0
        }
    ));
}

#[tokio::test]
async fn higher_user_version_than_migration_ledger_is_rejected() {
    let pool = memory_pool().await;
    migrate(&pool)
        .await
        .expect("initial migration should succeed");
    sqlx::query("PRAGMA user_version = 11")
        .execute(&pool)
        .await
        .expect("test fixture should raise user_version");

    let error = migrate(&pool)
        .await
        .expect_err("higher user_version drift must be rejected");

    assert!(matches!(
        error,
        MigrationError::UserVersionMismatch {
            expected: 10,
            actual: 11
        }
    ));
}

#[tokio::test]
async fn failed_migration_rolls_back_ddl_ledger_and_user_version() {
    let pool = memory_pool().await;
    migrate(&pool)
        .await
        .expect("initial migration should succeed");
    let migrator = Migrator::new([
        embedded_migrations()[0],
        embedded_migrations()[1],
        embedded_migrations()[2],
        embedded_migrations()[3],
        embedded_migrations()[4],
        embedded_migrations()[5],
        embedded_migrations()[6],
        embedded_migrations()[7],
        embedded_migrations()[8],
        embedded_migrations()[9],
        Migration::new(
            11,
            "broken",
            "CREATE TABLE rollback_probe (id INTEGER PRIMARY KEY);\
             INSERT INTO missing_table DEFAULT VALUES;",
        ),
    ])
    .expect("migration list should be valid");

    let error = migrator
        .run(&pool)
        .await
        .expect_err("the broken migration must fail");
    assert!(matches!(error, MigrationError::Database(_)));

    let probe_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(\
             SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'rollback_probe'\
         )",
    )
    .fetch_one(&pool)
    .await
    .expect("rollback probe state should be readable");
    let migration_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM schema_migrations")
        .fetch_one(&pool)
        .await
        .expect("migration count should be readable");
    let user_version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&pool)
        .await
        .expect("user_version should be readable");

    assert!(!probe_exists);
    assert_eq!(migration_count, 10);
    assert_eq!(user_version, 10);
}

#[test]
fn migration_versions_must_start_at_one_and_be_consecutive() {
    let missing_first = Migrator::new([Migration::new(2, "second", "SELECT 1;")])
        .expect_err("version 2 cannot be first");
    assert!(matches!(
        missing_first,
        MigrationError::InvalidSequence {
            expected: 1,
            found: 2
        }
    ));

    let gap = Migrator::new([
        Migration::new(1, "first", "SELECT 1;"),
        Migration::new(3, "third", "SELECT 3;"),
    ])
    .expect_err("migration versions cannot skip numbers");
    assert!(matches!(
        gap,
        MigrationError::InvalidSequence {
            expected: 2,
            found: 3
        }
    ));
}
