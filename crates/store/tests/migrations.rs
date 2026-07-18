use sinan_store::{migrate, Migration, MigrationError, Migrator};
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};

const INIT_SQL: &str = include_str!("../migrations/V0001__init.sql");
const STATE_STORE_SQL: &str = include_str!("../migrations/V0002__state_store_schema.sql");
const EXECUTION_DURABILITY_SQL: &str =
    include_str!("../migrations/V0003__execution_durability.sql");
const RECONCILIATION_DURABILITY_SQL: &str =
    include_str!("../migrations/V0004__reconciliation_durability.sql");

fn embedded_migrations() -> [Migration; 4] {
    [
        Migration::new(1, "init", INIT_SQL),
        Migration::new(2, "state_store_schema", STATE_STORE_SQL),
        Migration::new(3, "execution_durability", EXECUTION_DURABILITY_SQL),
        Migration::new(
            4,
            "reconciliation_durability",
            RECONCILIATION_DURABILITY_SQL,
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

    assert_eq!(rows.len(), 4);
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
    assert_eq!(user_version, 4);
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

    assert_eq!(count, 4);
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

    assert_eq!(versions, vec![1, 2, 3, 4]);
    assert_eq!(user_version, 4);
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
    assert_eq!(versions, vec![1, 2, 3, 4]);
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
         VALUES (5, 'unknown', 'unknown', 1)",
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
            applied: 5,
            available: 4
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
            expected: 4,
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
    sqlx::query("PRAGMA user_version = 5")
        .execute(&pool)
        .await
        .expect("test fixture should raise user_version");

    let error = migrate(&pool)
        .await
        .expect_err("higher user_version drift must be rejected");

    assert!(matches!(
        error,
        MigrationError::UserVersionMismatch {
            expected: 4,
            actual: 5
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
        Migration::new(
            5,
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
    assert_eq!(migration_count, 4);
    assert_eq!(user_version, 4);
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
