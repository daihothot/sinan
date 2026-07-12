use sinan_store::{migrate, Migration, MigrationError, Migrator};
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};

const INIT_SQL: &str = include_str!("../migrations/V0001__init.sql");

async fn memory_pool() -> SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("in-memory SQLite should open")
}

#[tokio::test]
async fn first_run_applies_initial_migration() {
    let pool = memory_pool().await;

    migrate(&pool).await.expect("migration should succeed");

    let row: (i64, String, String, i64) =
        sqlx::query_as("SELECT version, name, checksum, applied_at FROM schema_migrations")
            .fetch_one(&pool)
            .await
            .expect("migration record should exist");
    let user_version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&pool)
        .await
        .expect("user_version should be readable");

    assert_eq!(row.0, 1);
    assert_eq!(row.1, "init");
    assert_eq!(row.2, Migration::new(1, "init", INIT_SQL).checksum());
    assert!(row.3 > 0);
    assert_eq!(user_version, 1);
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

    assert_eq!(count, 1);
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
    let changed = Migrator::new([Migration::new(
        1,
        "init",
        "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY);",
    )])
    .expect("migration list should be valid");

    let error = changed
        .run(&pool)
        .await
        .expect_err("checksum drift must be rejected");

    assert!(matches!(
        error,
        MigrationError::ChecksumMismatch {
            version: 1,
            name: "init"
        }
    ));
}

#[tokio::test]
async fn changed_migration_name_is_rejected() {
    let pool = memory_pool().await;
    migrate(&pool)
        .await
        .expect("initial migration should succeed");
    let renamed = Migrator::new([Migration::new(1, "renamed", INIT_SQL)])
        .expect("migration list should be valid");

    let error = renamed
        .run(&pool)
        .await
        .expect_err("name drift must be rejected");

    assert!(matches!(
        error,
        MigrationError::NameMismatch {
            version: 1,
            expected: "renamed",
            actual
        } if actual == "init"
    ));
}

#[tokio::test]
async fn gaps_in_applied_versions_are_rejected() {
    let pool = memory_pool().await;
    migrate(&pool)
        .await
        .expect("initial migration should succeed");
    sqlx::query("UPDATE schema_migrations SET version = 2 WHERE version = 1")
        .execute(&pool)
        .await
        .expect("test fixture should update the stored version");

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
         VALUES (2, 'unknown', 'unknown', 1)",
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
            applied: 2,
            available: 1
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
            expected: 1,
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
    sqlx::query("PRAGMA user_version = 2")
        .execute(&pool)
        .await
        .expect("test fixture should raise user_version");

    let error = migrate(&pool)
        .await
        .expect_err("higher user_version drift must be rejected");

    assert!(matches!(
        error,
        MigrationError::UserVersionMismatch {
            expected: 1,
            actual: 2
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
        Migration::new(1, "init", INIT_SQL),
        Migration::new(
            2,
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
    assert_eq!(migration_count, 1);
    assert_eq!(user_version, 1);
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
