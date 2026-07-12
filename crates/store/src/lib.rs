#![forbid(unsafe_code)]

//! Forward-only SQLite schema migrations.

use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use sqlx::{Row, SqlitePool};
use thiserror::Error;

const SCHEMA_MIGRATIONS_TABLE: &str = "schema_migrations";

const EMBEDDED_MIGRATIONS: &[Migration] = &[Migration::new(
    1,
    "init",
    include_str!("../migrations/V0001__init.sql"),
)];

/// One immutable, forward-only database migration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Migration {
    version: u32,
    name: &'static str,
    sql: &'static str,
}

impl Migration {
    /// Defines a migration. Versions must form the sequence `1..=N` in a [`Migrator`].
    pub const fn new(version: u32, name: &'static str, sql: &'static str) -> Self {
        Self { version, name, sql }
    }

    pub const fn version(&self) -> u32 {
        self.version
    }

    pub const fn name(&self) -> &'static str {
        self.name
    }

    pub const fn sql(&self) -> &'static str {
        self.sql
    }

    /// Returns the lowercase SHA-256 hex digest of the migration file contents.
    pub fn checksum(&self) -> String {
        format!("{:x}", Sha256::digest(self.sql.as_bytes()))
    }
}

/// A validated, ordered set of forward-only migrations.
#[derive(Clone, Debug)]
pub struct Migrator {
    migrations: Vec<Migration>,
}

impl Migrator {
    /// Builds a migrator and rejects lists that do not contain exactly `1..=N`.
    pub fn new(migrations: impl IntoIterator<Item = Migration>) -> Result<Self, MigrationError> {
        let migrations: Vec<_> = migrations.into_iter().collect();

        for (index, migration) in migrations.iter().enumerate() {
            let expected = u32::try_from(index)
                .ok()
                .and_then(|index| index.checked_add(1))
                .ok_or(MigrationError::TooManyMigrations)?;

            if migration.version != expected {
                return Err(MigrationError::InvalidSequence {
                    expected,
                    found: migration.version,
                });
            }
        }

        Ok(Self { migrations })
    }

    /// Applies all pending migrations and verifies every previously applied migration.
    pub async fn run(&self, pool: &SqlitePool) -> Result<(), MigrationError> {
        let applied = load_applied_migrations(pool).await?;
        validate_applied_migrations(&self.migrations, &applied)?;
        validate_user_version(pool, &applied).await?;

        for migration in self.migrations.iter().skip(applied.len()) {
            apply_migration(pool, migration).await?;
        }

        Ok(())
    }
}

/// Applies the migrations embedded in this crate.
pub async fn migrate(pool: &SqlitePool) -> Result<(), MigrationError> {
    Migrator::new(EMBEDDED_MIGRATIONS.iter().copied())?
        .run(pool)
        .await
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("migration versions must be consecutive from 1: expected {expected}, found {found}")]
    InvalidSequence { expected: u32, found: u32 },

    #[error(
        "applied migration versions must be consecutive from 1: expected {expected}, found {found}"
    )]
    InvalidAppliedSequence { expected: u32, found: u32 },

    #[error("database migration version {applied} is newer than available version {available}")]
    DatabaseAhead { applied: u32, available: u32 },

    #[error("stored migration version {found} is outside the supported u32 range")]
    InvalidStoredVersion { found: i64 },

    #[error(
        "PRAGMA user_version mismatch: schema_migrations expects {expected}, database contains {actual}"
    )]
    UserVersionMismatch { expected: u32, actual: i64 },

    #[error(
        "migration {version} name mismatch: expected {expected:?}, database contains {actual:?}"
    )]
    NameMismatch {
        version: u32,
        expected: &'static str,
        actual: String,
    },

    #[error("migration {version} ({name}) checksum mismatch")]
    ChecksumMismatch { version: u32, name: &'static str },

    #[error("system time is before the Unix epoch")]
    TimeBeforeUnixEpoch,

    #[error("current Unix timestamp does not fit in SQLite INTEGER")]
    TimestampOverflow,

    #[error("migration list is too large to represent with u32 versions")]
    TooManyMigrations,

    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

#[derive(Debug)]
struct AppliedMigration {
    version: u32,
    name: String,
    checksum: String,
}

async fn load_applied_migrations(
    pool: &SqlitePool,
) -> Result<Vec<AppliedMigration>, MigrationError> {
    let table_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(\
             SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?\
         )",
    )
    .bind(SCHEMA_MIGRATIONS_TABLE)
    .fetch_one(pool)
    .await?;

    if !table_exists {
        return Ok(Vec::new());
    }

    let rows =
        sqlx::query("SELECT version, name, checksum FROM schema_migrations ORDER BY version ASC")
            .fetch_all(pool)
            .await?;

    rows.into_iter()
        .map(|row| {
            let version: i64 = row.try_get("version")?;
            let version = u32::try_from(version)
                .map_err(|_| MigrationError::InvalidStoredVersion { found: version })?;

            Ok(AppliedMigration {
                version,
                name: row.try_get("name")?,
                checksum: row.try_get("checksum")?,
            })
        })
        .collect()
}

fn validate_applied_migrations(
    migrations: &[Migration],
    applied: &[AppliedMigration],
) -> Result<(), MigrationError> {
    for (index, record) in applied.iter().enumerate() {
        let expected_version = u32::try_from(index)
            .ok()
            .and_then(|index| index.checked_add(1))
            .ok_or(MigrationError::TooManyMigrations)?;

        if record.version != expected_version {
            return Err(MigrationError::InvalidAppliedSequence {
                expected: expected_version,
                found: record.version,
            });
        }

        let Some(migration) = migrations.get(index) else {
            return Err(MigrationError::DatabaseAhead {
                applied: record.version,
                available: migrations.last().map_or(0, Migration::version),
            });
        };

        if record.name != migration.name {
            return Err(MigrationError::NameMismatch {
                version: migration.version,
                expected: migration.name,
                actual: record.name.clone(),
            });
        }

        if record.checksum != migration.checksum() {
            return Err(MigrationError::ChecksumMismatch {
                version: migration.version,
                name: migration.name,
            });
        }
    }

    Ok(())
}

async fn validate_user_version(
    pool: &SqlitePool,
    applied: &[AppliedMigration],
) -> Result<(), MigrationError> {
    let actual: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(pool)
        .await?;
    let expected = applied.last().map_or(0, |migration| migration.version);

    if actual != i64::from(expected) {
        return Err(MigrationError::UserVersionMismatch { expected, actual });
    }

    Ok(())
}

async fn apply_migration(pool: &SqlitePool, migration: &Migration) -> Result<(), MigrationError> {
    let mut transaction = pool.begin().await?;

    sqlx::raw_sql(migration.sql)
        .execute(&mut *transaction)
        .await?;

    sqlx::query(
        "INSERT INTO schema_migrations (version, name, checksum, applied_at) VALUES (?, ?, ?, ?)",
    )
    .bind(i64::from(migration.version))
    .bind(migration.name)
    .bind(migration.checksum())
    .bind(unix_timestamp_millis()?)
    .execute(&mut *transaction)
    .await?;

    sqlx::query(&format!("PRAGMA user_version = {}", migration.version))
        .execute(&mut *transaction)
        .await?;

    transaction.commit().await?;
    Ok(())
}

fn unix_timestamp_millis() -> Result<i64, MigrationError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| MigrationError::TimeBeforeUnixEpoch)?;

    i64::try_from(elapsed.as_millis()).map_err(|_| MigrationError::TimestampOverflow)
}
