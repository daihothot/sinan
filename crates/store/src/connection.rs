use std::{str::FromStr, time::Duration};

use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
    Sqlite, SqliteConnection, SqlitePool, Transaction,
};

use crate::error::StoreError;

#[derive(Clone, Debug)]
pub struct StoreOptions {
    pub database_url: String,
    pub max_connections: u32,
    pub busy_timeout: Duration,
    pub create_if_missing: bool,
}

impl StoreOptions {
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            max_connections: 5,
            busy_timeout: Duration::from_secs(5),
            create_if_missing: true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SqliteStateStore {
    pub(crate) pool: SqlitePool,
}

impl SqliteStateStore {
    /// Opens a configured SQLite pool and applies the embedded migrations.
    pub async fn connect(options: StoreOptions) -> Result<Self, StoreError> {
        let connect_options = SqliteConnectOptions::from_str(&options.database_url)?
            .create_if_missing(options.create_if_missing)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(options.busy_timeout);
        let pool = SqlitePoolOptions::new()
            .max_connections(options.max_connections)
            .connect_with(connect_options)
            .await?;

        crate::migrate(&pool)
            .await
            .map_err(|error| StoreError::Initialization(error.to_string()))?;

        Ok(Self { pool })
    }

    pub async fn begin_write(&self) -> Result<WriteTransaction, StoreError> {
        // Reserve SQLite's single writer before any read/compare step so a
        // deferred transaction cannot fail later with SQLITE_BUSY_SNAPSHOT.
        Ok(WriteTransaction {
            inner: self.pool.begin_with("BEGIN IMMEDIATE").await?,
        })
    }

    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

pub struct WriteTransaction {
    pub(crate) inner: Transaction<'static, Sqlite>,
}

impl WriteTransaction {
    pub async fn commit(self) -> Result<(), StoreError> {
        self.inner.commit().await?;
        Ok(())
    }

    pub async fn rollback(self) -> Result<(), StoreError> {
        self.inner.rollback().await?;
        Ok(())
    }

    pub(crate) fn connection(&mut self) -> &mut SqliteConnection {
        &mut self.inner
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{SqliteStateStore, StoreOptions};

    static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);

    struct DatabaseFiles(PathBuf);

    impl DatabaseFiles {
        fn unique() -> Self {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("test clock should be after Unix epoch")
                .as_nanos();
            let sequence = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!(
                "sinan-store-{}-{timestamp}-{sequence}.sqlite",
                std::process::id()
            )))
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for DatabaseFiles {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
            let _ = fs::remove_file(format!("{}-wal", self.0.display()));
            let _ = fs::remove_file(format!("{}-shm", self.0.display()));
        }
    }

    #[tokio::test]
    async fn connect_creates_migrates_and_configures_every_pool_connection() {
        let database = DatabaseFiles::unique();
        assert!(!database.path().exists());

        let mut options = StoreOptions::new(format!("sqlite://{}", database.path().display()));
        options.max_connections = 2;
        let store = SqliteStateStore::connect(options)
            .await
            .expect("state store should connect");

        assert!(database.path().exists(), "database file should be created");
        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&store.pool)
            .await
            .expect("schema version should be readable");
        assert_eq!(version, 2);

        let mut first = store.pool.acquire().await.expect("first connection");
        let mut second = store.pool.acquire().await.expect("second connection");
        let first_foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
            .fetch_one(&mut *first)
            .await
            .expect("foreign_keys should be readable");
        let second_foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
            .fetch_one(&mut *second)
            .await
            .expect("foreign_keys should be readable");
        let first_journal: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&mut *first)
            .await
            .expect("journal mode should be readable");
        let second_journal: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&mut *second)
            .await
            .expect("journal mode should be readable");

        assert_eq!(first_foreign_keys, 1);
        assert_eq!(second_foreign_keys, 1);
        assert_eq!(first_journal, "wal");
        assert_eq!(second_journal, "wal");

        drop(first);
        drop(second);
        store.pool.close().await;
    }
}
