use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use sinan_store::{SqliteStateStore, StoreOptions};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
    SqlitePool,
};

static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);

pub(crate) struct TestDatabase(PathBuf);

impl TestDatabase {
    fn unique() -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("test clock should be after Unix epoch")
            .as_nanos();
        let sequence = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "sinan-store-integration-{}-{timestamp}-{sequence}.sqlite",
            std::process::id()
        )))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
        let _ = fs::remove_file(format!("{}-wal", self.0.display()));
        let _ = fs::remove_file(format!("{}-shm", self.0.display()));
    }
}

pub(crate) async fn test_store() -> (TestDatabase, SqliteStateStore, SqlitePool) {
    let database = TestDatabase::unique();
    let database_url = format!("sqlite://{}", database.path().display());
    let mut store_options = StoreOptions::new(database_url.clone());
    store_options.max_connections = 4;
    store_options.busy_timeout = Duration::from_secs(5);
    let store = SqliteStateStore::connect(store_options)
        .await
        .expect("test state store should connect and migrate");

    let raw_options = SqliteConnectOptions::from_str(&database_url)
        .expect("test database URL should parse")
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(5));
    let raw_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(raw_options)
        .await
        .expect("raw test pool should connect");

    (database, store, raw_pool)
}
