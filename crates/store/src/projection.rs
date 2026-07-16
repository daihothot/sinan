//! Latest-state projection writes, reads, and durable rebuilds.

use std::collections::BTreeSet;

use serde::de::DeserializeOwned;
use sinan_types::{
    AccountId, AccountSnapshot, MarketBar, MarketSnapshot, OrderSnapshot, PositionSnapshot,
    SymbolMetadataSnapshot,
};
use sqlx::{sqlite::SqliteRow, QueryBuilder, Row, Sqlite, SqliteConnection};

use crate::{
    connection::SqliteStateStore,
    error::StoreError,
    json::CanonicalJson,
    model::{CoreEventMetadata, NewCoreEvent, WriteOutcome},
    repository::append_core_event_on,
};

const ACCOUNT_SNAPSHOT_EVENT: &str = "account.snapshot";
const SYMBOL_METADATA_EVENT: &str = "symbol.metadata";
const POSITION_SNAPSHOT_EVENT: &str = "position.snapshot";
const ORDER_SNAPSHOT_EVENT: &str = "order.snapshot";
const MARKET_BAR_EVENT: &str = "market.bar";

/// Result of atomically appending a durable fact and applying its projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionWriteOutcome {
    /// A new fact changed the projection.
    Applied,
    /// A new, older fact was retained without replacing newer projected state.
    FactAppendedProjectionIgnored,
    /// A new fact described the state already present at the same observation time.
    FactAppendedProjectionUnchanged,
    /// The durable fact had already been accepted; its stored projection was
    /// reconciled idempotently before returning this outcome.
    Duplicate,
    /// A non-durable latest-only observation was older than projected state.
    ProjectionIgnored,
    /// A non-durable latest-only observation was identical to projected state.
    ProjectionUnchanged,
}

/// Explicit account authorization for projection reads.
///
/// An empty scope authorizes no accounts. It never means "all accounts".
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AuthorizedAccountScope {
    account_ids: BTreeSet<AccountId>,
}

impl AuthorizedAccountScope {
    pub fn new(account_ids: impl IntoIterator<Item = AccountId>) -> Self {
        Self {
            account_ids: account_ids.into_iter().collect(),
        }
    }

    pub fn empty() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.account_ids.is_empty()
    }

    pub fn contains(&self, account_id: &AccountId) -> bool {
        self.account_ids.contains(account_id)
    }

    fn iter(&self) -> impl Iterator<Item = &AccountId> {
        self.account_ids.iter()
    }
}

/// Account-scoped latest state loaded from one SQLite read snapshot.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LatestStateProjection {
    pub accounts: Vec<AccountSnapshot>,
    pub positions: Vec<PositionSnapshot>,
    pub orders: Vec<OrderSnapshot>,
    pub symbols: Vec<SymbolMetadataSnapshot>,
    pub markets: Vec<AccountMarketSnapshot>,
}

/// A market DTO paired with the account-scoped projection key omitted by the wire payload.
#[derive(Clone, Debug, PartialEq)]
pub struct AccountMarketSnapshot {
    pub account_id: AccountId,
    pub snapshot: MarketSnapshot,
}

/// Statistics from rebuilding the projections backed by durable facts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StateIngestProjectionRebuildReport {
    pub replayed_facts: u64,
    pub applied: u64,
    pub ignored_older: u64,
    pub unchanged: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ApplyDecision {
    Applied,
    IgnoredOlder,
    Unchanged,
}

enum DurableProjection<'a> {
    Account(&'a AccountSnapshot),
    Symbol(&'a SymbolMetadataSnapshot),
    Position(&'a PositionSnapshot),
    Order(&'a OrderSnapshot),
    MarketBar(&'a MarketBar),
}

impl DurableProjection<'_> {
    fn event_type(&self) -> &'static str {
        match self {
            Self::Account(_) => ACCOUNT_SNAPSHOT_EVENT,
            Self::Symbol(_) => SYMBOL_METADATA_EVENT,
            Self::Position(_) => POSITION_SNAPSHOT_EVENT,
            Self::Order(_) => ORDER_SNAPSHOT_EVENT,
            Self::MarketBar(_) => MARKET_BAR_EVENT,
        }
    }

    fn canonical_json(&self) -> Result<CanonicalJson, StoreError> {
        match self {
            Self::Account(value) => CanonicalJson::from_serializable(value),
            Self::Symbol(value) => CanonicalJson::from_serializable(value),
            Self::Position(value) => CanonicalJson::from_serializable(value),
            Self::Order(value) => CanonicalJson::from_serializable(value),
            Self::MarketBar(value) => CanonicalJson::from_serializable(value),
        }
    }

    fn validate_identity(&self, metadata: &CoreEventMetadata) -> Result<(), StoreError> {
        if metadata.event_type != self.event_type() {
            return Err(StoreError::IdentityConflict {
                entity: "core_event",
                key: metadata.event_id.clone(),
            });
        }

        match self {
            Self::Account(value) => validate_account_identity(metadata, &value.account_id),
            Self::Symbol(value) => validate_account_identity(metadata, &value.account_id),
            Self::Position(value) => validate_account_identity(metadata, &value.account_id),
            Self::Order(value) => validate_account_identity(metadata, &value.account_id),
            Self::MarketBar(_) => require_event_account(metadata).map(|_| ()),
        }
    }

    async fn apply(
        &self,
        connection: &mut SqliteConnection,
        metadata: &CoreEventMetadata,
        payload: &CanonicalJson,
    ) -> Result<ApplyDecision, StoreError> {
        match self {
            Self::Account(value) => {
                apply_account(connection, value, payload, metadata.received_at).await
            }
            Self::Symbol(value) => {
                apply_symbol(connection, value, payload, metadata.received_at).await
            }
            Self::Position(value) => {
                apply_position(connection, value, payload, metadata.received_at).await
            }
            Self::Order(value) => {
                apply_order(connection, value, payload, metadata.received_at).await
            }
            Self::MarketBar(value) => {
                let account_id = require_event_account(metadata)?;
                apply_market_bar(connection, account_id, value, payload, metadata.received_at).await
            }
        }
    }
}

impl SqliteStateStore {
    pub async fn ingest_account_snapshot(
        &self,
        metadata: CoreEventMetadata,
        snapshot: &AccountSnapshot,
    ) -> Result<ProjectionWriteOutcome, StoreError> {
        self.ingest_durable(metadata, DurableProjection::Account(snapshot))
            .await
    }

    pub async fn ingest_symbol_metadata(
        &self,
        metadata: CoreEventMetadata,
        snapshot: &SymbolMetadataSnapshot,
    ) -> Result<ProjectionWriteOutcome, StoreError> {
        self.ingest_durable(metadata, DurableProjection::Symbol(snapshot))
            .await
    }

    pub async fn ingest_position_snapshot(
        &self,
        metadata: CoreEventMetadata,
        snapshot: &PositionSnapshot,
    ) -> Result<ProjectionWriteOutcome, StoreError> {
        self.ingest_durable(metadata, DurableProjection::Position(snapshot))
            .await
    }

    pub async fn ingest_order_snapshot(
        &self,
        metadata: CoreEventMetadata,
        snapshot: &OrderSnapshot,
    ) -> Result<ProjectionWriteOutcome, StoreError> {
        self.ingest_durable(metadata, DurableProjection::Order(snapshot))
            .await
    }

    pub async fn ingest_market_bar(
        &self,
        metadata: CoreEventMetadata,
        bar: &MarketBar,
    ) -> Result<ProjectionWriteOutcome, StoreError> {
        self.ingest_durable(metadata, DurableProjection::MarketBar(bar))
            .await
    }

    /// Updates the latest-only market projection without manufacturing a durable tick fact.
    pub async fn update_market_snapshot(
        &self,
        account_id: &AccountId,
        snapshot: &MarketSnapshot,
        updated_at: i64,
    ) -> Result<ProjectionWriteOutcome, StoreError> {
        let payload = CanonicalJson::from_serializable(snapshot)?;
        let mut transaction = self.begin_write().await?;
        let decision = apply_market_snapshot(
            transaction.connection(),
            account_id,
            snapshot,
            &payload,
            updated_at,
        )
        .await;

        match decision {
            Ok(decision) => {
                transaction.commit().await?;
                Ok(match decision {
                    ApplyDecision::Applied => ProjectionWriteOutcome::Applied,
                    ApplyDecision::IgnoredOlder => ProjectionWriteOutcome::ProjectionIgnored,
                    ApplyDecision::Unchanged => ProjectionWriteOutcome::ProjectionUnchanged,
                })
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn ingest_durable(
        &self,
        metadata: CoreEventMetadata,
        projection: DurableProjection<'_>,
    ) -> Result<ProjectionWriteOutcome, StoreError> {
        projection.validate_identity(&metadata)?;
        let payload = projection.canonical_json()?;
        let mut transaction = self.begin_write().await?;

        let result = async {
            let append = append_core_event_on(
                transaction.connection(),
                NewCoreEvent {
                    metadata: metadata.clone(),
                    payload: payload.clone(),
                },
            )
            .await?;
            let fact_was_duplicate = matches!(append, WriteOutcome::Duplicate(_));
            let fact = append.into_record();

            let projection = projection
                .apply(transaction.connection(), &fact.metadata, &fact.payload)
                .await?;
            if fact_was_duplicate {
                return Ok(ProjectionWriteOutcome::Duplicate);
            }
            Ok(match projection {
                ApplyDecision::Applied => ProjectionWriteOutcome::Applied,
                ApplyDecision::IgnoredOlder => {
                    ProjectionWriteOutcome::FactAppendedProjectionIgnored
                }
                ApplyDecision::Unchanged => ProjectionWriteOutcome::FactAppendedProjectionUnchanged,
            })
        }
        .await;

        match result {
            Ok(outcome) => {
                transaction.commit().await?;
                Ok(outcome)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    /// Loads every account-bound latest table using one SQLite read transaction.
    pub async fn load_latest_state(
        &self,
        scope: &AuthorizedAccountScope,
    ) -> Result<LatestStateProjection, StoreError> {
        if scope.is_empty() {
            return Ok(LatestStateProjection::default());
        }

        let mut transaction = self.pool.begin().await?;
        let result = async {
            let accounts = load_accounts(&mut transaction, scope).await?;
            let positions = load_positions(&mut transaction, scope).await?;
            let orders = load_orders(&mut transaction, scope).await?;
            let symbols = load_symbols(&mut transaction, scope).await?;
            let markets = load_markets(&mut transaction, scope).await?;

            Ok(LatestStateProjection {
                accounts,
                positions,
                orders,
                symbols,
                markets,
            })
        }
        .await;

        match result {
            Ok(state) => {
                transaction.commit().await?;
                Ok(state)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    /// Rebuilds the account, symbol, position, order, and market-bar ingest projections.
    ///
    /// `market_snapshots` is excluded because raw ticks are latest-only. Execution command,
    /// leg, and plan lifecycle projections belong to the execution projector and are also
    /// deliberately outside this state-ingest rebuild.
    pub async fn rebuild_ingest_projections(
        &self,
    ) -> Result<StateIngestProjectionRebuildReport, StoreError> {
        let mut transaction = self.begin_write().await?;
        let result = rebuild_ingest_projections_on(transaction.connection()).await;

        match result {
            Ok(report) => {
                transaction.commit().await?;
                Ok(report)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }
}

fn require_event_account(metadata: &CoreEventMetadata) -> Result<&AccountId, StoreError> {
    metadata
        .account_id
        .as_ref()
        .ok_or_else(|| StoreError::IdentityConflict {
            entity: "core_event.account_id",
            key: metadata.event_id.clone(),
        })
}

fn validate_account_identity(
    metadata: &CoreEventMetadata,
    payload_account_id: &AccountId,
) -> Result<(), StoreError> {
    let event_account_id = require_event_account(metadata)?;
    if event_account_id == payload_account_id {
        Ok(())
    } else {
        Err(StoreError::IdentityConflict {
            entity: "account_id",
            key: metadata.event_id.clone(),
        })
    }
}

fn classify_latest(
    entity: &'static str,
    key: String,
    incoming_observed_at: i64,
    incoming_hash: &str,
    existing: Option<(i64, String)>,
) -> Result<ApplyDecision, StoreError> {
    let Some((existing_observed_at, existing_hash)) = existing else {
        return Ok(ApplyDecision::Applied);
    };

    match incoming_observed_at.cmp(&existing_observed_at) {
        std::cmp::Ordering::Greater => Ok(ApplyDecision::Applied),
        std::cmp::Ordering::Less => Ok(ApplyDecision::IgnoredOlder),
        std::cmp::Ordering::Equal if incoming_hash == existing_hash => Ok(ApplyDecision::Unchanged),
        std::cmp::Ordering::Equal => Err(StoreError::ObservationConflict {
            entity,
            key,
            observed_at: incoming_observed_at,
        }),
    }
}

async fn apply_account(
    connection: &mut SqliteConnection,
    snapshot: &AccountSnapshot,
    payload: &CanonicalJson,
    updated_at: i64,
) -> Result<ApplyDecision, StoreError> {
    let existing = sqlx::query_as::<_, (i64, String)>(
        "SELECT observed_at, payload_hash FROM account_snapshots_latest WHERE account_id = ?",
    )
    .bind(snapshot.account_id.as_str())
    .fetch_optional(&mut *connection)
    .await?;
    let decision = classify_latest(
        "account_snapshot",
        snapshot.account_id.to_string(),
        snapshot.observed_at,
        payload.sha256_hex(),
        existing,
    )?;

    if decision == ApplyDecision::Applied {
        sqlx::query(
            "INSERT INTO account_snapshots_latest (\
                 account_id, payload_json, payload_hash, observed_at, updated_at\
             ) VALUES (?, ?, ?, ?, ?)\
             ON CONFLICT(account_id) DO UPDATE SET \
                 payload_json = excluded.payload_json,\
                 payload_hash = excluded.payload_hash,\
                 observed_at = excluded.observed_at,\
                 updated_at = excluded.updated_at \
             WHERE excluded.observed_at > account_snapshots_latest.observed_at",
        )
        .bind(snapshot.account_id.as_str())
        .bind(payload.as_str())
        .bind(payload.sha256_hex())
        .bind(snapshot.observed_at)
        .bind(updated_at)
        .execute(connection)
        .await?;
    }
    Ok(decision)
}

async fn apply_symbol(
    connection: &mut SqliteConnection,
    snapshot: &SymbolMetadataSnapshot,
    payload: &CanonicalJson,
    updated_at: i64,
) -> Result<ApplyDecision, StoreError> {
    let existing = sqlx::query_as::<_, (i64, String)>(
        "SELECT observed_at, payload_hash FROM symbol_metadata_latest \
         WHERE account_id = ? AND broker_symbol = ?",
    )
    .bind(snapshot.account_id.as_str())
    .bind(&snapshot.broker_symbol)
    .fetch_optional(&mut *connection)
    .await?;
    let key = format!("{}:{}", snapshot.account_id, snapshot.broker_symbol);
    let decision = classify_latest(
        "symbol_metadata",
        key,
        snapshot.observed_at,
        payload.sha256_hex(),
        existing,
    )?;

    if decision == ApplyDecision::Applied {
        sqlx::query(
            "INSERT INTO symbol_metadata_latest (\
                 account_id, broker_symbol, symbol, payload_json, payload_hash, observed_at, updated_at\
             ) VALUES (?, ?, ?, ?, ?, ?, ?)\
             ON CONFLICT(account_id, broker_symbol) DO UPDATE SET \
                 symbol = excluded.symbol,\
                 payload_json = excluded.payload_json,\
                 payload_hash = excluded.payload_hash,\
                 observed_at = excluded.observed_at,\
                 updated_at = excluded.updated_at \
             WHERE excluded.observed_at > symbol_metadata_latest.observed_at",
        )
        .bind(snapshot.account_id.as_str())
        .bind(&snapshot.broker_symbol)
        .bind(snapshot.symbol.as_str())
        .bind(payload.as_str())
        .bind(payload.sha256_hex())
        .bind(snapshot.observed_at)
        .bind(updated_at)
        .execute(connection)
        .await?;
    }
    Ok(decision)
}

async fn apply_position(
    connection: &mut SqliteConnection,
    snapshot: &PositionSnapshot,
    payload: &CanonicalJson,
    updated_at: i64,
) -> Result<ApplyDecision, StoreError> {
    let existing = sqlx::query_as::<_, (i64, String)>(
        "SELECT observed_at, payload_hash FROM position_snapshots_latest \
         WHERE account_id = ? AND position_id = ?",
    )
    .bind(snapshot.account_id.as_str())
    .bind(snapshot.position_id.as_str())
    .fetch_optional(&mut *connection)
    .await?;
    let key = format!("{}:{}", snapshot.account_id, snapshot.position_id);
    let decision = classify_latest(
        "position_snapshot",
        key,
        snapshot.observed_at,
        payload.sha256_hex(),
        existing,
    )?;

    if decision == ApplyDecision::Applied {
        sqlx::query(
            "INSERT INTO position_snapshots_latest (\
                 account_id, position_id, symbol, payload_json, payload_hash, observed_at, updated_at\
             ) VALUES (?, ?, ?, ?, ?, ?, ?)\
             ON CONFLICT(account_id, position_id) DO UPDATE SET \
                 symbol = excluded.symbol,\
                 payload_json = excluded.payload_json,\
                 payload_hash = excluded.payload_hash,\
                 observed_at = excluded.observed_at,\
                 updated_at = excluded.updated_at \
             WHERE excluded.observed_at > position_snapshots_latest.observed_at",
        )
        .bind(snapshot.account_id.as_str())
        .bind(snapshot.position_id.as_str())
        .bind(snapshot.symbol.as_str())
        .bind(payload.as_str())
        .bind(payload.sha256_hex())
        .bind(snapshot.observed_at)
        .bind(updated_at)
        .execute(connection)
        .await?;
    }
    Ok(decision)
}

async fn apply_order(
    connection: &mut SqliteConnection,
    snapshot: &OrderSnapshot,
    payload: &CanonicalJson,
    updated_at: i64,
) -> Result<ApplyDecision, StoreError> {
    let existing = sqlx::query_as::<_, (i64, String)>(
        "SELECT observed_at, payload_hash FROM order_snapshots_latest \
         WHERE account_id = ? AND broker_order_id = ?",
    )
    .bind(snapshot.account_id.as_str())
    .bind(snapshot.broker_order_id.as_str())
    .fetch_optional(&mut *connection)
    .await?;
    let key = format!("{}:{}", snapshot.account_id, snapshot.broker_order_id);
    let decision = classify_latest(
        "order_snapshot",
        key,
        snapshot.observed_at,
        payload.sha256_hex(),
        existing,
    )?;

    if decision == ApplyDecision::Applied {
        sqlx::query(
            "INSERT INTO order_snapshots_latest (\
                 account_id, broker_order_id, payload_json, payload_hash, observed_at, updated_at\
             ) VALUES (?, ?, ?, ?, ?, ?)\
             ON CONFLICT(account_id, broker_order_id) DO UPDATE SET \
                 payload_json = excluded.payload_json,\
                 payload_hash = excluded.payload_hash,\
                 observed_at = excluded.observed_at,\
                 updated_at = excluded.updated_at \
             WHERE excluded.observed_at > order_snapshots_latest.observed_at",
        )
        .bind(snapshot.account_id.as_str())
        .bind(snapshot.broker_order_id.as_str())
        .bind(payload.as_str())
        .bind(payload.sha256_hex())
        .bind(snapshot.observed_at)
        .bind(updated_at)
        .execute(connection)
        .await?;
    }
    Ok(decision)
}

async fn apply_market_snapshot(
    connection: &mut SqliteConnection,
    account_id: &AccountId,
    snapshot: &MarketSnapshot,
    payload: &CanonicalJson,
    updated_at: i64,
) -> Result<ApplyDecision, StoreError> {
    let existing = sqlx::query_as::<_, (i64, String)>(
        "SELECT observed_at, payload_hash FROM market_snapshots \
         WHERE account_id = ? AND symbol = ?",
    )
    .bind(account_id.as_str())
    .bind(snapshot.symbol.as_str())
    .fetch_optional(&mut *connection)
    .await?;
    let key = format!("{}:{}", account_id, snapshot.symbol);
    let decision = classify_latest(
        "market_snapshot",
        key,
        snapshot.observed_at,
        payload.sha256_hex(),
        existing,
    )?;

    if decision == ApplyDecision::Applied {
        sqlx::query(
            "INSERT INTO market_snapshots (\
                 account_id, symbol, payload_json, payload_hash, observed_at, updated_at\
             ) VALUES (?, ?, ?, ?, ?, ?)\
             ON CONFLICT(account_id, symbol) DO UPDATE SET \
                 payload_json = excluded.payload_json,\
                 payload_hash = excluded.payload_hash,\
                 observed_at = excluded.observed_at,\
                 updated_at = excluded.updated_at \
             WHERE excluded.observed_at > market_snapshots.observed_at",
        )
        .bind(account_id.as_str())
        .bind(snapshot.symbol.as_str())
        .bind(payload.as_str())
        .bind(payload.sha256_hex())
        .bind(snapshot.observed_at)
        .bind(updated_at)
        .execute(connection)
        .await?;
    }
    Ok(decision)
}

async fn apply_market_bar(
    connection: &mut SqliteConnection,
    account_id: &AccountId,
    bar: &MarketBar,
    payload: &CanonicalJson,
    received_at: i64,
) -> Result<ApplyDecision, StoreError> {
    let existing: Option<String> = sqlx::query_scalar(
        "SELECT payload_hash FROM market_bars \
         WHERE account_id = ? AND symbol = ? AND timeframe = ? AND timestamp = ?",
    )
    .bind(account_id.as_str())
    .bind(bar.symbol.as_str())
    .bind(bar.timeframe.as_str())
    .bind(bar.timestamp)
    .fetch_optional(&mut *connection)
    .await?;

    let decision = match existing {
        None => ApplyDecision::Applied,
        Some(existing_hash) if existing_hash == payload.sha256_hex() => ApplyDecision::Unchanged,
        Some(_) => {
            return Err(StoreError::ObservationConflict {
                entity: "market_bar",
                key: format!(
                    "{}:{}:{}:{}",
                    account_id, bar.symbol, bar.timeframe, bar.timestamp
                ),
                observed_at: bar.timestamp,
            })
        }
    };

    if decision == ApplyDecision::Applied {
        sqlx::query(
            "INSERT INTO market_bars (\
                 account_id, symbol, timeframe, timestamp, payload_json, payload_hash, received_at\
             ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(account_id.as_str())
        .bind(bar.symbol.as_str())
        .bind(bar.timeframe.as_str())
        .bind(bar.timestamp)
        .bind(payload.as_str())
        .bind(payload.sha256_hex())
        .bind(received_at)
        .execute(connection)
        .await?;
    }
    Ok(decision)
}

async fn fetch_scoped_rows(
    connection: &mut SqliteConnection,
    select: &str,
    scope: &AuthorizedAccountScope,
    order_by: &str,
) -> Result<Vec<SqliteRow>, StoreError> {
    let mut query = QueryBuilder::<Sqlite>::new(select);
    query.push(" WHERE account_id IN (");
    {
        let mut separated = query.separated(", ");
        for account_id in scope.iter() {
            separated.push_bind(account_id.as_str().to_owned());
        }
    }
    query.push(") ").push(order_by);
    Ok(query.build().fetch_all(connection).await?)
}

fn decode_payload<T: DeserializeOwned>(
    entity: &'static str,
    key: &str,
    row: &SqliteRow,
) -> Result<T, StoreError> {
    let payload_json: String = row.try_get("payload_json")?;
    let payload_hash: String = row.try_get("payload_hash")?;
    let canonical = CanonicalJson::from_stored(entity, key, payload_json, payload_hash)?;
    serde_json::from_str(canonical.as_str())
        .map_err(|error| StoreError::corrupt(entity, key, error.to_string()))
}

fn validate_observed_at(
    entity: &'static str,
    key: &str,
    row: &SqliteRow,
    payload_observed_at: i64,
) -> Result<(), StoreError> {
    let stored_observed_at: i64 = row.try_get("observed_at")?;
    if stored_observed_at == payload_observed_at {
        Ok(())
    } else {
        Err(StoreError::corrupt(
            entity,
            key,
            format!(
                "projection observed_at {stored_observed_at} does not match payload observed_at {payload_observed_at}"
            ),
        ))
    }
}

async fn load_accounts(
    connection: &mut SqliteConnection,
    scope: &AuthorizedAccountScope,
) -> Result<Vec<AccountSnapshot>, StoreError> {
    let rows = fetch_scoped_rows(
        connection,
        "SELECT account_id, payload_json, payload_hash, observed_at FROM account_snapshots_latest",
        scope,
        "ORDER BY account_id",
    )
    .await?;
    rows.into_iter()
        .map(|row| {
            let account_id: String = row.try_get("account_id")?;
            let value: AccountSnapshot = decode_payload("account_snapshot", &account_id, &row)?;
            validate_observed_at("account_snapshot", &account_id, &row, value.observed_at)?;
            if value.account_id.as_str() != account_id {
                return Err(StoreError::corrupt(
                    "account_snapshot",
                    account_id,
                    "payload account_id does not match projection key",
                ));
            }
            Ok(value)
        })
        .collect()
}

async fn load_positions(
    connection: &mut SqliteConnection,
    scope: &AuthorizedAccountScope,
) -> Result<Vec<PositionSnapshot>, StoreError> {
    let rows = fetch_scoped_rows(
        connection,
        "SELECT account_id, position_id, symbol, payload_json, payload_hash, observed_at \
         FROM position_snapshots_latest",
        scope,
        "ORDER BY account_id, position_id",
    )
    .await?;
    rows.into_iter()
        .map(|row| {
            let account_id: String = row.try_get("account_id")?;
            let position_id: String = row.try_get("position_id")?;
            let symbol: String = row.try_get("symbol")?;
            let key = format!("{account_id}:{position_id}");
            let value: PositionSnapshot = decode_payload("position_snapshot", &key, &row)?;
            validate_observed_at("position_snapshot", &key, &row, value.observed_at)?;
            if value.account_id.as_str() != account_id
                || value.position_id.as_str() != position_id
                || value.symbol.as_str() != symbol
            {
                return Err(StoreError::corrupt(
                    "position_snapshot",
                    key,
                    "payload identity does not match projection key",
                ));
            }
            Ok(value)
        })
        .collect()
}

async fn load_orders(
    connection: &mut SqliteConnection,
    scope: &AuthorizedAccountScope,
) -> Result<Vec<OrderSnapshot>, StoreError> {
    let rows = fetch_scoped_rows(
        connection,
        "SELECT account_id, broker_order_id, payload_json, payload_hash, observed_at \
         FROM order_snapshots_latest",
        scope,
        "ORDER BY account_id, broker_order_id",
    )
    .await?;
    rows.into_iter()
        .map(|row| {
            let account_id: String = row.try_get("account_id")?;
            let broker_order_id: String = row.try_get("broker_order_id")?;
            let key = format!("{account_id}:{broker_order_id}");
            let value: OrderSnapshot = decode_payload("order_snapshot", &key, &row)?;
            validate_observed_at("order_snapshot", &key, &row, value.observed_at)?;
            if value.account_id.as_str() != account_id
                || value.broker_order_id.as_str() != broker_order_id
            {
                return Err(StoreError::corrupt(
                    "order_snapshot",
                    key,
                    "payload identity does not match projection key",
                ));
            }
            Ok(value)
        })
        .collect()
}

async fn load_symbols(
    connection: &mut SqliteConnection,
    scope: &AuthorizedAccountScope,
) -> Result<Vec<SymbolMetadataSnapshot>, StoreError> {
    let rows = fetch_scoped_rows(
        connection,
        "SELECT account_id, broker_symbol, symbol, payload_json, payload_hash, observed_at \
         FROM symbol_metadata_latest",
        scope,
        "ORDER BY account_id, broker_symbol",
    )
    .await?;
    rows.into_iter()
        .map(|row| {
            let account_id: String = row.try_get("account_id")?;
            let broker_symbol: String = row.try_get("broker_symbol")?;
            let symbol: String = row.try_get("symbol")?;
            let key = format!("{account_id}:{broker_symbol}");
            let value: SymbolMetadataSnapshot = decode_payload("symbol_metadata", &key, &row)?;
            validate_observed_at("symbol_metadata", &key, &row, value.observed_at)?;
            if value.account_id.as_str() != account_id
                || value.broker_symbol != broker_symbol
                || value.symbol.as_str() != symbol
            {
                return Err(StoreError::corrupt(
                    "symbol_metadata",
                    key,
                    "payload identity does not match projection key",
                ));
            }
            Ok(value)
        })
        .collect()
}

async fn load_markets(
    connection: &mut SqliteConnection,
    scope: &AuthorizedAccountScope,
) -> Result<Vec<AccountMarketSnapshot>, StoreError> {
    let rows = fetch_scoped_rows(
        connection,
        "SELECT account_id, symbol, payload_json, payload_hash, observed_at FROM market_snapshots",
        scope,
        "ORDER BY account_id, symbol",
    )
    .await?;
    rows.into_iter()
        .map(|row| {
            let account_id: String = row.try_get("account_id")?;
            let symbol: String = row.try_get("symbol")?;
            let key = format!("{account_id}:{symbol}");
            let value: MarketSnapshot = decode_payload("market_snapshot", &key, &row)?;
            validate_observed_at("market_snapshot", &key, &row, value.observed_at)?;
            if value.symbol.as_str() != symbol {
                return Err(StoreError::corrupt(
                    "market_snapshot",
                    key,
                    "payload symbol does not match projection key",
                ));
            }
            Ok(AccountMarketSnapshot {
                account_id: AccountId::from(account_id),
                snapshot: value,
            })
        })
        .collect()
}

async fn rebuild_ingest_projections_on(
    connection: &mut SqliteConnection,
) -> Result<StateIngestProjectionRebuildReport, StoreError> {
    let rows = sqlx::query(
        "SELECT event_id, event_type, account_id, received_at, payload_json, payload_hash \
         FROM core_events \
         WHERE event_type IN (\
             'account.snapshot', 'symbol.metadata', 'position.snapshot', 'order.snapshot', 'market.bar'\
         ) \
         ORDER BY received_at, created_at, event_id",
    )
    .fetch_all(&mut *connection)
    .await?;

    for table in [
        "account_snapshots_latest",
        "symbol_metadata_latest",
        "position_snapshots_latest",
        "order_snapshots_latest",
        "market_bars",
    ] {
        sqlx::query(&format!("DELETE FROM {table}"))
            .execute(&mut *connection)
            .await?;
    }

    let mut report = StateIngestProjectionRebuildReport::default();
    for row in rows {
        let event_id: String = row.try_get("event_id")?;
        let event_type: String = row.try_get("event_type")?;
        let account_id: Option<String> = row.try_get("account_id")?;
        let received_at: i64 = row.try_get("received_at")?;
        let payload_json: String = row.try_get("payload_json")?;
        let payload_hash: String = row.try_get("payload_hash")?;
        let payload =
            CanonicalJson::from_stored("core_event", &event_id, payload_json, payload_hash)?;
        let account_id = account_id.ok_or_else(|| {
            StoreError::corrupt(
                "core_event",
                &event_id,
                "account_id is required for projection",
            )
        })?;

        let decision = match event_type.as_str() {
            ACCOUNT_SNAPSHOT_EVENT => {
                let value: AccountSnapshot = decode_fact(&event_id, &payload)?;
                ensure_fact_account(&event_id, &account_id, &value.account_id)?;
                apply_account(connection, &value, &payload, received_at).await?
            }
            SYMBOL_METADATA_EVENT => {
                let value: SymbolMetadataSnapshot = decode_fact(&event_id, &payload)?;
                ensure_fact_account(&event_id, &account_id, &value.account_id)?;
                apply_symbol(connection, &value, &payload, received_at).await?
            }
            POSITION_SNAPSHOT_EVENT => {
                let value: PositionSnapshot = decode_fact(&event_id, &payload)?;
                ensure_fact_account(&event_id, &account_id, &value.account_id)?;
                apply_position(connection, &value, &payload, received_at).await?
            }
            ORDER_SNAPSHOT_EVENT => {
                let value: OrderSnapshot = decode_fact(&event_id, &payload)?;
                ensure_fact_account(&event_id, &account_id, &value.account_id)?;
                apply_order(connection, &value, &payload, received_at).await?
            }
            MARKET_BAR_EVENT => {
                let value: MarketBar = decode_fact(&event_id, &payload)?;
                let account_id = AccountId::from(account_id);
                apply_market_bar(connection, &account_id, &value, &payload, received_at).await?
            }
            _ => continue,
        };

        report.replayed_facts += 1;
        match decision {
            ApplyDecision::Applied => report.applied += 1,
            ApplyDecision::IgnoredOlder => report.ignored_older += 1,
            ApplyDecision::Unchanged => report.unchanged += 1,
        }
    }

    Ok(report)
}

fn decode_fact<T: DeserializeOwned>(
    event_id: &str,
    payload: &CanonicalJson,
) -> Result<T, StoreError> {
    serde_json::from_str(payload.as_str())
        .map_err(|error| StoreError::corrupt("core_event", event_id, error.to_string()))
}

fn ensure_fact_account(
    event_id: &str,
    stored_account_id: &str,
    payload_account_id: &AccountId,
) -> Result<(), StoreError> {
    if stored_account_id == payload_account_id.as_str() {
        Ok(())
    } else {
        Err(StoreError::corrupt(
            "core_event",
            event_id,
            "payload account_id does not match fact account_id",
        ))
    }
}
