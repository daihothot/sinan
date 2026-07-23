use sinan_types::{AccountId, RiskCapacity, StrategyId};
use sqlx::{Row, SqliteConnection};

use crate::{
    CanonicalJson, ProjectionWriteOutcome, SqliteStateStore, StoreError, WriteTransaction,
};

#[derive(Clone, Debug, PartialEq)]
pub struct NewRiskCapacitySnapshot {
    pub capacity: RiskCapacity,
    pub recorded_at: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StoredRiskCapacitySnapshot {
    pub capacity: RiskCapacity,
    pub payload: CanonicalJson,
    pub recorded_at: i64,
}

impl SqliteStateStore {
    pub async fn record_risk_capacity_snapshot(
        &self,
        snapshot: NewRiskCapacitySnapshot,
    ) -> Result<ProjectionWriteOutcome, StoreError> {
        let mut transaction = self.begin_write().await?;
        let outcome = transaction.record_risk_capacity_snapshot(snapshot).await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn get_latest_risk_capacity_snapshot(
        &self,
        account_id: &AccountId,
        strategy_id: &StrategyId,
    ) -> Result<Option<StoredRiskCapacitySnapshot>, StoreError> {
        let mut connection = self.pool().acquire().await?;
        fetch_latest_risk_capacity_snapshot_on(&mut connection, account_id, strategy_id).await
    }
}

impl WriteTransaction {
    pub async fn record_risk_capacity_snapshot(
        &mut self,
        snapshot: NewRiskCapacitySnapshot,
    ) -> Result<ProjectionWriteOutcome, StoreError> {
        record_risk_capacity_snapshot_on(self.connection(), snapshot).await
    }

    pub async fn get_latest_risk_capacity_snapshot(
        &mut self,
        account_id: &AccountId,
        strategy_id: &StrategyId,
    ) -> Result<Option<StoredRiskCapacitySnapshot>, StoreError> {
        fetch_latest_risk_capacity_snapshot_on(self.connection(), account_id, strategy_id).await
    }
}

pub(crate) async fn record_risk_capacity_snapshot_on(
    connection: &mut SqliteConnection,
    snapshot: NewRiskCapacitySnapshot,
) -> Result<ProjectionWriteOutcome, StoreError> {
    validate_capacity(&snapshot.capacity, snapshot.recorded_at)?;
    let capacity = &snapshot.capacity;
    let key = capacity_key(&capacity.account_id, &capacity.strategy_id);
    let payload = CanonicalJson::from_serializable(capacity)?;

    let inserted = sqlx::query(
        "INSERT INTO risk_capacity_snapshots ( \
             account_id, strategy_id, observed_at, payload_json, payload_hash, recorded_at \
         ) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
    )
    .bind(capacity.account_id.as_str())
    .bind(capacity.strategy_id.as_str())
    .bind(capacity.observed_at)
    .bind(payload.as_str())
    .bind(payload.sha256_hex())
    .bind(snapshot.recorded_at)
    .execute(&mut *connection)
    .await?;

    if inserted.rows_affected() == 0 {
        let existing = fetch_risk_capacity_fact_on(
            connection,
            &capacity.account_id,
            &capacity.strategy_id,
            capacity.observed_at,
        )
        .await?
        .ok_or_else(|| {
            StoreError::corrupt(
                "risk_capacity_snapshot",
                &key,
                "conflicting fact disappeared",
            )
        })?;
        if existing.payload != payload || existing.recorded_at != snapshot.recorded_at {
            return Err(StoreError::ObservationConflict {
                entity: "risk_capacity_snapshot",
                key,
                observed_at: capacity.observed_at,
            });
        }
        ensure_latest_projection(connection, &existing).await?;
        return Ok(ProjectionWriteOutcome::Duplicate);
    }

    let latest = fetch_latest_risk_capacity_snapshot_on(
        connection,
        &capacity.account_id,
        &capacity.strategy_id,
    )
    .await?;
    let outcome = match latest {
        None => ProjectionWriteOutcome::Applied,
        Some(existing) if existing.capacity.observed_at < capacity.observed_at => {
            ProjectionWriteOutcome::Applied
        }
        Some(existing) if existing.capacity.observed_at > capacity.observed_at => {
            ProjectionWriteOutcome::FactAppendedProjectionIgnored
        }
        Some(existing) if existing.payload == payload => {
            ProjectionWriteOutcome::FactAppendedProjectionUnchanged
        }
        Some(_) => {
            return Err(StoreError::ObservationConflict {
                entity: "risk_capacity_snapshot",
                key,
                observed_at: capacity.observed_at,
            });
        }
    };

    if outcome == ProjectionWriteOutcome::Applied {
        sqlx::query(
            "INSERT INTO risk_capacity_snapshots_latest ( \
                 account_id, strategy_id, observed_at, payload_json, payload_hash, recorded_at \
             ) VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT(account_id, strategy_id) DO UPDATE SET \
                 observed_at = excluded.observed_at, \
                 payload_json = excluded.payload_json, \
                 payload_hash = excluded.payload_hash, \
                 recorded_at = excluded.recorded_at \
             WHERE excluded.observed_at > risk_capacity_snapshots_latest.observed_at",
        )
        .bind(capacity.account_id.as_str())
        .bind(capacity.strategy_id.as_str())
        .bind(capacity.observed_at)
        .bind(payload.as_str())
        .bind(payload.sha256_hex())
        .bind(snapshot.recorded_at)
        .execute(connection)
        .await?;
    }
    Ok(outcome)
}

pub(crate) async fn fetch_latest_risk_capacity_snapshot_on(
    connection: &mut SqliteConnection,
    account_id: &AccountId,
    strategy_id: &StrategyId,
) -> Result<Option<StoredRiskCapacitySnapshot>, StoreError> {
    let row = sqlx::query(
        "SELECT account_id, strategy_id, observed_at, payload_json, payload_hash, recorded_at \
         FROM risk_capacity_snapshots_latest WHERE account_id = ? AND strategy_id = ?",
    )
    .bind(account_id.as_str())
    .bind(strategy_id.as_str())
    .fetch_optional(connection)
    .await?;
    row.map(|row| decode_capacity_row("risk_capacity_snapshot_latest", row))
        .transpose()
}

async fn fetch_risk_capacity_fact_on(
    connection: &mut SqliteConnection,
    account_id: &AccountId,
    strategy_id: &StrategyId,
    observed_at: i64,
) -> Result<Option<StoredRiskCapacitySnapshot>, StoreError> {
    let row = sqlx::query(
        "SELECT account_id, strategy_id, observed_at, payload_json, payload_hash, recorded_at \
         FROM risk_capacity_snapshots \
         WHERE account_id = ? AND strategy_id = ? AND observed_at = ?",
    )
    .bind(account_id.as_str())
    .bind(strategy_id.as_str())
    .bind(observed_at)
    .fetch_optional(connection)
    .await?;
    row.map(|row| decode_capacity_row("risk_capacity_snapshot", row))
        .transpose()
}

fn decode_capacity_row(
    entity: &'static str,
    row: sqlx::sqlite::SqliteRow,
) -> Result<StoredRiskCapacitySnapshot, StoreError> {
    let account_id: String = row.try_get("account_id")?;
    let strategy_id: String = row.try_get("strategy_id")?;
    let key = format!("{account_id}:{strategy_id}");
    let observed_at: i64 = row.try_get("observed_at")?;
    let recorded_at: i64 = row.try_get("recorded_at")?;
    let payload = CanonicalJson::from_stored(
        entity,
        &key,
        row.try_get("payload_json")?,
        row.try_get("payload_hash")?,
    )?;
    let capacity: RiskCapacity = serde_json::from_str(payload.as_str())
        .map_err(|error| StoreError::corrupt(entity, &key, error.to_string()))?;
    validate_capacity(&capacity, recorded_at)
        .map_err(|error| StoreError::corrupt(entity, &key, error.to_string()))?;
    if capacity.account_id.as_str() != account_id
        || capacity.strategy_id.as_str() != strategy_id
        || capacity.observed_at != observed_at
    {
        return Err(StoreError::corrupt(
            entity,
            key,
            "payload identity does not match denormalized columns",
        ));
    }
    Ok(StoredRiskCapacitySnapshot {
        capacity,
        payload,
        recorded_at,
    })
}

async fn ensure_latest_projection(
    connection: &mut SqliteConnection,
    fact: &StoredRiskCapacitySnapshot,
) -> Result<(), StoreError> {
    let latest = fetch_latest_risk_capacity_snapshot_on(
        connection,
        &fact.capacity.account_id,
        &fact.capacity.strategy_id,
    )
    .await?;
    if latest
        .as_ref()
        .is_some_and(|latest| latest.capacity.observed_at >= fact.capacity.observed_at)
    {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO risk_capacity_snapshots_latest ( \
             account_id, strategy_id, observed_at, payload_json, payload_hash, recorded_at \
         ) VALUES (?, ?, ?, ?, ?, ?) \
         ON CONFLICT(account_id, strategy_id) DO UPDATE SET \
             observed_at = excluded.observed_at, payload_json = excluded.payload_json, \
             payload_hash = excluded.payload_hash, recorded_at = excluded.recorded_at \
         WHERE excluded.observed_at > risk_capacity_snapshots_latest.observed_at",
    )
    .bind(fact.capacity.account_id.as_str())
    .bind(fact.capacity.strategy_id.as_str())
    .bind(fact.capacity.observed_at)
    .bind(fact.payload.as_str())
    .bind(fact.payload.sha256_hex())
    .bind(fact.recorded_at)
    .execute(connection)
    .await?;
    Ok(())
}

fn validate_capacity(capacity: &RiskCapacity, recorded_at: i64) -> Result<(), StoreError> {
    let key = capacity_key(&capacity.account_id, &capacity.strategy_id);
    if capacity.account_id.as_str().trim().is_empty()
        || capacity.strategy_id.as_str().trim().is_empty()
    {
        return Err(StoreError::InvalidRecord {
            entity: "risk_capacity_snapshot",
            key,
            reason: "account_id and strategy_id must not be empty".to_owned(),
        });
    }
    if capacity.observed_at < 0 || recorded_at < capacity.observed_at {
        return Err(StoreError::InvalidRecord {
            entity: "risk_capacity_snapshot",
            key,
            reason: "observed_at must be non-negative and not exceed recorded_at".to_owned(),
        });
    }
    for (field, value) in [
        ("daily_realized_loss_pct", capacity.daily_realized_loss_pct),
        ("equity_drawdown_pct", capacity.equity_drawdown_pct),
        (
            "remaining_account_risk_pct",
            capacity.remaining_account_risk_pct,
        ),
        (
            "remaining_portfolio_risk_pct",
            capacity.remaining_portfolio_risk_pct,
        ),
    ] {
        if !value.is_finite() || value < 0.0 {
            return Err(StoreError::InvalidRecord {
                entity: "risk_capacity_snapshot",
                key,
                reason: format!("{field} must be finite and non-negative"),
            });
        }
    }
    Ok(())
}

fn capacity_key(account_id: &AccountId, strategy_id: &StrategyId) -> String {
    format!("{account_id}:{strategy_id}")
}
