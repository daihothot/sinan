use std::collections::{BTreeMap, BTreeSet};

use serde::de::DeserializeOwned;
use sinan_types::{
    AccountId, AccountSnapshot, ExecutionAction, ExecutionCommandState, ExecutionCommandStatus,
    IntentId, OrderSnapshot, OrderSnapshotStatus, PositionSnapshot, SymbolCode,
    SymbolMetadataSnapshot, TradeIntentStatus,
};
use sqlx::{Row, SqliteConnection};

use crate::{
    reconciliation::fetch_checkpoint_on,
    repository::{
        fetch_execution_command_by_id, fetch_execution_command_state_by_id,
        fetch_latest_circuit_breaker_snapshot, fetch_trade_intent_by_id,
        validate_persisted_command_state,
    },
    risk_capacity::fetch_latest_risk_capacity_snapshot_on,
    AccountMarketSnapshot, AccountReconciliationCheckpoint, CanonicalJson, StoreError,
    StoredCircuitBreakerSnapshot, StoredExecutionCommand, StoredRiskCapacitySnapshot,
    StoredTradeIntent, WriteTransaction,
};

#[derive(Clone, Debug, PartialEq)]
pub struct PendingCommandSnapshot {
    pub command: StoredExecutionCommand,
    pub state: ExecutionCommandState,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TrustedRiskSnapshot {
    pub intent: StoredTradeIntent,
    pub account: AccountSnapshot,
    pub checkpoint: AccountReconciliationCheckpoint,
    pub positions: Vec<PositionSnapshot>,
    pub orders: Vec<OrderSnapshot>,
    pub symbol_metadata: Vec<SymbolMetadataSnapshot>,
    pub markets: Vec<AccountMarketSnapshot>,
    pub pending_commands: Vec<PendingCommandSnapshot>,
    pub circuit_breaker: Option<StoredCircuitBreakerSnapshot>,
    pub capacity: StoredRiskCapacitySnapshot,
}

impl WriteTransaction {
    /// Selects one initial hard-risk work item deterministically. `LIMIT 1`
    /// applies only to work scheduling, never to any risk input collection.
    pub async fn next_pending_risk_intent_id(&mut self) -> Result<Option<IntentId>, StoreError> {
        let intent_id: Option<String> = sqlx::query_scalar(
            "SELECT i.intent_id FROM trade_intents i \
             JOIN account_snapshots_latest a ON a.account_id = i.account_id \
             JOIN account_reconciliation_checkpoints c ON c.account_id = i.account_id \
             JOIN risk_capacity_snapshots_latest capacity \
               ON capacity.account_id = i.account_id \
              AND capacity.strategy_id = i.strategy_id \
             WHERE i.status = 'ACCEPTED' \
               AND i.decision_timestamp IS NOT NULL \
               AND c.pending_commands_reconciled_at IS NOT NULL \
               AND NOT EXISTS (SELECT 1 FROM risk_results r WHERE r.intent_id = i.intent_id) \
               AND NOT EXISTS (\
                 SELECT 1 FROM position_snapshots_latest p \
                 WHERE p.account_id = i.account_id \
                   AND p.observed_at > c.positions_observed_at\
               ) \
               AND NOT EXISTS (\
                 SELECT 1 FROM order_snapshots_latest o \
                 WHERE o.account_id = i.account_id \
                   AND o.observed_at > c.orders_observed_at\
               ) \
               AND NOT EXISTS (\
                 SELECT 1 FROM execution_commands command \
                 LEFT JOIN execution_command_states state \
                   ON state.command_id = command.command_id \
                 WHERE command.account_id = i.account_id \
                   AND (state.command_id IS NULL \
                     OR state.updated_at > c.pending_commands_reconciled_at)\
               ) \
               AND EXISTS (\
                 SELECT 1 FROM circuit_breaker_snapshots breaker \
                 WHERE breaker.scope = 'GLOBAL'\
               ) \
               AND (\
                 (json_type(i.payload_json, '$.proposed_legs') = 'array' \
                   AND NOT EXISTS (\
                     SELECT 1 FROM json_each(i.payload_json, '$.proposed_legs') leg \
                     WHERE NOT EXISTS (\
                       SELECT 1 FROM symbol_metadata_latest metadata \
                       WHERE metadata.account_id = i.account_id \
                         AND metadata.symbol = json_extract(leg.value, '$.symbol')\
                     ) OR NOT EXISTS (\
                       SELECT 1 FROM market_snapshots market \
                       WHERE market.account_id = i.account_id \
                         AND market.symbol = json_extract(leg.value, '$.symbol')\
                     )\
                   )) \
                 OR ((json_type(i.payload_json, '$.proposed_legs') IS NULL \
                       OR json_type(i.payload_json, '$.proposed_legs') = 'null') \
                   AND EXISTS (\
                     SELECT 1 FROM symbol_metadata_latest metadata \
                     WHERE metadata.account_id = i.account_id AND metadata.symbol = i.symbol\
                   ) \
                   AND EXISTS (\
                     SELECT 1 FROM market_snapshots market \
                     WHERE market.account_id = i.account_id AND market.symbol = i.symbol\
                   ))\
               ) \
               AND NOT EXISTS (\
                 SELECT 1 FROM position_snapshots_latest position \
                 WHERE position.account_id = i.account_id \
                   AND (NOT EXISTS (\
                     SELECT 1 FROM symbol_metadata_latest metadata \
                     WHERE metadata.account_id = i.account_id \
                       AND metadata.symbol = position.symbol\
                   ) OR NOT EXISTS (\
                     SELECT 1 FROM market_snapshots market \
                     WHERE market.account_id = i.account_id \
                       AND market.symbol = position.symbol\
                   ))\
               ) \
               AND NOT EXISTS (\
                 SELECT 1 FROM order_snapshots_latest orders \
                 WHERE orders.account_id = i.account_id \
                   AND json_extract(orders.payload_json, '$.status') \
                     IN ('PLACED', 'PARTIALLY_FILLED', 'UNKNOWN') \
                   AND (NOT EXISTS (\
                     SELECT 1 FROM symbol_metadata_latest metadata \
                     WHERE metadata.account_id = i.account_id \
                       AND metadata.symbol = json_extract(orders.payload_json, '$.symbol')\
                   ) OR NOT EXISTS (\
                     SELECT 1 FROM market_snapshots market \
                     WHERE market.account_id = i.account_id \
                       AND market.symbol = json_extract(orders.payload_json, '$.symbol')\
                   ))\
               ) \
               AND NOT EXISTS (\
                 SELECT 1 FROM execution_commands command \
                 JOIN execution_command_states state ON state.command_id = command.command_id \
                 WHERE command.account_id = i.account_id \
                   AND command.action IN ('BUY', 'SELL') \
                   AND state.status NOT IN (\
                     'DELIVERY_FAILED', 'REJECTED', 'FILLED', 'FAILED', 'EXPIRED', 'CANCELLED'\
                   ) \
                   AND (NOT EXISTS (\
                     SELECT 1 FROM symbol_metadata_latest metadata \
                     WHERE metadata.account_id = i.account_id \
                       AND metadata.symbol = command.symbol\
                   ) OR NOT EXISTS (\
                     SELECT 1 FROM market_snapshots market \
                     WHERE market.account_id = i.account_id \
                       AND market.symbol = command.symbol\
                   ))\
               ) \
             ORDER BY i.requested_at, i.intent_id LIMIT 1",
        )
        .fetch_optional(self.connection())
        .await?;
        Ok(intent_id.map(IntentId::from))
    }

    /// Loads all mutable hard-risk inputs and the work eligibility decision from
    /// this transaction's single `BEGIN IMMEDIATE` snapshot.
    pub async fn load_trusted_risk_snapshot(
        &mut self,
        intent_id: &IntentId,
    ) -> Result<Option<TrustedRiskSnapshot>, StoreError> {
        load_trusted_risk_snapshot_on(self.connection(), intent_id).await
    }
}

async fn load_trusted_risk_snapshot_on(
    connection: &mut SqliteConnection,
    intent_id: &IntentId,
) -> Result<Option<TrustedRiskSnapshot>, StoreError> {
    let Some(intent) = fetch_trade_intent_by_id(&mut *connection, intent_id).await? else {
        return Err(StoreError::NotFound {
            entity: "trade_intent",
            key: intent_id.to_string(),
        });
    };
    if intent.status != TradeIntentStatus::Accepted
        || risk_result_exists(connection, intent_id).await?
    {
        return Ok(None);
    }

    let account_id = &intent.intent.account_id;
    let account = load_account(connection, account_id)
        .await?
        .ok_or_else(|| unavailable(intent_id, "account snapshot is unavailable"))?;
    let checkpoint = fetch_checkpoint_on(connection, account_id)
        .await?
        .ok_or_else(|| unavailable(intent_id, "reconciliation checkpoint is unavailable"))?;
    let pending_watermark = checkpoint.pending_commands_reconciled_at.ok_or_else(|| {
        unavailable(
            intent_id,
            "pending-command reconciliation watermark is unavailable",
        )
    })?;

    reject_partial_state_after_checkpoint(connection, intent_id, &checkpoint).await?;
    let positions = load_position_members(connection, &checkpoint).await?;
    let orders = load_order_members(connection, &checkpoint).await?;
    let pending_commands =
        load_pending_commands(connection, intent_id, account_id, pending_watermark).await?;
    let required_symbols = required_symbols(&intent, &positions, &orders, &pending_commands);
    let symbol_metadata =
        load_required_metadata(connection, intent_id, account_id, &required_symbols).await?;
    let markets =
        load_required_markets(connection, intent_id, account_id, &required_symbols).await?;
    let circuit_breaker = fetch_latest_circuit_breaker_snapshot(&mut *connection).await?;
    let capacity =
        fetch_latest_risk_capacity_snapshot_on(connection, account_id, &intent.intent.strategy_id)
            .await?
            .ok_or_else(|| unavailable(intent_id, "risk capacity is unavailable"))?;

    Ok(Some(TrustedRiskSnapshot {
        intent,
        account,
        checkpoint,
        positions,
        orders,
        symbol_metadata,
        markets,
        pending_commands,
        circuit_breaker,
        capacity,
    }))
}

async fn risk_result_exists(
    connection: &mut SqliteConnection,
    intent_id: &IntentId,
) -> Result<bool, StoreError> {
    Ok(sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM risk_results WHERE intent_id = ?)",
    )
    .bind(intent_id.as_str())
    .fetch_one(connection)
    .await?)
}

async fn load_account(
    connection: &mut SqliteConnection,
    account_id: &AccountId,
) -> Result<Option<AccountSnapshot>, StoreError> {
    let row = sqlx::query(
        "SELECT account_id, observed_at, payload_json, payload_hash \
         FROM account_snapshots_latest WHERE account_id = ?",
    )
    .bind(account_id.as_str())
    .fetch_optional(connection)
    .await?;
    row.map(|row| {
        let value: AccountSnapshot = decode_payload("account_snapshot", account_id.as_str(), &row)?;
        let stored_at: i64 = row.try_get("observed_at")?;
        let stored_account: String = row.try_get("account_id")?;
        if value.account_id != *account_id
            || stored_account != account_id.as_str()
            || value.observed_at != stored_at
        {
            return Err(StoreError::corrupt(
                "account_snapshot",
                account_id.to_string(),
                "payload identity does not match projection columns",
            ));
        }
        Ok(value)
    })
    .transpose()
}

async fn reject_partial_state_after_checkpoint(
    connection: &mut SqliteConnection,
    intent_id: &IntentId,
    checkpoint: &AccountReconciliationCheckpoint,
) -> Result<(), StoreError> {
    let newer_position: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM position_snapshots_latest \
         WHERE account_id = ? AND observed_at > ?)",
    )
    .bind(checkpoint.account_id.as_str())
    .bind(checkpoint.positions_observed_at)
    .fetch_one(&mut *connection)
    .await?;
    if newer_position {
        return Err(unavailable(
            intent_id,
            "position snapshot changed after the full-set watermark",
        ));
    }
    let newer_order: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM order_snapshots_latest \
         WHERE account_id = ? AND observed_at > ?)",
    )
    .bind(checkpoint.account_id.as_str())
    .bind(checkpoint.orders_observed_at)
    .fetch_one(connection)
    .await?;
    if newer_order {
        return Err(unavailable(
            intent_id,
            "order snapshot changed after the full-set watermark",
        ));
    }
    Ok(())
}

async fn load_position_members(
    connection: &mut SqliteConnection,
    checkpoint: &AccountReconciliationCheckpoint,
) -> Result<Vec<PositionSnapshot>, StoreError> {
    let rows = sqlx::query(
        "SELECT account_id, set_observed_at, position_id, payload_json, payload_hash \
         FROM reconciliation_position_set_members WHERE account_id = ? ORDER BY position_id",
    )
    .bind(checkpoint.account_id.as_str())
    .fetch_all(connection)
    .await?;
    rows.into_iter()
        .map(|row| {
            let position_id: String = row.try_get("position_id")?;
            let key = format!("{}:{position_id}", checkpoint.account_id);
            let value: PositionSnapshot =
                decode_payload("reconciliation_position_set_member", &key, &row)?;
            if row.try_get::<String, _>("account_id")? != checkpoint.account_id.as_str()
                || row.try_get::<i64, _>("set_observed_at")? != checkpoint.positions_observed_at
                || value.account_id != checkpoint.account_id
                || value.position_id.as_str() != position_id
                || value.observed_at != checkpoint.positions_observed_at
            {
                return Err(StoreError::corrupt(
                    "reconciliation_position_set_member",
                    key,
                    "payload identity or watermark does not match checkpoint",
                ));
            }
            Ok(value)
        })
        .collect()
}

async fn load_order_members(
    connection: &mut SqliteConnection,
    checkpoint: &AccountReconciliationCheckpoint,
) -> Result<Vec<OrderSnapshot>, StoreError> {
    let rows = sqlx::query(
        "SELECT account_id, set_observed_at, broker_order_id, payload_json, payload_hash \
         FROM reconciliation_order_set_members WHERE account_id = ? ORDER BY broker_order_id",
    )
    .bind(checkpoint.account_id.as_str())
    .fetch_all(connection)
    .await?;
    rows.into_iter()
        .map(|row| {
            let order_id: String = row.try_get("broker_order_id")?;
            let key = format!("{}:{order_id}", checkpoint.account_id);
            let value: OrderSnapshot =
                decode_payload("reconciliation_order_set_member", &key, &row)?;
            if row.try_get::<String, _>("account_id")? != checkpoint.account_id.as_str()
                || row.try_get::<i64, _>("set_observed_at")? != checkpoint.orders_observed_at
                || value.account_id != checkpoint.account_id
                || value.broker_order_id.as_str() != order_id
                || value.observed_at != checkpoint.orders_observed_at
            {
                return Err(StoreError::corrupt(
                    "reconciliation_order_set_member",
                    key,
                    "payload identity or watermark does not match checkpoint",
                ));
            }
            Ok(value)
        })
        .collect()
}

async fn load_pending_commands(
    connection: &mut SqliteConnection,
    intent_id: &IntentId,
    account_id: &AccountId,
    reconciled_at: i64,
) -> Result<Vec<PendingCommandSnapshot>, StoreError> {
    let missing_state: Option<String> = sqlx::query_scalar(
        "SELECT c.command_id FROM execution_commands c \
         LEFT JOIN execution_command_states s ON s.command_id = c.command_id \
         WHERE c.account_id = ? AND s.command_id IS NULL ORDER BY c.command_id LIMIT 1",
    )
    .bind(account_id.as_str())
    .fetch_optional(&mut *connection)
    .await?;
    if let Some(command_id) = missing_state {
        return Err(StoreError::corrupt(
            "risk_input.pending_command",
            command_id,
            "execution command has no lifecycle state",
        ));
    }
    let newer_state: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM execution_command_states \
         WHERE account_id = ? AND updated_at > ?)",
    )
    .bind(account_id.as_str())
    .bind(reconciled_at)
    .fetch_one(&mut *connection)
    .await?;
    if newer_state {
        return Err(unavailable(
            intent_id,
            "command lifecycle changed after the reconciliation watermark",
        ));
    }

    let command_ids: Vec<String> = sqlx::query_scalar(
        "SELECT c.command_id FROM execution_commands c \
         JOIN execution_command_states s ON s.command_id = c.command_id \
         WHERE c.account_id = ? AND s.status NOT IN \
           ('DELIVERY_FAILED', 'REJECTED', 'FILLED', 'FAILED', 'EXPIRED', 'CANCELLED') \
         ORDER BY c.command_id",
    )
    .bind(account_id.as_str())
    .fetch_all(&mut *connection)
    .await?;
    let mut snapshots = Vec::with_capacity(command_ids.len());
    for command_id in command_ids {
        let command_id = sinan_types::CommandId::from(command_id);
        let command = fetch_execution_command_by_id(&mut *connection, &command_id)
            .await?
            .ok_or_else(|| {
                StoreError::corrupt(
                    "risk_input.pending_command",
                    command_id.to_string(),
                    "command disappeared while loading one transaction",
                )
            })?;
        let state = fetch_execution_command_state_by_id(&mut *connection, &command_id)
            .await?
            .ok_or_else(|| {
                StoreError::corrupt(
                    "risk_input.pending_command",
                    command_id.to_string(),
                    "command state disappeared while loading one transaction",
                )
            })?;
        validate_persisted_command_state(&state, &command).map_err(|reason| {
            StoreError::corrupt("risk_input.pending_command", command_id.to_string(), reason)
        })?;
        if command.command.account_id != *account_id || state.account_id != *account_id {
            return Err(StoreError::corrupt(
                "risk_input.pending_command",
                command_id.to_string(),
                "command or state belongs to another account",
            ));
        }
        snapshots.push(PendingCommandSnapshot { command, state });
    }
    Ok(snapshots)
}

fn required_symbols(
    intent: &StoredTradeIntent,
    positions: &[PositionSnapshot],
    orders: &[OrderSnapshot],
    commands: &[PendingCommandSnapshot],
) -> BTreeSet<SymbolCode> {
    let mut symbols = BTreeSet::new();
    if let Some(legs) = &intent.intent.proposed_legs {
        symbols.extend(legs.iter().map(|leg| leg.symbol.clone()));
    } else {
        symbols.insert(intent.intent.symbol.clone());
    }
    symbols.extend(positions.iter().map(|position| position.symbol.clone()));
    symbols.extend(
        orders
            .iter()
            .filter(|order| order_is_active(order.status))
            .map(|order| order.symbol.clone()),
    );
    symbols.extend(
        commands
            .iter()
            .filter(|value| {
                matches!(
                    value.command.command.action,
                    ExecutionAction::Buy | ExecutionAction::Sell
                ) && !command_state_is_terminal(value.state.status)
            })
            .map(|value| value.command.command.symbol.clone()),
    );
    symbols
}

async fn load_required_metadata(
    connection: &mut SqliteConnection,
    intent_id: &IntentId,
    account_id: &AccountId,
    required: &BTreeSet<SymbolCode>,
) -> Result<Vec<SymbolMetadataSnapshot>, StoreError> {
    let rows = sqlx::query(
        "SELECT account_id, broker_symbol, symbol, observed_at, payload_json, payload_hash \
         FROM symbol_metadata_latest WHERE account_id = ? ORDER BY broker_symbol",
    )
    .bind(account_id.as_str())
    .fetch_all(connection)
    .await?;
    let mut by_symbol = BTreeMap::new();
    for row in rows {
        let broker_symbol: String = row.try_get("broker_symbol")?;
        let key = format!("{account_id}:{broker_symbol}");
        let value: SymbolMetadataSnapshot = decode_payload("symbol_metadata", &key, &row)?;
        if value.account_id != *account_id
            || value.broker_symbol != broker_symbol
            || value.symbol.as_str() != row.try_get::<String, _>("symbol")?
            || value.observed_at != row.try_get::<i64, _>("observed_at")?
        {
            return Err(StoreError::corrupt(
                "symbol_metadata",
                key,
                "payload identity does not match projection columns",
            ));
        }
        if required.contains(&value.symbol)
            && by_symbol.insert(value.symbol.clone(), value).is_some()
        {
            return Err(StoreError::IdentityConflict {
                entity: "risk_input.symbol_metadata",
                key: account_id.to_string(),
            });
        }
    }
    let missing: Vec<_> = required
        .iter()
        .filter(|symbol| !by_symbol.contains_key(*symbol))
        .map(ToString::to_string)
        .collect();
    if !missing.is_empty() {
        return Err(unavailable(
            intent_id,
            "required symbol metadata set is incomplete",
        ));
    }
    Ok(by_symbol.into_values().collect())
}

async fn load_required_markets(
    connection: &mut SqliteConnection,
    intent_id: &IntentId,
    account_id: &AccountId,
    required: &BTreeSet<SymbolCode>,
) -> Result<Vec<AccountMarketSnapshot>, StoreError> {
    let rows = sqlx::query(
        "SELECT account_id, symbol, observed_at, payload_json, payload_hash \
         FROM market_snapshots WHERE account_id = ? ORDER BY symbol",
    )
    .bind(account_id.as_str())
    .fetch_all(connection)
    .await?;
    let mut values = BTreeMap::new();
    for row in rows {
        let symbol: String = row.try_get("symbol")?;
        let key = format!("{account_id}:{symbol}");
        let value: sinan_types::MarketSnapshot = decode_payload("market_snapshot", &key, &row)?;
        if row.try_get::<String, _>("account_id")? != account_id.as_str()
            || value.symbol.as_str() != symbol
            || value.observed_at != row.try_get::<i64, _>("observed_at")?
        {
            return Err(StoreError::corrupt(
                "market_snapshot",
                key,
                "payload identity does not match projection columns",
            ));
        }
        if required.contains(&value.symbol) {
            let symbol = value.symbol.clone();
            if values
                .insert(
                    symbol,
                    AccountMarketSnapshot {
                        account_id: account_id.clone(),
                        snapshot: value,
                    },
                )
                .is_some()
            {
                return Err(StoreError::IdentityConflict {
                    entity: "risk_input.market_snapshot",
                    key: account_id.to_string(),
                });
            }
        }
    }
    let missing: Vec<_> = required
        .iter()
        .filter(|symbol| !values.contains_key(*symbol))
        .map(ToString::to_string)
        .collect();
    if !missing.is_empty() {
        return Err(unavailable(
            intent_id,
            "required market snapshot set is incomplete",
        ));
    }
    Ok(values.into_values().collect())
}

fn decode_payload<T: DeserializeOwned>(
    entity: &'static str,
    key: &str,
    row: &sqlx::sqlite::SqliteRow,
) -> Result<T, StoreError> {
    let payload = CanonicalJson::from_stored(
        entity,
        key,
        row.try_get("payload_json")?,
        row.try_get("payload_hash")?,
    )?;
    serde_json::from_str(payload.as_str())
        .map_err(|error| StoreError::corrupt(entity, key, error.to_string()))
}

const fn order_is_active(status: OrderSnapshotStatus) -> bool {
    matches!(
        status,
        OrderSnapshotStatus::Placed
            | OrderSnapshotStatus::PartiallyFilled
            | OrderSnapshotStatus::Unknown
    )
}

const fn command_state_is_terminal(status: ExecutionCommandStatus) -> bool {
    matches!(
        status,
        ExecutionCommandStatus::DeliveryFailed
            | ExecutionCommandStatus::Rejected
            | ExecutionCommandStatus::Filled
            | ExecutionCommandStatus::Failed
            | ExecutionCommandStatus::Expired
            | ExecutionCommandStatus::Cancelled
    )
}

fn unavailable(intent_id: &IntentId, input: &'static str) -> StoreError {
    StoreError::SnapshotUnavailable {
        intent_id: intent_id.to_string(),
        input,
    }
}
