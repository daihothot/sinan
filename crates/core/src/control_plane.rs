use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::Value;
use sinan_http::{
    AuthorizedControlPlaneQuery, CircuitBreakerReason as HttpCircuitBreakerReason,
    CircuitBreakerStatus as HttpCircuitBreakerStatus, CircuitBreakerSummary, ClockHealth,
    CommandPayloadDisclosure, ControlPlaneFuture, ControlPlanePortError, ControlPlaneQueryPort,
    ExecutionCommandStatusResponse, ExecutionStateSummary, RiskStateSummary,
    ScopedExecutionCommandStatus, ScopedTradeIntentStatus, SessionSummary, SessionSummaryStatus,
    SubmitTradeIntentCommand, TradeIntentApplicationPort, TradeIntentIntakeOutcome,
    TradeIntentIntakeRecord, TradeIntentStateRef, TradeIntentStatusResponse,
    TradingCoreStateResponse, TradingCoreTimePolicy,
};
use sinan_risk::{restore_circuit_breaker_snapshot, CircuitBreakerReason, CircuitBreakerStatus};
use sinan_store::{
    ControlPlaneStateLimits, NewTradeIntent, SqliteStateStore, StoreError, WriteOutcome,
};
use sinan_types::{ErrorCode, ErrorCodeOrString, TradeIntentAction, TradeIntentStatus};

pub trait TradingCoreClock: Send + Sync {
    fn now_ms(&self) -> i64;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemTradingCoreClock;

impl TradingCoreClock for SystemTradingCoreClock {
    fn now_ms(&self) -> i64 {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_millis());
        i64::try_from(millis).unwrap_or(i64::MAX)
    }
}

impl sinan_http::HttpServerClock for SystemTradingCoreClock {
    fn now_ms(&self) -> i64 {
        TradingCoreClock::now_ms(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqliteControlPlaneServiceConfig {
    pub state_limits: ControlPlaneStateLimits,
    pub command_event_limit: usize,
    pub time_policy: TradingCoreTimePolicy,
}

impl Default for SqliteControlPlaneServiceConfig {
    fn default() -> Self {
        Self {
            state_limits: ControlPlaneStateLimits::default(),
            command_event_limit: 256,
            time_policy: TradingCoreTimePolicy {
                clock_health: ClockHealth::Healthy,
                max_internal_server_skew_ms: 250,
                max_decision_time_skew_ms: 2_000,
                max_decision_time_sync_age_ms: 30_000,
                max_decision_time_sync_rtt_ms: 1_000,
                control_plane_time_sync_interval_ms: 10_000,
                max_decision_intent_age_ms: 30_000,
            },
        }
    }
}

#[derive(Clone)]
pub struct SqliteControlPlaneService {
    store: SqliteStateStore,
    clock: Arc<dyn TradingCoreClock>,
    config: SqliteControlPlaneServiceConfig,
}

impl SqliteControlPlaneService {
    pub fn new(
        store: SqliteStateStore,
        clock: Arc<dyn TradingCoreClock>,
        config: SqliteControlPlaneServiceConfig,
    ) -> Result<Self, ControlPlaneServiceConfigurationError> {
        validate_config(&config)?;
        Ok(Self {
            store,
            clock,
            config,
        })
    }

    pub fn store(&self) -> &SqliteStateStore {
        &self.store
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ControlPlaneServiceConfigurationError {
    #[error("{0} must be greater than zero")]
    NonPositive(&'static str),
    #[error("{0} must not be negative")]
    Negative(&'static str),
}

fn validate_config(
    config: &SqliteControlPlaneServiceConfig,
) -> Result<(), ControlPlaneServiceConfigurationError> {
    if config.command_event_limit == 0 {
        return Err(ControlPlaneServiceConfigurationError::NonPositive(
            "command_event_limit",
        ));
    }
    for (field, value) in [
        (
            "max_internal_server_skew_ms",
            config.time_policy.max_internal_server_skew_ms,
        ),
        (
            "max_decision_time_skew_ms",
            config.time_policy.max_decision_time_skew_ms,
        ),
        (
            "max_decision_time_sync_age_ms",
            config.time_policy.max_decision_time_sync_age_ms,
        ),
        (
            "max_decision_time_sync_rtt_ms",
            config.time_policy.max_decision_time_sync_rtt_ms,
        ),
        (
            "control_plane_time_sync_interval_ms",
            config.time_policy.control_plane_time_sync_interval_ms,
        ),
        (
            "max_decision_intent_age_ms",
            config.time_policy.max_decision_intent_age_ms,
        ),
    ] {
        if value < 0 {
            return Err(ControlPlaneServiceConfigurationError::Negative(field));
        }
    }
    Ok(())
}

impl TradeIntentApplicationPort for SqliteControlPlaneService {
    fn submit_trade_intent(
        &self,
        command: SubmitTradeIntentCommand,
    ) -> ControlPlaneFuture<'_, Result<TradeIntentIntakeOutcome, ControlPlanePortError>> {
        Box::pin(async move {
            if !command
                .principal
                .account_scope()
                .contains(&command.intent.account_id)
            {
                return Err(ControlPlanePortError::Internal);
            }
            let now = self.clock.now_ms();
            validate_intent_for_intake(&command.intent, now, &self.config.time_policy)?;
            let account_scope = command.principal.account_scope().clone();
            let outcome = self
                .store
                .insert_trade_intent(NewTradeIntent {
                    intent: command.intent.clone(),
                    initial_status: TradeIntentStatus::Accepted,
                    recorded_at: now,
                })
                .await
                .map_err(map_intake_store_error)?;

            match outcome {
                WriteOutcome::Inserted(stored) => Ok(TradeIntentIntakeOutcome::Inserted(
                    TradeIntentIntakeRecord {
                        accepted_at: stored.created_at,
                        state_ref: None,
                    },
                )),
                WriteOutcome::Duplicate(stored) => {
                    let workflow = self
                        .store
                        .get_trade_intent_workflow_status(&account_scope, &stored.intent.intent_id)
                        .await
                        .map_err(map_query_store_error)?;
                    let state_ref = workflow.and_then(|workflow| {
                        let risk_id = workflow
                            .latest_risk_result
                            .as_ref()
                            .map(|result| result.result.risk_id.clone());
                        let plan_id = workflow.map_plan_id();
                        (risk_id.is_some() || plan_id.is_some())
                            .then_some(TradeIntentStateRef { plan_id, risk_id })
                    });
                    Ok(TradeIntentIntakeOutcome::Duplicate(
                        TradeIntentIntakeRecord {
                            accepted_at: stored.created_at,
                            state_ref,
                        },
                    ))
                }
            }
        })
    }
}

trait WorkflowPlanId {
    fn map_plan_id(&self) -> Option<sinan_types::PlanId>;
}

impl WorkflowPlanId for sinan_store::TradeIntentWorkflowStatus {
    fn map_plan_id(&self) -> Option<sinan_types::PlanId> {
        self.plan
            .as_ref()
            .map(|plan| plan.plan.definition.plan_id.clone())
    }
}

impl ControlPlaneQueryPort for SqliteControlPlaneService {
    fn get_state(
        &self,
        query: AuthorizedControlPlaneQuery,
    ) -> ControlPlaneFuture<'_, Result<TradingCoreStateResponse, ControlPlanePortError>> {
        Box::pin(async move {
            let now = self.clock.now_ms();
            let snapshot = self
                .store
                .load_control_plane_state(query.account_scope(), self.config.state_limits)
                .await
                .map_err(map_query_store_error)?;
            let circuit_breaker = snapshot
                .circuit_breaker
                .as_ref()
                .map(|stored| circuit_breaker_summary(stored.payload.as_str(), now));
            Ok(TradingCoreStateResponse {
                server_time: now,
                clock_health: self.config.time_policy.clock_health,
                accounts: snapshot.latest.accounts,
                positions: snapshot.latest.positions,
                orders: snapshot.latest.orders,
                symbols: snapshot.latest.symbols,
                sessions: snapshot
                    .sessions
                    .into_iter()
                    .filter_map(|session| {
                        let status = match session.status {
                            sinan_types::SessionStatus::Active => SessionSummaryStatus::Active,
                            sinan_types::SessionStatus::Stale => SessionSummaryStatus::Stale,
                            sinan_types::SessionStatus::Disconnected => {
                                SessionSummaryStatus::Disconnected
                            }
                            sinan_types::SessionStatus::Rejected => return None,
                        };
                        Some(SessionSummary {
                            session_id: session.session_id,
                            client_id: session.client_id,
                            account_id: session.account_id,
                            terminal_id: session.terminal_id,
                            platform: session.platform,
                            status,
                            clock_sync_status: session.clock_sync_status,
                            last_heartbeat_at: session.last_heartbeat_at,
                        })
                    })
                    .collect(),
                execution: ExecutionStateSummary {
                    open_plans: snapshot
                        .open_plans
                        .into_iter()
                        .map(|plan| plan.plan)
                        .collect(),
                    pending_commands: snapshot.pending_commands,
                    recent_events: snapshot
                        .recent_events
                        .into_iter()
                        .map(|event| event.event)
                        .collect(),
                },
                risk: RiskStateSummary {
                    latest_results: snapshot
                        .latest_risk_results
                        .into_iter()
                        .map(|result| result.result)
                        .collect(),
                    circuit_breaker_active: circuit_breaker
                        .as_ref()
                        .is_some_and(|summary| summary.status != HttpCircuitBreakerStatus::Closed),
                    circuit_breaker,
                },
            })
        })
    }

    fn get_time_policy(
        &self,
        _query: AuthorizedControlPlaneQuery,
    ) -> ControlPlaneFuture<'_, Result<TradingCoreTimePolicy, ControlPlanePortError>> {
        Box::pin(async move { Ok(self.config.time_policy.clone()) })
    }

    fn get_trade_intent_status(
        &self,
        query: AuthorizedControlPlaneQuery,
        intent_id: sinan_types::IntentId,
    ) -> ControlPlaneFuture<'_, Result<Option<ScopedTradeIntentStatus>, ControlPlanePortError>>
    {
        Box::pin(async move {
            let Some(workflow) = self
                .store
                .get_trade_intent_workflow_status(query.account_scope(), &intent_id)
                .await
                .map_err(map_query_store_error)?
            else {
                return Ok(None);
            };
            let risk_id = workflow
                .latest_risk_result
                .as_ref()
                .map(|result| result.result.risk_id.clone());
            let reason = workflow
                .latest_risk_result
                .as_ref()
                .map(|result| result.result.reason.clone())
                .or_else(|| Some(ErrorCodeOrString::from("OK")));
            let plan_id = workflow.map_plan_id();
            let status = effective_trade_intent_status(
                workflow.intent.status,
                workflow
                    .latest_risk_result
                    .as_ref()
                    .map(|stored| &stored.result),
            );
            let updated_at = workflow_updated_at(
                workflow.intent.updated_at,
                workflow.plan.as_ref().map(|plan| plan.updated_at),
                workflow
                    .latest_risk_result
                    .as_ref()
                    .map(|stored| stored.result.evaluated_at),
            );
            Ok(Some(ScopedTradeIntentStatus {
                account_id: workflow.intent.intent.account_id.clone(),
                response: TradeIntentStatusResponse {
                    intent_id: workflow.intent.intent.intent_id,
                    status,
                    reason,
                    risk_id,
                    plan_id,
                    command_ids: workflow.command_ids,
                    created_at: workflow.intent.created_at,
                    updated_at,
                },
            }))
        })
    }

    fn get_execution_command_status(
        &self,
        query: AuthorizedControlPlaneQuery,
        command_id: sinan_types::CommandId,
        disclosure: CommandPayloadDisclosure,
    ) -> ControlPlaneFuture<'_, Result<Option<ScopedExecutionCommandStatus>, ControlPlanePortError>>
    {
        Box::pin(async move {
            let Some(bundle) = self
                .store
                .get_execution_command_status_bundle(
                    query.account_scope(),
                    &command_id,
                    self.config.command_event_limit,
                )
                .await
                .map_err(map_query_store_error)?
            else {
                return Ok(None);
            };
            let account_id = bundle.state.account_id.clone();
            Ok(Some(ScopedExecutionCommandStatus {
                account_id,
                response: ExecutionCommandStatusResponse {
                    command_id,
                    state: bundle.state,
                    command: (disclosure == CommandPayloadDisclosure::IncludeSensitivePayload)
                        .then_some(bundle.command.command),
                    events: bundle.events.into_iter().map(|event| event.event).collect(),
                },
            }))
        })
    }
}

fn validate_intent_for_intake(
    intent: &sinan_types::TradeIntent,
    now: i64,
    policy: &TradingCoreTimePolicy,
) -> Result<(), ControlPlanePortError> {
    if now < 0
        || intent.requested_at < 0
        || intent.signal_expires_at <= intent.requested_at
        || intent.requested_at > now.saturating_add(policy.max_decision_time_skew_ms)
        || now.saturating_sub(intent.requested_at) > policy.max_decision_intent_age_ms
    {
        return Err(unprocessable(
            ErrorCode::TradeIntentTimeInvalid,
            "TradeIntent timestamps are outside the server-time contract",
        ));
    }
    if intent.signal_expires_at <= now {
        return Err(unprocessable(
            ErrorCode::TradeIntentExpired,
            "TradeIntent is outside its admissible time window",
        ));
    }
    let invalid_action_shape = match intent.action {
        TradeIntentAction::Buy | TradeIntentAction::Sell => {
            intent.proposed_risk_pct <= 0.0 || intent.proposed_risk_pct > 100.0
        }
        TradeIntentAction::Hold => {
            intent.proposed_risk_pct != 0.0
                || intent.proposed_sl.is_some()
                || intent.proposed_tp.is_some()
                || intent.proposed_legs.is_some()
        }
        TradeIntentAction::Close => intent.proposed_risk_pct < 0.0,
    };
    if intent.intent_id.is_empty()
        || intent.decision_id.is_empty()
        || intent.strategy_id.is_empty()
        || intent.correlation_id.is_empty()
        || intent.idempotency_key.is_empty()
        || intent.account_id.is_empty()
        || intent.symbol.is_empty()
        || intent.timeframe.is_empty()
        || intent.reason.trim().is_empty()
        || !intent.confidence.is_finite()
        || !(0.0..=1.0).contains(&intent.confidence)
        || !intent.proposed_risk_pct.is_finite()
        || invalid_action_shape
    {
        return Err(unprocessable(
            ErrorCode::RiskInputInvalid,
            "TradeIntent failed durable intake validation",
        ));
    }
    Ok(())
}

fn effective_trade_intent_status(
    stored_status: TradeIntentStatus,
    latest_risk_result: Option<&sinan_types::RiskResult>,
) -> TradeIntentStatus {
    if stored_status == TradeIntentStatus::Accepted
        && latest_risk_result.is_some_and(|result| !result.approved)
    {
        TradeIntentStatus::RiskBlocked
    } else {
        stored_status
    }
}

fn workflow_updated_at(
    intent_updated_at: i64,
    plan_updated_at: Option<i64>,
    risk_evaluated_at: Option<i64>,
) -> i64 {
    plan_updated_at
        .into_iter()
        .chain(risk_evaluated_at)
        .fold(intent_updated_at, i64::max)
}

fn unprocessable(code: ErrorCode, message: &str) -> ControlPlanePortError {
    ControlPlanePortError::Unprocessable {
        code,
        message: message.to_owned(),
        details: None,
    }
}

fn map_intake_store_error(error: StoreError) -> ControlPlanePortError {
    match error {
        StoreError::IdentityConflict { .. } | StoreError::ObservationConflict { .. } => {
            ControlPlanePortError::Conflict {
                code: ErrorCode::IdempotencyKeyConflict,
                message: "TradeIntent identity or idempotency key conflicts with durable state"
                    .to_owned(),
                details: None,
            }
        }
        other => map_query_store_error(other),
    }
}

fn map_query_store_error(error: StoreError) -> ControlPlanePortError {
    match error {
        StoreError::Database(_) | StoreError::Initialization(_) => {
            ControlPlanePortError::Unavailable {
                code: ErrorCode::StateStoreUnavailable,
                message: "State Store is unavailable".to_owned(),
                details: None,
            }
        }
        _ => ControlPlanePortError::Internal,
    }
}

fn circuit_breaker_summary(payload: &str, now: i64) -> CircuitBreakerSummary {
    let outcome = restore_circuit_breaker_snapshot(Some(payload), now);
    let state = outcome.state;
    CircuitBreakerSummary {
        status: match state.status() {
            CircuitBreakerStatus::Closed => HttpCircuitBreakerStatus::Closed,
            CircuitBreakerStatus::Open => HttpCircuitBreakerStatus::Open,
            CircuitBreakerStatus::HalfOpen => HttpCircuitBreakerStatus::HalfOpen,
        },
        reason: match state.reason() {
            CircuitBreakerReason::Ok => HttpCircuitBreakerReason::Ok,
            CircuitBreakerReason::DailyRealizedLossLimit => {
                HttpCircuitBreakerReason::DailyRealizedLossLimit
            }
            CircuitBreakerReason::EquityDrawdownLimit => {
                HttpCircuitBreakerReason::EquityDrawdownLimit
            }
            CircuitBreakerReason::ConsecutiveBrokerRejections => {
                HttpCircuitBreakerReason::ConsecutiveBrokerRejections
            }
            CircuitBreakerReason::ConsecutiveCommandFailures => {
                HttpCircuitBreakerReason::ConsecutiveCommandFailures
            }
            CircuitBreakerReason::ManualReconciliationRequired => {
                HttpCircuitBreakerReason::ManualReconciliationRequired
            }
            CircuitBreakerReason::StoreRecoveryReconciliationPending => {
                HttpCircuitBreakerReason::StoreRecoveryReconciliationPending
            }
            CircuitBreakerReason::TimeSyncUnhealthy => HttpCircuitBreakerReason::TimeSyncUnhealthy,
            CircuitBreakerReason::SnapshotStale => HttpCircuitBreakerReason::SnapshotStale,
            CircuitBreakerReason::SymbolMetadataStale => {
                HttpCircuitBreakerReason::SymbolMetadataStale
            }
            CircuitBreakerReason::ManualTrigger => HttpCircuitBreakerReason::ManualTrigger,
            CircuitBreakerReason::HardRiskViolationDuringRecovery => {
                HttpCircuitBreakerReason::HardRiskViolationDuringRecovery
            }
            CircuitBreakerReason::SafetyInvariantViolation => {
                HttpCircuitBreakerReason::SafetyInvariantViolation
            }
        },
        triggered_at: state.triggered_at_ms(),
        triggered_by: state.triggered_by().map(|source| format!("{source:?}")),
        reset_at: state.reset_at_ms(),
        reset_by: state.reset_by().map(str::to_owned),
        blocked_intent_count: state.blocked_intent_count(),
        metadata: outcome.error.map(|error| {
            BTreeMap::from([("restore_error".to_owned(), Value::String(error.to_string()))])
        }),
    }
}

#[cfg(test)]
mod tests {
    use sinan_http::ControlPlanePortError;
    use sinan_types::{
        AccountId, CorrelationId, DecisionId, ErrorCode, IdempotencyKey, IntentId, StrategyId,
        SymbolCode, TimeframeCode, TradeIntent, TradeIntentAction, TradeIntentStatus,
    };

    use super::{
        effective_trade_intent_status, validate_intent_for_intake, workflow_updated_at,
        SqliteControlPlaneServiceConfig,
    };

    const NOW: i64 = 10_000;

    fn intent(action: TradeIntentAction) -> TradeIntent {
        TradeIntent {
            intent_id: IntentId::from("intent-1"),
            decision_id: DecisionId::from("decision-1"),
            strategy_id: StrategyId::from("strategy-1"),
            correlation_id: CorrelationId::from("correlation-1"),
            idempotency_key: IdempotencyKey::from("key-1"),
            account_id: AccountId::from("account-1"),
            symbol: SymbolCode::from("EURUSD"),
            timeframe: TimeframeCode::from("M1"),
            action,
            confidence: 0.8,
            reason: "test".to_owned(),
            proposed_risk_pct: 1.0,
            proposed_sl: Some(1.09),
            proposed_tp: Some(1.12),
            proposed_legs: None,
            signal_expires_at: NOW + 1_000,
            requested_at: NOW,
        }
    }

    fn assert_unprocessable_code(error: ControlPlanePortError, expected: ErrorCode) {
        assert!(matches!(
            error,
            ControlPlanePortError::Unprocessable { code, .. } if code == expected
        ));
    }

    #[test]
    fn buy_and_sell_risk_must_be_within_percentage_point_bounds() {
        let policy = SqliteControlPlaneServiceConfig::default().time_policy;
        for action in [TradeIntentAction::Buy, TradeIntentAction::Sell] {
            let mut boundary = intent(action);
            boundary.proposed_risk_pct = 100.0;
            validate_intent_for_intake(&boundary, NOW, &policy).unwrap();

            for invalid in [0.0, -0.01, 100.01] {
                let mut malformed = intent(action);
                malformed.proposed_risk_pct = invalid;
                assert_unprocessable_code(
                    validate_intent_for_intake(&malformed, NOW, &policy).unwrap_err(),
                    ErrorCode::RiskInputInvalid,
                );
            }
        }
    }

    #[test]
    fn hold_requires_zero_risk_and_no_actionable_fields() {
        let policy = SqliteControlPlaneServiceConfig::default().time_policy;
        let mut valid = intent(TradeIntentAction::Hold);
        valid.proposed_risk_pct = 0.0;
        valid.proposed_sl = None;
        valid.proposed_tp = None;
        validate_intent_for_intake(&valid, NOW, &policy).unwrap();

        let mut with_risk = valid.clone();
        with_risk.proposed_risk_pct = 0.01;
        let mut with_sl = valid.clone();
        with_sl.proposed_sl = Some(1.09);
        let mut with_tp = valid.clone();
        with_tp.proposed_tp = Some(1.12);
        let mut with_legs = valid;
        with_legs.proposed_legs = Some(Vec::new());

        for malformed in [with_risk, with_sl, with_tp, with_legs] {
            assert_unprocessable_code(
                validate_intent_for_intake(&malformed, NOW, &policy).unwrap_err(),
                ErrorCode::RiskInputInvalid,
            );
        }
    }

    #[test]
    fn intake_distinguishes_invalid_request_time_from_signal_expiry() {
        let mut policy = SqliteControlPlaneServiceConfig::default().time_policy;
        policy.max_decision_intent_age_ms = 100;
        policy.max_decision_time_skew_ms = 50;

        let mut too_old = intent(TradeIntentAction::Buy);
        too_old.requested_at = NOW - 101;
        let mut too_far_future = intent(TradeIntentAction::Buy);
        too_far_future.requested_at = NOW + 51;
        for malformed in [too_old, too_far_future] {
            assert_unprocessable_code(
                validate_intent_for_intake(&malformed, NOW, &policy).unwrap_err(),
                ErrorCode::TradeIntentTimeInvalid,
            );
        }

        let mut expired = intent(TradeIntentAction::Buy);
        expired.requested_at = NOW - 1;
        expired.signal_expires_at = NOW;
        assert_unprocessable_code(
            validate_intent_for_intake(&expired, NOW, &policy).unwrap_err(),
            ErrorCode::TradeIntentExpired,
        );
    }

    #[test]
    fn rejected_risk_blocks_intent_and_advances_workflow_time() {
        let mut rejected = sinan_types::RiskResult {
            risk_id: "risk-1".into(),
            request_id: "risk-request-1".into(),
            intent_id: "intent-1".into(),
            account_id: "account-1".into(),
            risk_request_hash: "a".repeat(64),
            approved: false,
            reason: ErrorCode::RiskLimitExceeded.into(),
            message: None,
            sizing_version: None,
            risk_base_amount: None,
            risk_budget_amount: None,
            adjusted_risk_pct: None,
            sizing_candidates: None,
            adjusted_legs: None,
            decision_id: "decision-1".into(),
            snapshot_age_ms: 0,
            market_snapshot_age_ms: 0,
            symbol_metadata_age_ms: 0,
            capacity_age_ms: 0,
            evaluated_at: 300,
            valid_until: 300,
        };
        assert_eq!(
            effective_trade_intent_status(TradeIntentStatus::Accepted, Some(&rejected)),
            TradeIntentStatus::RiskBlocked
        );
        assert_eq!(
            effective_trade_intent_status(TradeIntentStatus::Cancelled, Some(&rejected)),
            TradeIntentStatus::Cancelled
        );
        assert_eq!(workflow_updated_at(100, Some(200), Some(300)), 300);

        rejected.approved = true;
        assert_eq!(
            effective_trade_intent_status(TradeIntentStatus::Accepted, Some(&rejected)),
            TradeIntentStatus::Accepted
        );
    }
}
