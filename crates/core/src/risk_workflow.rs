use sinan_execution::{
    build_execution, ExecutionBuildError, ExecutionBuildOutcome, ExecutionBuildRequest,
    ResolvedLegExecution,
};
use sinan_risk::{
    evaluate, restore_circuit_breaker_snapshot, CircuitBreakerState, PositionSizingCandidate,
    RiskEvaluationError, RiskMarketSnapshot, RiskPolicy, RiskRequest, RiskStateWatermarks,
    StrategyDecision, StrategyRiskPolicy,
};
use sinan_store::{
    NewExecutionCommand, NewExecutionPlan, NewExecutionWorkflow, NewRiskResult, NewTradeIntent,
    SqliteStateStore, StoreError, TrustedRiskSnapshot,
};
use sinan_types::{
    single_leg_id, AdjustedRiskLegAction, ClientId, CommandId, ExecutionCommand, ExecutionPlan,
    ExecutionPolicy, FillingPolicy, IdempotencyKey, IntentId, LegId, OrderType, PlanId, RequestId,
    RiskId, RiskResult, SymbolCode, TerminalId, TimePolicy, TradeIntent, TradeIntentAction,
    TradeIntentLegAction, TradeIntentStatus,
};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct RiskWorkflowLeg {
    pub leg_id: LegId,
    pub symbol: SymbolCode,
    pub action: AdjustedRiskLegAction,
    pub ratio: f64,
    pub proposed_stop_loss: Option<f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TrustedLegExecutionParameters {
    pub dependency: Vec<LegId>,
    pub terminal_id: Option<TerminalId>,
    pub client_id: Option<ClientId>,
    pub order_type: OrderType,
    pub price: Option<f64>,
    pub deviation_points: Option<i64>,
    pub magic: i64,
    pub comment: Option<String>,
    pub filling_policy: Option<FillingPolicy>,
    pub time_policy: Option<TimePolicy>,
    pub expiration_time: Option<i64>,
    pub estimated_cost_per_lot: f64,
}

/// Resolves trusted local execution configuration. Implementations must be
/// deterministic and must not perform network or Store I/O.
pub trait TrustedExecutionResolver: Send + Sync {
    fn resolve(
        &self,
        intent: &TradeIntent,
        leg: &RiskWorkflowLeg,
    ) -> Result<TrustedLegExecutionParameters, String>;
}

/// Immutable trusted policy and configuration for one workflow invocation.
///
/// This type deliberately does not implement `Debug`, so the signing secret
/// cannot be emitted by a derived formatter.
pub struct TrustedRiskWorkflowContext<'a> {
    risk_policy: &'a RiskPolicy,
    strategy_policy: &'a StrategyRiskPolicy,
    execution_policy: &'a ExecutionPolicy,
    execution_resolver: &'a dyn TrustedExecutionResolver,
    signing_secret: &'a [u8],
}

impl<'a> TrustedRiskWorkflowContext<'a> {
    pub fn new(
        risk_policy: &'a RiskPolicy,
        strategy_policy: &'a StrategyRiskPolicy,
        execution_policy: &'a ExecutionPolicy,
        execution_resolver: &'a dyn TrustedExecutionResolver,
        signing_secret: &'a [u8],
    ) -> Result<Self, RiskWorkflowError> {
        if signing_secret.is_empty() {
            return Err(RiskWorkflowError::InvalidTrustedInput(
                "command signing secret must not be empty".to_owned(),
            ));
        }
        Ok(Self {
            risk_policy,
            strategy_policy,
            execution_policy,
            execution_resolver,
            signing_secret,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum RiskWorkflowOutcome {
    NoPendingIntent,
    AlreadyProcessed {
        intent_id: IntentId,
    },
    RiskOnly {
        result: RiskResult,
    },
    ExecutionReady {
        result: RiskResult,
        plan: ExecutionPlan,
        commands: Vec<ExecutionCommand>,
    },
}

#[derive(Debug, Error)]
pub enum RiskWorkflowError {
    #[error(transparent)]
    Store(#[from] StoreError),

    #[error(transparent)]
    RiskEvaluation(#[from] RiskEvaluationError),

    #[error(transparent)]
    ExecutionBuild(#[from] ExecutionBuildError),

    #[error("trusted risk workflow input is invalid: {0}")]
    InvalidTrustedInput(String),

    #[error("durable circuit breaker state is unavailable or invalid")]
    CircuitBreakerUnavailable,
}

#[derive(Clone, Debug)]
pub struct RiskWorkflowProcessor {
    store: SqliteStateStore,
}

impl RiskWorkflowProcessor {
    pub fn new(store: SqliteStateStore) -> Self {
        Self { store }
    }

    pub async fn process_next(
        &self,
        evaluated_at: i64,
        context: &TrustedRiskWorkflowContext<'_>,
    ) -> Result<RiskWorkflowOutcome, RiskWorkflowError> {
        let mut transaction = self.store.begin_write().await?;
        let Some(intent_id) = transaction.next_pending_risk_intent_id().await? else {
            transaction.commit().await?;
            return Ok(RiskWorkflowOutcome::NoPendingIntent);
        };
        finish_in_transaction(transaction, &intent_id, evaluated_at, context).await
    }

    pub async fn process_intent(
        &self,
        intent_id: &IntentId,
        evaluated_at: i64,
        context: &TrustedRiskWorkflowContext<'_>,
    ) -> Result<RiskWorkflowOutcome, RiskWorkflowError> {
        let transaction = self.store.begin_write().await?;
        finish_in_transaction(transaction, intent_id, evaluated_at, context).await
    }
}

struct AssembledRiskWorkflow {
    request: RiskRequest,
    breaker: CircuitBreakerState,
    resolved_legs: Vec<ResolvedLegExecution>,
    intent_created_at: i64,
}

async fn finish_in_transaction(
    mut transaction: sinan_store::WriteTransaction,
    intent_id: &IntentId,
    evaluated_at: i64,
    context: &TrustedRiskWorkflowContext<'_>,
) -> Result<RiskWorkflowOutcome, RiskWorkflowError> {
    let result = process_in_transaction(&mut transaction, intent_id, evaluated_at, context).await;
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

async fn process_in_transaction(
    transaction: &mut sinan_store::WriteTransaction,
    intent_id: &IntentId,
    evaluated_at: i64,
    context: &TrustedRiskWorkflowContext<'_>,
) -> Result<RiskWorkflowOutcome, RiskWorkflowError> {
    validate_evaluated_at(evaluated_at)?;
    let Some(snapshot) = transaction.load_trusted_risk_snapshot(intent_id).await? else {
        return Ok(RiskWorkflowOutcome::AlreadyProcessed {
            intent_id: intent_id.clone(),
        });
    };
    let assembled = assemble_risk_request(snapshot, evaluated_at, context)?;
    let result = evaluate(&assembled.request, &assembled.breaker)?;

    if !result.approved || assembled.request.intent.action == TradeIntentAction::Hold {
        transaction
            .insert_risk_result(NewRiskResult {
                result: result.clone(),
            })
            .await?;
        return Ok(RiskWorkflowOutcome::RiskOnly { result });
    }

    let build = build_execution(ExecutionBuildRequest {
        plan_id: initial_plan_id(intent_id),
        now_ms: evaluated_at,
        risk_request: &assembled.request,
        risk_result: &result,
        policy: context.execution_policy,
        resolved_legs: &assembled.resolved_legs,
        signing_secret: context.signing_secret,
    })?;
    let ExecutionBuildOutcome::Built(bundle) = build else {
        transaction
            .insert_risk_result(NewRiskResult {
                result: result.clone(),
            })
            .await?;
        return Ok(RiskWorkflowOutcome::RiskOnly { result });
    };

    transaction
        .commit_execution_workflow(NewExecutionWorkflow {
            intent: NewTradeIntent {
                intent: assembled.request.intent.clone(),
                initial_status: TradeIntentStatus::Accepted,
                recorded_at: assembled.intent_created_at,
            },
            risk_result: NewRiskResult {
                result: result.clone(),
            },
            plan: NewExecutionPlan {
                plan: bundle.plan.clone(),
                risk_id: assembled.request.risk_id.clone(),
                intent_id: intent_id.clone(),
                recorded_at: evaluated_at,
            },
            commands: bundle
                .commands
                .iter()
                .cloned()
                .map(|command| NewExecutionCommand {
                    command,
                    risk_id: assembled.request.risk_id.clone(),
                    created_at: evaluated_at,
                })
                .collect(),
            command_states: bundle.command_states.clone(),
        })
        .await?;
    Ok(RiskWorkflowOutcome::ExecutionReady {
        result,
        plan: bundle.plan,
        commands: bundle.commands,
    })
}

fn assemble_risk_request(
    snapshot: TrustedRiskSnapshot,
    evaluated_at: i64,
    context: &TrustedRiskWorkflowContext<'_>,
) -> Result<AssembledRiskWorkflow, RiskWorkflowError> {
    let intent_created_at = snapshot.intent.created_at;
    let intent = snapshot.intent.intent;
    let breaker = snapshot
        .circuit_breaker
        .as_ref()
        .ok_or(RiskWorkflowError::CircuitBreakerUnavailable)?;
    let restored = restore_circuit_breaker_snapshot(Some(breaker.payload.as_str()), evaluated_at);
    if restored.error.is_some() {
        return Err(RiskWorkflowError::CircuitBreakerUnavailable);
    }

    let legs = workflow_legs(&intent);
    let mut candidates = Vec::with_capacity(legs.len());
    let mut resolved_legs = Vec::with_capacity(legs.len());
    for leg in &legs {
        let Some(stop_loss_price) = leg.proposed_stop_loss else {
            continue;
        };
        let Some(metadata) = snapshot
            .symbol_metadata
            .iter()
            .find(|value| value.symbol == leg.symbol)
        else {
            return Err(RiskWorkflowError::InvalidTrustedInput(format!(
                "symbol metadata for {} is missing from the trusted snapshot",
                leg.symbol
            )));
        };
        let Some(market) = snapshot
            .markets
            .iter()
            .find(|value| value.snapshot.symbol == leg.symbol)
        else {
            return Err(RiskWorkflowError::InvalidTrustedInput(format!(
                "market snapshot for {} is missing from the trusted snapshot",
                leg.symbol
            )));
        };
        if !metadata.point.is_finite() || metadata.point <= 0.0 {
            continue;
        }
        let parameters = context
            .execution_resolver
            .resolve(&intent, leg)
            .map_err(|reason| {
                RiskWorkflowError::InvalidTrustedInput(format!(
                    "execution parameters for {}: {reason}",
                    leg.leg_id
                ))
            })?;
        validate_execution_parameters(&parameters)?;
        let deviation = parameters.deviation_points.unwrap_or(0) as f64 * metadata.point;
        let reference_price = match (leg.action, parameters.price) {
            (_, Some(price)) => price,
            (AdjustedRiskLegAction::Buy, None) => market.snapshot.ask,
            (AdjustedRiskLegAction::Sell, None) => market.snapshot.bid,
        };
        let worst_entry_price = match leg.action {
            AdjustedRiskLegAction::Buy => reference_price + deviation,
            AdjustedRiskLegAction::Sell => reference_price - deviation,
        };
        candidates.push(PositionSizingCandidate {
            leg_id: leg.leg_id.clone(),
            symbol: leg.symbol.clone(),
            action: leg.action,
            ratio: leg.ratio,
            worst_entry_price,
            stop_loss_price,
            estimated_cost_per_lot: parameters.estimated_cost_per_lot,
        });
        resolved_legs.push(ResolvedLegExecution {
            leg_id: leg.leg_id.clone(),
            dependency: parameters.dependency,
            command_id: initial_command_id(&intent.intent_id, &leg.leg_id),
            idempotency_key: initial_command_idempotency_key(&intent.intent_id, &leg.leg_id),
            terminal_id: parameters.terminal_id,
            client_id: parameters.client_id,
            order_type: parameters.order_type,
            price: parameters.price,
            deviation_points: parameters.deviation_points,
            magic: parameters.magic,
            comment: parameters.comment,
            filling_policy: parameters.filling_policy,
            time_policy: parameters.time_policy,
            expiration_time: parameters.expiration_time,
        });
    }

    let pending_commands = snapshot
        .pending_commands
        .iter()
        .map(|value| value.command.command.clone())
        .collect();
    let pending_command_states = snapshot
        .pending_commands
        .iter()
        .map(|value| value.state.clone())
        .collect();
    let request = RiskRequest {
        request_id: initial_request_id(&intent.intent_id),
        risk_id: initial_risk_id(&intent.intent_id),
        evaluated_at,
        decision: StrategyDecision {
            decision_id: intent.decision_id.clone(),
            strategy_id: intent.strategy_id.clone(),
            symbol: intent.symbol.clone(),
            timeframe: intent.timeframe.clone(),
            action: intent.action,
            confidence: intent.confidence,
            reason: intent.reason.clone(),
            proposed_risk_pct: intent.proposed_risk_pct,
            proposed_sl: intent.proposed_sl,
            proposed_tp: intent.proposed_tp,
            timestamp: intent.decision_timestamp,
            signal_expires_at: intent.signal_expires_at,
        },
        intent,
        agent_review: None,
        account: snapshot.account,
        positions: snapshot.positions,
        orders: snapshot.orders,
        symbol_metadata: snapshot.symbol_metadata,
        pending_commands,
        pending_command_states,
        policy: context.risk_policy.clone(),
        strategy_policy: context.strategy_policy.clone(),
        markets: snapshot
            .markets
            .into_iter()
            .map(|value| RiskMarketSnapshot {
                account_id: value.account_id,
                snapshot: value.snapshot,
            })
            .collect(),
        sizing_candidates: candidates,
        state_watermarks: RiskStateWatermarks {
            positions_observed_at: snapshot.checkpoint.positions_observed_at,
            orders_observed_at: snapshot.checkpoint.orders_observed_at,
            pending_commands_reconciled_at: snapshot
                .checkpoint
                .pending_commands_reconciled_at
                .expect("Store snapshot guarantees a pending-command watermark"),
        },
        capacity: snapshot.capacity.capacity,
    };
    Ok(AssembledRiskWorkflow {
        request,
        breaker: restored.state,
        resolved_legs,
        intent_created_at,
    })
}

fn workflow_legs(intent: &TradeIntent) -> Vec<RiskWorkflowLeg> {
    if let Some(legs) = &intent.proposed_legs {
        return legs
            .iter()
            .filter_map(|leg| {
                let action = match leg.action {
                    TradeIntentLegAction::Buy => AdjustedRiskLegAction::Buy,
                    TradeIntentLegAction::Sell => AdjustedRiskLegAction::Sell,
                    TradeIntentLegAction::Close => return None,
                };
                Some(RiskWorkflowLeg {
                    leg_id: leg.leg_id.clone(),
                    symbol: leg.symbol.clone(),
                    action,
                    ratio: leg.ratio,
                    proposed_stop_loss: leg.proposed_sl,
                })
            })
            .collect();
    }
    let action = match intent.action {
        TradeIntentAction::Buy => AdjustedRiskLegAction::Buy,
        TradeIntentAction::Sell => AdjustedRiskLegAction::Sell,
        TradeIntentAction::Close | TradeIntentAction::Hold => return Vec::new(),
    };
    vec![RiskWorkflowLeg {
        leg_id: single_leg_id(&intent.intent_id),
        symbol: intent.symbol.clone(),
        action,
        ratio: 1.0,
        proposed_stop_loss: intent.proposed_sl,
    }]
}

fn validate_evaluated_at(evaluated_at: i64) -> Result<(), RiskWorkflowError> {
    if evaluated_at < 0 {
        return Err(RiskWorkflowError::InvalidTrustedInput(
            "evaluated_at must be non-negative".to_owned(),
        ));
    }
    Ok(())
}

fn validate_execution_parameters(
    value: &TrustedLegExecutionParameters,
) -> Result<(), RiskWorkflowError> {
    if !value.estimated_cost_per_lot.is_finite() || value.estimated_cost_per_lot < 0.0 {
        return Err(RiskWorkflowError::InvalidTrustedInput(
            "estimated_cost_per_lot must be finite and non-negative".to_owned(),
        ));
    }
    if value.deviation_points.is_some_and(|points| points < 0) {
        return Err(RiskWorkflowError::InvalidTrustedInput(
            "deviation_points must be non-negative".to_owned(),
        ));
    }
    if value
        .price
        .is_some_and(|price| !price.is_finite() || price <= 0.0)
    {
        return Err(RiskWorkflowError::InvalidTrustedInput(
            "execution price must be positive and finite".to_owned(),
        ));
    }
    Ok(())
}

fn initial_request_id(intent_id: &IntentId) -> RequestId {
    RequestId::from(format!("risk-request:{intent_id}:initial"))
}

fn initial_risk_id(intent_id: &IntentId) -> RiskId {
    RiskId::from(format!("risk:{intent_id}:initial"))
}

fn initial_plan_id(intent_id: &IntentId) -> PlanId {
    PlanId::from(format!("plan:{intent_id}:initial"))
}

fn initial_command_id(intent_id: &IntentId, leg_id: &LegId) -> CommandId {
    CommandId::from(format!("command:{intent_id}:{leg_id}"))
}

fn initial_command_idempotency_key(intent_id: &IntentId, leg_id: &LegId) -> IdempotencyKey {
    IdempotencyKey::from(format!("command:{intent_id}:{leg_id}:initial"))
}
