//! Pure, deterministic circuit-breaker domain logic.
//!
//! The caller owns persistence and supplies `server_now_ms` for every
//! transition. This module never reads a wall clock or performs I/O.

use std::{error::Error, fmt};

/// A percentage expressed in basis points (`100 bps == 1%`).
pub type BasisPoints = u32;

/// Configuration for the global hard-risk circuit breaker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CircuitBreakerPolicy {
    pub max_daily_realized_loss_bps: BasisPoints,
    pub max_equity_drawdown_bps: BasisPoints,
    pub max_consecutive_broker_rejections: u32,
    pub max_consecutive_command_failures: u32,
    pub max_time_sync_unhealthy_ms: i64,
    pub max_consecutive_snapshot_stale: u32,
    pub max_consecutive_symbol_metadata_stale: u32,
    pub half_open_observation_ms: i64,
    pub auto_reset: bool,
}

impl CircuitBreakerPolicy {
    fn validate(&self) -> Result<(), CircuitBreakerError> {
        let required_positive = [
            (
                "max_daily_realized_loss_bps",
                i64::from(self.max_daily_realized_loss_bps),
            ),
            (
                "max_equity_drawdown_bps",
                i64::from(self.max_equity_drawdown_bps),
            ),
            (
                "max_consecutive_broker_rejections",
                i64::from(self.max_consecutive_broker_rejections),
            ),
            (
                "max_consecutive_command_failures",
                i64::from(self.max_consecutive_command_failures),
            ),
            ("half_open_observation_ms", self.half_open_observation_ms),
        ];

        for (field, value) in required_positive {
            if value == 0 {
                return Err(CircuitBreakerError::InvalidPolicy {
                    field,
                    requirement: "must be greater than zero",
                });
            }
        }

        if self.max_time_sync_unhealthy_ms < 0 {
            return Err(CircuitBreakerError::InvalidPolicy {
                field: "max_time_sync_unhealthy_ms",
                requirement: "must not be negative",
            });
        }

        Ok(())
    }
}

/// The status exposed by the global circuit breaker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CircuitBreakerStatus {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitBreakerStatus {
    pub const fn is_active(self) -> bool {
        !matches!(self, Self::Closed)
    }
}

/// Stable reason stored with a circuit-breaker incident.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CircuitBreakerReason {
    Ok,
    DailyRealizedLossLimit,
    EquityDrawdownLimit,
    ConsecutiveBrokerRejections,
    ConsecutiveCommandFailures,
    ManualReconciliationRequired,
    StoreRecoveryReconciliationPending,
    TimeSyncUnhealthy,
    SnapshotStale,
    SymbolMetadataStale,
    ManualTrigger,
    HardRiskViolationDuringRecovery,
    SafetyInvariantViolation,
}

/// Component or operator that caused the current incident.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CircuitBreakerTriggerSource {
    RiskTelemetry,
    Broker,
    Execution,
    Reconciliation,
    StoreRecovery,
    Clock,
    SnapshotHealth,
    Operator(String),
    SafetyInvariant,
}

/// In-memory domain state. The application integration must durably restore it
/// rather than defaulting to `CLOSED` after a process restart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CircuitBreakerState {
    status: CircuitBreakerStatus,
    reason: CircuitBreakerReason,
    triggered_at_ms: Option<i64>,
    triggered_by: Option<CircuitBreakerTriggerSource>,
    last_incident_fingerprint: Option<CircuitBreakerIncidentFingerprint>,
    incident_evidence_cleared_at_ms: Option<i64>,
    half_opened_at_ms: Option<i64>,
    half_open_daily_loss_baseline_bps: Option<BasisPoints>,
    half_open_drawdown_baseline_bps: Option<BasisPoints>,
    reset_at_ms: Option<i64>,
    reset_by: Option<String>,
    blocked_intent_count: u64,
}

impl Default for CircuitBreakerState {
    fn default() -> Self {
        Self {
            status: CircuitBreakerStatus::Closed,
            reason: CircuitBreakerReason::Ok,
            triggered_at_ms: None,
            triggered_by: None,
            last_incident_fingerprint: None,
            incident_evidence_cleared_at_ms: None,
            half_opened_at_ms: None,
            half_open_daily_loss_baseline_bps: None,
            half_open_drawdown_baseline_bps: None,
            reset_at_ms: None,
            reset_by: None,
            blocked_intent_count: 0,
        }
    }
}

impl CircuitBreakerState {
    pub fn new() -> Self {
        Self::default()
    }

    pub const fn status(&self) -> CircuitBreakerStatus {
        self.status
    }

    pub const fn reason(&self) -> CircuitBreakerReason {
        self.reason
    }

    pub const fn triggered_at_ms(&self) -> Option<i64> {
        self.triggered_at_ms
    }

    pub fn triggered_by(&self) -> Option<&CircuitBreakerTriggerSource> {
        self.triggered_by.as_ref()
    }

    pub const fn incident_evidence_cleared_at_ms(&self) -> Option<i64> {
        self.incident_evidence_cleared_at_ms
    }

    pub const fn half_opened_at_ms(&self) -> Option<i64> {
        self.half_opened_at_ms
    }

    pub const fn reset_at_ms(&self) -> Option<i64> {
        self.reset_at_ms
    }

    pub fn reset_by(&self) -> Option<&str> {
        self.reset_by.as_deref()
    }

    pub const fn blocked_intent_count(&self) -> u64 {
        self.blocked_intent_count
    }

    /// Applies the circuit-breaker action matrix without mutating state.
    pub const fn authorize(&self, action: CircuitBreakerAction) -> CircuitBreakerGateDecision {
        let allowed = match self.status {
            CircuitBreakerStatus::Closed => true,
            CircuitBreakerStatus::Open | CircuitBreakerStatus::HalfOpen => !matches!(
                action,
                CircuitBreakerAction::RiskIncreasingTradeIntent
                    | CircuitBreakerAction::RiskIncreasingCommand
            ),
        };

        CircuitBreakerGateDecision {
            allowed,
            status: self.status,
            blocked_reason: if allowed { None } else { Some(self.reason) },
        }
    }

    fn latest_transition_at_ms(&self) -> Option<i64> {
        [
            self.triggered_at_ms,
            self.incident_evidence_cleared_at_ms,
            self.half_opened_at_ms,
            self.reset_at_ms,
        ]
        .into_iter()
        .flatten()
        .max()
    }
}

/// An action crossing the hard-risk boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CircuitBreakerAction {
    RiskIncreasingTradeIntent,
    RiskIncreasingCommand,
    RiskReducingCommand,
    ReadState,
    IngestSnapshot,
    IngestExecutionEvent,
    Reconciliation,
    ManualReview,
    NoOpValidation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CircuitBreakerGateDecision {
    pub allowed: bool,
    pub status: CircuitBreakerStatus,
    pub blocked_reason: Option<CircuitBreakerReason>,
}

/// Operator evidence for an explicit manual trigger.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManualTrigger {
    pub operator_id: String,
    pub reason: String,
}

/// Current counters and health signals evaluated by the breaker.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CircuitBreakerInput {
    pub daily_realized_loss_bps: BasisPoints,
    pub equity_drawdown_bps: BasisPoints,
    pub consecutive_broker_rejections: u32,
    pub consecutive_command_failures: u32,
    pub manual_reconciliation_required_count: u32,
    pub store_recovery_reconciliation_pending: bool,
    pub time_sync_unhealthy_since_ms: Option<i64>,
    pub consecutive_snapshot_stale_count: u32,
    pub consecutive_symbol_metadata_stale_count: u32,
    pub manual_trigger: Option<ManualTrigger>,
}

/// Evidence that all snapshots and pending commands were refreshed/reconciled.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HalfOpenReadiness {
    pub account_refreshed_at_ms: Option<i64>,
    pub positions_refreshed_at_ms: Option<i64>,
    pub orders_refreshed_at_ms: Option<i64>,
    pub symbol_metadata_refreshed_at_ms: Option<i64>,
    pub pending_commands_reconciled_at_ms: Option<i64>,
}

/// Health evidence required to finish the half-open observation period.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryHealth {
    pub clock_healthy: bool,
    pub state_store_healthy: bool,
    pub new_hard_risk_violation: bool,
    pub input: CircuitBreakerInput,
}

/// Required evidence for a manual reset audit event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManualResetEvidence {
    pub audit_event_id: String,
    pub operator_id: String,
    pub reason: String,
    pub evidence: String,
}

/// Authority used to finish a reset. Automatic reset is policy-gated.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResetAuthorization {
    Automatic,
    Operator(ManualResetEvidence),
}

/// A state-machine request. It contains no implicit clock source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransitionRequest {
    Observe(CircuitBreakerInput),
    EnterHalfOpen {
        readiness: HalfOpenReadiness,
        input: CircuitBreakerInput,
    },
    Close {
        health: RecoveryHealth,
        authorization: ResetAuthorization,
    },
    /// Projects an absolute durable total, making retries idempotent.
    RecordBlockedIntentCount {
        observed_total: u64,
    },
}

/// Exact trigger evidence returned to event/audit adapters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CircuitBreakerViolation {
    DailyRealizedLoss {
        observed_bps: BasisPoints,
        threshold_bps: BasisPoints,
    },
    DailyRealizedLossWorsened {
        baseline_bps: BasisPoints,
        observed_bps: BasisPoints,
    },
    EquityDrawdown {
        observed_bps: BasisPoints,
        threshold_bps: BasisPoints,
    },
    EquityDrawdownWorsened {
        baseline_bps: BasisPoints,
        observed_bps: BasisPoints,
    },
    ConsecutiveBrokerRejections {
        observed: u32,
        threshold: u32,
    },
    ConsecutiveCommandFailures {
        observed: u32,
        threshold: u32,
    },
    ManualReconciliationRequired {
        count: u32,
    },
    StoreRecoveryReconciliationPending,
    TimeSyncUnhealthy {
        unhealthy_for_ms: i64,
        threshold_ms: i64,
    },
    SnapshotStale {
        consecutive_count: u32,
        threshold: u32,
    },
    SymbolMetadataStale {
        consecutive_count: u32,
        threshold: u32,
    },
    ManualTrigger {
        operator_id: String,
        reason: String,
    },
    HardRiskViolationDuringRecovery,
    SafetyInvariantViolation,
}

/// Exact, collision-free identity for the hard-risk evidence behind an
/// incident epoch. Time-sync evidence records the unhealthy start time rather
/// than the growing duration so polling the same fault remains idempotent.
#[derive(Clone, Debug, Eq, PartialEq)]
struct CircuitBreakerIncidentFingerprint(Vec<CircuitBreakerIncidentEvidence>);

#[derive(Clone, Debug, Eq, PartialEq)]
enum CircuitBreakerIncidentEvidence {
    DailyRealizedLoss {
        observed_bps: BasisPoints,
        threshold_bps: BasisPoints,
    },
    DailyRealizedLossWorsened {
        baseline_bps: BasisPoints,
        observed_bps: BasisPoints,
    },
    EquityDrawdown {
        observed_bps: BasisPoints,
        threshold_bps: BasisPoints,
    },
    EquityDrawdownWorsened {
        baseline_bps: BasisPoints,
        observed_bps: BasisPoints,
    },
    ConsecutiveBrokerRejections {
        observed: u32,
        threshold: u32,
    },
    ConsecutiveCommandFailures {
        observed: u32,
        threshold: u32,
    },
    ManualReconciliationRequired {
        count: u32,
    },
    StoreRecoveryReconciliationPending,
    TimeSyncUnhealthy {
        unhealthy_since_ms: Option<i64>,
        threshold_ms: i64,
    },
    SnapshotStale {
        consecutive_count: u32,
        threshold: u32,
    },
    SymbolMetadataStale {
        consecutive_count: u32,
        threshold: u32,
    },
    ManualTrigger {
        operator_id: String,
        reason: String,
    },
    HardRiskViolationDuringRecovery,
    SafetyInvariantViolation,
    SafetyInvariantError(CircuitBreakerError),
}

impl CircuitBreakerViolation {
    pub const fn reason(&self) -> CircuitBreakerReason {
        match self {
            Self::DailyRealizedLoss { .. } | Self::DailyRealizedLossWorsened { .. } => {
                CircuitBreakerReason::DailyRealizedLossLimit
            }
            Self::EquityDrawdown { .. } | Self::EquityDrawdownWorsened { .. } => {
                CircuitBreakerReason::EquityDrawdownLimit
            }
            Self::ConsecutiveBrokerRejections { .. } => {
                CircuitBreakerReason::ConsecutiveBrokerRejections
            }
            Self::ConsecutiveCommandFailures { .. } => {
                CircuitBreakerReason::ConsecutiveCommandFailures
            }
            Self::ManualReconciliationRequired { .. } => {
                CircuitBreakerReason::ManualReconciliationRequired
            }
            Self::StoreRecoveryReconciliationPending => {
                CircuitBreakerReason::StoreRecoveryReconciliationPending
            }
            Self::TimeSyncUnhealthy { .. } => CircuitBreakerReason::TimeSyncUnhealthy,
            Self::SnapshotStale { .. } => CircuitBreakerReason::SnapshotStale,
            Self::SymbolMetadataStale { .. } => CircuitBreakerReason::SymbolMetadataStale,
            Self::ManualTrigger { .. } => CircuitBreakerReason::ManualTrigger,
            Self::HardRiskViolationDuringRecovery => {
                CircuitBreakerReason::HardRiskViolationDuringRecovery
            }
            Self::SafetyInvariantViolation => CircuitBreakerReason::SafetyInvariantViolation,
        }
    }

    fn source(&self) -> CircuitBreakerTriggerSource {
        match self {
            Self::DailyRealizedLoss { .. }
            | Self::DailyRealizedLossWorsened { .. }
            | Self::EquityDrawdown { .. }
            | Self::EquityDrawdownWorsened { .. } => CircuitBreakerTriggerSource::RiskTelemetry,
            Self::ConsecutiveBrokerRejections { .. } => CircuitBreakerTriggerSource::Broker,
            Self::ConsecutiveCommandFailures { .. } | Self::HardRiskViolationDuringRecovery => {
                CircuitBreakerTriggerSource::Execution
            }
            Self::ManualReconciliationRequired { .. } => {
                CircuitBreakerTriggerSource::Reconciliation
            }
            Self::StoreRecoveryReconciliationPending => CircuitBreakerTriggerSource::StoreRecovery,
            Self::TimeSyncUnhealthy { .. } => CircuitBreakerTriggerSource::Clock,
            Self::SnapshotStale { .. } | Self::SymbolMetadataStale { .. } => {
                CircuitBreakerTriggerSource::SnapshotHealth
            }
            Self::ManualTrigger { operator_id, .. } => {
                CircuitBreakerTriggerSource::Operator(operator_id.clone())
            }
            Self::SafetyInvariantViolation => CircuitBreakerTriggerSource::SafetyInvariant,
        }
    }
}

fn incident_fingerprint(
    violations: &[CircuitBreakerViolation],
    input: Option<&CircuitBreakerInput>,
) -> CircuitBreakerIncidentFingerprint {
    CircuitBreakerIncidentFingerprint(
        violations
            .iter()
            .map(|violation| match violation {
                CircuitBreakerViolation::DailyRealizedLoss {
                    observed_bps,
                    threshold_bps,
                } => CircuitBreakerIncidentEvidence::DailyRealizedLoss {
                    observed_bps: *observed_bps,
                    threshold_bps: *threshold_bps,
                },
                CircuitBreakerViolation::DailyRealizedLossWorsened {
                    baseline_bps,
                    observed_bps,
                } => CircuitBreakerIncidentEvidence::DailyRealizedLossWorsened {
                    baseline_bps: *baseline_bps,
                    observed_bps: *observed_bps,
                },
                CircuitBreakerViolation::EquityDrawdown {
                    observed_bps,
                    threshold_bps,
                } => CircuitBreakerIncidentEvidence::EquityDrawdown {
                    observed_bps: *observed_bps,
                    threshold_bps: *threshold_bps,
                },
                CircuitBreakerViolation::EquityDrawdownWorsened {
                    baseline_bps,
                    observed_bps,
                } => CircuitBreakerIncidentEvidence::EquityDrawdownWorsened {
                    baseline_bps: *baseline_bps,
                    observed_bps: *observed_bps,
                },
                CircuitBreakerViolation::ConsecutiveBrokerRejections {
                    observed,
                    threshold,
                } => CircuitBreakerIncidentEvidence::ConsecutiveBrokerRejections {
                    observed: *observed,
                    threshold: *threshold,
                },
                CircuitBreakerViolation::ConsecutiveCommandFailures {
                    observed,
                    threshold,
                } => CircuitBreakerIncidentEvidence::ConsecutiveCommandFailures {
                    observed: *observed,
                    threshold: *threshold,
                },
                CircuitBreakerViolation::ManualReconciliationRequired { count } => {
                    CircuitBreakerIncidentEvidence::ManualReconciliationRequired { count: *count }
                }
                CircuitBreakerViolation::StoreRecoveryReconciliationPending => {
                    CircuitBreakerIncidentEvidence::StoreRecoveryReconciliationPending
                }
                CircuitBreakerViolation::TimeSyncUnhealthy { threshold_ms, .. } => {
                    CircuitBreakerIncidentEvidence::TimeSyncUnhealthy {
                        unhealthy_since_ms: input
                            .and_then(|input| input.time_sync_unhealthy_since_ms),
                        threshold_ms: *threshold_ms,
                    }
                }
                CircuitBreakerViolation::SnapshotStale {
                    consecutive_count,
                    threshold,
                } => CircuitBreakerIncidentEvidence::SnapshotStale {
                    consecutive_count: *consecutive_count,
                    threshold: *threshold,
                },
                CircuitBreakerViolation::SymbolMetadataStale {
                    consecutive_count,
                    threshold,
                } => CircuitBreakerIncidentEvidence::SymbolMetadataStale {
                    consecutive_count: *consecutive_count,
                    threshold: *threshold,
                },
                CircuitBreakerViolation::ManualTrigger {
                    operator_id,
                    reason,
                } => CircuitBreakerIncidentEvidence::ManualTrigger {
                    operator_id: operator_id.clone(),
                    reason: reason.clone(),
                },
                CircuitBreakerViolation::HardRiskViolationDuringRecovery => {
                    CircuitBreakerIncidentEvidence::HardRiskViolationDuringRecovery
                }
                CircuitBreakerViolation::SafetyInvariantViolation => {
                    CircuitBreakerIncidentEvidence::SafetyInvariantViolation
                }
            })
            .collect(),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryPrerequisite {
    AccountRefresh,
    PositionRefresh,
    OrderRefresh,
    SymbolMetadataRefresh,
    PendingCommandReconciliation,
    ClockHealth,
    StateStoreHealth,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransitionRequestKind {
    Observe,
    EnterHalfOpen,
    Close,
    RecordBlockedIntentCount,
}

/// A domain error is returned inside the outcome so the fail-closed state is
/// never lost by an early `Result::Err` return.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CircuitBreakerError {
    InvalidPolicy {
        field: &'static str,
        requirement: &'static str,
    },
    InvalidInput {
        field: &'static str,
        requirement: &'static str,
    },
    ServerTimeRegressed {
        latest_transition_at_ms: i64,
        server_now_ms: i64,
    },
    InvalidTransition {
        status: CircuitBreakerStatus,
        request: TransitionRequestKind,
    },
    RecoveryPrerequisitesMissing(Vec<RecoveryPrerequisite>),
    RecoveryBlockedByViolations(Vec<CircuitBreakerReason>),
    HalfOpenObservationNotComplete {
        remaining_ms: i64,
    },
    AutomaticResetDisabled,
    ManualResetEvidenceMissing {
        field: &'static str,
    },
    HalfOpenBaselineMissing,
}

impl fmt::Display for CircuitBreakerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPolicy { field, requirement } => {
                write!(
                    formatter,
                    "invalid circuit-breaker policy {field}: {requirement}"
                )
            }
            Self::InvalidInput { field, requirement } => {
                write!(
                    formatter,
                    "invalid circuit-breaker input {field}: {requirement}"
                )
            }
            Self::ServerTimeRegressed {
                latest_transition_at_ms,
                server_now_ms,
            } => write!(
                formatter,
                "server time regressed from {latest_transition_at_ms} to {server_now_ms}"
            ),
            Self::InvalidTransition { status, request } => {
                write!(formatter, "request {request:?} is invalid from {status:?}")
            }
            Self::RecoveryPrerequisitesMissing(prerequisites) => {
                write!(
                    formatter,
                    "recovery prerequisites missing: {prerequisites:?}"
                )
            }
            Self::RecoveryBlockedByViolations(reasons) => {
                write!(formatter, "recovery blocked by violations: {reasons:?}")
            }
            Self::HalfOpenObservationNotComplete { remaining_ms } => write!(
                formatter,
                "half-open observation has {remaining_ms}ms remaining"
            ),
            Self::AutomaticResetDisabled => formatter.write_str("automatic reset is disabled"),
            Self::ManualResetEvidenceMissing { field } => {
                write!(formatter, "manual reset evidence field {field} is required")
            }
            Self::HalfOpenBaselineMissing => {
                formatter.write_str("half-open risk baseline is missing")
            }
        }
    }
}

impl Error for CircuitBreakerError {}

/// Audit payload that must be persisted atomically with a manual close.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManualResetAuditRecord {
    pub audit_event_id: String,
    pub operator_id: String,
    pub reason: String,
    pub evidence: String,
    pub before_state: CircuitBreakerStatus,
    pub after_state: CircuitBreakerStatus,
    pub recorded_at_ms: i64,
}

/// Observable state change for callers and tests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CircuitBreakerTransition {
    NoChange,
    Opened,
    EnteredHalfOpen,
    Reopened,
    Closed,
    SafetyFallbackOpened,
    IncidentEvidenceCleared,
    BlockedIntentCountAdvanced,
}

/// A transition always returns the resulting state, including on error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CircuitBreakerOutcome {
    pub state: CircuitBreakerState,
    pub transition: CircuitBreakerTransition,
    pub violations: Vec<CircuitBreakerViolation>,
    pub error: Option<CircuitBreakerError>,
    pub manual_reset_audit: Option<ManualResetAuditRecord>,
}

impl CircuitBreakerOutcome {
    fn unchanged(state: &CircuitBreakerState) -> Self {
        Self {
            state: state.clone(),
            transition: CircuitBreakerTransition::NoChange,
            violations: Vec::new(),
            error: None,
            manual_reset_audit: None,
        }
    }

    fn with_error(state: &CircuitBreakerState, error: CircuitBreakerError) -> Self {
        Self {
            state: state.clone(),
            transition: CircuitBreakerTransition::NoChange,
            violations: Vec::new(),
            error: Some(error),
            manual_reset_audit: None,
        }
    }
}

/// Applies one deterministic state-machine request.
///
/// Invalid policy, clock, or telemetry input is fail-closed: the returned
/// state remains active or becomes `OPEN`, and the error is retained in the
/// outcome for audit/event publication.
pub fn transition(
    policy: &CircuitBreakerPolicy,
    state: &CircuitBreakerState,
    request: &TransitionRequest,
    server_now_ms: i64,
) -> CircuitBreakerOutcome {
    if server_now_ms < 0 {
        return safety_fallback(
            state,
            server_now_ms,
            CircuitBreakerError::InvalidInput {
                field: "server_now_ms",
                requirement: "must not be negative",
            },
        );
    }

    if let Err(error) = policy.validate() {
        return safety_fallback(state, server_now_ms, error);
    }

    if let Some(latest_transition_at_ms) = state.latest_transition_at_ms() {
        if server_now_ms < latest_transition_at_ms {
            return safety_fallback(
                state,
                server_now_ms,
                CircuitBreakerError::ServerTimeRegressed {
                    latest_transition_at_ms,
                    server_now_ms,
                },
            );
        }
    }

    match request {
        TransitionRequest::Observe(input) => observe(policy, state, input, server_now_ms),
        TransitionRequest::EnterHalfOpen { readiness, input } => {
            enter_half_open(policy, state, *readiness, input, server_now_ms)
        }
        TransitionRequest::Close {
            health,
            authorization,
        } => close(policy, state, health, authorization, server_now_ms),
        TransitionRequest::RecordBlockedIntentCount { observed_total } => {
            record_blocked_intent_count(state, *observed_total)
        }
    }
}

fn observe(
    policy: &CircuitBreakerPolicy,
    state: &CircuitBreakerState,
    input: &CircuitBreakerInput,
    server_now_ms: i64,
) -> CircuitBreakerOutcome {
    let violations = match collect_violations(policy, input, server_now_ms) {
        Ok(violations) => violations,
        Err(error) => return safety_fallback(state, server_now_ms, error),
    };

    match state.status {
        CircuitBreakerStatus::Closed if !violations.is_empty() => {
            let fingerprint = incident_fingerprint(&violations, Some(input));
            open(state, violations, fingerprint, server_now_ms, false)
        }
        CircuitBreakerStatus::HalfOpen => {
            observe_half_open(state, violations, input, server_now_ms)
        }
        CircuitBreakerStatus::Open if !violations.is_empty() => {
            let fingerprint = incident_fingerprint(&violations, Some(input));
            if state.last_incident_fingerprint.as_ref() == Some(&fingerprint) {
                let mut outcome = CircuitBreakerOutcome::unchanged(state);
                outcome.violations = violations;
                outcome
            } else {
                open(state, violations, fingerprint, server_now_ms, true)
            }
        }
        CircuitBreakerStatus::Open if state.last_incident_fingerprint.is_some() => {
            // A healthy observation separates fault episodes. Identical hard-risk
            // evidence seen later must start a new recovery epoch.
            let mut next = state.clone();
            next.last_incident_fingerprint = None;
            next.incident_evidence_cleared_at_ms = Some(server_now_ms);
            CircuitBreakerOutcome {
                state: next,
                transition: CircuitBreakerTransition::IncidentEvidenceCleared,
                violations,
                error: None,
                manual_reset_audit: None,
            }
        }
        CircuitBreakerStatus::Closed | CircuitBreakerStatus::Open => {
            let mut outcome = CircuitBreakerOutcome::unchanged(state);
            outcome.violations = violations;
            outcome
        }
    }
}

fn enter_half_open(
    policy: &CircuitBreakerPolicy,
    state: &CircuitBreakerState,
    readiness: HalfOpenReadiness,
    input: &CircuitBreakerInput,
    server_now_ms: i64,
) -> CircuitBreakerOutcome {
    if state.status == CircuitBreakerStatus::Closed {
        return CircuitBreakerOutcome::unchanged(state);
    }

    if state.status == CircuitBreakerStatus::HalfOpen {
        return observe(policy, state, input, server_now_ms);
    }

    let violations = match collect_violations(policy, input, server_now_ms) {
        Ok(violations) => violations,
        Err(error) => return safety_fallback(state, server_now_ms, error),
    };
    if !violations.is_empty() {
        let fingerprint = incident_fingerprint(&violations, Some(input));
        if state.last_incident_fingerprint.as_ref() != Some(&fingerprint) {
            return open(state, violations, fingerprint, server_now_ms, true);
        }
    }
    let blocking_violations: Vec<_> = violations
        .iter()
        .filter(|violation| {
            !matches!(
                violation,
                CircuitBreakerViolation::DailyRealizedLoss { .. }
                    | CircuitBreakerViolation::EquityDrawdown { .. }
            )
        })
        .cloned()
        .collect();

    if !blocking_violations.is_empty() {
        let mut outcome = CircuitBreakerOutcome::with_error(
            state,
            CircuitBreakerError::RecoveryBlockedByViolations(
                blocking_violations
                    .iter()
                    .map(CircuitBreakerViolation::reason)
                    .collect(),
            ),
        );
        outcome.violations = violations;
        return outcome;
    }

    let triggered_at_ms = match state.triggered_at_ms {
        Some(value) => value,
        None => {
            return safety_fallback(
                state,
                server_now_ms,
                CircuitBreakerError::InvalidInput {
                    field: "state.triggered_at_ms",
                    requirement: "must be present while the breaker is open",
                },
            )
        }
    };
    let missing = match missing_half_open_prerequisites(readiness, triggered_at_ms, server_now_ms) {
        Ok(missing) => missing,
        Err(error) => return safety_fallback(state, server_now_ms, error),
    };
    if !missing.is_empty() {
        return CircuitBreakerOutcome::with_error(
            state,
            CircuitBreakerError::RecoveryPrerequisitesMissing(missing),
        );
    }

    let mut next = state.clone();
    next.status = CircuitBreakerStatus::HalfOpen;
    next.half_opened_at_ms = Some(server_now_ms);
    next.half_open_daily_loss_baseline_bps = Some(input.daily_realized_loss_bps);
    next.half_open_drawdown_baseline_bps = Some(input.equity_drawdown_bps);

    CircuitBreakerOutcome {
        state: next,
        transition: CircuitBreakerTransition::EnteredHalfOpen,
        violations,
        error: None,
        manual_reset_audit: None,
    }
}

fn close(
    policy: &CircuitBreakerPolicy,
    state: &CircuitBreakerState,
    health: &RecoveryHealth,
    authorization: &ResetAuthorization,
    server_now_ms: i64,
) -> CircuitBreakerOutcome {
    if state.status == CircuitBreakerStatus::Closed {
        return CircuitBreakerOutcome::unchanged(state);
    }
    if state.status != CircuitBreakerStatus::HalfOpen {
        return CircuitBreakerOutcome::with_error(
            state,
            CircuitBreakerError::InvalidTransition {
                status: state.status,
                request: TransitionRequestKind::Close,
            },
        );
    }

    let violations = match half_open_violations(policy, state, &health.input, server_now_ms) {
        Ok(mut violations) => {
            if health.new_hard_risk_violation {
                violations.push(CircuitBreakerViolation::HardRiskViolationDuringRecovery);
            }
            violations
        }
        Err(error) => return safety_fallback(state, server_now_ms, error),
    };
    if !violations.is_empty() {
        let fingerprint = incident_fingerprint(&violations, Some(&health.input));
        return open(state, violations, fingerprint, server_now_ms, true);
    }

    let mut unhealthy = Vec::new();
    if !health.clock_healthy {
        unhealthy.push(RecoveryPrerequisite::ClockHealth);
    }
    if !health.state_store_healthy {
        unhealthy.push(RecoveryPrerequisite::StateStoreHealth);
    }
    if !unhealthy.is_empty() {
        return CircuitBreakerOutcome::with_error(
            state,
            CircuitBreakerError::RecoveryPrerequisitesMissing(unhealthy),
        );
    }

    let half_opened_at_ms = match state.half_opened_at_ms {
        Some(value) => value,
        None => {
            return safety_fallback(
                state,
                server_now_ms,
                CircuitBreakerError::HalfOpenBaselineMissing,
            )
        }
    };
    let elapsed_ms = server_now_ms - half_opened_at_ms;
    if elapsed_ms < policy.half_open_observation_ms {
        return CircuitBreakerOutcome::with_error(
            state,
            CircuitBreakerError::HalfOpenObservationNotComplete {
                remaining_ms: policy.half_open_observation_ms - elapsed_ms,
            },
        );
    }

    let (reset_by, manual_reset_audit) = match authorization {
        ResetAuthorization::Automatic if policy.auto_reset => (String::from("AUTOMATIC"), None),
        ResetAuthorization::Automatic => {
            return CircuitBreakerOutcome::with_error(
                state,
                CircuitBreakerError::AutomaticResetDisabled,
            )
        }
        ResetAuthorization::Operator(evidence) => {
            if let Err(error) = validate_manual_reset_evidence(evidence) {
                return CircuitBreakerOutcome::with_error(state, error);
            }
            (
                evidence.operator_id.clone(),
                Some(ManualResetAuditRecord {
                    audit_event_id: evidence.audit_event_id.clone(),
                    operator_id: evidence.operator_id.clone(),
                    reason: evidence.reason.clone(),
                    evidence: evidence.evidence.clone(),
                    before_state: CircuitBreakerStatus::HalfOpen,
                    after_state: CircuitBreakerStatus::Closed,
                    recorded_at_ms: server_now_ms,
                }),
            )
        }
    };

    let mut next = state.clone();
    next.status = CircuitBreakerStatus::Closed;
    next.reason = CircuitBreakerReason::Ok;
    next.half_opened_at_ms = None;
    next.half_open_daily_loss_baseline_bps = None;
    next.half_open_drawdown_baseline_bps = None;
    next.reset_at_ms = Some(server_now_ms);
    next.reset_by = Some(reset_by);

    CircuitBreakerOutcome {
        state: next,
        transition: CircuitBreakerTransition::Closed,
        violations: Vec::new(),
        error: None,
        manual_reset_audit,
    }
}

fn record_blocked_intent_count(
    state: &CircuitBreakerState,
    observed_total: u64,
) -> CircuitBreakerOutcome {
    if observed_total <= state.blocked_intent_count {
        return CircuitBreakerOutcome::unchanged(state);
    }

    let mut next = state.clone();
    next.blocked_intent_count = observed_total;
    CircuitBreakerOutcome {
        state: next,
        transition: CircuitBreakerTransition::BlockedIntentCountAdvanced,
        violations: Vec::new(),
        error: None,
        manual_reset_audit: None,
    }
}

fn observe_half_open(
    state: &CircuitBreakerState,
    all_violations: Vec<CircuitBreakerViolation>,
    input: &CircuitBreakerInput,
    server_now_ms: i64,
) -> CircuitBreakerOutcome {
    let violations = match filter_half_open_violations(state, all_violations, input) {
        Ok(violations) => violations,
        Err(error) => return safety_fallback(state, server_now_ms, error),
    };

    if violations.is_empty() {
        CircuitBreakerOutcome::unchanged(state)
    } else {
        let fingerprint = incident_fingerprint(&violations, Some(input));
        open(state, violations, fingerprint, server_now_ms, true)
    }
}

fn half_open_violations(
    policy: &CircuitBreakerPolicy,
    state: &CircuitBreakerState,
    input: &CircuitBreakerInput,
    server_now_ms: i64,
) -> Result<Vec<CircuitBreakerViolation>, CircuitBreakerError> {
    let all = collect_violations(policy, input, server_now_ms)?;
    filter_half_open_violations(state, all, input)
}

fn filter_half_open_violations(
    state: &CircuitBreakerState,
    all_violations: Vec<CircuitBreakerViolation>,
    input: &CircuitBreakerInput,
) -> Result<Vec<CircuitBreakerViolation>, CircuitBreakerError> {
    let daily_baseline = state
        .half_open_daily_loss_baseline_bps
        .ok_or(CircuitBreakerError::HalfOpenBaselineMissing)?;
    let drawdown_baseline = state
        .half_open_drawdown_baseline_bps
        .ok_or(CircuitBreakerError::HalfOpenBaselineMissing)?;

    let mut violations = Vec::new();
    if input.daily_realized_loss_bps > daily_baseline {
        violations.push(
            all_violations
                .iter()
                .find(|violation| {
                    matches!(violation, CircuitBreakerViolation::DailyRealizedLoss { .. })
                })
                .cloned()
                .unwrap_or(CircuitBreakerViolation::DailyRealizedLossWorsened {
                    baseline_bps: daily_baseline,
                    observed_bps: input.daily_realized_loss_bps,
                }),
        );
    }
    if input.equity_drawdown_bps > drawdown_baseline {
        violations.push(
            all_violations
                .iter()
                .find(|violation| {
                    matches!(violation, CircuitBreakerViolation::EquityDrawdown { .. })
                })
                .cloned()
                .unwrap_or(CircuitBreakerViolation::EquityDrawdownWorsened {
                    baseline_bps: drawdown_baseline,
                    observed_bps: input.equity_drawdown_bps,
                }),
        );
    }
    violations.extend(all_violations.into_iter().filter(|violation| {
        !matches!(
            violation,
            CircuitBreakerViolation::DailyRealizedLoss { .. }
                | CircuitBreakerViolation::EquityDrawdown { .. }
        )
    }));

    Ok(violations)
}

fn collect_violations(
    policy: &CircuitBreakerPolicy,
    input: &CircuitBreakerInput,
    server_now_ms: i64,
) -> Result<Vec<CircuitBreakerViolation>, CircuitBreakerError> {
    if let Some(trigger) = &input.manual_trigger {
        validate_non_blank("manual_trigger.operator_id", &trigger.operator_id)?;
        validate_non_blank("manual_trigger.reason", &trigger.reason)?;
    }

    let mut violations = Vec::new();
    if input.daily_realized_loss_bps >= policy.max_daily_realized_loss_bps {
        violations.push(CircuitBreakerViolation::DailyRealizedLoss {
            observed_bps: input.daily_realized_loss_bps,
            threshold_bps: policy.max_daily_realized_loss_bps,
        });
    }
    if input.equity_drawdown_bps >= policy.max_equity_drawdown_bps {
        violations.push(CircuitBreakerViolation::EquityDrawdown {
            observed_bps: input.equity_drawdown_bps,
            threshold_bps: policy.max_equity_drawdown_bps,
        });
    }
    if input.consecutive_broker_rejections >= policy.max_consecutive_broker_rejections {
        violations.push(CircuitBreakerViolation::ConsecutiveBrokerRejections {
            observed: input.consecutive_broker_rejections,
            threshold: policy.max_consecutive_broker_rejections,
        });
    }
    if input.consecutive_command_failures >= policy.max_consecutive_command_failures {
        violations.push(CircuitBreakerViolation::ConsecutiveCommandFailures {
            observed: input.consecutive_command_failures,
            threshold: policy.max_consecutive_command_failures,
        });
    }
    if input.manual_reconciliation_required_count > 0 {
        violations.push(CircuitBreakerViolation::ManualReconciliationRequired {
            count: input.manual_reconciliation_required_count,
        });
    }
    if input.store_recovery_reconciliation_pending {
        violations.push(CircuitBreakerViolation::StoreRecoveryReconciliationPending);
    }
    if let Some(unhealthy_since_ms) = input.time_sync_unhealthy_since_ms {
        if unhealthy_since_ms < 0 || unhealthy_since_ms > server_now_ms {
            return Err(CircuitBreakerError::InvalidInput {
                field: "time_sync_unhealthy_since_ms",
                requirement: "must be between zero and server_now_ms",
            });
        }
        let unhealthy_for_ms = server_now_ms.checked_sub(unhealthy_since_ms).ok_or(
            CircuitBreakerError::InvalidInput {
                field: "time_sync_unhealthy_since_ms",
                requirement: "duration calculation must not overflow",
            },
        )?;
        if unhealthy_for_ms > policy.max_time_sync_unhealthy_ms {
            violations.push(CircuitBreakerViolation::TimeSyncUnhealthy {
                unhealthy_for_ms,
                threshold_ms: policy.max_time_sync_unhealthy_ms,
            });
        }
    }
    if input.consecutive_snapshot_stale_count > policy.max_consecutive_snapshot_stale {
        violations.push(CircuitBreakerViolation::SnapshotStale {
            consecutive_count: input.consecutive_snapshot_stale_count,
            threshold: policy.max_consecutive_snapshot_stale,
        });
    }
    if input.consecutive_symbol_metadata_stale_count > policy.max_consecutive_symbol_metadata_stale
    {
        violations.push(CircuitBreakerViolation::SymbolMetadataStale {
            consecutive_count: input.consecutive_symbol_metadata_stale_count,
            threshold: policy.max_consecutive_symbol_metadata_stale,
        });
    }
    if let Some(trigger) = &input.manual_trigger {
        violations.push(CircuitBreakerViolation::ManualTrigger {
            operator_id: trigger.operator_id.clone(),
            reason: trigger.reason.clone(),
        });
    }

    Ok(violations)
}

fn missing_half_open_prerequisites(
    readiness: HalfOpenReadiness,
    triggered_at_ms: i64,
    server_now_ms: i64,
) -> Result<Vec<RecoveryPrerequisite>, CircuitBreakerError> {
    let mut missing = Vec::new();
    for (field, observed_at_ms, prerequisite) in [
        (
            "readiness.account_refreshed_at_ms",
            readiness.account_refreshed_at_ms,
            RecoveryPrerequisite::AccountRefresh,
        ),
        (
            "readiness.positions_refreshed_at_ms",
            readiness.positions_refreshed_at_ms,
            RecoveryPrerequisite::PositionRefresh,
        ),
        (
            "readiness.orders_refreshed_at_ms",
            readiness.orders_refreshed_at_ms,
            RecoveryPrerequisite::OrderRefresh,
        ),
        (
            "readiness.symbol_metadata_refreshed_at_ms",
            readiness.symbol_metadata_refreshed_at_ms,
            RecoveryPrerequisite::SymbolMetadataRefresh,
        ),
        (
            "readiness.pending_commands_reconciled_at_ms",
            readiness.pending_commands_reconciled_at_ms,
            RecoveryPrerequisite::PendingCommandReconciliation,
        ),
    ] {
        if let Some(observed_at_ms) = observed_at_ms {
            if observed_at_ms < 0 || observed_at_ms > server_now_ms {
                return Err(CircuitBreakerError::InvalidInput {
                    field,
                    requirement: "must be between zero and server_now_ms",
                });
            }
        }
        if observed_at_ms.is_none_or(|value| value < triggered_at_ms) {
            missing.push(prerequisite);
        }
    }
    Ok(missing)
}

fn validate_manual_reset_evidence(
    evidence: &ManualResetEvidence,
) -> Result<(), CircuitBreakerError> {
    for (field, value) in [
        ("audit_event_id", evidence.audit_event_id.as_str()),
        ("operator_id", evidence.operator_id.as_str()),
        ("reason", evidence.reason.as_str()),
        ("evidence", evidence.evidence.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(CircuitBreakerError::ManualResetEvidenceMissing { field });
        }
    }
    Ok(())
}

fn validate_non_blank(field: &'static str, value: &str) -> Result<(), CircuitBreakerError> {
    if value.trim().is_empty() {
        Err(CircuitBreakerError::InvalidInput {
            field,
            requirement: "must not be blank",
        })
    } else {
        Ok(())
    }
}

fn open(
    state: &CircuitBreakerState,
    violations: Vec<CircuitBreakerViolation>,
    fingerprint: CircuitBreakerIncidentFingerprint,
    server_now_ms: i64,
    reopened: bool,
) -> CircuitBreakerOutcome {
    let primary = violations
        .first()
        .expect("open requires at least one violation");
    let mut next = state.clone();
    next.status = CircuitBreakerStatus::Open;
    next.reason = primary.reason();
    next.triggered_at_ms = Some(server_now_ms);
    next.triggered_by = Some(primary.source());
    next.last_incident_fingerprint = Some(fingerprint);
    next.incident_evidence_cleared_at_ms = None;
    next.half_opened_at_ms = None;
    next.half_open_daily_loss_baseline_bps = None;
    next.half_open_drawdown_baseline_bps = None;
    next.reset_at_ms = None;
    next.reset_by = None;

    CircuitBreakerOutcome {
        state: next,
        transition: if reopened {
            CircuitBreakerTransition::Reopened
        } else {
            CircuitBreakerTransition::Opened
        },
        violations,
        error: None,
        manual_reset_audit: None,
    }
}

fn safety_fallback(
    state: &CircuitBreakerState,
    server_now_ms: i64,
    error: CircuitBreakerError,
) -> CircuitBreakerOutcome {
    let violations = vec![CircuitBreakerViolation::SafetyInvariantViolation];
    let fingerprint = CircuitBreakerIncidentFingerprint(vec![
        CircuitBreakerIncidentEvidence::SafetyInvariantError(error.clone()),
    ]);
    let fallback_at_ms = state
        .latest_transition_at_ms()
        .map_or(server_now_ms.max(0), |latest| {
            latest.max(server_now_ms.max(0))
        });
    if state.status == CircuitBreakerStatus::Open {
        if state.last_incident_fingerprint.as_ref() != Some(&fingerprint) {
            let mut outcome = open(state, violations, fingerprint, fallback_at_ms, true);
            outcome.transition = CircuitBreakerTransition::SafetyFallbackOpened;
            outcome.error = Some(error);
            return outcome;
        }
        return CircuitBreakerOutcome {
            state: state.clone(),
            transition: CircuitBreakerTransition::NoChange,
            violations,
            error: Some(error),
            manual_reset_audit: None,
        };
    }

    let mut outcome = open(
        state,
        violations,
        fingerprint,
        fallback_at_ms,
        state.status == CircuitBreakerStatus::HalfOpen,
    );
    outcome.transition = CircuitBreakerTransition::SafetyFallbackOpened;
    outcome.error = Some(error);
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 10_000;

    fn policy() -> CircuitBreakerPolicy {
        CircuitBreakerPolicy {
            max_daily_realized_loss_bps: 300,
            max_equity_drawdown_bps: 1_000,
            max_consecutive_broker_rejections: 3,
            max_consecutive_command_failures: 2,
            max_time_sync_unhealthy_ms: 5_000,
            max_consecutive_snapshot_stale: 2,
            max_consecutive_symbol_metadata_stale: 2,
            half_open_observation_ms: 1_000,
            auto_reset: false,
        }
    }

    fn healthy_input() -> CircuitBreakerInput {
        CircuitBreakerInput {
            daily_realized_loss_bps: 10,
            equity_drawdown_bps: 20,
            ..CircuitBreakerInput::default()
        }
    }

    fn readiness_at(completed_at_ms: i64) -> HalfOpenReadiness {
        HalfOpenReadiness {
            account_refreshed_at_ms: Some(completed_at_ms),
            positions_refreshed_at_ms: Some(completed_at_ms),
            orders_refreshed_at_ms: Some(completed_at_ms),
            symbol_metadata_refreshed_at_ms: Some(completed_at_ms),
            pending_commands_reconciled_at_ms: Some(completed_at_ms),
        }
    }

    fn readiness() -> HalfOpenReadiness {
        readiness_at(NOW)
    }

    fn manual_reset() -> ResetAuthorization {
        ResetAuthorization::Operator(ManualResetEvidence {
            audit_event_id: String::from("audit-1"),
            operator_id: String::from("operator-1"),
            reason: String::from("broker positions reconciled"),
            evidence: String::from("ticket-42"),
        })
    }

    fn opened(input: CircuitBreakerInput) -> CircuitBreakerState {
        transition(
            &policy(),
            &CircuitBreakerState::new(),
            &TransitionRequest::Observe(input),
            NOW,
        )
        .state
    }

    fn half_open(input: CircuitBreakerInput) -> CircuitBreakerState {
        let state = opened(CircuitBreakerInput {
            manual_trigger: Some(ManualTrigger {
                operator_id: String::from("operator-1"),
                reason: String::from("incident"),
            }),
            ..healthy_input()
        });
        let observed = transition(
            &policy(),
            &state,
            &TransitionRequest::Observe(input.clone()),
            NOW + 50,
        );
        transition(
            &policy(),
            &observed.state,
            &TransitionRequest::EnterHalfOpen {
                readiness: readiness_at(NOW + 50),
                input,
            },
            NOW + 100,
        )
        .state
    }

    #[test]
    fn closed_allows_every_action() {
        let state = CircuitBreakerState::new();
        let actions = [
            CircuitBreakerAction::RiskIncreasingTradeIntent,
            CircuitBreakerAction::RiskIncreasingCommand,
            CircuitBreakerAction::RiskReducingCommand,
            CircuitBreakerAction::ReadState,
            CircuitBreakerAction::IngestSnapshot,
            CircuitBreakerAction::IngestExecutionEvent,
            CircuitBreakerAction::Reconciliation,
            CircuitBreakerAction::ManualReview,
            CircuitBreakerAction::NoOpValidation,
        ];

        assert!(actions
            .into_iter()
            .all(|action| state.authorize(action).allowed));
    }

    #[test]
    fn active_breaker_only_blocks_risk_increasing_actions() {
        let open = opened(CircuitBreakerInput {
            consecutive_command_failures: 2,
            ..healthy_input()
        });
        let half_open = transition(
            &policy(),
            &open,
            &TransitionRequest::EnterHalfOpen {
                readiness: readiness(),
                input: healthy_input(),
            },
            NOW + 1,
        )
        .state;

        for state in [open, half_open] {
            assert!(
                !state
                    .authorize(CircuitBreakerAction::RiskIncreasingTradeIntent)
                    .allowed
            );
            assert!(
                !state
                    .authorize(CircuitBreakerAction::RiskIncreasingCommand)
                    .allowed
            );
            for allowed in [
                CircuitBreakerAction::RiskReducingCommand,
                CircuitBreakerAction::ReadState,
                CircuitBreakerAction::IngestSnapshot,
                CircuitBreakerAction::IngestExecutionEvent,
                CircuitBreakerAction::Reconciliation,
                CircuitBreakerAction::ManualReview,
                CircuitBreakerAction::NoOpValidation,
            ] {
                assert!(state.authorize(allowed).allowed);
            }
        }
    }

    #[test]
    fn inclusive_financial_and_failure_thresholds_open_breaker() {
        let cases = [
            CircuitBreakerInput {
                daily_realized_loss_bps: 300,
                ..healthy_input()
            },
            CircuitBreakerInput {
                equity_drawdown_bps: 1_000,
                ..healthy_input()
            },
            CircuitBreakerInput {
                consecutive_broker_rejections: 3,
                ..healthy_input()
            },
            CircuitBreakerInput {
                consecutive_command_failures: 2,
                ..healthy_input()
            },
            CircuitBreakerInput {
                manual_reconciliation_required_count: 1,
                ..healthy_input()
            },
            CircuitBreakerInput {
                store_recovery_reconciliation_pending: true,
                ..healthy_input()
            },
        ];

        for input in cases {
            let outcome = transition(
                &policy(),
                &CircuitBreakerState::new(),
                &TransitionRequest::Observe(input),
                NOW,
            );
            assert_eq!(outcome.state.status(), CircuitBreakerStatus::Open);
            assert_eq!(outcome.transition, CircuitBreakerTransition::Opened);
        }
    }

    #[test]
    fn stale_and_time_sync_thresholds_are_strictly_exceeded() {
        let at_threshold = CircuitBreakerInput {
            time_sync_unhealthy_since_ms: Some(NOW - 5_000),
            consecutive_snapshot_stale_count: 2,
            consecutive_symbol_metadata_stale_count: 2,
            ..healthy_input()
        };
        let unchanged = transition(
            &policy(),
            &CircuitBreakerState::new(),
            &TransitionRequest::Observe(at_threshold),
            NOW,
        );
        assert_eq!(unchanged.state.status(), CircuitBreakerStatus::Closed);

        for input in [
            CircuitBreakerInput {
                time_sync_unhealthy_since_ms: Some(NOW - 5_001),
                ..healthy_input()
            },
            CircuitBreakerInput {
                consecutive_snapshot_stale_count: 3,
                ..healthy_input()
            },
            CircuitBreakerInput {
                consecutive_symbol_metadata_stale_count: 3,
                ..healthy_input()
            },
        ] {
            assert_eq!(
                opened(input).status(),
                CircuitBreakerStatus::Open,
                "threshold must open the breaker"
            );
        }
    }

    #[test]
    fn all_simultaneous_violations_are_reported_in_deterministic_order() {
        let outcome = transition(
            &policy(),
            &CircuitBreakerState::new(),
            &TransitionRequest::Observe(CircuitBreakerInput {
                daily_realized_loss_bps: 300,
                equity_drawdown_bps: 1_000,
                consecutive_broker_rejections: 3,
                consecutive_command_failures: 2,
                manual_reconciliation_required_count: 1,
                store_recovery_reconciliation_pending: true,
                time_sync_unhealthy_since_ms: Some(NOW - 5_001),
                consecutive_snapshot_stale_count: 3,
                consecutive_symbol_metadata_stale_count: 3,
                manual_trigger: Some(ManualTrigger {
                    operator_id: String::from("operator-1"),
                    reason: String::from("incident"),
                }),
            }),
            NOW,
        );

        assert_eq!(outcome.violations.len(), 10);
        assert!(matches!(
            outcome.violations.first(),
            Some(CircuitBreakerViolation::DailyRealizedLoss { .. })
        ));
        assert_eq!(
            outcome.state.reason(),
            CircuitBreakerReason::DailyRealizedLossLimit
        );
    }

    #[test]
    fn invalid_policy_or_future_unhealthy_timestamp_fails_closed() {
        let mut invalid_policy = policy();
        invalid_policy.max_daily_realized_loss_bps = 0;
        let bad_policy = transition(
            &invalid_policy,
            &CircuitBreakerState::new(),
            &TransitionRequest::Observe(healthy_input()),
            NOW,
        );
        assert_eq!(bad_policy.state.status(), CircuitBreakerStatus::Open);
        assert_eq!(
            bad_policy.transition,
            CircuitBreakerTransition::SafetyFallbackOpened
        );
        assert!(matches!(
            bad_policy.error,
            Some(CircuitBreakerError::InvalidPolicy { .. })
        ));

        let bad_time = transition(
            &policy(),
            &CircuitBreakerState::new(),
            &TransitionRequest::Observe(CircuitBreakerInput {
                time_sync_unhealthy_since_ms: Some(NOW + 1),
                ..healthy_input()
            }),
            NOW,
        );
        assert_eq!(bad_time.state.status(), CircuitBreakerStatus::Open);
        assert!(matches!(
            bad_time.error,
            Some(CircuitBreakerError::InvalidInput { .. })
        ));
    }

    #[test]
    fn negative_server_time_fails_closed_without_negative_state_timestamps() {
        let outcome = transition(
            &policy(),
            &CircuitBreakerState::new(),
            &TransitionRequest::Observe(healthy_input()),
            -1,
        );

        assert_eq!(outcome.state.status(), CircuitBreakerStatus::Open);
        assert_eq!(outcome.state.triggered_at_ms(), Some(0));
        assert!(outcome
            .state
            .latest_transition_at_ms()
            .is_some_and(|timestamp| timestamp >= 0));
        assert!(matches!(
            outcome.error,
            Some(CircuitBreakerError::InvalidInput {
                field: "server_now_ms",
                ..
            })
        ));
    }

    #[test]
    fn safety_fault_fingerprint_is_idempotent_but_distinguishes_errors() {
        let open = transition(
            &policy(),
            &CircuitBreakerState::new(),
            &TransitionRequest::Observe(CircuitBreakerInput {
                consecutive_command_failures: 2,
                ..healthy_input()
            }),
            NOW,
        );
        let mut invalid_policy = policy();
        invalid_policy.max_daily_realized_loss_bps = 0;
        let policy_fault = transition(
            &invalid_policy,
            &open.state,
            &TransitionRequest::Observe(healthy_input()),
            NOW + 100,
        );
        let repeated_policy_fault = transition(
            &invalid_policy,
            &policy_fault.state,
            &TransitionRequest::Observe(healthy_input()),
            NOW + 200,
        );

        assert_eq!(
            policy_fault.transition,
            CircuitBreakerTransition::SafetyFallbackOpened
        );
        assert_eq!(policy_fault.state.triggered_at_ms(), Some(NOW + 100));
        assert_eq!(
            repeated_policy_fault.transition,
            CircuitBreakerTransition::NoChange
        );
        assert_eq!(repeated_policy_fault.state, policy_fault.state);

        let invalid_input = CircuitBreakerInput {
            time_sync_unhealthy_since_ms: Some(NOW + 10_000),
            ..healthy_input()
        };
        let input_fault = transition(
            &policy(),
            &repeated_policy_fault.state,
            &TransitionRequest::Observe(invalid_input.clone()),
            NOW + 300,
        );
        let repeated_input_fault = transition(
            &policy(),
            &input_fault.state,
            &TransitionRequest::Observe(invalid_input),
            NOW + 400,
        );

        assert_eq!(
            input_fault.transition,
            CircuitBreakerTransition::SafetyFallbackOpened
        );
        assert_eq!(input_fault.state.triggered_at_ms(), Some(NOW + 300));
        assert_eq!(
            repeated_input_fault.transition,
            CircuitBreakerTransition::NoChange
        );
        assert_eq!(repeated_input_fault.state, input_fault.state);
    }

    #[test]
    fn repeated_trigger_and_absolute_blocked_count_are_idempotent() {
        let request = TransitionRequest::Observe(CircuitBreakerInput {
            consecutive_command_failures: 2,
            ..healthy_input()
        });
        let first = transition(&policy(), &CircuitBreakerState::new(), &request, NOW);
        let second = transition(&policy(), &first.state, &request, NOW + 100);
        assert_eq!(second.transition, CircuitBreakerTransition::NoChange);
        assert_eq!(second.state, first.state);
        assert_eq!(second.state.triggered_at_ms(), Some(NOW));

        let count_request = TransitionRequest::RecordBlockedIntentCount { observed_total: 7 };
        let counted = transition(&policy(), &second.state, &count_request, NOW + 100);
        assert_eq!(counted.state.blocked_intent_count(), 7);
        let retried = transition(&policy(), &counted.state, &count_request, NOW + 100);
        assert_eq!(retried.transition, CircuitBreakerTransition::NoChange);
        assert_eq!(retried.state, counted.state);
    }

    #[test]
    fn new_open_incident_invalidates_readiness_from_previous_epoch() {
        let first = transition(
            &policy(),
            &CircuitBreakerState::new(),
            &TransitionRequest::Observe(CircuitBreakerInput {
                consecutive_command_failures: 2,
                ..healthy_input()
            }),
            NOW,
        );
        let readiness_before_second_incident = readiness_at(NOW + 100);

        let second = transition(
            &policy(),
            &first.state,
            &TransitionRequest::Observe(CircuitBreakerInput {
                consecutive_broker_rejections: 3,
                ..healthy_input()
            }),
            NOW + 200,
        );

        assert_eq!(second.transition, CircuitBreakerTransition::Reopened);
        assert_eq!(second.state.triggered_at_ms(), Some(NOW + 200));
        assert_eq!(
            second.state.reason(),
            CircuitBreakerReason::ConsecutiveBrokerRejections
        );

        let recovery = transition(
            &policy(),
            &second.state,
            &TransitionRequest::EnterHalfOpen {
                readiness: readiness_before_second_incident,
                input: healthy_input(),
            },
            NOW + 300,
        );

        assert_eq!(recovery.state.status(), CircuitBreakerStatus::Open);
        assert!(matches!(
            recovery.error,
            Some(CircuitBreakerError::RecoveryPrerequisitesMissing(ref missing))
                if missing.len() == 5
        ));
    }

    #[test]
    fn polling_same_time_sync_incident_does_not_advance_recovery_epoch() {
        let input = CircuitBreakerInput {
            time_sync_unhealthy_since_ms: Some(NOW - 5_001),
            ..healthy_input()
        };
        let first = transition(
            &policy(),
            &CircuitBreakerState::new(),
            &TransitionRequest::Observe(input.clone()),
            NOW,
        );
        let polled = transition(
            &policy(),
            &first.state,
            &TransitionRequest::Observe(input),
            NOW + 500,
        );

        assert_eq!(polled.transition, CircuitBreakerTransition::NoChange);
        assert_eq!(polled.state, first.state);
        assert_eq!(polled.state.triggered_at_ms(), Some(NOW));
        assert!(matches!(
            polled.violations.as_slice(),
            [CircuitBreakerViolation::TimeSyncUnhealthy {
                unhealthy_for_ms: 5_501,
                ..
            }]
        ));
    }

    #[test]
    fn same_violation_after_healthy_observation_starts_a_new_incident_epoch() {
        let violation = CircuitBreakerInput {
            consecutive_command_failures: 2,
            ..healthy_input()
        };
        let first = transition(
            &policy(),
            &CircuitBreakerState::new(),
            &TransitionRequest::Observe(violation.clone()),
            NOW,
        );
        let healthy = transition(
            &policy(),
            &first.state,
            &TransitionRequest::Observe(healthy_input()),
            NOW + 100,
        );
        let healthy_retry = transition(
            &policy(),
            &healthy.state,
            &TransitionRequest::Observe(healthy_input()),
            NOW + 150,
        );
        let recurrence = transition(
            &policy(),
            &healthy_retry.state,
            &TransitionRequest::Observe(violation),
            NOW + 200,
        );

        assert_eq!(
            healthy.transition,
            CircuitBreakerTransition::IncidentEvidenceCleared
        );
        assert_eq!(healthy.state.status(), CircuitBreakerStatus::Open);
        assert_eq!(healthy.state.triggered_at_ms(), Some(NOW));
        assert_eq!(
            healthy.state.incident_evidence_cleared_at_ms(),
            Some(NOW + 100)
        );
        assert_eq!(healthy_retry.transition, CircuitBreakerTransition::NoChange);
        assert_eq!(healthy_retry.state, healthy.state);
        assert_eq!(recurrence.transition, CircuitBreakerTransition::Reopened);
        assert_eq!(recurrence.state.triggered_at_ms(), Some(NOW + 200));
    }

    #[test]
    fn incident_evidence_clear_timestamp_rejects_late_violation_observation() {
        let violation = CircuitBreakerInput {
            consecutive_command_failures: 2,
            ..healthy_input()
        };
        let open = transition(
            &policy(),
            &CircuitBreakerState::new(),
            &TransitionRequest::Observe(violation.clone()),
            NOW,
        );
        let cleared = transition(
            &policy(),
            &open.state,
            &TransitionRequest::Observe(healthy_input()),
            NOW + 200,
        );
        let late = transition(
            &policy(),
            &cleared.state,
            &TransitionRequest::Observe(violation),
            NOW + 100,
        );

        assert_eq!(
            cleared.state.incident_evidence_cleared_at_ms(),
            Some(NOW + 200)
        );
        assert_eq!(late.state.status(), CircuitBreakerStatus::Open);
        assert_eq!(late.state.triggered_at_ms(), Some(NOW + 200));
        assert!(matches!(
            late.error,
            Some(CircuitBreakerError::ServerTimeRegressed {
                latest_transition_at_ms: latest,
                server_now_ms: observed,
            }) if latest == NOW + 200 && observed == NOW + 100
        ));
    }

    #[test]
    fn new_blocking_violation_during_half_open_entry_invalidates_old_readiness() {
        let first = transition(
            &policy(),
            &CircuitBreakerState::new(),
            &TransitionRequest::Observe(CircuitBreakerInput {
                consecutive_command_failures: 2,
                ..healthy_input()
            }),
            NOW,
        );
        let old_readiness = readiness_at(NOW + 100);
        let new_incident = transition(
            &policy(),
            &first.state,
            &TransitionRequest::EnterHalfOpen {
                readiness: old_readiness,
                input: CircuitBreakerInput {
                    consecutive_broker_rejections: 3,
                    ..healthy_input()
                },
            },
            NOW + 200,
        );

        assert_eq!(new_incident.transition, CircuitBreakerTransition::Reopened);
        assert_eq!(new_incident.state.triggered_at_ms(), Some(NOW + 200));

        let recovery = transition(
            &policy(),
            &new_incident.state,
            &TransitionRequest::EnterHalfOpen {
                readiness: old_readiness,
                input: healthy_input(),
            },
            NOW + 300,
        );
        assert_eq!(recovery.state.status(), CircuitBreakerStatus::Open);
        assert!(matches!(
            recovery.error,
            Some(CircuitBreakerError::RecoveryPrerequisitesMissing(ref missing))
                if missing.len() == 5
        ));
    }

    #[test]
    fn changed_financial_evidence_during_half_open_entry_starts_a_new_epoch() {
        let cases = [
            (
                CircuitBreakerInput {
                    daily_realized_loss_bps: 300,
                    ..healthy_input()
                },
                CircuitBreakerInput {
                    daily_realized_loss_bps: 301,
                    ..healthy_input()
                },
            ),
            (
                CircuitBreakerInput {
                    equity_drawdown_bps: 1_000,
                    ..healthy_input()
                },
                CircuitBreakerInput {
                    equity_drawdown_bps: 1_001,
                    ..healthy_input()
                },
            ),
            (
                CircuitBreakerInput {
                    consecutive_command_failures: 2,
                    ..healthy_input()
                },
                CircuitBreakerInput {
                    daily_realized_loss_bps: 300,
                    ..healthy_input()
                },
            ),
        ];

        for (initial, changed_financial_evidence) in cases {
            let open = transition(
                &policy(),
                &CircuitBreakerState::new(),
                &TransitionRequest::Observe(initial),
                NOW,
            );
            let old_readiness = readiness_at(NOW + 100);
            let changed = transition(
                &policy(),
                &open.state,
                &TransitionRequest::EnterHalfOpen {
                    readiness: old_readiness,
                    input: changed_financial_evidence.clone(),
                },
                NOW + 200,
            );

            assert_eq!(changed.transition, CircuitBreakerTransition::Reopened);
            assert_eq!(changed.state.triggered_at_ms(), Some(NOW + 200));

            let stale_recovery = transition(
                &policy(),
                &changed.state,
                &TransitionRequest::EnterHalfOpen {
                    readiness: old_readiness,
                    input: changed_financial_evidence.clone(),
                },
                NOW + 300,
            );
            assert!(matches!(
                stale_recovery.error,
                Some(CircuitBreakerError::RecoveryPrerequisitesMissing(ref missing))
                    if missing.len() == 5
            ));

            let refreshed_recovery = transition(
                &policy(),
                &stale_recovery.state,
                &TransitionRequest::EnterHalfOpen {
                    readiness: readiness_at(NOW + 200),
                    input: changed_financial_evidence,
                },
                NOW + 301,
            );
            assert_eq!(
                refreshed_recovery.transition,
                CircuitBreakerTransition::EnteredHalfOpen
            );
        }
    }

    #[test]
    fn open_to_half_open_requires_refresh_reconciliation_and_clear_operational_faults() {
        let open = opened(CircuitBreakerInput {
            daily_realized_loss_bps: 300,
            ..healthy_input()
        });
        let missing = transition(
            &policy(),
            &open,
            &TransitionRequest::EnterHalfOpen {
                readiness: HalfOpenReadiness::default(),
                input: CircuitBreakerInput {
                    daily_realized_loss_bps: 300,
                    ..healthy_input()
                },
            },
            NOW + 1,
        );
        assert_eq!(missing.state.status(), CircuitBreakerStatus::Open);
        assert!(matches!(
            missing.error,
            Some(CircuitBreakerError::RecoveryPrerequisitesMissing(_))
        ));

        let still_unhealthy = transition(
            &policy(),
            &open,
            &TransitionRequest::EnterHalfOpen {
                readiness: readiness(),
                input: CircuitBreakerInput {
                    manual_reconciliation_required_count: 1,
                    ..healthy_input()
                },
            },
            NOW + 1,
        );
        assert_eq!(
            still_unhealthy.transition,
            CircuitBreakerTransition::Reopened
        );
        assert_eq!(still_unhealthy.state.triggered_at_ms(), Some(NOW + 1));

        let repeated_fault = transition(
            &policy(),
            &still_unhealthy.state,
            &TransitionRequest::EnterHalfOpen {
                readiness: readiness(),
                input: CircuitBreakerInput {
                    manual_reconciliation_required_count: 1,
                    ..healthy_input()
                },
            },
            NOW + 2,
        );
        assert!(matches!(
            repeated_fault.error,
            Some(CircuitBreakerError::RecoveryBlockedByViolations(_))
        ));
        assert_eq!(repeated_fault.state.triggered_at_ms(), Some(NOW + 1));

        let recovered = transition(
            &policy(),
            &open,
            &TransitionRequest::EnterHalfOpen {
                readiness: readiness(),
                input: CircuitBreakerInput {
                    daily_realized_loss_bps: 300,
                    ..healthy_input()
                },
            },
            NOW + 1,
        );
        assert_eq!(recovered.state.status(), CircuitBreakerStatus::HalfOpen);
    }

    #[test]
    fn half_open_readiness_at_trigger_time_is_accepted() {
        let open = opened(CircuitBreakerInput {
            consecutive_command_failures: 2,
            ..healthy_input()
        });
        let outcome = transition(
            &policy(),
            &open,
            &TransitionRequest::EnterHalfOpen {
                readiness: readiness(),
                input: healthy_input(),
            },
            NOW + 1,
        );

        assert_eq!(open.triggered_at_ms(), Some(NOW));
        assert_eq!(outcome.state.status(), CircuitBreakerStatus::HalfOpen);
        assert_eq!(
            outcome.transition,
            CircuitBreakerTransition::EnteredHalfOpen
        );
    }

    #[test]
    fn each_half_open_readiness_timestamp_must_not_predate_trigger() {
        let open = opened(CircuitBreakerInput {
            consecutive_command_failures: 2,
            ..healthy_input()
        });
        let cases = [
            (
                HalfOpenReadiness {
                    account_refreshed_at_ms: Some(NOW - 1),
                    ..readiness()
                },
                RecoveryPrerequisite::AccountRefresh,
            ),
            (
                HalfOpenReadiness {
                    positions_refreshed_at_ms: Some(NOW - 1),
                    ..readiness()
                },
                RecoveryPrerequisite::PositionRefresh,
            ),
            (
                HalfOpenReadiness {
                    orders_refreshed_at_ms: Some(NOW - 1),
                    ..readiness()
                },
                RecoveryPrerequisite::OrderRefresh,
            ),
            (
                HalfOpenReadiness {
                    symbol_metadata_refreshed_at_ms: Some(NOW - 1),
                    ..readiness()
                },
                RecoveryPrerequisite::SymbolMetadataRefresh,
            ),
            (
                HalfOpenReadiness {
                    pending_commands_reconciled_at_ms: Some(NOW - 1),
                    ..readiness()
                },
                RecoveryPrerequisite::PendingCommandReconciliation,
            ),
        ];

        for (readiness, expected_missing) in cases {
            let outcome = transition(
                &policy(),
                &open,
                &TransitionRequest::EnterHalfOpen {
                    readiness,
                    input: healthy_input(),
                },
                NOW + 1,
            );

            assert_eq!(outcome.state.status(), CircuitBreakerStatus::Open);
            assert_eq!(
                outcome.error,
                Some(CircuitBreakerError::RecoveryPrerequisitesMissing(vec![
                    expected_missing,
                ]))
            );
        }
    }

    #[test]
    fn future_half_open_readiness_timestamp_fails_closed() {
        let open = opened(CircuitBreakerInput {
            consecutive_command_failures: 2,
            ..healthy_input()
        });
        let outcome = transition(
            &policy(),
            &open,
            &TransitionRequest::EnterHalfOpen {
                readiness: HalfOpenReadiness {
                    account_refreshed_at_ms: Some(NOW + 2),
                    ..readiness()
                },
                input: healthy_input(),
            },
            NOW + 1,
        );

        assert_eq!(outcome.state.status(), CircuitBreakerStatus::Open);
        assert!(matches!(
            outcome.error,
            Some(CircuitBreakerError::InvalidInput {
                field: "readiness.account_refreshed_at_ms",
                ..
            })
        ));
        assert_eq!(
            outcome.violations,
            vec![CircuitBreakerViolation::SafetyInvariantViolation]
        );
    }

    #[test]
    fn open_cannot_transition_directly_to_closed() {
        let open = opened(CircuitBreakerInput {
            consecutive_command_failures: 2,
            ..healthy_input()
        });
        let outcome = transition(
            &policy(),
            &open,
            &TransitionRequest::Close {
                health: RecoveryHealth {
                    clock_healthy: true,
                    state_store_healthy: true,
                    new_hard_risk_violation: false,
                    input: healthy_input(),
                },
                authorization: manual_reset(),
            },
            NOW + 2_000,
        );

        assert_eq!(outcome.state.status(), CircuitBreakerStatus::Open);
        assert!(matches!(
            outcome.error,
            Some(CircuitBreakerError::InvalidTransition { .. })
        ));
    }

    #[test]
    fn half_open_allows_persistent_loss_but_reopens_if_it_worsens() {
        let baseline = CircuitBreakerInput {
            daily_realized_loss_bps: 300,
            equity_drawdown_bps: 1_000,
            ..healthy_input()
        };
        let half_open = half_open(baseline.clone());

        let unchanged = transition(
            &policy(),
            &half_open,
            &TransitionRequest::Observe(baseline.clone()),
            NOW + 200,
        );
        assert_eq!(unchanged.state.status(), CircuitBreakerStatus::HalfOpen);

        let reopened = transition(
            &policy(),
            &half_open,
            &TransitionRequest::Observe(CircuitBreakerInput {
                daily_realized_loss_bps: 301,
                ..baseline
            }),
            NOW + 200,
        );
        assert_eq!(reopened.state.status(), CircuitBreakerStatus::Open);
        assert_eq!(reopened.transition, CircuitBreakerTransition::Reopened);
        assert_eq!(
            reopened.state.reason(),
            CircuitBreakerReason::DailyRealizedLossLimit
        );
    }

    #[test]
    fn half_open_reopens_on_worsening_below_the_policy_limit() {
        let half_open = half_open(healthy_input());
        let outcome = transition(
            &policy(),
            &half_open,
            &TransitionRequest::Observe(CircuitBreakerInput {
                daily_realized_loss_bps: 11,
                ..healthy_input()
            }),
            NOW + 200,
        );

        assert_eq!(outcome.transition, CircuitBreakerTransition::Reopened);
        assert!(matches!(
            outcome.violations.as_slice(),
            [CircuitBreakerViolation::DailyRealizedLossWorsened {
                baseline_bps: 10,
                observed_bps: 11,
            }]
        ));
    }

    #[test]
    fn half_open_reopens_on_any_new_hard_risk_violation() {
        let half_open = half_open(healthy_input());
        let outcome = transition(
            &policy(),
            &half_open,
            &TransitionRequest::Close {
                health: RecoveryHealth {
                    clock_healthy: true,
                    state_store_healthy: true,
                    new_hard_risk_violation: true,
                    input: healthy_input(),
                },
                authorization: manual_reset(),
            },
            NOW + 1_100,
        );
        assert_eq!(outcome.transition, CircuitBreakerTransition::Reopened);
        assert_eq!(
            outcome.state.reason(),
            CircuitBreakerReason::HardRiskViolationDuringRecovery
        );
    }

    #[test]
    fn close_requires_observation_health_and_authorization() {
        let half_open = half_open(healthy_input());
        let healthy = RecoveryHealth {
            clock_healthy: true,
            state_store_healthy: true,
            new_hard_risk_violation: false,
            input: healthy_input(),
        };
        let early = transition(
            &policy(),
            &half_open,
            &TransitionRequest::Close {
                health: healthy.clone(),
                authorization: manual_reset(),
            },
            NOW + 1_099,
        );
        assert!(matches!(
            early.error,
            Some(CircuitBreakerError::HalfOpenObservationNotComplete { .. })
        ));

        let unhealthy = transition(
            &policy(),
            &half_open,
            &TransitionRequest::Close {
                health: RecoveryHealth {
                    clock_healthy: false,
                    ..healthy.clone()
                },
                authorization: manual_reset(),
            },
            NOW + 1_100,
        );
        assert!(matches!(
            unhealthy.error,
            Some(CircuitBreakerError::RecoveryPrerequisitesMissing(_))
        ));

        let automatic = transition(
            &policy(),
            &half_open,
            &TransitionRequest::Close {
                health: healthy,
                authorization: ResetAuthorization::Automatic,
            },
            NOW + 1_100,
        );
        assert_eq!(automatic.state.status(), CircuitBreakerStatus::HalfOpen);
        assert_eq!(
            automatic.error,
            Some(CircuitBreakerError::AutomaticResetDisabled)
        );
    }

    #[test]
    fn manual_reset_requires_and_returns_complete_audit_record() {
        let half_open = half_open(healthy_input());
        let authorization = manual_reset();
        let request = TransitionRequest::Close {
            health: RecoveryHealth {
                clock_healthy: true,
                state_store_healthy: true,
                new_hard_risk_violation: false,
                input: healthy_input(),
            },
            authorization: authorization.clone(),
        };
        let outcome = transition(&policy(), &half_open, &request, NOW + 1_100);

        assert_eq!(outcome.transition, CircuitBreakerTransition::Closed);
        assert_eq!(outcome.state.status(), CircuitBreakerStatus::Closed);
        assert_eq!(outcome.state.reset_by(), Some("operator-1"));
        assert_eq!(
            outcome.manual_reset_audit,
            Some(ManualResetAuditRecord {
                audit_event_id: String::from("audit-1"),
                operator_id: String::from("operator-1"),
                reason: String::from("broker positions reconciled"),
                evidence: String::from("ticket-42"),
                before_state: CircuitBreakerStatus::HalfOpen,
                after_state: CircuitBreakerStatus::Closed,
                recorded_at_ms: NOW + 1_100,
            })
        );

        let retry = transition(&policy(), &outcome.state, &request, NOW + 1_100);
        assert_eq!(retry.transition, CircuitBreakerTransition::NoChange);
        assert!(retry.manual_reset_audit.is_none());
    }

    #[test]
    fn incomplete_manual_reset_evidence_cannot_close() {
        let half_open = half_open(healthy_input());
        let outcome = transition(
            &policy(),
            &half_open,
            &TransitionRequest::Close {
                health: RecoveryHealth {
                    clock_healthy: true,
                    state_store_healthy: true,
                    new_hard_risk_violation: false,
                    input: healthy_input(),
                },
                authorization: ResetAuthorization::Operator(ManualResetEvidence {
                    audit_event_id: String::from("audit-1"),
                    operator_id: String::from("operator-1"),
                    reason: String::from("   "),
                    evidence: String::from("ticket-42"),
                }),
            },
            NOW + 1_100,
        );
        assert_eq!(outcome.state.status(), CircuitBreakerStatus::HalfOpen);
        assert!(matches!(
            outcome.error,
            Some(CircuitBreakerError::ManualResetEvidenceMissing { field: "reason" })
        ));
    }

    #[test]
    fn automatic_reset_only_closes_when_policy_enables_it() {
        let mut auto_policy = policy();
        auto_policy.auto_reset = true;
        let half_open = half_open(healthy_input());
        let outcome = transition(
            &auto_policy,
            &half_open,
            &TransitionRequest::Close {
                health: RecoveryHealth {
                    clock_healthy: true,
                    state_store_healthy: true,
                    new_hard_risk_violation: false,
                    input: healthy_input(),
                },
                authorization: ResetAuthorization::Automatic,
            },
            NOW + 1_100,
        );
        assert_eq!(outcome.state.status(), CircuitBreakerStatus::Closed);
        assert_eq!(outcome.state.reset_by(), Some("AUTOMATIC"));
        assert!(outcome.manual_reset_audit.is_none());
    }

    #[test]
    fn regressed_server_time_fails_closed() {
        let half_open = half_open(healthy_input());
        let outcome = transition(
            &policy(),
            &half_open,
            &TransitionRequest::Observe(healthy_input()),
            NOW,
        );
        assert_eq!(outcome.state.status(), CircuitBreakerStatus::Open);
        assert_eq!(
            outcome.transition,
            CircuitBreakerTransition::SafetyFallbackOpened
        );
        assert!(matches!(
            outcome.error,
            Some(CircuitBreakerError::ServerTimeRegressed { .. })
        ));
    }
}
