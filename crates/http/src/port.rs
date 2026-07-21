use std::{collections::BTreeMap, future::Future, pin::Pin};

use serde_json::Value;
use sinan_store::AuthorizedAccountScope;
use sinan_types::{AccountId, CommandId, ErrorCode, IntentId, TradeIntent};
use thiserror::Error;

use crate::{
    ControlPlanePrincipal, ExecutionCommandStatusResponse, TradeIntentStateRef,
    TradeIntentStatusResponse, TradingCoreStateResponse, TradingCoreTimePolicy,
};

pub type ControlPlaneFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizedControlPlaneQuery {
    pub principal: ControlPlanePrincipal,
    pub request_id: String,
    pub correlation_id: Option<String>,
}

impl AuthorizedControlPlaneQuery {
    pub fn account_scope(&self) -> &AuthorizedAccountScope {
        self.principal.account_scope()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SubmitTradeIntentCommand {
    pub principal: ControlPlanePrincipal,
    pub request_id: String,
    pub correlation_id: Option<String>,
    pub intent: TradeIntent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TradeIntentIntakeRecord {
    /// Original durable insert time. Duplicate replay returns the same value.
    pub accepted_at: i64,
    pub state_ref: Option<TradeIntentStateRef>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TradeIntentIntakeOutcome {
    Inserted(TradeIntentIntakeRecord),
    Duplicate(TradeIntentIntakeRecord),
}

pub trait TradeIntentApplicationPort: Send + Sync {
    fn submit_trade_intent(
        &self,
        command: SubmitTradeIntentCommand,
    ) -> ControlPlaneFuture<'_, Result<TradeIntentIntakeOutcome, ControlPlanePortError>>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandPayloadDisclosure {
    SummaryOnly,
    IncludeSensitivePayload,
}

/// Account ownership travels beside a response so HTTP can apply a final
/// non-disclosure check. Store adapters must still filter by `account_scope`
/// in the query itself, before returning `Some`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScopedTradeIntentStatus {
    pub account_id: AccountId,
    pub response: TradeIntentStatusResponse,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScopedExecutionCommandStatus {
    pub account_id: AccountId,
    pub response: ExecutionCommandStatusResponse,
}

pub trait ControlPlaneQueryPort: Send + Sync {
    fn get_state(
        &self,
        query: AuthorizedControlPlaneQuery,
    ) -> ControlPlaneFuture<'_, Result<TradingCoreStateResponse, ControlPlanePortError>>;

    fn get_time_policy(
        &self,
        query: AuthorizedControlPlaneQuery,
    ) -> ControlPlaneFuture<'_, Result<TradingCoreTimePolicy, ControlPlanePortError>>;

    fn get_trade_intent_status(
        &self,
        query: AuthorizedControlPlaneQuery,
        intent_id: IntentId,
    ) -> ControlPlaneFuture<'_, Result<Option<ScopedTradeIntentStatus>, ControlPlanePortError>>;

    fn get_execution_command_status(
        &self,
        query: AuthorizedControlPlaneQuery,
        command_id: CommandId,
        disclosure: CommandPayloadDisclosure,
    ) -> ControlPlaneFuture<'_, Result<Option<ScopedExecutionCommandStatus>, ControlPlanePortError>>;
}

#[derive(Clone, Debug, Error, PartialEq)]
pub enum ControlPlanePortError {
    #[error("conflict: {message}")]
    Conflict {
        code: ErrorCode,
        message: String,
        details: Option<BTreeMap<String, Value>>,
    },
    #[error("request cannot enter the workflow: {message}")]
    Unprocessable {
        code: ErrorCode,
        message: String,
        details: Option<BTreeMap<String, Value>>,
    },
    #[error("request was rate limited: {message}")]
    RateLimited {
        code: ErrorCode,
        message: String,
        details: Option<BTreeMap<String, Value>>,
    },
    #[error("service is unavailable: {message}")]
    Unavailable {
        code: ErrorCode,
        message: String,
        details: Option<BTreeMap<String, Value>>,
    },
    #[error("internal control-plane error")]
    Internal,
}
