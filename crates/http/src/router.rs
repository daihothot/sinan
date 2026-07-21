use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Body,
    extract::{
        ws::WebSocketUpgrade, DefaultBodyLimit, Extension, FromRequest, Path, Request, State,
    },
    http::{header::WWW_AUTHENTICATE, HeaderMap, HeaderName, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::Value;
use sinan_events::EventStreamManager;
use sinan_types::{CommandId, ErrorCode, ErrorCodeOrString, IdempotencyKey, IntentId};

use crate::{
    upgrade_event_websocket, AuthorizedControlPlaneQuery, CommandPayloadDisclosure,
    ControlPlanePortError, ControlPlanePrincipal, ControlPlaneQueryPort, ControlPlaneScope,
    EventWebSocketConfig, EventWebSocketConfigurationError, EventWebSocketService,
    FixedBearerTokenRegistry, HttpErrorResponse, SubmitTradeIntentCommand,
    SubmitTradeIntentRequest, SubmitTradeIntentResponse, SubmitTradeIntentStatus,
    TradeIntentApplicationPort, TradeIntentIntakeOutcome, TradingCoreStateResponse,
    TradingCoreTimePolicy, TradingCoreTimeResponse,
};

const MAX_JSON_BODY_BYTES: usize = 64 * 1024;
const MAX_REQUEST_HEADER_ID_BYTES: usize = 256;
const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
const X_CORRELATION_ID: HeaderName = HeaderName::from_static("x-correlation-id");
const X_IDEMPOTENCY_KEY: HeaderName = HeaderName::from_static("x-idempotency-key");

pub trait HttpServerClock: Send + Sync {
    fn now_ms(&self) -> i64;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemHttpServerClock;

impl HttpServerClock for SystemHttpServerClock {
    fn now_ms(&self) -> i64 {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_millis());
        i64::try_from(millis).unwrap_or(i64::MAX)
    }
}

#[derive(Clone)]
pub struct ControlPlaneHttpState {
    token_registry: FixedBearerTokenRegistry,
    application: Arc<dyn TradeIntentApplicationPort>,
    queries: Arc<dyn ControlPlaneQueryPort>,
    clock: Arc<dyn HttpServerClock>,
    events: Option<EventWebSocketService>,
}

impl ControlPlaneHttpState {
    pub fn new(
        token_registry: FixedBearerTokenRegistry,
        application: Arc<dyn TradeIntentApplicationPort>,
        queries: Arc<dyn ControlPlaneQueryPort>,
    ) -> Self {
        Self {
            token_registry,
            application,
            queries,
            clock: Arc::new(SystemHttpServerClock),
            events: None,
        }
    }

    pub fn with_clock(mut self, clock: Arc<dyn HttpServerClock>) -> Self {
        self.clock = clock;
        self
    }

    pub fn with_event_stream(
        mut self,
        manager: Arc<EventStreamManager>,
        config: EventWebSocketConfig,
    ) -> Result<Self, EventWebSocketConfigurationError> {
        self.events = Some(EventWebSocketService::new(manager, config)?);
        Ok(self)
    }

    pub fn token_registry(&self) -> &FixedBearerTokenRegistry {
        &self.token_registry
    }
}

/// Authenticated metadata inserted by the REST middleware.
///
/// It is public so `/events` can share the same identity model when that route
/// is composed by the HTTP/WebSocket adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlPlaneRequestContext {
    pub principal: ControlPlanePrincipal,
    pub request_id: String,
    pub correlation_id: Option<String>,
    pub server_receive_at: i64,
}

pub fn control_plane_router(state: ControlPlaneHttpState) -> Router {
    Router::new()
        .route("/trade-intents", post(submit_trade_intent))
        .route("/state", get(get_state))
        .route("/time", get(get_time))
        .route("/events", get(get_events))
        .route("/trade-intents/{intent_id}", get(get_trade_intent))
        .route(
            "/execution/commands/{command_id}",
            get(get_execution_command),
        )
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .with_state(state.clone())
        .layer(DefaultBodyLimit::max(MAX_JSON_BODY_BYTES))
        .layer(middleware::from_fn_with_state(state, authenticate_request))
}

async fn get_events(
    State(state): State<ControlPlaneHttpState>,
    Extension(context): Extension<ControlPlaneRequestContext>,
    upgrade: WebSocketUpgrade,
) -> Response {
    if let Err(response) = require_scope(&state, &context, ControlPlaneScope::SubscribeEvents) {
        return response;
    }
    let Some(events) = state.events.clone() else {
        return context_error(
            &state,
            &context,
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::ServiceUnavailable,
            "event stream is not configured".to_owned(),
            None,
        );
    };
    upgrade_event_websocket(upgrade, context, events, Arc::clone(&state.clock))
}

async fn authenticate_request(
    State(state): State<ControlPlaneHttpState>,
    mut request: Request,
    next: Next,
) -> Response {
    let server_receive_at = state.clock.now_ms();
    let request_id = match required_header(request.headers(), &X_REQUEST_ID) {
        Ok(value) => value,
        Err(reason) => {
            return error_response(
                &state,
                StatusCode::BAD_REQUEST,
                ErrorCode::MissingRequiredField,
                reason,
                String::new(),
                optional_header_lossy(request.headers(), &X_CORRELATION_ID),
                None,
                false,
            );
        }
    };
    let correlation_id = match optional_header(request.headers(), &X_CORRELATION_ID) {
        Ok(value) => value,
        Err(reason) => {
            return error_response(
                &state,
                StatusCode::BAD_REQUEST,
                ErrorCode::BadRequest,
                reason,
                request_id,
                None,
                None,
                false,
            );
        }
    };
    let principal = match state.token_registry.authenticate(request.headers()) {
        Ok(principal) => principal,
        Err(_) => {
            return error_response(
                &state,
                StatusCode::UNAUTHORIZED,
                ErrorCode::AuthenticationFailed,
                "authentication failed".to_owned(),
                request_id,
                correlation_id,
                None,
                true,
            );
        }
    };

    request.extensions_mut().insert(ControlPlaneRequestContext {
        principal,
        request_id,
        correlation_id,
        server_receive_at,
    });
    next.run(request).await
}

async fn submit_trade_intent(
    State(state): State<ControlPlaneHttpState>,
    request: Request,
) -> Response {
    let context = match request_context(&state, &request) {
        Ok(context) => context,
        Err(response) => return response,
    };
    if let Err(response) = require_scope(&state, &context, ControlPlaneScope::WriteIntent) {
        return response;
    }
    let header_idempotency_key = match required_header(request.headers(), &X_IDEMPOTENCY_KEY) {
        Ok(value) => IdempotencyKey::from(value),
        Err(reason) => {
            return context_error(
                &state,
                &context,
                StatusCode::BAD_REQUEST,
                ErrorCode::MissingRequiredField,
                reason,
                None,
            );
        }
    };
    let Json(payload) = match Json::<SubmitTradeIntentRequest>::from_request(request, &state).await
    {
        Ok(payload) => payload,
        Err(rejection) => {
            return context_error(
                &state,
                &context,
                StatusCode::BAD_REQUEST,
                ErrorCode::SchemaValidationFailed,
                "request body is not a valid SubmitTradeIntentRequest".to_owned(),
                Some(BTreeMap::from([(
                    "rejection".to_owned(),
                    Value::String(rejection.body_text()),
                )])),
            );
        }
    };
    if header_idempotency_key != payload.intent.idempotency_key {
        return context_error(
            &state,
            &context,
            StatusCode::CONFLICT,
            ErrorCode::IdempotencyKeyConflict,
            "X-Idempotency-Key must equal intent.idempotency_key".to_owned(),
            None,
        );
    }
    if !context
        .principal
        .account_scope()
        .contains(&payload.intent.account_id)
    {
        return forbidden(&state, &context, "account is outside the authorized scope");
    }

    let intent_id = payload.intent.intent_id.clone();
    let correlation_id = payload.intent.correlation_id.clone();
    let outcome = state
        .application
        .submit_trade_intent(SubmitTradeIntentCommand {
            principal: context.principal.clone(),
            request_id: context.request_id.clone(),
            correlation_id: context.correlation_id.clone(),
            intent: payload.intent,
        })
        .await;

    let (status_code, status, reason, record) = match outcome {
        Ok(TradeIntentIntakeOutcome::Inserted(record)) => (
            StatusCode::ACCEPTED,
            SubmitTradeIntentStatus::Accepted,
            ErrorCodeOrString::from("OK"),
            record,
        ),
        Ok(TradeIntentIntakeOutcome::Duplicate(record)) => (
            StatusCode::OK,
            SubmitTradeIntentStatus::Duplicate,
            ErrorCodeOrString::from(ErrorCode::DuplicateTradeIntent),
            record,
        ),
        Err(error) => return port_error_response(&state, &context, error),
    };
    success_response(
        status_code,
        &context.request_id,
        Json(SubmitTradeIntentResponse {
            intent_id,
            status,
            reason,
            correlation_id,
            accepted_at: record.accepted_at,
            state_ref: record.state_ref,
        }),
    )
}

async fn get_state(State(state): State<ControlPlaneHttpState>, request: Request) -> Response {
    let context = match request_context(&state, &request) {
        Ok(context) => context,
        Err(response) => return response,
    };
    if let Err(response) = require_scope(&state, &context, ControlPlaneScope::ReadState) {
        return response;
    }
    let mut response = match state.queries.get_state(query_from(&context)).await {
        Ok(response) => response,
        Err(error) => return port_error_response(&state, &context, error),
    };
    enforce_state_scope(&context.principal, &mut response);
    response.risk.normalize_circuit_breaker_active();
    success_response(StatusCode::OK, &context.request_id, Json(response))
}

async fn get_time(State(state): State<ControlPlaneHttpState>, request: Request) -> Response {
    let context = match request_context(&state, &request) {
        Ok(context) => context,
        Err(response) => return response,
    };
    if let Err(response) = require_scope(&state, &context, ControlPlaneScope::ReadState) {
        return response;
    }
    let policy = match state.queries.get_time_policy(query_from(&context)).await {
        Ok(policy) => policy,
        Err(error) => return port_error_response(&state, &context, error),
    };
    let send_at = state.clock.now_ms();
    let response = time_response(context.server_receive_at, send_at, policy);
    success_response(StatusCode::OK, &context.request_id, Json(response))
}

async fn get_trade_intent(
    State(state): State<ControlPlaneHttpState>,
    Path(intent_id): Path<String>,
    request: Request,
) -> Response {
    let context = match request_context(&state, &request) {
        Ok(context) => context,
        Err(response) => return response,
    };
    if let Err(response) = require_scope(&state, &context, ControlPlaneScope::ReadState) {
        return response;
    }
    if intent_id.trim().is_empty() {
        return context_error(
            &state,
            &context,
            StatusCode::BAD_REQUEST,
            ErrorCode::BadRequest,
            "intent_id must not be empty".to_owned(),
            None,
        );
    }
    let intent_id = IntentId::from(intent_id);
    let scoped = match state
        .queries
        .get_trade_intent_status(query_from(&context), intent_id.clone())
        .await
    {
        Ok(Some(scoped)) => scoped,
        Ok(None) => return not_found_response(&state, &context, "trade intent was not found"),
        Err(error) => return port_error_response(&state, &context, error),
    };
    if !context
        .principal
        .account_scope()
        .contains(&scoped.account_id)
    {
        return not_found_response(&state, &context, "trade intent was not found");
    }
    if scoped.response.intent_id != intent_id {
        return internal_error(&state, &context);
    }
    success_response(StatusCode::OK, &context.request_id, Json(scoped.response))
}

async fn get_execution_command(
    State(state): State<ControlPlaneHttpState>,
    Path(command_id): Path<String>,
    request: Request,
) -> Response {
    let context = match request_context(&state, &request) {
        Ok(context) => context,
        Err(response) => return response,
    };
    if let Err(response) = require_scope(&state, &context, ControlPlaneScope::ReadState) {
        return response;
    }
    if command_id.trim().is_empty() {
        return context_error(
            &state,
            &context,
            StatusCode::BAD_REQUEST,
            ErrorCode::BadRequest,
            "command_id must not be empty".to_owned(),
            None,
        );
    }
    let command_id = CommandId::from(command_id);
    let may_read_sensitive_payload = context.principal.has_scope(ControlPlaneScope::DebugRead)
        && context
            .principal
            .has_scope(ControlPlaneScope::ExecutionDebugSensitive);
    let disclosure = if may_read_sensitive_payload {
        CommandPayloadDisclosure::IncludeSensitivePayload
    } else {
        CommandPayloadDisclosure::SummaryOnly
    };
    let scoped = match state
        .queries
        .get_execution_command_status(query_from(&context), command_id.clone(), disclosure)
        .await
    {
        Ok(Some(scoped)) => scoped,
        Ok(None) => return not_found_response(&state, &context, "execution command was not found"),
        Err(error) => return port_error_response(&state, &context, error),
    };
    if !context
        .principal
        .account_scope()
        .contains(&scoped.account_id)
    {
        return not_found_response(&state, &context, "execution command was not found");
    }
    let mut response = scoped.response;
    if response.command_id != command_id
        || response.state.command_id != command_id
        || response.state.account_id != scoped.account_id
        || response
            .events
            .iter()
            .any(|event| event.command_id != command_id || event.account_id != scoped.account_id)
        || response.command.as_ref().is_some_and(|command| {
            command.command_id != command_id || command.account_id != scoped.account_id
        })
    {
        return internal_error(&state, &context);
    }
    if !may_read_sensitive_payload {
        response.command = None;
    }
    success_response(StatusCode::OK, &context.request_id, Json(response))
}

async fn not_found(State(state): State<ControlPlaneHttpState>, request: Request<Body>) -> Response {
    let context = match request_context(&state, &request) {
        Ok(context) => context,
        Err(response) => return response,
    };
    not_found_response(&state, &context, "resource was not found")
}

async fn method_not_allowed(
    State(state): State<ControlPlaneHttpState>,
    request: Request<Body>,
) -> Response {
    let context = match request_context(&state, &request) {
        Ok(context) => context,
        Err(response) => return response,
    };
    context_error(
        &state,
        &context,
        StatusCode::METHOD_NOT_ALLOWED,
        ErrorCode::MethodNotAllowed,
        "method is not allowed for this resource".to_owned(),
        None,
    )
}

fn request_context(
    state: &ControlPlaneHttpState,
    request: &Request<Body>,
) -> Result<ControlPlaneRequestContext, Response> {
    request
        .extensions()
        .get::<ControlPlaneRequestContext>()
        .cloned()
        .ok_or_else(|| {
            error_response(
                state,
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::InternalError,
                "internal server error".to_owned(),
                String::new(),
                None,
                None,
                false,
            )
        })
}

fn query_from(context: &ControlPlaneRequestContext) -> AuthorizedControlPlaneQuery {
    AuthorizedControlPlaneQuery {
        principal: context.principal.clone(),
        request_id: context.request_id.clone(),
        correlation_id: context.correlation_id.clone(),
    }
}

fn require_scope(
    state: &ControlPlaneHttpState,
    context: &ControlPlaneRequestContext,
    scope: ControlPlaneScope,
) -> Result<(), Response> {
    if context.principal.has_scope(scope) {
        return Ok(());
    }
    Err(forbidden(
        state,
        context,
        &format!("required scope {scope} is missing"),
    ))
}

fn forbidden(
    state: &ControlPlaneHttpState,
    context: &ControlPlaneRequestContext,
    message: &str,
) -> Response {
    context_error(
        state,
        context,
        StatusCode::FORBIDDEN,
        ErrorCode::Forbidden,
        message.to_owned(),
        None,
    )
}

fn not_found_response(
    state: &ControlPlaneHttpState,
    context: &ControlPlaneRequestContext,
    message: &str,
) -> Response {
    context_error(
        state,
        context,
        StatusCode::NOT_FOUND,
        ErrorCode::NotFound,
        message.to_owned(),
        None,
    )
}

fn internal_error(state: &ControlPlaneHttpState, context: &ControlPlaneRequestContext) -> Response {
    context_error(
        state,
        context,
        StatusCode::INTERNAL_SERVER_ERROR,
        ErrorCode::InternalError,
        "internal server error".to_owned(),
        None,
    )
}

fn port_error_response(
    state: &ControlPlaneHttpState,
    context: &ControlPlaneRequestContext,
    error: ControlPlanePortError,
) -> Response {
    match error {
        ControlPlanePortError::Conflict {
            code,
            message,
            details,
        } => context_error(state, context, StatusCode::CONFLICT, code, message, details),
        ControlPlanePortError::Unprocessable {
            code,
            message,
            details,
        } => context_error(
            state,
            context,
            StatusCode::UNPROCESSABLE_ENTITY,
            code,
            message,
            details,
        ),
        ControlPlanePortError::RateLimited {
            code,
            message,
            details,
        } => context_error(
            state,
            context,
            StatusCode::TOO_MANY_REQUESTS,
            code,
            message,
            details,
        ),
        ControlPlanePortError::Unavailable {
            code,
            message,
            details,
        } => context_error(
            state,
            context,
            StatusCode::SERVICE_UNAVAILABLE,
            code,
            message,
            details,
        ),
        ControlPlanePortError::Internal => internal_error(state, context),
    }
}

fn time_response(
    server_receive_at: i64,
    server_send_at: i64,
    policy: TradingCoreTimePolicy,
) -> TradingCoreTimeResponse {
    TradingCoreTimeResponse {
        server_now_ms: server_send_at,
        server_receive_at,
        server_send_at,
        clock_health: policy.clock_health,
        max_internal_server_skew_ms: policy.max_internal_server_skew_ms,
        max_decision_time_skew_ms: policy.max_decision_time_skew_ms,
        max_decision_time_sync_age_ms: policy.max_decision_time_sync_age_ms,
        max_decision_time_sync_rtt_ms: policy.max_decision_time_sync_rtt_ms,
        control_plane_time_sync_interval_ms: policy.control_plane_time_sync_interval_ms,
        max_decision_intent_age_ms: policy.max_decision_intent_age_ms,
    }
}

fn enforce_state_scope(principal: &ControlPlanePrincipal, response: &mut TradingCoreStateResponse) {
    let scope = principal.account_scope();
    response
        .accounts
        .retain(|account| scope.contains(&account.account_id));
    response
        .positions
        .retain(|position| scope.contains(&position.account_id));
    response
        .orders
        .retain(|order| scope.contains(&order.account_id));
    response
        .symbols
        .retain(|symbol| scope.contains(&symbol.account_id));
    response
        .sessions
        .retain(|session| scope.contains(&session.account_id));
    response
        .execution
        .open_plans
        .retain(|plan| scope.contains(&plan.definition.account_id));
    response
        .execution
        .pending_commands
        .retain(|command| scope.contains(&command.account_id));
    response
        .execution
        .recent_events
        .retain(|event| scope.contains(&event.account_id));
    response
        .risk
        .latest_results
        .retain(|result| scope.contains(&result.account_id));
}

fn required_header(headers: &HeaderMap, name: &HeaderName) -> Result<String, String> {
    match optional_header(headers, name)? {
        Some(value) => Ok(value),
        None => Err(format!("{} header is required", name.as_str())),
    }
}

fn optional_header(headers: &HeaderMap, name: &HeaderName) -> Result<Option<String>, String> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(format!("{} header must occur exactly once", name.as_str()));
    }
    let value = value
        .to_str()
        .map_err(|_| format!("{} header must be valid ASCII", name.as_str()))?;
    if value.trim().is_empty() || value.len() > MAX_REQUEST_HEADER_ID_BYTES {
        return Err(format!(
            "{} header must contain 1..={} bytes",
            name.as_str(),
            MAX_REQUEST_HEADER_ID_BYTES
        ));
    }
    Ok(Some(value.to_owned()))
}

fn optional_header_lossy(headers: &HeaderMap, name: &HeaderName) -> Option<String> {
    optional_header(headers, name).ok().flatten()
}

fn success_response<T>(status: StatusCode, request_id: &str, body: Json<T>) -> Response
where
    T: serde::Serialize,
{
    let mut response = (status, body).into_response();
    insert_request_id(&mut response, request_id);
    response
}

fn context_error(
    state: &ControlPlaneHttpState,
    context: &ControlPlaneRequestContext,
    status: StatusCode,
    code: ErrorCode,
    message: String,
    details: Option<BTreeMap<String, Value>>,
) -> Response {
    error_response(
        state,
        status,
        code,
        message,
        context.request_id.clone(),
        context.correlation_id.clone(),
        details,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
fn error_response(
    state: &ControlPlaneHttpState,
    status: StatusCode,
    code: ErrorCode,
    message: String,
    request_id: String,
    correlation_id: Option<String>,
    details: Option<BTreeMap<String, Value>>,
    authenticate: bool,
) -> Response {
    let mut response = (
        status,
        Json(HttpErrorResponse {
            error_code: code,
            message,
            request_id: request_id.clone(),
            correlation_id,
            server_time: state.clock.now_ms(),
            details,
        }),
    )
        .into_response();
    insert_request_id(&mut response, &request_id);
    if authenticate {
        response.headers_mut().insert(
            WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer realm=\"sinan-control-plane\""),
        );
    }
    response
}

fn insert_request_id(response: &mut Response, request_id: &str) {
    if let Ok(value) = HeaderValue::from_str(request_id) {
        response.headers_mut().insert(X_REQUEST_ID, value);
    }
}
