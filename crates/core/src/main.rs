#![forbid(unsafe_code)]

use std::{
    collections::BTreeMap,
    env,
    error::Error,
    future::{Future, IntoFuture},
    io,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use serde::Deserialize;
use sinan_core::{
    compose_production_gateway_persistence, restore_durable_circuit_breaker, CoreInboundProcessor,
    CoreSessionResumeProcessor, DurableOutboundConfig, DurableOutboundProcessOutcome,
    DurableOutboundProcessor, RiskWorkflowError, RiskWorkflowProcessor, SqliteControlPlaneService,
    SqliteControlPlaneServiceConfig, SystemTradingCoreClock, TradingCoreClock,
    TrustedExecutionResolver, TrustedLegExecutionParameters, TrustedRiskWorkflowContext,
};
use sinan_events::{EventStreamManager, EventStreamManagerConfig};
use sinan_gateway::{
    ClientCredential, ConfiguredClientAuthenticator, DurableRecoveryConfig,
    DurableRecoveryDispatcher, ExecutionTransportConfig, ExecutionWebSocketBinding,
    GatewayConnectionService, GatewayOutboundAdapter, GatewayOutboundConfig, GatewaySessionConfig,
    GatewaySessionRegistry, LiveSessionRegistry, NativeTcpBinding, UuidGatewayIdGenerator,
};
use sinan_http::{
    control_plane_router, ControlPlaneHttpState, ControlPlanePrincipal, ControlPlaneQueryPort,
    ControlPlaneScope, ControlPlaneTokenGrant, EventWebSocketConfig, FixedBearerTokenRegistry,
    TradeIntentApplicationPort,
};
use sinan_risk::{RiskPolicy, StrategyRiskPolicy};
use sinan_store::{EventRetentionPolicy, SqliteStateStore, StoreError, StoreOptions};
use sinan_types::{
    AccountId, ClientId, ExecutionPolicy, FillingPolicy, LegId, OrderType, SymbolCode, TerminalId,
    TimePolicy,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BatchItemOutcome {
    Processed,
    NoWork,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InterruptibleBatchOutcome {
    Completed,
    Shutdown,
}

struct RuntimeConfig {
    database_url: String,
    http_addr: SocketAddr,
    token: String,
    token_subject: String,
    accounts: Vec<AccountId>,
    scopes: Vec<ControlPlaneScope>,
    event_live_capacity: usize,
    event_replay_limit: u64,
    event_max_message_bytes: usize,
    event_write_timeout: Duration,
    event_retain_latest: u64,
    event_retention_age: Duration,
    event_retention_interval: Duration,
    native_tcp_addr: SocketAddr,
    execution_ws_addr: SocketAddr,
    execution_credentials: Vec<ClientCredential>,
    recovery_interval: Duration,
    recovery_batch_size: usize,
    recovery_lease: Duration,
    recovery_handler_timeout: Duration,
    recovery_finalization_budget: Duration,
    outbound_interval: Duration,
    outbound_batch_size: usize,
    outbound_lease: Duration,
    outbound_confirmation_timeout_ms: u64,
    outbound_retry_base_delay: Duration,
    outbound_retry_max_delay: Duration,
    risk_workflow: Option<RiskWorkflowRuntimeConfig>,
    risk_workflow_interval: Duration,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionCredentialConfig {
    client_id: ClientId,
    account_id: AccountId,
    active_secret: String,
    #[serde(default)]
    next_secret: Option<String>,
}

struct RiskWorkflowRuntimeConfig {
    risk_policy: RiskPolicy,
    strategy_policy: StrategyRiskPolicy,
    execution_policy: ExecutionPolicy,
    execution_resolver: ConfiguredExecutionResolver,
    signing_secret: Vec<u8>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionRouteConfig {
    account_id: AccountId,
    symbol: SymbolCode,
    #[serde(default)]
    dependency: Vec<LegId>,
    #[serde(default)]
    terminal_id: Option<TerminalId>,
    #[serde(default)]
    client_id: Option<ClientId>,
    order_type: OrderType,
    #[serde(default)]
    price: Option<f64>,
    #[serde(default)]
    deviation_points: Option<i64>,
    magic: i64,
    #[serde(default)]
    comment: Option<String>,
    #[serde(default)]
    filling_policy: Option<FillingPolicy>,
    #[serde(default)]
    time_policy: Option<TimePolicy>,
    #[serde(default)]
    expiration_time: Option<i64>,
    estimated_cost_per_lot: f64,
}

struct ConfiguredExecutionResolver {
    routes: BTreeMap<(AccountId, SymbolCode), ExecutionRouteConfig>,
}

impl ConfiguredExecutionResolver {
    fn new(routes: Vec<ExecutionRouteConfig>) -> Result<Self, io::Error> {
        let mut indexed = BTreeMap::new();
        for route in routes {
            let key = (route.account_id.clone(), route.symbol.clone());
            if indexed.insert(key, route).is_some() {
                return Err(invalid_config(
                    "SINAN_EXECUTION_ROUTES_JSON contains a duplicate account/symbol route",
                ));
            }
        }
        if indexed.is_empty() {
            return Err(invalid_config(
                "SINAN_EXECUTION_ROUTES_JSON must contain at least one route",
            ));
        }
        Ok(Self { routes: indexed })
    }
}

impl TrustedExecutionResolver for ConfiguredExecutionResolver {
    fn resolve(
        &self,
        intent: &sinan_types::TradeIntent,
        leg: &sinan_core::RiskWorkflowLeg,
    ) -> Result<TrustedLegExecutionParameters, String> {
        let route = self
            .routes
            .get(&(intent.account_id.clone(), leg.symbol.clone()))
            .ok_or_else(|| {
                format!(
                    "no trusted execution route for account {} and symbol {}",
                    intent.account_id, leg.symbol
                )
            })?;
        Ok(TrustedLegExecutionParameters {
            dependency: route.dependency.clone(),
            terminal_id: route.terminal_id.clone(),
            client_id: route.client_id.clone(),
            order_type: route.order_type,
            price: route.price,
            deviation_points: route.deviation_points,
            magic: route.magic,
            comment: route.comment.clone(),
            filling_policy: route.filling_policy,
            time_policy: route.time_policy,
            expiration_time: route.expiration_time,
            estimated_cost_per_lot: route.estimated_cost_per_lot,
        })
    }
}

impl RuntimeConfig {
    fn from_env() -> Result<Self, Box<dyn Error>> {
        let token = required_env("SINAN_CONTROL_PLANE_TOKEN")?;
        let accounts = required_env("SINAN_CONTROL_PLANE_ACCOUNTS")?
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(AccountId::from)
            .collect();
        let scopes = env::var("SINAN_CONTROL_PLANE_SCOPES")
            .unwrap_or_else(|_| {
                "control-plane:write-intent,control-plane:read-state,event:subscribe".to_owned()
            })
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(parse_scope)
            .collect::<Result<Vec<_>, _>>()?;
        if scopes.is_empty() {
            return Err(invalid_config("SINAN_CONTROL_PLANE_SCOPES must not be empty").into());
        }

        let native_tcp_addr =
            optional_socket_addr("SINAN_EXECUTION_NATIVE_TCP_ADDR", "127.0.0.1:9100")?;
        let execution_ws_addr = optional_socket_addr("SINAN_EXECUTION_WS_ADDR", "127.0.0.1:9101")?;
        require_loopback("SINAN_EXECUTION_NATIVE_TCP_ADDR", native_tcp_addr)?;
        require_loopback("SINAN_EXECUTION_WS_ADDR", execution_ws_addr)?;
        let execution_credentials = env::var("SINAN_EXECUTION_CLIENT_CREDENTIALS")
            .ok()
            .map(|value| serde_json::from_str::<Vec<ExecutionCredentialConfig>>(&value))
            .transpose()
            .map_err(|_| {
                invalid_config("SINAN_EXECUTION_CLIENT_CREDENTIALS must be a valid JSON array")
            })?
            .unwrap_or_default()
            .into_iter()
            .map(|credential| {
                ClientCredential::new(
                    credential.client_id,
                    credential.account_id,
                    credential.active_secret,
                    credential.next_secret,
                )
            })
            .collect();
        let risk_workflow = risk_workflow_config_from_env()?;

        Ok(Self {
            database_url: env::var("SINAN_DATABASE_URL")
                .unwrap_or_else(|_| "sqlite://sinan.sqlite".to_owned()),
            http_addr: env::var("SINAN_HTTP_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:8080".to_owned())
                .parse()?,
            token,
            token_subject: env::var("SINAN_CONTROL_PLANE_SUBJECT")
                .unwrap_or_else(|_| "control-plane".to_owned()),
            accounts,
            scopes,
            event_live_capacity: optional_number("SINAN_EVENT_LIVE_CAPACITY", 1_024)?,
            event_replay_limit: optional_number("SINAN_EVENT_REPLAY_LIMIT", 1_000)?,
            event_max_message_bytes: optional_number("SINAN_EVENT_MAX_MESSAGE_BYTES", 64 * 1024)?,
            event_write_timeout: Duration::from_millis(optional_number(
                "SINAN_EVENT_WRITE_TIMEOUT_MS",
                5_000_u64,
            )?),
            event_retain_latest: optional_number("SINAN_EVENT_RETAIN_LATEST", 10_000)?,
            event_retention_age: Duration::from_millis(optional_number(
                "SINAN_EVENT_RETENTION_AGE_MS",
                15 * 60 * 1_000_u64,
            )?),
            event_retention_interval: Duration::from_millis(optional_number(
                "SINAN_EVENT_RETENTION_INTERVAL_MS",
                60_000_u64,
            )?),
            native_tcp_addr,
            execution_ws_addr,
            execution_credentials,
            recovery_interval: Duration::from_millis(optional_number(
                "SINAN_DURABLE_RECOVERY_INTERVAL_MS",
                100_u64,
            )?),
            recovery_batch_size: optional_number("SINAN_DURABLE_RECOVERY_BATCH_SIZE", 64)?,
            recovery_lease: Duration::from_millis(optional_number(
                "SINAN_DURABLE_RECOVERY_LEASE_MS",
                30_000_u64,
            )?),
            recovery_handler_timeout: Duration::from_millis(optional_number(
                "SINAN_DURABLE_RECOVERY_HANDLER_TIMEOUT_MS",
                10_000_u64,
            )?),
            recovery_finalization_budget: Duration::from_millis(optional_number(
                "SINAN_DURABLE_RECOVERY_FINALIZATION_BUDGET_MS",
                5_000_u64,
            )?),
            outbound_interval: Duration::from_millis(optional_number(
                "SINAN_DURABLE_OUTBOUND_INTERVAL_MS",
                100_u64,
            )?),
            outbound_batch_size: optional_number("SINAN_DURABLE_OUTBOUND_BATCH_SIZE", 64)?,
            outbound_lease: Duration::from_millis(optional_number(
                "SINAN_DURABLE_OUTBOUND_LEASE_MS",
                30_000_u64,
            )?),
            outbound_confirmation_timeout_ms: optional_number(
                "SINAN_DURABLE_OUTBOUND_CONFIRMATION_TIMEOUT_MS",
                5_000_u64,
            )?,
            outbound_retry_base_delay: Duration::from_millis(optional_number(
                "SINAN_DURABLE_OUTBOUND_RETRY_BASE_MS",
                250_u64,
            )?),
            outbound_retry_max_delay: Duration::from_millis(optional_number(
                "SINAN_DURABLE_OUTBOUND_RETRY_MAX_MS",
                30_000_u64,
            )?),
            risk_workflow,
            risk_workflow_interval: Duration::from_millis(optional_number(
                "SINAN_RISK_WORKFLOW_INTERVAL_MS",
                250_u64,
            )?),
        })
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("sinan-core failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let config = RuntimeConfig::from_env()?;
    validate_positive_duration("SINAN_EVENT_WRITE_TIMEOUT_MS", config.event_write_timeout)?;
    validate_positive_duration("SINAN_EVENT_RETENTION_AGE_MS", config.event_retention_age)?;
    validate_positive_duration(
        "SINAN_EVENT_RETENTION_INTERVAL_MS",
        config.event_retention_interval,
    )?;
    validate_positive_duration(
        "SINAN_DURABLE_RECOVERY_INTERVAL_MS",
        config.recovery_interval,
    )?;
    validate_positive_duration(
        "SINAN_DURABLE_OUTBOUND_INTERVAL_MS",
        config.outbound_interval,
    )?;
    validate_positive_duration("SINAN_DURABLE_OUTBOUND_LEASE_MS", config.outbound_lease)?;
    validate_positive_duration(
        "SINAN_DURABLE_OUTBOUND_RETRY_BASE_MS",
        config.outbound_retry_base_delay,
    )?;
    validate_positive_duration(
        "SINAN_DURABLE_OUTBOUND_RETRY_MAX_MS",
        config.outbound_retry_max_delay,
    )?;
    validate_positive_duration(
        "SINAN_RISK_WORKFLOW_INTERVAL_MS",
        config.risk_workflow_interval,
    )?;
    if config.event_retain_latest == 0 {
        return Err(invalid_config("SINAN_EVENT_RETAIN_LATEST must be positive").into());
    }
    if config.recovery_batch_size == 0 {
        return Err(invalid_config("SINAN_DURABLE_RECOVERY_BATCH_SIZE must be positive").into());
    }
    if config.outbound_batch_size == 0 {
        return Err(invalid_config("SINAN_DURABLE_OUTBOUND_BATCH_SIZE must be positive").into());
    }
    if config.outbound_lease <= Duration::from_millis(config.outbound_confirmation_timeout_ms) {
        return Err(invalid_config(
            "SINAN_DURABLE_OUTBOUND_LEASE_MS must exceed SINAN_DURABLE_OUTBOUND_CONFIRMATION_TIMEOUT_MS",
        )
        .into());
    }

    let store = SqliteStateStore::connect(StoreOptions::new(&config.database_url)).await?;
    let _circuit_breaker_restore = restore_durable_circuit_breaker(&store, system_now_ms()).await?;
    let event_stream = Arc::new(EventStreamManager::new(
        store.clone(),
        EventStreamManagerConfig {
            live_capacity: config.event_live_capacity,
            replay_limit: config.event_replay_limit,
        },
    )?);
    let gateway_persistence =
        compose_production_gateway_persistence(store.clone(), Arc::clone(&event_stream));

    let clock = Arc::new(SystemTradingCoreClock);
    let control_plane = Arc::new(SqliteControlPlaneService::new(
        store.clone(),
        clock.clone(),
        SqliteControlPlaneServiceConfig::default(),
    )?);
    let application: Arc<dyn TradeIntentApplicationPort> = control_plane.clone();
    let queries: Arc<dyn ControlPlaneQueryPort> = control_plane;
    let registry = FixedBearerTokenRegistry::new([ControlPlaneTokenGrant::new(
        config.token,
        ControlPlanePrincipal::new(config.token_subject, config.scopes, config.accounts),
    )])?;
    let http_state = ControlPlaneHttpState::new(registry, application, queries)
        .with_clock(clock.clone())
        .with_event_stream(
            Arc::clone(&event_stream),
            EventWebSocketConfig {
                max_message_bytes: config.event_max_message_bytes,
                write_timeout: config.event_write_timeout,
            },
        )?;

    let transport_config = ExecutionTransportConfig::default();
    if config.outbound_lease <= transport_config.write_timeout {
        return Err(invalid_config(
            "SINAN_DURABLE_OUTBOUND_LEASE_MS must exceed the Execution transport write timeout",
        )
        .into());
    }
    let live_sessions = Arc::new(LiveSessionRegistry::new());
    let server_clock: Arc<dyn sinan_execution::ServerClock> = clock.clone();
    let sessions = GatewaySessionRegistry::new(
        store.clone(),
        live_sessions,
        server_clock.clone(),
        GatewaySessionConfig {
            max_clock_offset_ms: transport_config.max_clock_offset_ms,
            max_time_sync_age_ms: transport_config.heartbeat_timeout_ms,
            max_time_sync_rtt_ms: transport_config.max_time_sync_rtt_ms,
        },
    )?;
    sessions
        .fence_startup("PROCESS_RESTART_BEFORE_TRANSPORT_REBIND")
        .await?;
    let authenticator = Arc::new(ConfiguredClientAuthenticator::new(
        config.execution_credentials,
    )?);
    let connection_service = GatewayConnectionService::new(
        sessions.clone(),
        authenticator,
        Arc::new(UuidGatewayIdGenerator),
        gateway_persistence.inbound.clone(),
        gateway_persistence.resume.clone(),
        gateway_persistence.transport_events.clone(),
        transport_config.clone(),
    )?;
    let native_tcp = NativeTcpBinding::new(connection_service.clone())
        .bind(config.native_tcp_addr)
        .await?;
    let execution_ws = ExecutionWebSocketBinding::new(connection_service)
        .bind(config.execution_ws_addr)
        .await?;
    let outbound_delivery: Arc<dyn sinan_execution::OutboundDeliveryPort> =
        Arc::new(GatewayOutboundAdapter::new(
            sessions,
            GatewayOutboundConfig {
                confirmation_timeout_ms: config.outbound_confirmation_timeout_ms,
            },
        )?);
    let outbound = DurableOutboundProcessor::new(
        store.clone(),
        outbound_delivery,
        server_clock.clone(),
        DurableOutboundConfig {
            worker_id: format!("sinan-core-outbound-{}", std::process::id()),
            lease_duration_ms: duration_ms_i64(
                "SINAN_DURABLE_OUTBOUND_LEASE_MS",
                config.outbound_lease,
            )?,
            retry_base_delay_ms: duration_ms_i64(
                "SINAN_DURABLE_OUTBOUND_RETRY_BASE_MS",
                config.outbound_retry_base_delay,
            )?,
            retry_max_delay_ms: duration_ms_i64(
                "SINAN_DURABLE_OUTBOUND_RETRY_MAX_MS",
                config.outbound_retry_max_delay,
            )?,
        },
    )?;
    let recovery = DurableRecoveryDispatcher::new(
        store.clone(),
        Arc::new(CoreInboundProcessor),
        Arc::new(CoreSessionResumeProcessor),
        server_clock,
        DurableRecoveryConfig {
            worker_id: format!("sinan-core-{}", std::process::id()),
            max_items_per_batch: config.recovery_batch_size,
            lease_duration: config.recovery_lease,
            handler_timeout: config.recovery_handler_timeout,
            finalization_budget: config.recovery_finalization_budget,
        },
    )?;

    let http_listener = tokio::net::TcpListener::bind(config.http_addr).await?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (fatal_tx, mut fatal_rx) = mpsc::unbounded_channel::<String>();

    let native_shutdown = shutdown_rx.clone();
    let native_task = spawn_supervised_task(
        "Native TCP listener",
        shutdown_rx.clone(),
        fatal_tx.clone(),
        async move {
            native_tcp
                .serve(native_shutdown)
                .await
                .map_err(|error| format!("Native TCP listener failed: {error}"))
        },
    );

    let websocket_shutdown = shutdown_rx.clone();
    let websocket_task = spawn_supervised_task(
        "Execution WebSocket listener",
        shutdown_rx.clone(),
        fatal_tx.clone(),
        async move {
            execution_ws
                .serve(websocket_shutdown)
                .await
                .map_err(|error| format!("Execution WebSocket listener failed: {error}"))
        },
    );

    let mut recovery_shutdown = shutdown_rx.clone();
    let recovery_interval = config.recovery_interval;
    let recovery_task = spawn_supervised_task(
        "Durable recovery worker",
        shutdown_rx.clone(),
        fatal_tx.clone(),
        async move {
            let mut interval = tokio::time::interval(recovery_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    changed = recovery_shutdown.changed() => {
                        if changed.is_err() || *recovery_shutdown.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        if let Err(error) = recovery.dispatch_batch().await {
                            eprintln!("durable recovery batch failed: {error}");
                        }
                    }
                }
            }
            Ok(())
        },
    );

    let mut outbound_shutdown = shutdown_rx.clone();
    let outbound_interval = config.outbound_interval;
    let outbound_batch_size = config.outbound_batch_size;
    let outbound_task = spawn_supervised_task(
        "Durable outbound worker",
        shutdown_rx.clone(),
        fatal_tx.clone(),
        async move {
            let mut interval = tokio::time::interval(outbound_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    changed = outbound_shutdown.changed() => {
                        if changed.is_err() || *outbound_shutdown.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        match run_interruptible_batch(
                            &mut outbound_shutdown,
                            outbound_batch_size,
                            || {
                                let outbound = outbound.clone();
                                async move {
                                    match outbound.process_next().await {
                                        Ok(DurableOutboundProcessOutcome::NoWork) => {
                                            Ok(BatchItemOutcome::NoWork)
                                        }
                                        Ok(_) => Ok(BatchItemOutcome::Processed),
                                        Err(error) => Err(error),
                                    }
                                }
                            },
                        )
                        .await
                        {
                            Ok(InterruptibleBatchOutcome::Completed) => {}
                            Ok(InterruptibleBatchOutcome::Shutdown) => break,
                            Err(error) => {
                                eprintln!("durable outbound batch failed: {error}");
                            }
                        }
                    }
                }
            }
            Ok(())
        },
    );

    let risk_workflow_task = config.risk_workflow.map(|workflow| {
        let processor = RiskWorkflowProcessor::new(store.clone());
        let risk_clock = clock.clone();
        let mut risk_shutdown = shutdown_rx.clone();
        let risk_interval = config.risk_workflow_interval;
        spawn_supervised_task(
            "Risk workflow worker",
            shutdown_rx.clone(),
            fatal_tx.clone(),
            async move {
                let context = TrustedRiskWorkflowContext::new(
                    &workflow.risk_policy,
                    &workflow.strategy_policy,
                    &workflow.execution_policy,
                    &workflow.execution_resolver,
                    &workflow.signing_secret,
                )
                .map_err(|error| format!("Risk workflow configuration failed: {error}"))?;
                let mut interval = tokio::time::interval(risk_interval);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        changed = risk_shutdown.changed() => {
                            if changed.is_err() || *risk_shutdown.borrow() {
                                break;
                            }
                        }
                        _ = interval.tick() => {
                            let evaluated_at = sinan_execution::ServerClock::now_ms(risk_clock.as_ref());
                            match processor.process_next(evaluated_at, &context).await {
                                Ok(_) | Err(RiskWorkflowError::Store(StoreError::SnapshotUnavailable { .. })) => {}
                                Err(error) => eprintln!("trusted Risk workflow failed: {error}"),
                            }
                        }
                    }
                }
                Ok(())
            },
        )
    });

    let retention_store = store;
    let retention_interval = config.event_retention_interval;
    let retention_age = config.event_retention_age;
    let retain_latest = config.event_retain_latest;
    let mut retention_shutdown = shutdown_rx.clone();
    let retention_task = spawn_supervised_task(
        "Event retention worker",
        shutdown_rx.clone(),
        fatal_tx.clone(),
        async move {
            let mut interval = tokio::time::interval(retention_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    changed = retention_shutdown.changed() => {
                        if changed.is_err() || *retention_shutdown.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        let now = system_now_ms();
                        let age_ms = i64::try_from(retention_age.as_millis()).unwrap_or(i64::MAX);
                        if let Err(error) = retention_store
                            .prune_event_stream(EventRetentionPolicy {
                                retain_latest: Some(retain_latest),
                                created_at_or_after: Some(now.saturating_sub(age_ms)),
                            })
                            .await
                        {
                            eprintln!("event retention failed: {error}");
                        }
                    }
                }
            }
            Ok(())
        },
    );

    let http_shutdown = shutdown_rx.clone();
    let http = axum::serve(http_listener, control_plane_router(http_state))
        .with_graceful_shutdown(wait_for_shutdown(http_shutdown))
        .into_future();
    tokio::pin!(http);
    let result: Result<(), Box<dyn Error>> = tokio::select! {
        result = &mut http => {
            result?;
            Ok(())
        }
        () = shutdown_signal() => {
            let _ = shutdown_tx.send(true);
            http.await?;
            Ok(())
        }
        failure = fatal_rx.recv() => {
            let failure = failure.unwrap_or_else(|| "Gateway supervisor stopped".to_owned());
            let _ = shutdown_tx.send(true);
            let _ = http.await;
            Err(io::Error::other(failure).into())
        }
    };
    let _ = shutdown_tx.send(true);
    drop(fatal_tx);
    let mut runtime_tasks = vec![
        native_task,
        websocket_task,
        recovery_task,
        outbound_task,
        retention_task,
    ];
    runtime_tasks.extend(risk_workflow_task);
    let mut runtime_failure = None;
    for task in runtime_tasks {
        let failure = match task.await {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error),
            Err(error) => Some(format!("runtime supervisor task failed: {error}")),
        };
        if runtime_failure.is_none() {
            runtime_failure = failure;
        }
    }
    if result.is_ok() {
        if let Some(failure) = runtime_failure {
            return Err(io::Error::other(failure).into());
        }
    }
    result
}

fn spawn_supervised_task<F>(
    name: &'static str,
    shutdown: watch::Receiver<bool>,
    fatal: mpsc::UnboundedSender<String>,
    task: F,
) -> JoinHandle<Result<(), String>>
where
    F: Future<Output = Result<(), String>> + Send + 'static,
{
    tokio::spawn(async move {
        let outcome = tokio::spawn(task).await;
        let shutting_down = *shutdown.borrow();
        let result = match outcome {
            Ok(Ok(())) if shutting_down => Ok(()),
            Ok(Ok(())) => Err(format!("{name} stopped unexpectedly")),
            Ok(Err(_)) if shutting_down => Ok(()),
            Ok(Err(error)) => Err(error),
            Err(error) => Err(format!("{name} task failed: {error}")),
        };
        if !shutting_down {
            if let Err(error) = &result {
                let _ = fatal.send(error.clone());
            }
        }
        result
    })
}

async fn run_interruptible_batch<F, Fut, E>(
    shutdown: &mut watch::Receiver<bool>,
    batch_size: usize,
    mut process_next: F,
) -> Result<InterruptibleBatchOutcome, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<BatchItemOutcome, E>>,
{
    for _ in 0..batch_size {
        if *shutdown.borrow_and_update() {
            return Ok(InterruptibleBatchOutcome::Shutdown);
        }

        let next = process_next();
        tokio::pin!(next);
        let outcome = loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow_and_update() {
                        return Ok(InterruptibleBatchOutcome::Shutdown);
                    }
                }
                outcome = &mut next => break outcome?,
            }
        };
        if outcome == BatchItemOutcome::NoWork {
            return Ok(InterruptibleBatchOutcome::Completed);
        }
    }
    Ok(InterruptibleBatchOutcome::Completed)
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

fn required_env(name: &'static str) -> Result<String, io::Error> {
    let value = env::var(name).map_err(|_| invalid_config(&format!("{name} is required")))?;
    if value.trim().is_empty() {
        return Err(invalid_config(&format!("{name} must not be empty")));
    }
    Ok(value)
}

fn optional_number<T>(name: &'static str, default: T) -> Result<T, io::Error>
where
    T: std::str::FromStr,
{
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| invalid_config(&format!("{name} has an invalid number"))),
        Err(_) => Ok(default),
    }
}

fn optional_socket_addr(name: &'static str, default: &str) -> Result<SocketAddr, io::Error> {
    env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .parse()
        .map_err(|_| invalid_config(&format!("{name} has an invalid socket address")))
}

fn risk_workflow_config_from_env() -> Result<Option<RiskWorkflowRuntimeConfig>, io::Error> {
    const REQUIRED: [&str; 5] = [
        "SINAN_RISK_POLICY_JSON",
        "SINAN_STRATEGY_RISK_POLICY_JSON",
        "SINAN_EXECUTION_POLICY_JSON",
        "SINAN_EXECUTION_ROUTES_JSON",
        "SINAN_EXECUTION_COMMAND_SIGNING_SECRET",
    ];
    let configured = REQUIRED.map(|name| env::var(name).ok());
    if configured.iter().all(Option::is_none) {
        return Ok(None);
    }
    let mut values = Vec::with_capacity(REQUIRED.len());
    for (name, value) in REQUIRED.into_iter().zip(configured) {
        let value = value.ok_or_else(|| {
            invalid_config(&format!(
                "{name} is required when the trusted Risk workflow is enabled"
            ))
        })?;
        if value.trim().is_empty() {
            return Err(invalid_config(&format!("{name} must not be empty")));
        }
        values.push(value);
    }
    let mut values = values.into_iter();
    let risk_policy = parse_json_config(
        "SINAN_RISK_POLICY_JSON",
        &values.next().expect("fixed field count"),
    )?;
    let strategy_policy = parse_json_config(
        "SINAN_STRATEGY_RISK_POLICY_JSON",
        &values.next().expect("fixed field count"),
    )?;
    let execution_policy = parse_json_config(
        "SINAN_EXECUTION_POLICY_JSON",
        &values.next().expect("fixed field count"),
    )?;
    let routes: Vec<ExecutionRouteConfig> = parse_json_config(
        "SINAN_EXECUTION_ROUTES_JSON",
        &values.next().expect("fixed field count"),
    )?;
    let signing_secret = values.next().expect("fixed field count").into_bytes();
    Ok(Some(RiskWorkflowRuntimeConfig {
        risk_policy,
        strategy_policy,
        execution_policy,
        execution_resolver: ConfiguredExecutionResolver::new(routes)?,
        signing_secret,
    }))
}

fn parse_json_config<T: serde::de::DeserializeOwned>(
    name: &'static str,
    value: &str,
) -> Result<T, io::Error> {
    serde_json::from_str(value)
        .map_err(|_| invalid_config(&format!("{name} contains invalid JSON configuration")))
}

fn require_loopback(name: &'static str, address: SocketAddr) -> Result<(), io::Error> {
    if address.ip().is_loopback() {
        Ok(())
    } else {
        Err(invalid_config(&format!(
            "{name} must bind loopback while the Execution Client transport is plaintext"
        )))
    }
}

fn parse_scope(value: &str) -> Result<ControlPlaneScope, io::Error> {
    match value {
        "control-plane:write-intent" => Ok(ControlPlaneScope::WriteIntent),
        "control-plane:read-state" => Ok(ControlPlaneScope::ReadState),
        "event:subscribe" => Ok(ControlPlaneScope::SubscribeEvents),
        "debug:read" => Ok(ControlPlaneScope::DebugRead),
        "execution:debug-sensitive" => Ok(ControlPlaneScope::ExecutionDebugSensitive),
        "admin:maintenance" => Ok(ControlPlaneScope::AdminMaintenance),
        _ => Err(invalid_config(&format!(
            "SINAN_CONTROL_PLANE_SCOPES contains unknown scope {value:?}"
        ))),
    }
}

fn validate_positive_duration(name: &'static str, value: Duration) -> Result<(), io::Error> {
    if value.is_zero() {
        Err(invalid_config(&format!("{name} must be positive")))
    } else {
        Ok(())
    }
}

fn duration_ms_i64(name: &'static str, value: Duration) -> Result<i64, io::Error> {
    i64::try_from(value.as_millis())
        .map_err(|_| invalid_config(&format!("{name} exceeds the supported millisecond range")))
}

fn invalid_config(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.to_owned())
}

fn system_now_ms() -> i64 {
    SystemTradingCoreClock.now_ms()
}

#[cfg(test)]
mod tests {
    use std::{
        future,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use tokio::{
        sync::{mpsc, watch, Notify},
        time::timeout,
    };

    use super::{
        require_loopback, run_interruptible_batch, spawn_supervised_task, BatchItemOutcome,
        InterruptibleBatchOutcome,
    };

    #[test]
    fn plaintext_execution_listeners_are_loopback_only() {
        assert!(require_loopback("test", "127.0.0.1:9100".parse().unwrap()).is_ok());
        assert!(require_loopback("test", "[::1]:9100".parse().unwrap()).is_ok());
        assert!(require_loopback("test", "0.0.0.0:9100".parse().unwrap()).is_err());
        assert!(require_loopback("test", "192.0.2.10:9100".parse().unwrap()).is_err());
    }

    #[tokio::test]
    async fn outbound_batch_cancels_a_pending_item_on_shutdown() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = Arc::clone(&calls);
        let second_item_started = Arc::new(Notify::new());
        let observed_start = Arc::clone(&second_item_started);

        let batch = tokio::spawn(async move {
            run_interruptible_batch(&mut shutdown_rx, 64, move || {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                let second_item_started = Arc::clone(&second_item_started);
                async move {
                    if call == 0 {
                        Ok::<_, ()>(BatchItemOutcome::Processed)
                    } else {
                        second_item_started.notify_one();
                        future::pending::<Result<BatchItemOutcome, ()>>().await
                    }
                }
            })
            .await
        });

        observed_start.notified().await;
        shutdown_tx.send(true).unwrap();
        let outcome = timeout(Duration::from_millis(250), batch)
            .await
            .expect("shutdown should cancel the pending batch item")
            .unwrap()
            .unwrap();

        assert_eq!(outcome, InterruptibleBatchOutcome::Shutdown);
        assert_eq!(observed_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn supervisor_reports_worker_panic_immediately() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (fatal_tx, mut fatal_rx) = mpsc::unbounded_channel();
        let task = spawn_supervised_task("Test worker", shutdown_rx, fatal_tx, async move {
            if std::hint::black_box(true) {
                panic!("expected test panic");
            }
            Ok(())
        });

        let failure = timeout(Duration::from_millis(250), fatal_rx.recv())
            .await
            .expect("panic should reach the supervisor immediately")
            .expect("fatal channel should remain available");
        assert!(failure.contains("Test worker task failed:"));
        assert!(failure.contains("panicked"));
        assert_eq!(task.await.unwrap().unwrap_err(), failure);
    }

    #[tokio::test]
    async fn supervisor_distinguishes_unexpected_exit_from_clean_shutdown() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let clean_shutdown_rx = shutdown_tx.subscribe();
        let (fatal_tx, mut fatal_rx) = mpsc::unbounded_channel();
        let unexpected =
            spawn_supervised_task("Unexpected worker", shutdown_rx, fatal_tx.clone(), async {
                Ok(())
            });

        assert_eq!(
            fatal_rx.recv().await.unwrap(),
            "Unexpected worker stopped unexpectedly"
        );
        assert_eq!(
            unexpected.await.unwrap().unwrap_err(),
            "Unexpected worker stopped unexpectedly"
        );

        shutdown_tx.send(true).unwrap();
        let expected = spawn_supervised_task(
            "Shutdown worker",
            clean_shutdown_rx,
            fatal_tx.clone(),
            async { Ok(()) },
        );
        assert_eq!(expected.await.unwrap(), Ok(()));
        drop(fatal_tx);
        assert_eq!(fatal_rx.recv().await, None);
    }
}
