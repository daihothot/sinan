#![forbid(unsafe_code)]

use std::{env, error::Error, io, net::SocketAddr, sync::Arc, time::Duration};

use sinan_core::{
    compose_production_gateway_persistence, restore_durable_circuit_breaker,
    SqliteControlPlaneService, SqliteControlPlaneServiceConfig, SystemTradingCoreClock,
    TradingCoreClock,
};
use sinan_events::{EventStreamManager, EventStreamManagerConfig};
use sinan_http::{
    control_plane_router, ControlPlaneHttpState, ControlPlanePrincipal, ControlPlaneQueryPort,
    ControlPlaneScope, ControlPlaneTokenGrant, EventWebSocketConfig, FixedBearerTokenRegistry,
    TradeIntentApplicationPort,
};
use sinan_store::{EventRetentionPolicy, SqliteStateStore, StoreOptions};
use sinan_types::AccountId;

#[derive(Debug)]
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
    if config.event_retain_latest == 0 {
        return Err(invalid_config("SINAN_EVENT_RETAIN_LATEST must be positive").into());
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
    let _gateway_persistence =
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
        .with_clock(clock)
        .with_event_stream(
            Arc::clone(&event_stream),
            EventWebSocketConfig {
                max_message_bytes: config.event_max_message_bytes,
                write_timeout: config.event_write_timeout,
            },
        )?;

    let retention_store = store;
    let retention_interval = config.event_retention_interval;
    let retention_age = config.event_retention_age;
    let retain_latest = config.event_retain_latest;
    let retention_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(retention_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
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
    });

    let listener = tokio::net::TcpListener::bind(config.http_addr).await?;
    let result = axum::serve(listener, control_plane_router(http_state))
        .with_graceful_shutdown(shutdown_signal())
        .await;
    retention_task.abort();
    result?;
    Ok(())
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

fn invalid_config(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.to_owned())
}

fn system_now_ms() -> i64 {
    SystemTradingCoreClock.now_ms()
}
