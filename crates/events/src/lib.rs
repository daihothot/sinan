#![forbid(unsafe_code)]

//! Durable summary-event publication and bounded replay-to-live fanout.

use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};

use sinan_store::{
    AuthorizedAccountScope, EventReplayError, EventStreamFilter, EventStreamRecord,
    NewEventStreamRecord, SqliteStateStore, StoreError, WriteOutcome,
};
use sinan_types::{AccountId, EventStreamTopic};
use thiserror::Error;
use tokio::sync::{broadcast, Mutex};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventStreamManagerConfig {
    /// Number of committed live records retained by the in-memory fanout ring.
    pub live_capacity: usize,
    /// Maximum number of durable records accepted in one cursor resume.
    pub replay_limit: u64,
}

impl Default for EventStreamManagerConfig {
    fn default() -> Self {
        Self {
            live_capacity: 1_024,
            replay_limit: 1_000,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventSubscriptionRequest {
    pub topics: Vec<EventStreamTopic>,
    /// Narrows the principal's scope to one account; it can never broaden it.
    pub account_id: Option<AccountId>,
    pub last_event_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct EventStreamManager {
    store: SqliteStateStore,
    live: broadcast::Sender<EventStreamRecord>,
    replay_limit: u64,
    // Serializing append + fanout preserves stream_sequence order for concurrent publishers.
    publish_lock: Arc<Mutex<()>>,
}

impl EventStreamManager {
    pub fn new(
        store: SqliteStateStore,
        config: EventStreamManagerConfig,
    ) -> Result<Self, EventStreamManagerError> {
        if config.live_capacity == 0 {
            return Err(EventStreamManagerError::InvalidConfiguration(
                "live_capacity must be greater than zero",
            ));
        }
        if config.replay_limit == 0 {
            return Err(EventStreamManagerError::InvalidConfiguration(
                "replay_limit must be greater than zero",
            ));
        }
        let (live, _) = broadcast::channel(config.live_capacity);
        Ok(Self {
            store,
            live,
            replay_limit: config.replay_limit,
            publish_lock: Arc::new(Mutex::new(())),
        })
    }

    /// Persists the event before offering it to live subscribers. Duplicate
    /// appends are not fanned out a second time.
    pub async fn publish(
        &self,
        event: NewEventStreamRecord,
    ) -> Result<WriteOutcome<EventStreamRecord>, EventStreamManagerError> {
        let _guard = self.publish_lock.lock().await;
        let outcome = self.store.append_event_stream_record(event).await?;
        if let WriteOutcome::Inserted(record) = &outcome {
            // No subscribers is not a publication failure: the durable replay
            // log remains the recovery source.
            let _ = self.live.send(record.clone());
        }
        Ok(outcome)
    }

    /// Establishes the live receiver first, then captures a durable high-water
    /// replay snapshot. Records at or below that boundary are de-duplicated when
    /// the subscription transitions to live delivery.
    pub async fn subscribe(
        &self,
        request: EventSubscriptionRequest,
        principal_accounts: &AuthorizedAccountScope,
    ) -> Result<EventSubscription, EventStreamManagerError> {
        let filter = subscription_filter(&request, principal_accounts)?;
        let live = self.live.subscribe();
        let window = self
            .store
            .replay_event_stream(request.last_event_id.as_deref(), &filter, self.replay_limit)
            .await
            .map_err(EventStreamManagerError::from)?;
        if window.has_more {
            return Err(EventStreamManagerError::ReplayLimitExceeded {
                limit: self.replay_limit,
            });
        }
        let high_water = window.high_water.unwrap_or(0);
        Ok(EventSubscription {
            replay: window.records.into(),
            live,
            filter,
            high_water,
            last_delivered_sequence: 0,
            terminal: None,
        })
    }

    pub fn store(&self) -> &SqliteStateStore {
        &self.store
    }
}

#[derive(Debug)]
pub struct EventSubscription {
    replay: VecDeque<EventStreamRecord>,
    live: broadcast::Receiver<EventStreamRecord>,
    filter: EventStreamFilter,
    high_water: u64,
    last_delivered_sequence: u64,
    terminal: Option<SubscriptionCloseReason>,
}

impl EventSubscription {
    pub fn high_water(&self) -> u64 {
        self.high_water
    }

    /// Returns only ordered, matching records. Once a live gap is observed the
    /// subscription is permanently closed; callers must resume from their last
    /// delivered event id rather than continuing after an unknown gap.
    pub async fn recv(&mut self) -> SubscriptionOutcome {
        if let Some(reason) = &self.terminal {
            return SubscriptionOutcome::Closed(reason.clone());
        }

        if let Some(record) = self.replay.pop_front() {
            self.last_delivered_sequence = record.stream_sequence;
            return SubscriptionOutcome::Event(record);
        }

        loop {
            match self.live.recv().await {
                Ok(record) => {
                    if record.stream_sequence <= self.high_water
                        || record.stream_sequence <= self.last_delivered_sequence
                        || !self.filter.matches(&record)
                    {
                        continue;
                    }
                    self.last_delivered_sequence = record.stream_sequence;
                    return SubscriptionOutcome::Event(record);
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    let reason = SubscriptionCloseReason::SlowConsumer { skipped };
                    self.terminal = Some(reason.clone());
                    return SubscriptionOutcome::Closed(reason);
                }
                Err(broadcast::error::RecvError::Closed) => {
                    let reason = SubscriptionCloseReason::ManagerClosed;
                    self.terminal = Some(reason.clone());
                    return SubscriptionOutcome::Closed(reason);
                }
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubscriptionOutcome {
    Event(EventStreamRecord),
    Closed(SubscriptionCloseReason),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubscriptionCloseReason {
    SlowConsumer { skipped: u64 },
    ManagerClosed,
}

#[derive(Debug, Error)]
pub enum EventStreamManagerError {
    #[error("invalid event stream configuration: {0}")]
    InvalidConfiguration(&'static str),

    #[error("at least one event topic is required")]
    EmptyTopics,

    #[error("event topic {topic} is repeated")]
    DuplicateTopic { topic: EventStreamTopic },

    #[error("requested account {account_id} is outside the principal account scope")]
    UnauthorizedAccount { account_id: AccountId },

    #[error("last_event_id must not be empty")]
    EmptyCursor,

    #[error("event cursor {event_id} is expired or unavailable")]
    CursorExpired { event_id: String },

    #[error("event replay exceeds the configured limit of {limit} records")]
    ReplayLimitExceeded { limit: u64 },

    #[error(transparent)]
    Store(#[from] StoreError),
}

impl From<EventReplayError> for EventStreamManagerError {
    fn from(error: EventReplayError) -> Self {
        match error {
            EventReplayError::CursorExpired { event_id } => Self::CursorExpired { event_id },
            EventReplayError::InvalidLimit => {
                Self::InvalidConfiguration("replay_limit must be greater than zero")
            }
            EventReplayError::Store(error) => Self::Store(error),
        }
    }
}

fn subscription_filter(
    request: &EventSubscriptionRequest,
    principal_accounts: &AuthorizedAccountScope,
) -> Result<EventStreamFilter, EventStreamManagerError> {
    if request.topics.is_empty() {
        return Err(EventStreamManagerError::EmptyTopics);
    }
    let mut topics = HashSet::with_capacity(request.topics.len());
    for topic in &request.topics {
        if !topics.insert(*topic) {
            return Err(EventStreamManagerError::DuplicateTopic { topic: *topic });
        }
    }
    if request.last_event_id.as_ref().is_some_and(String::is_empty) {
        return Err(EventStreamManagerError::EmptyCursor);
    }

    let account_scope = match &request.account_id {
        Some(account_id) if !principal_accounts.contains(account_id) => {
            return Err(EventStreamManagerError::UnauthorizedAccount {
                account_id: account_id.clone(),
            });
        }
        Some(account_id) => AuthorizedAccountScope::new([account_id.clone()]),
        None => principal_accounts.clone(),
    };
    Ok(EventStreamFilter::new(topics, account_scope))
}
