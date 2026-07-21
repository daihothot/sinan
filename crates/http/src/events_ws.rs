use std::{sync::Arc, time::Duration};

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sinan_events::{
    EventStreamManager, EventStreamManagerError, EventSubscription, EventSubscriptionRequest,
    SubscriptionCloseReason, SubscriptionOutcome,
};
use sinan_store::EventStreamRecord;
use sinan_types::{AccountId, EventStreamTopic};

use crate::{ControlPlaneRequestContext, HttpServerClock};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventWebSocketConfig {
    pub max_message_bytes: usize,
    pub write_timeout: Duration,
}

impl Default for EventWebSocketConfig {
    fn default() -> Self {
        Self {
            max_message_bytes: 64 * 1024,
            write_timeout: Duration::from_secs(5),
        }
    }
}

impl EventWebSocketConfig {
    fn validate(self) -> Result<Self, EventWebSocketConfigurationError> {
        if self.max_message_bytes == 0 {
            return Err(EventWebSocketConfigurationError::ZeroMaxMessageBytes);
        }
        if self.write_timeout.is_zero() {
            return Err(EventWebSocketConfigurationError::ZeroWriteTimeout);
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EventWebSocketConfigurationError {
    #[error("max_message_bytes must be greater than zero")]
    ZeroMaxMessageBytes,
    #[error("write_timeout must be greater than zero")]
    ZeroWriteTimeout,
}

#[derive(Clone)]
pub(crate) struct EventWebSocketService {
    manager: Arc<EventStreamManager>,
    config: EventWebSocketConfig,
}

impl EventWebSocketService {
    pub(crate) fn new(
        manager: Arc<EventStreamManager>,
        config: EventWebSocketConfig,
    ) -> Result<Self, EventWebSocketConfigurationError> {
        let config = config.validate()?;
        Ok(Self { manager, config })
    }
}

#[derive(Debug)]
enum EventClientMessage {
    Subscribe {
        topics: Vec<EventStreamTopic>,
        account_id: Option<AccountId>,
        last_event_id: Option<String>,
    },
    Unsubscribe,
    Ping,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscribeClientMessage {
    op: String,
    topics: Vec<EventStreamTopic>,
    #[serde(default)]
    account_id: Option<AccountId>,
    #[serde(default)]
    last_event_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyClientMessage {
    op: String,
}

impl<'de> Deserialize<'de> for EventClientMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let op = value
            .get("op")
            .and_then(Value::as_str)
            .ok_or_else(|| D::Error::custom("op must be a string"))?;
        match op {
            "subscribe" => {
                let message: SubscribeClientMessage =
                    serde_json::from_value(value).map_err(D::Error::custom)?;
                if message.op != "subscribe" {
                    return Err(D::Error::custom("invalid subscribe op"));
                }
                Ok(Self::Subscribe {
                    topics: message.topics,
                    account_id: message.account_id,
                    last_event_id: message.last_event_id,
                })
            }
            "unsubscribe" | "ping" => {
                let message: EmptyClientMessage =
                    serde_json::from_value(value).map_err(D::Error::custom)?;
                match message.op.as_str() {
                    "unsubscribe" => Ok(Self::Unsubscribe),
                    "ping" => Ok(Self::Ping),
                    _ => Err(D::Error::custom("invalid event client op")),
                }
            }
            _ => Err(D::Error::custom("unknown event client op")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum EventSubscriptionStatus {
    Subscribed,
    ResumeFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum EventResumeFailureReason {
    CursorExpired,
    GapDetected,
    Unauthorized,
}

#[derive(Debug, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum EventServerMessage {
    Subscription {
        status: EventSubscriptionStatus,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<EventResumeFailureReason>,
        server_time: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        next_event_id: Option<String>,
        requires_state_reload: bool,
    },
    Event {
        event_id: String,
        topic: EventStreamTopic,
        #[serde(skip_serializing_if = "Option::is_none")]
        account_id: Option<AccountId>,
        event_type: String,
        created_at: i64,
        payload: Value,
    },
    Pong {
        server_time: i64,
    },
}

const CURSOR_RECOVERY_CLOSE_CODE: u16 = 1013;
const CURSOR_RECOVERY_CLOSE_REASON: &str = "event cursor recovery required";

pub(crate) fn upgrade_event_websocket(
    upgrade: WebSocketUpgrade,
    context: ControlPlaneRequestContext,
    service: EventWebSocketService,
    clock: Arc<dyn HttpServerClock>,
) -> axum::response::Response {
    let max_message_bytes = service.config.max_message_bytes;
    upgrade
        .max_message_size(max_message_bytes)
        .max_frame_size(max_message_bytes)
        .on_upgrade(move |socket| run_event_websocket(socket, context, service, clock))
}

async fn run_event_websocket(
    mut socket: WebSocket,
    context: ControlPlaneRequestContext,
    service: EventWebSocketService,
    clock: Arc<dyn HttpServerClock>,
) {
    let mut subscription: Option<EventSubscription> = None;
    loop {
        if let Some(active) = subscription.as_mut() {
            tokio::select! {
                incoming = socket.recv() => {
                    if !handle_incoming(
                        incoming,
                        &mut socket,
                        &context,
                        &service,
                        &clock,
                        &mut subscription,
                    ).await {
                        break;
                    }
                }
                outcome = active.recv() => {
                    match outcome {
                        SubscriptionOutcome::Event(record) => {
                            if let Err(error) = send_record(
                                &mut socket,
                                record,
                                service.config.max_message_bytes,
                                service.config.write_timeout,
                            ).await {
                                let (code, reason) = match error {
                                    EventSendError::TooLarge => (1009, "event message is too large"),
                                    EventSendError::Encode | EventSendError::Write => {
                                        (1011, "event write failed")
                                    }
                                };
                                close(&mut socket, code, reason, service.config.write_timeout).await;
                                break;
                            }
                        }
                        SubscriptionOutcome::Closed(reason) => {
                            let (code, reason) = subscription_close_frame(&reason);
                            close(&mut socket, code, reason, service.config.write_timeout).await;
                            break;
                        }
                    }
                }
            }
        } else {
            let incoming = socket.recv().await;
            if !handle_incoming(
                incoming,
                &mut socket,
                &context,
                &service,
                &clock,
                &mut subscription,
            )
            .await
            {
                break;
            }
        }
    }
}

async fn handle_incoming(
    incoming: Option<Result<Message, axum::Error>>,
    socket: &mut WebSocket,
    context: &ControlPlaneRequestContext,
    service: &EventWebSocketService,
    clock: &Arc<dyn HttpServerClock>,
    subscription: &mut Option<EventSubscription>,
) -> bool {
    let Some(incoming) = incoming else {
        return false;
    };
    let message = match incoming {
        Ok(message) => message,
        Err(_) => return false,
    };
    let text = match message {
        Message::Text(text) if text.len() <= service.config.max_message_bytes => text,
        Message::Text(_) => {
            close(
                socket,
                1009,
                "event message is too large",
                service.config.write_timeout,
            )
            .await;
            return false;
        }
        Message::Binary(_) => {
            close(
                socket,
                1003,
                "binary event messages are not supported",
                service.config.write_timeout,
            )
            .await;
            return false;
        }
        Message::Close(_) => return false,
        Message::Ping(_) | Message::Pong(_) => return true,
    };
    let client_message = match serde_json::from_str::<EventClientMessage>(text.as_str()) {
        Ok(message) => message,
        Err(_) => {
            close(
                socket,
                1002,
                "invalid event client message",
                service.config.write_timeout,
            )
            .await;
            return false;
        }
    };

    match client_message {
        EventClientMessage::Subscribe {
            topics,
            account_id,
            last_event_id,
        } => {
            let request = EventSubscriptionRequest {
                topics,
                account_id,
                last_event_id,
            };
            match service
                .manager
                .subscribe(request, context.principal.account_scope())
                .await
            {
                Ok(active) => {
                    let response = EventServerMessage::Subscription {
                        status: EventSubscriptionStatus::Subscribed,
                        reason: None,
                        server_time: clock.now_ms(),
                        next_event_id: None,
                        requires_state_reload: false,
                    };
                    if send_json(
                        socket,
                        &response,
                        service.config.max_message_bytes,
                        service.config.write_timeout,
                    )
                    .await
                    .is_err()
                    {
                        return false;
                    }
                    *subscription = Some(active);
                }
                Err(error) => {
                    let reason = match error {
                        EventStreamManagerError::UnauthorizedAccount { .. } => {
                            EventResumeFailureReason::Unauthorized
                        }
                        EventStreamManagerError::CursorExpired { .. } => {
                            EventResumeFailureReason::CursorExpired
                        }
                        EventStreamManagerError::ReplayLimitExceeded { .. } => {
                            EventResumeFailureReason::GapDetected
                        }
                        EventStreamManagerError::EmptyTopics
                        | EventStreamManagerError::DuplicateTopic { .. }
                        | EventStreamManagerError::EmptyCursor => {
                            close(
                                socket,
                                1002,
                                "invalid event subscription",
                                service.config.write_timeout,
                            )
                            .await;
                            return false;
                        }
                        EventStreamManagerError::InvalidConfiguration(_)
                        | EventStreamManagerError::Store(_) => {
                            close(
                                socket,
                                1011,
                                "event service unavailable",
                                service.config.write_timeout,
                            )
                            .await;
                            return false;
                        }
                    };
                    *subscription = None;
                    let fail_closed = reason == EventResumeFailureReason::GapDetected;
                    let response = EventServerMessage::Subscription {
                        status: EventSubscriptionStatus::ResumeFailed,
                        reason: Some(reason),
                        server_time: clock.now_ms(),
                        next_event_id: None,
                        requires_state_reload: true,
                    };
                    if send_json(
                        socket,
                        &response,
                        service.config.max_message_bytes,
                        service.config.write_timeout,
                    )
                    .await
                    .is_err()
                    {
                        return false;
                    }
                    if fail_closed {
                        close(
                            socket,
                            CURSOR_RECOVERY_CLOSE_CODE,
                            CURSOR_RECOVERY_CLOSE_REASON,
                            service.config.write_timeout,
                        )
                        .await;
                        return false;
                    }
                }
            }
        }
        EventClientMessage::Unsubscribe => *subscription = None,
        EventClientMessage::Ping => {
            if send_json(
                socket,
                &EventServerMessage::Pong {
                    server_time: clock.now_ms(),
                },
                service.config.max_message_bytes,
                service.config.write_timeout,
            )
            .await
            .is_err()
            {
                return false;
            }
        }
    }
    true
}

fn subscription_close_frame(reason: &SubscriptionCloseReason) -> (u16, &'static str) {
    match reason {
        SubscriptionCloseReason::SlowConsumer { .. } | SubscriptionCloseReason::ManagerClosed => {
            (CURSOR_RECOVERY_CLOSE_CODE, CURSOR_RECOVERY_CLOSE_REASON)
        }
    }
}

async fn send_record(
    socket: &mut WebSocket,
    record: EventStreamRecord,
    max_message_bytes: usize,
    timeout: Duration,
) -> Result<(), EventSendError> {
    if record.payload.as_str().len() > max_message_bytes {
        return Err(EventSendError::TooLarge);
    }
    let payload =
        serde_json::from_str(record.payload.as_str()).map_err(|_| EventSendError::Encode)?;
    send_json(
        socket,
        &EventServerMessage::Event {
            event_id: record.event_id,
            topic: record.topic,
            account_id: record.account_id,
            event_type: record.event_type,
            created_at: record.created_at,
            payload,
        },
        max_message_bytes,
        timeout,
    )
    .await
}

async fn send_json<T: Serialize>(
    socket: &mut WebSocket,
    value: &T,
    max_message_bytes: usize,
    timeout: Duration,
) -> Result<(), EventSendError> {
    let text = encode_json(value, max_message_bytes)?;
    tokio::time::timeout(timeout, socket.send(Message::Text(text.into())))
        .await
        .map_err(|_| EventSendError::Write)?
        .map_err(|_| EventSendError::Write)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EventSendError {
    Encode,
    TooLarge,
    Write,
}

fn encode_json<T: Serialize>(
    value: &T,
    max_message_bytes: usize,
) -> Result<String, EventSendError> {
    let text = serde_json::to_string(value).map_err(|_| EventSendError::Encode)?;
    if text.len() > max_message_bytes {
        Err(EventSendError::TooLarge)
    } else {
        Ok(text)
    }
}

async fn close(socket: &mut WebSocket, code: u16, reason: &'static str, timeout: Duration) {
    let _ = tokio::time::timeout(
        timeout,
        socket.send(Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        }))),
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_messages_are_strict_and_tagged() {
        let subscribe = serde_json::from_str::<EventClientMessage>(
            r#"{"op":"subscribe","topics":["system.event"],"last_event_id":"event-1"}"#,
        );
        assert!(matches!(
            subscribe,
            Ok(EventClientMessage::Subscribe { .. })
        ));
        assert!(serde_json::from_str::<EventClientMessage>(
            r#"{"op":"subscribe","topics":["execution.command"]}"#
        )
        .is_err());
        assert!(serde_json::from_str::<EventClientMessage>(
            r#"{"op":"ping","execution_command":{}}"#
        )
        .is_err());
    }

    #[test]
    fn event_config_rejects_unbounded_values() {
        let config = EventWebSocketConfig {
            max_message_bytes: 0,
            ..EventWebSocketConfig::default()
        };
        assert_eq!(
            config.validate().unwrap_err(),
            EventWebSocketConfigurationError::ZeroMaxMessageBytes
        );
    }

    #[test]
    fn terminal_subscription_outcomes_require_cursor_recovery() {
        for reason in [
            SubscriptionCloseReason::SlowConsumer { skipped: 1 },
            SubscriptionCloseReason::ManagerClosed,
        ] {
            assert_eq!(
                subscription_close_frame(&reason),
                (CURSOR_RECOVERY_CLOSE_CODE, CURSOR_RECOVERY_CLOSE_REASON)
            );
        }
    }

    #[test]
    fn outbound_messages_obey_the_configured_text_limit() {
        let message = EventServerMessage::Pong { server_time: 1 };
        let encoded = serde_json::to_string(&message).unwrap();
        assert_eq!(
            encode_json(&message, encoded.len() - 1),
            Err(EventSendError::TooLarge)
        );
        assert_eq!(encode_json(&message, encoded.len()).unwrap(), encoded);
    }
}
