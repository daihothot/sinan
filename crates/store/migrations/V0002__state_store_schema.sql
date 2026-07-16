CREATE TABLE core_events (
  event_id TEXT PRIMARY KEY,
  event_type TEXT NOT NULL,
  aggregate_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  message_id TEXT,
  schema_version TEXT NOT NULL,
  correlation_id TEXT,
  causation_id TEXT,
  account_id TEXT,
  client_id TEXT,
  terminal_id TEXT,
  strategy_id TEXT,
  intent_id TEXT,
  plan_id TEXT,
  leg_id TEXT,
  command_id TEXT,
  idempotency_key TEXT,
  event_at INTEGER NOT NULL,
  received_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  source TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*')
);

CREATE INDEX idx_core_events_type_time
ON core_events(event_type, event_at);

CREATE INDEX idx_core_events_command
ON core_events(command_id);

CREATE INDEX idx_core_events_intent
ON core_events(intent_id);

CREATE INDEX idx_core_events_account_time
ON core_events(account_id, event_at);

CREATE INDEX idx_core_events_aggregate_time
ON core_events(aggregate_type, aggregate_id, event_at, event_id);

CREATE UNIQUE INDEX idx_core_events_message_id
ON core_events(message_id)
WHERE message_id IS NOT NULL;

CREATE TABLE deadletter_events (
  deadletter_id TEXT PRIMARY KEY,
  message_id TEXT,
  message_type TEXT,
  schema_version TEXT,
  reason TEXT NOT NULL,
  raw_payload TEXT,
  received_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE system_events (
  system_event_id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  severity TEXT NOT NULL
    CHECK (severity IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')),
  component TEXT NOT NULL,
  message TEXT NOT NULL,
  metadata_json TEXT CHECK (metadata_json IS NULL OR json_valid(metadata_json)),
  timestamp INTEGER NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE execution_client_sessions (
  session_id TEXT PRIMARY KEY,
  client_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  terminal_id TEXT,
  platform TEXT NOT NULL
    CHECK (platform IN ('MT5', 'BINANCE', 'OKX', 'IBKR', 'PAPER', 'BACKTEST', 'EXCHANGE')),
  status TEXT NOT NULL
    CHECK (status IN ('ACTIVE', 'STALE', 'DISCONNECTED', 'REJECTED')),
  capabilities_json TEXT NOT NULL CHECK (json_valid(capabilities_json)),
  remote_addr TEXT,
  connected_at INTEGER NOT NULL,
  last_heartbeat_at INTEGER,
  last_time_sync_at INTEGER,
  clock_sync_status TEXT
    CHECK (clock_sync_status IS NULL OR clock_sync_status IN ('SYNCED', 'DEGRADED', 'UNSYNCED')),
  disconnected_at INTEGER
);

CREATE UNIQUE INDEX idx_active_session_identity
ON execution_client_sessions(client_id, account_id, COALESCE(terminal_id, ''))
WHERE status = 'ACTIVE';

CREATE INDEX idx_execution_client_sessions_account_status
ON execution_client_sessions(account_id, status);

CREATE TABLE trade_intents (
  intent_id TEXT PRIMARY KEY,
  decision_id TEXT NOT NULL,
  strategy_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  action TEXT NOT NULL
    CHECK (action IN ('BUY', 'SELL', 'CLOSE', 'HOLD')),
  status TEXT NOT NULL
    CHECK (status IN ('ACCEPTED', 'RISK_BLOCKED', 'REJECTED', 'DUPLICATE', 'EXPIRED', 'CANCELLED')),
  requested_at INTEGER NOT NULL,
  signal_expires_at INTEGER NOT NULL,
  idempotency_key TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE (intent_id, account_id)
);

CREATE UNIQUE INDEX idx_trade_intents_idempotency
ON trade_intents(idempotency_key);

CREATE INDEX idx_trade_intents_account_status_time
ON trade_intents(account_id, status, requested_at);

CREATE TABLE risk_results (
  risk_id TEXT PRIMARY KEY,
  intent_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  approved INTEGER NOT NULL CHECK (approved IN (0, 1)),
  reason TEXT NOT NULL,
  snapshot_age_ms INTEGER NOT NULL CHECK (snapshot_age_ms >= 0),
  symbol_metadata_age_ms INTEGER NOT NULL CHECK (symbol_metadata_age_ms >= 0),
  evaluated_at INTEGER NOT NULL,
  valid_until INTEGER NOT NULL CHECK (valid_until >= evaluated_at),
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  UNIQUE (risk_id, account_id),
  UNIQUE (risk_id, intent_id, account_id),
  FOREIGN KEY (intent_id, account_id)
    REFERENCES trade_intents(intent_id, account_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE INDEX idx_risk_results_intent_time
ON risk_results(intent_id, evaluated_at);

CREATE TABLE execution_plans (
  plan_id TEXT PRIMARY KEY,
  risk_id TEXT NOT NULL,
  intent_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  strategy_id TEXT NOT NULL,
  status TEXT NOT NULL
    CHECK (status IN (
      'PENDING',
      'RECONCILING',
      'MANUAL_RECONCILIATION_REQUIRED',
      'PARTIAL',
      'COMPLETED',
      'FAILED',
      'EXPIRED',
      'CANCELLED'
    )),
  mode TEXT NOT NULL
    CHECK (mode IN ('sequential', 'simultaneous', 'best_effort_atomic')),
  failure_policy TEXT NOT NULL
    CHECK (failure_policy IN ('cancel_all', 'partial_fill', 'retry')),
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE (plan_id, risk_id, account_id),
  FOREIGN KEY (risk_id, intent_id, account_id)
    REFERENCES risk_results(risk_id, intent_id, account_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE INDEX idx_execution_plans_intent
ON execution_plans(intent_id);

CREATE INDEX idx_execution_plans_account_status
ON execution_plans(account_id, status);

CREATE TABLE execution_legs (
  leg_id TEXT PRIMARY KEY,
  plan_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  action TEXT NOT NULL
    CHECK (action IN ('BUY', 'SELL', 'CLOSE', 'MODIFY', 'CANCEL')),
  status TEXT NOT NULL
    CHECK (status IN (
      'PENDING',
      'SENT',
      'DELIVERY_UNCONFIRMED',
      'RECONCILING',
      'MANUAL_RECONCILIATION_REQUIRED',
      'COMMAND_RECEIVED',
      'ACCEPTED',
      'REJECTED',
      'ORDER_SENT',
      'PARTIALLY_FILLED',
      'FILLED',
      'FAILED',
      'EXPIRED',
      'CANCELLED'
    )),
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  updated_at INTEGER NOT NULL,
  UNIQUE (plan_id, leg_id),
  FOREIGN KEY (plan_id)
    REFERENCES execution_plans(plan_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE INDEX idx_execution_legs_plan_status
ON execution_legs(plan_id, status);

CREATE TABLE execution_commands (
  command_id TEXT PRIMARY KEY,
  risk_id TEXT NOT NULL,
  plan_id TEXT,
  leg_id TEXT,
  account_id TEXT NOT NULL,
  client_id TEXT,
  terminal_id TEXT,
  symbol TEXT NOT NULL,
  action TEXT NOT NULL
    CHECK (action IN ('BUY', 'SELL', 'CLOSE', 'MODIFY', 'CANCEL')),
  expires_at INTEGER NOT NULL,
  idempotency_key TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  hmac TEXT NOT NULL
    CHECK (length(hmac) = 64 AND hmac NOT GLOB '*[^0-9a-f]*'),
  created_at INTEGER NOT NULL,
  CHECK (leg_id IS NULL OR plan_id IS NOT NULL),
  UNIQUE (command_id, account_id),
  FOREIGN KEY (risk_id, account_id)
    REFERENCES risk_results(risk_id, account_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT,
  FOREIGN KEY (plan_id, risk_id, account_id)
    REFERENCES execution_plans(plan_id, risk_id, account_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT,
  FOREIGN KEY (plan_id, leg_id)
    REFERENCES execution_legs(plan_id, leg_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE UNIQUE INDEX idx_execution_commands_idempotency
ON execution_commands(idempotency_key);

CREATE INDEX idx_execution_commands_account_expiry
ON execution_commands(account_id, expires_at);

CREATE TABLE execution_command_states (
  command_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  plan_id TEXT,
  leg_id TEXT,
  status TEXT NOT NULL
    CHECK (status IN (
      'CREATED',
      'DISPATCHED',
      'DELIVERY_UNCONFIRMED',
      'DELIVERY_FAILED',
      'RECONCILING',
      'MANUAL_RECONCILIATION_REQUIRED',
      'COMMAND_RECEIVED',
      'ACCEPTED',
      'REJECTED',
      'ORDER_SENT',
      'PARTIALLY_FILLED',
      'FILLED',
      'FAILED',
      'EXPIRED',
      'CANCELLED'
    )),
  delivery_attempts INTEGER NOT NULL DEFAULT 0 CHECK (delivery_attempts >= 0),
  last_delivery_error TEXT,
  created_at INTEGER NOT NULL,
  dispatched_at INTEGER,
  command_received_at INTEGER,
  reconciling_at INTEGER,
  completed_at INTEGER,
  updated_at INTEGER NOT NULL,
  CHECK (leg_id IS NULL OR plan_id IS NOT NULL),
  FOREIGN KEY (command_id, account_id)
    REFERENCES execution_commands(command_id, account_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT,
  FOREIGN KEY (plan_id)
    REFERENCES execution_plans(plan_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT,
  FOREIGN KEY (plan_id, leg_id)
    REFERENCES execution_legs(plan_id, leg_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE INDEX idx_execution_command_states_account_status
ON execution_command_states(account_id, status);

CREATE INDEX idx_execution_command_states_plan
ON execution_command_states(plan_id);

CREATE INDEX idx_execution_command_states_leg
ON execution_command_states(leg_id);

CREATE TABLE execution_events (
  execution_id TEXT PRIMARY KEY,
  command_id TEXT NOT NULL,
  plan_id TEXT,
  leg_id TEXT,
  account_id TEXT NOT NULL,
  status TEXT NOT NULL
    CHECK (status IN (
      'ACCEPTED',
      'ORDER_SENT',
      'REJECTED',
      'FILLED',
      'PARTIALLY_FILLED',
      'FAILED',
      'EXPIRED',
      'CANCELLED'
    )),
  broker_order_id TEXT,
  position_ticket TEXT,
  event_at INTEGER NOT NULL,
  filled_at INTEGER,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  created_at INTEGER NOT NULL
);

CREATE INDEX idx_execution_events_command_time
ON execution_events(command_id, event_at);

CREATE INDEX idx_execution_events_plan_time
ON execution_events(plan_id, event_at);

CREATE INDEX idx_execution_events_account_time
ON execution_events(account_id, event_at);

CREATE TABLE wire_inbox (
  message_id TEXT PRIMARY KEY,
  session_id TEXT,
  message_type TEXT NOT NULL
    CHECK (message_type IN (
      'session.hello',
      'session.accepted',
      'session.rejected',
      'time.sync.request',
      'time.sync.response',
      'heartbeat',
      'transport.ack',
      'market.tick',
      'market.bar',
      'symbol.metadata',
      'account.snapshot',
      'position.snapshot',
      'order.snapshot',
      'execution.command',
      'command.received',
      'execution.event',
      'reconciliation.request',
      'reconciliation.result',
      'protocol.error'
    )),
  sequence INTEGER CHECK (sequence IS NULL OR sequence > 0),
  received_at INTEGER NOT NULL,
  handled_at INTEGER,
  status TEXT NOT NULL
    CHECK (status IN ('RECEIVED', 'ACKED', 'HANDLED', 'DUPLICATE', 'DEADLETTER', 'FAILED')),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  FOREIGN KEY (session_id)
    REFERENCES execution_client_sessions(session_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE UNIQUE INDEX idx_wire_inbox_session_sequence
ON wire_inbox(session_id, sequence)
WHERE session_id IS NOT NULL AND sequence IS NOT NULL;

CREATE INDEX idx_wire_inbox_status_time
ON wire_inbox(status, received_at);

CREATE TABLE wire_outbox (
  message_id TEXT PRIMARY KEY,
  session_id TEXT,
  message_type TEXT NOT NULL
    CHECK (message_type IN (
      'session.hello',
      'session.accepted',
      'session.rejected',
      'time.sync.request',
      'time.sync.response',
      'heartbeat',
      'transport.ack',
      'market.tick',
      'market.bar',
      'symbol.metadata',
      'account.snapshot',
      'position.snapshot',
      'order.snapshot',
      'execution.command',
      'command.received',
      'execution.event',
      'reconciliation.request',
      'reconciliation.result',
      'protocol.error'
    )),
  sequence INTEGER CHECK (sequence IS NULL OR sequence > 0),
  command_id TEXT,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  status TEXT NOT NULL
    CHECK (status IN ('PENDING', 'SENT', 'ACKED', 'FAILED', 'CANCELLED')),
  created_at INTEGER NOT NULL,
  sent_at INTEGER,
  acked_at INTEGER,
  last_error TEXT,
  FOREIGN KEY (session_id)
    REFERENCES execution_client_sessions(session_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT,
  FOREIGN KEY (command_id)
    REFERENCES execution_commands(command_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE UNIQUE INDEX idx_wire_outbox_session_sequence
ON wire_outbox(session_id, sequence)
WHERE session_id IS NOT NULL AND sequence IS NOT NULL;

CREATE INDEX idx_wire_outbox_status_time
ON wire_outbox(status, created_at);

CREATE INDEX idx_wire_outbox_command
ON wire_outbox(command_id);

CREATE TABLE command_delivery_attempts (
  attempt_id TEXT PRIMARY KEY,
  command_id TEXT NOT NULL,
  session_id TEXT,
  message_id TEXT,
  status TEXT NOT NULL
    CHECK (status IN (
      'PENDING',
      'SENT',
      'ACKED',
      'BACKPRESSURE',
      'NO_ACTIVE_SESSION',
      'FAILED',
      'TIMEOUT',
      'CANCELLED'
    )),
  attempted_at INTEGER NOT NULL,
  acked_at INTEGER,
  error TEXT,
  FOREIGN KEY (command_id)
    REFERENCES execution_commands(command_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT,
  FOREIGN KEY (session_id)
    REFERENCES execution_client_sessions(session_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT,
  FOREIGN KEY (message_id)
    REFERENCES wire_outbox(message_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE INDEX idx_command_delivery_attempts_command
ON command_delivery_attempts(command_id, attempted_at);

CREATE UNIQUE INDEX idx_command_delivery_attempts_message
ON command_delivery_attempts(message_id)
WHERE message_id IS NOT NULL;

CREATE TABLE market_bars (
  account_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  timestamp INTEGER NOT NULL,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  received_at INTEGER NOT NULL,
  PRIMARY KEY (account_id, symbol, timeframe, timestamp)
);

CREATE TABLE market_snapshots (
  account_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  observed_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (account_id, symbol)
);

CREATE TABLE symbol_metadata_latest (
  account_id TEXT NOT NULL,
  broker_symbol TEXT NOT NULL,
  symbol TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  observed_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (account_id, broker_symbol)
);

CREATE TABLE account_snapshots_latest (
  account_id TEXT PRIMARY KEY,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  observed_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE position_snapshots_latest (
  account_id TEXT NOT NULL,
  position_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  observed_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (account_id, position_id)
);

CREATE TABLE order_snapshots_latest (
  account_id TEXT NOT NULL,
  broker_order_id TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  observed_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (account_id, broker_order_id)
);

CREATE TABLE event_stream_log (
  event_id TEXT PRIMARY KEY,
  topic TEXT NOT NULL
    CHECK (topic IN (
      'market.snapshot',
      'risk.summary',
      'execution.summary',
      'system.event',
      'deadletter.summary'
    )),
  account_id TEXT,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  created_at INTEGER NOT NULL
);

CREATE INDEX idx_event_stream_topic_time
ON event_stream_log(topic, created_at, event_id);

CREATE TABLE outbound_spool (
  spool_id TEXT PRIMARY KEY,
  target TEXT NOT NULL,
  event_id TEXT,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  status TEXT NOT NULL
    CHECK (status IN ('PENDING', 'SENT', 'ACKED', 'FAILED', 'RETRYING', 'DEADLETTER')),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_retry_at INTEGER,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  FOREIGN KEY (event_id)
    REFERENCES event_stream_log(event_id)
    ON UPDATE RESTRICT ON DELETE SET NULL
);

CREATE INDEX idx_outbound_spool_due
ON outbound_spool(status, next_retry_at);

CREATE TRIGGER trg_core_events_no_update
BEFORE UPDATE ON core_events
BEGIN
  SELECT RAISE(ABORT, 'core_events is append-only');
END;

CREATE TRIGGER trg_core_events_no_delete
BEFORE DELETE ON core_events
BEGIN
  SELECT RAISE(ABORT, 'core_events is append-only');
END;

CREATE TRIGGER trg_deadletter_events_no_update
BEFORE UPDATE ON deadletter_events
BEGIN
  SELECT RAISE(ABORT, 'deadletter_events is append-only');
END;

CREATE TRIGGER trg_deadletter_events_no_delete
BEFORE DELETE ON deadletter_events
BEGIN
  SELECT RAISE(ABORT, 'deadletter_events is append-only');
END;

CREATE TRIGGER trg_system_events_no_update
BEFORE UPDATE ON system_events
BEGIN
  SELECT RAISE(ABORT, 'system_events is append-only');
END;

CREATE TRIGGER trg_system_events_no_delete
BEFORE DELETE ON system_events
BEGIN
  SELECT RAISE(ABORT, 'system_events is append-only');
END;

CREATE TRIGGER trg_risk_results_no_update
BEFORE UPDATE ON risk_results
BEGIN
  SELECT RAISE(ABORT, 'risk_results is immutable');
END;

CREATE TRIGGER trg_risk_results_no_delete
BEFORE DELETE ON risk_results
BEGIN
  SELECT RAISE(ABORT, 'risk_results is immutable');
END;

CREATE TRIGGER trg_execution_commands_no_update
BEFORE UPDATE ON execution_commands
BEGIN
  SELECT RAISE(ABORT, 'execution_commands is immutable');
END;

CREATE TRIGGER trg_execution_commands_no_delete
BEFORE DELETE ON execution_commands
BEGIN
  SELECT RAISE(ABORT, 'execution_commands is immutable');
END;

CREATE TRIGGER trg_execution_events_no_update
BEFORE UPDATE ON execution_events
BEGIN
  SELECT RAISE(ABORT, 'execution_events is append-only');
END;

CREATE TRIGGER trg_execution_events_no_delete
BEFORE DELETE ON execution_events
BEGIN
  SELECT RAISE(ABORT, 'execution_events is append-only');
END;

CREATE TRIGGER trg_event_stream_log_no_update
BEFORE UPDATE ON event_stream_log
BEGIN
  SELECT RAISE(ABORT, 'event_stream_log entries are immutable');
END;
