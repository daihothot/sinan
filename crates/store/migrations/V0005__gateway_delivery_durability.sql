ALTER TABLE execution_client_sessions
ADD COLUMN revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0);

ALTER TABLE execution_client_sessions
ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0
  CHECK (updated_at = 0 OR (
    updated_at >= connected_at
    AND (last_heartbeat_at IS NULL OR updated_at >= last_heartbeat_at)
    AND (last_time_sync_at IS NULL OR updated_at >= last_time_sync_at)
    AND (disconnected_at IS NULL OR updated_at >= disconnected_at)
  ));

ALTER TABLE execution_client_sessions
ADD COLUMN last_outbound_sequence INTEGER NOT NULL DEFAULT 1
  CHECK (last_outbound_sequence > 0 AND (revision <> 0 OR last_outbound_sequence = 1));

ALTER TABLE execution_client_sessions
ADD COLUMN max_inflight_commands INTEGER NOT NULL DEFAULT 1
  CHECK (max_inflight_commands > 0);

UPDATE execution_client_sessions
SET updated_at = MAX(
  connected_at,
  COALESCE(last_heartbeat_at, connected_at),
  COALESCE(last_time_sync_at, connected_at),
  COALESCE(disconnected_at, connected_at)
);

CREATE INDEX idx_execution_client_sessions_status_updated
ON execution_client_sessions(status, updated_at);

CREATE TRIGGER trg_execution_client_sessions_initial_revision
BEFORE INSERT ON execution_client_sessions
WHEN NEW.revision <> 0 OR NEW.last_outbound_sequence <> 1
BEGIN
  SELECT RAISE(ABORT, 'execution client session must start at revision 0 and outbound sequence 1');
END;

CREATE TRIGGER trg_execution_client_sessions_initial_time
BEFORE INSERT ON execution_client_sessions
WHEN NEW.updated_at <> 0 AND NEW.updated_at < MAX(
  NEW.connected_at,
  COALESCE(NEW.last_heartbeat_at, NEW.connected_at),
  COALESCE(NEW.last_time_sync_at, NEW.connected_at),
  COALESCE(NEW.disconnected_at, NEW.connected_at)
)
BEGIN
  SELECT RAISE(ABORT, 'execution client session updated_at precedes connected_at');
END;

CREATE TRIGGER trg_execution_client_sessions_default_time
AFTER INSERT ON execution_client_sessions
WHEN NEW.updated_at = 0
BEGIN
  UPDATE execution_client_sessions
  SET updated_at = MAX(
    NEW.connected_at,
    COALESCE(NEW.last_heartbeat_at, NEW.connected_at),
    COALESCE(NEW.last_time_sync_at, NEW.connected_at),
    COALESCE(NEW.disconnected_at, NEW.connected_at)
  )
  WHERE session_id = NEW.session_id;
END;

CREATE TRIGGER trg_execution_client_sessions_cas
BEFORE UPDATE ON execution_client_sessions
WHEN NOT (
    OLD.revision = 0
    AND NEW.revision = 0
    AND OLD.updated_at = 0
    AND NEW.updated_at = MAX(
      NEW.connected_at,
      COALESCE(NEW.last_heartbeat_at, NEW.connected_at),
      COALESCE(NEW.last_time_sync_at, NEW.connected_at),
      COALESCE(NEW.disconnected_at, NEW.connected_at)
    )
    AND NEW.last_outbound_sequence = OLD.last_outbound_sequence
  )
  AND (NEW.revision <> OLD.revision + 1
    OR NEW.updated_at < OLD.updated_at
    OR NEW.updated_at < MAX(
      NEW.connected_at,
      COALESCE(NEW.last_heartbeat_at, NEW.connected_at),
      COALESCE(NEW.last_time_sync_at, NEW.connected_at),
      COALESCE(NEW.disconnected_at, NEW.connected_at)
    )
    OR NEW.last_outbound_sequence < OLD.last_outbound_sequence)
BEGIN
  SELECT RAISE(ABORT, 'execution client session update violates revision or time monotonicity');
END;

CREATE TRIGGER trg_execution_client_sessions_identity_immutable
BEFORE UPDATE OF
  session_id,
  client_id,
  account_id,
  terminal_id,
  platform,
  capabilities_json,
  remote_addr,
  connected_at,
  max_inflight_commands
ON execution_client_sessions
BEGIN
  SELECT RAISE(ABORT, 'execution client session identity and negotiated limits are immutable');
END;

CREATE TRIGGER trg_execution_client_sessions_no_delete
BEFORE DELETE ON execution_client_sessions
BEGIN
  SELECT RAISE(ABORT, 'execution client sessions cannot be deleted');
END;

CREATE TABLE command_delivery_attempts_v0005_data AS
SELECT
  attempt_id,
  command_id,
  session_id,
  message_id,
  status,
  attempted_at,
  acked_at,
  error
FROM command_delivery_attempts;

DROP TABLE command_delivery_attempts;

CREATE TABLE wire_outbox_v0005 (
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
  request_id TEXT,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  status TEXT NOT NULL
    CHECK (status IN ('PENDING', 'WRITE_STARTED', 'SENT', 'ACKED', 'FAILED', 'CANCELLED')),
  revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL DEFAULT 0,
  sent_at INTEGER,
  acked_at INTEGER,
  last_error TEXT,
  CHECK (updated_at = 0 OR (
    updated_at >= created_at
    AND (sent_at IS NULL OR updated_at >= sent_at)
    AND (acked_at IS NULL OR updated_at >= acked_at)
  )),
  CHECK (sent_at IS NULL OR sent_at >= created_at),
  CHECK (acked_at IS NULL OR (sent_at IS NOT NULL AND acked_at >= sent_at)),
  CHECK ((status IN ('SENT', 'ACKED') AND sent_at IS NOT NULL)
      OR status NOT IN ('SENT', 'ACKED')),
  CHECK ((status = 'ACKED' AND acked_at IS NOT NULL)
      OR (status <> 'ACKED' AND acked_at IS NULL)),
  CHECK (
    (message_type = 'execution.command'
      AND command_id IS NOT NULL
      AND request_id IS NULL)
    OR (message_type = 'reconciliation.request'
      AND command_id IS NULL
      AND request_id IS NOT NULL)
    OR (message_type NOT IN ('execution.command', 'reconciliation.request')
      AND command_id IS NULL
      AND request_id IS NULL)
  ),
  CHECK (message_type NOT IN ('execution.command', 'reconciliation.request')
      OR (session_id IS NOT NULL AND sequence IS NOT NULL)),
  FOREIGN KEY (session_id)
    REFERENCES execution_client_sessions(session_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT,
  FOREIGN KEY (command_id)
    REFERENCES execution_commands(command_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT,
  FOREIGN KEY (request_id)
    REFERENCES reconciliation_runs(request_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT
);

INSERT INTO wire_outbox_v0005 (
  message_id,
  session_id,
  message_type,
  sequence,
  command_id,
  request_id,
  payload_json,
  payload_hash,
  status,
  revision,
  created_at,
  updated_at,
  sent_at,
  acked_at,
  last_error
)
SELECT
  message_id,
  session_id,
  message_type,
  sequence,
  command_id,
  NULL,
  payload_json,
  payload_hash,
  status,
  0,
  created_at,
  MAX(created_at, COALESCE(sent_at, created_at), COALESCE(acked_at, created_at)),
  CASE
    WHEN status IN ('SENT', 'ACKED') THEN COALESCE(sent_at, acked_at, created_at)
    ELSE sent_at
  END,
  CASE
    WHEN status = 'ACKED' THEN COALESCE(acked_at, sent_at, created_at)
    ELSE NULL
  END,
  last_error
FROM wire_outbox;

DROP TABLE wire_outbox;
ALTER TABLE wire_outbox_v0005 RENAME TO wire_outbox;

UPDATE execution_client_sessions
SET
  last_outbound_sequence = COALESCE((
    SELECT MAX(outbox.sequence)
    FROM wire_outbox AS outbox
    WHERE outbox.session_id = execution_client_sessions.session_id
  ), 1),
  revision = revision + 1,
  updated_at = updated_at
WHERE COALESCE((
  SELECT MAX(outbox.sequence)
  FROM wire_outbox AS outbox
  WHERE outbox.session_id = execution_client_sessions.session_id
), 1) > last_outbound_sequence;

CREATE UNIQUE INDEX idx_wire_outbox_session_sequence
ON wire_outbox(session_id, sequence)
WHERE session_id IS NOT NULL AND sequence IS NOT NULL;

CREATE INDEX idx_wire_outbox_status_time
ON wire_outbox(status, updated_at);

CREATE INDEX idx_wire_outbox_command
ON wire_outbox(command_id);

CREATE INDEX idx_wire_outbox_request
ON wire_outbox(request_id);

CREATE TRIGGER trg_wire_outbox_initial_revision
BEFORE INSERT ON wire_outbox
WHEN NEW.revision <> 0
BEGIN
  SELECT RAISE(ABORT, 'wire outbox row must start at revision 0');
END;

CREATE TRIGGER trg_wire_outbox_initial_time
BEFORE INSERT ON wire_outbox
WHEN NEW.updated_at <> 0 AND NEW.updated_at < MAX(
  NEW.created_at,
  COALESCE(NEW.sent_at, NEW.created_at),
  COALESCE(NEW.acked_at, NEW.created_at)
)
BEGIN
  SELECT RAISE(ABORT, 'wire outbox updated_at precedes created_at');
END;

CREATE TRIGGER trg_wire_outbox_default_time
AFTER INSERT ON wire_outbox
WHEN NEW.updated_at = 0
BEGIN
  UPDATE wire_outbox
  SET updated_at = MAX(
    NEW.created_at,
    COALESCE(NEW.sent_at, NEW.created_at),
    COALESCE(NEW.acked_at, NEW.created_at)
  )
  WHERE message_id = NEW.message_id;
END;

CREATE TRIGGER trg_wire_outbox_cas
BEFORE UPDATE ON wire_outbox
WHEN NOT (
    OLD.revision = 0
    AND NEW.revision = 0
    AND OLD.updated_at = 0
    AND NEW.updated_at = MAX(
      NEW.created_at,
      COALESCE(NEW.sent_at, NEW.created_at),
      COALESCE(NEW.acked_at, NEW.created_at)
    )
  )
  AND (NEW.revision <> OLD.revision + 1
    OR NEW.updated_at < OLD.updated_at
    OR NEW.updated_at < MAX(
      NEW.created_at,
      COALESCE(NEW.sent_at, NEW.created_at),
      COALESCE(NEW.acked_at, NEW.created_at)
    ))
BEGIN
  SELECT RAISE(ABORT, 'wire outbox update violates revision or time monotonicity');
END;

CREATE TRIGGER trg_wire_outbox_definition_immutable
BEFORE UPDATE OF
  message_id,
  session_id,
  message_type,
  sequence,
  command_id,
  request_id,
  payload_json,
  payload_hash,
  created_at
ON wire_outbox
BEGIN
  SELECT RAISE(ABORT, 'wire outbox delivery definition is immutable');
END;

CREATE TRIGGER trg_wire_outbox_no_delete
BEFORE DELETE ON wire_outbox
BEGIN
  SELECT RAISE(ABORT, 'wire outbox rows cannot be deleted');
END;

CREATE TABLE command_delivery_attempts_v0005 (
  attempt_id TEXT PRIMARY KEY,
  command_id TEXT,
  request_id TEXT,
  session_id TEXT,
  message_id TEXT,
  request_payload_json TEXT CHECK (
    request_payload_json IS NULL OR json_valid(request_payload_json)
  ),
  request_payload_hash TEXT CHECK (
    request_payload_hash IS NULL OR (
      length(request_payload_hash) = 64
      AND request_payload_hash NOT GLOB '*[^0-9a-f]*'
    )
  ),
  status TEXT NOT NULL
    CHECK (status IN (
      'PENDING',
      'SENT',
      'ACKED',
      'BACKPRESSURE',
      'NO_ACTIVE_SESSION',
      'FAILED',
      'UNCONFIRMED',
      'CANCELLED'
    )),
  revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
  attempted_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL DEFAULT 0,
  acked_at INTEGER,
  error TEXT,
  CHECK (updated_at = 0 OR (
    updated_at >= attempted_at
    AND (acked_at IS NULL OR updated_at >= acked_at)
  )),
  CHECK ((command_id IS NOT NULL AND request_id IS NULL)
      OR (command_id IS NULL AND request_id IS NOT NULL)),
  CHECK ((request_payload_json IS NULL AND request_payload_hash IS NULL)
      OR (request_payload_json IS NOT NULL AND request_payload_hash IS NOT NULL)),
  CHECK (message_id IS NULL OR session_id IS NOT NULL),
  CHECK (status NOT IN ('PENDING', 'SENT', 'ACKED', 'UNCONFIRMED')
      OR (session_id IS NOT NULL AND message_id IS NOT NULL)),
  CHECK ((status = 'ACKED' AND acked_at IS NOT NULL)
      OR (status <> 'ACKED' AND acked_at IS NULL)),
  CHECK (status <> 'ACKED' OR command_id IS NOT NULL),
  CHECK (status <> 'UNCONFIRMED'
      OR (error IS NOT NULL AND length(trim(error)) > 0)),
  FOREIGN KEY (command_id)
    REFERENCES execution_commands(command_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT,
  FOREIGN KEY (request_id)
    REFERENCES reconciliation_runs(request_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT,
  FOREIGN KEY (session_id)
    REFERENCES execution_client_sessions(session_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT,
  FOREIGN KEY (message_id)
    REFERENCES wire_outbox(message_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TRIGGER trg_command_delivery_attempts_migration_binding
BEFORE INSERT ON command_delivery_attempts_v0005
WHEN NEW.message_id IS NOT NULL AND NOT EXISTS (
  SELECT 1
  FROM wire_outbox AS outbox
  WHERE outbox.message_id = NEW.message_id
    AND outbox.session_id = NEW.session_id
    AND NEW.command_id IS NOT NULL
    AND outbox.command_id = NEW.command_id
    AND outbox.request_id IS NULL
)
BEGIN
  SELECT RAISE(ABORT, 'legacy delivery attempt does not match its wire outbox binding');
END;

INSERT INTO command_delivery_attempts_v0005 (
  attempt_id,
  command_id,
  request_id,
  session_id,
  message_id,
  request_payload_json,
  request_payload_hash,
  status,
  revision,
  attempted_at,
  updated_at,
  acked_at,
  error
)
SELECT
  attempt_id,
  command_id,
  NULL,
  session_id,
  message_id,
  NULL,
  NULL,
  CASE status WHEN 'TIMEOUT' THEN 'UNCONFIRMED' ELSE status END,
  0,
  attempted_at,
  MAX(attempted_at, COALESCE(acked_at, attempted_at)),
  CASE
    WHEN status = 'ACKED' THEN COALESCE(acked_at, attempted_at)
    ELSE NULL
  END,
  CASE
    WHEN status = 'TIMEOUT' THEN COALESCE(NULLIF(trim(error), ''), 'COMMAND_DELIVERY_TIMEOUT')
    ELSE error
  END
FROM command_delivery_attempts_v0005_data;

DROP TRIGGER trg_command_delivery_attempts_migration_binding;
DROP TABLE command_delivery_attempts_v0005_data;
ALTER TABLE command_delivery_attempts_v0005 RENAME TO command_delivery_attempts;

CREATE INDEX idx_command_delivery_attempts_command
ON command_delivery_attempts(command_id, attempted_at);

CREATE INDEX idx_command_delivery_attempts_request
ON command_delivery_attempts(request_id, attempted_at);

CREATE INDEX idx_command_delivery_attempts_session_status
ON command_delivery_attempts(session_id, status, updated_at);

CREATE UNIQUE INDEX idx_command_delivery_attempts_message
ON command_delivery_attempts(message_id)
WHERE message_id IS NOT NULL;

CREATE TRIGGER trg_command_delivery_attempts_initial_revision
BEFORE INSERT ON command_delivery_attempts
WHEN NEW.revision <> 0
BEGIN
  SELECT RAISE(ABORT, 'command delivery attempt must start at revision 0');
END;

CREATE TRIGGER trg_command_delivery_attempts_initial_time
BEFORE INSERT ON command_delivery_attempts
WHEN NEW.updated_at <> 0 AND NEW.updated_at < MAX(
  NEW.attempted_at,
  COALESCE(NEW.acked_at, NEW.attempted_at)
)
BEGIN
  SELECT RAISE(ABORT, 'command delivery attempt updated_at precedes attempted_at');
END;

CREATE TRIGGER trg_command_delivery_attempts_default_time
AFTER INSERT ON command_delivery_attempts
WHEN NEW.updated_at = 0
BEGIN
  UPDATE command_delivery_attempts
  SET updated_at = MAX(
    NEW.attempted_at,
    COALESCE(NEW.acked_at, NEW.attempted_at)
  )
  WHERE attempt_id = NEW.attempt_id;
END;

CREATE TRIGGER trg_command_delivery_attempts_outbox_binding_insert
BEFORE INSERT ON command_delivery_attempts
WHEN NEW.message_id IS NOT NULL AND NOT EXISTS (
  SELECT 1
  FROM wire_outbox AS outbox
  WHERE outbox.message_id = NEW.message_id
    AND outbox.session_id = NEW.session_id
    AND ((NEW.command_id IS NOT NULL
          AND outbox.command_id = NEW.command_id
          AND outbox.request_id IS NULL)
      OR (NEW.request_id IS NOT NULL
          AND outbox.request_id = NEW.request_id
          AND outbox.command_id IS NULL))
)
BEGIN
  SELECT RAISE(ABORT, 'delivery attempt does not match its wire outbox binding');
END;

CREATE TRIGGER trg_command_delivery_attempts_cas
BEFORE UPDATE ON command_delivery_attempts
WHEN NOT (
    OLD.revision = 0
    AND NEW.revision = 0
    AND OLD.updated_at = 0
    AND NEW.updated_at = MAX(
      NEW.attempted_at,
      COALESCE(NEW.acked_at, NEW.attempted_at)
    )
  )
  AND (NEW.revision <> OLD.revision + 1
    OR NEW.updated_at < OLD.updated_at
    OR NEW.updated_at < MAX(
      NEW.attempted_at,
      COALESCE(NEW.acked_at, NEW.attempted_at)
    ))
BEGIN
  SELECT RAISE(ABORT, 'command delivery attempt update violates revision or time monotonicity');
END;

CREATE TRIGGER trg_command_delivery_attempts_definition_immutable
BEFORE UPDATE OF
  attempt_id,
  command_id,
  request_id,
  session_id,
  message_id,
  request_payload_json,
  request_payload_hash,
  attempted_at
ON command_delivery_attempts
BEGIN
  SELECT RAISE(ABORT, 'command delivery attempt definition is immutable');
END;

CREATE TRIGGER trg_command_delivery_attempts_no_delete
BEFORE DELETE ON command_delivery_attempts
BEGIN
  SELECT RAISE(ABORT, 'command delivery attempts cannot be deleted');
END;
