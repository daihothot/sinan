CREATE TABLE inbound_admissions (
  message_id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  client_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  terminal_id TEXT,
  message_type TEXT NOT NULL,
  schema_version TEXT NOT NULL,
  sequence INTEGER NOT NULL CHECK (sequence > 0),
  correlation_id TEXT,
  causation_id TEXT,
  envelope_json TEXT NOT NULL CHECK (json_valid(envelope_json)),
  envelope_hash TEXT NOT NULL
    CHECK (length(envelope_hash) = 64 AND envelope_hash NOT GLOB '*[^0-9a-f]*'),
  received_at INTEGER NOT NULL CHECK (received_at >= 0),
  status TEXT NOT NULL
    CHECK (status IN ('PENDING', 'PROCESSING', 'HANDLED', 'FAILED')),
  lease_owner TEXT,
  lease_expires_at INTEGER,
  revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
  finished_at INTEGER,
  last_error TEXT,
  created_at INTEGER NOT NULL CHECK (created_at >= 0),
  updated_at INTEGER NOT NULL CHECK (updated_at >= created_at),
  UNIQUE (session_id, sequence),
  FOREIGN KEY (session_id)
    REFERENCES execution_client_sessions(session_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT,
  CHECK (
    (status = 'PENDING'
      AND lease_owner IS NULL AND lease_expires_at IS NULL
      AND finished_at IS NULL AND last_error IS NULL)
    OR
    (status = 'PROCESSING'
      AND length(lease_owner) > 0 AND lease_expires_at IS NOT NULL
      AND finished_at IS NULL AND last_error IS NULL)
    OR
    (status = 'HANDLED'
      AND lease_owner IS NULL AND lease_expires_at IS NULL
      AND finished_at IS NOT NULL AND last_error IS NULL)
    OR
    (status = 'FAILED'
      AND lease_owner IS NULL AND lease_expires_at IS NULL
      AND finished_at IS NOT NULL AND length(last_error) > 0)
  )
);

CREATE INDEX idx_inbound_admissions_claim
ON inbound_admissions(status, lease_expires_at, received_at, message_id);

CREATE INDEX idx_inbound_admissions_account_type
ON inbound_admissions(account_id, message_type, received_at, message_id);

CREATE TABLE inbound_rejections (
  rejection_id TEXT PRIMARY KEY,
  message_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  client_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  terminal_id TEXT,
  message_type TEXT NOT NULL,
  schema_version TEXT NOT NULL,
  sequence INTEGER NOT NULL CHECK (sequence > 0),
  correlation_id TEXT,
  causation_id TEXT,
  envelope_json TEXT NOT NULL CHECK (json_valid(envelope_json)),
  envelope_hash TEXT NOT NULL
    CHECK (length(envelope_hash) = 64 AND envelope_hash NOT GLOB '*[^0-9a-f]*'),
  reason TEXT NOT NULL,
  received_at INTEGER NOT NULL CHECK (received_at >= 0),
  created_at INTEGER NOT NULL CHECK (created_at >= 0),
  UNIQUE (message_id, envelope_hash, reason),
  FOREIGN KEY (session_id)
    REFERENCES execution_client_sessions(session_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE INDEX idx_inbound_rejections_message
ON inbound_rejections(message_id, received_at, rejection_id);

CREATE TABLE session_resume_admissions (
  hello_message_id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL UNIQUE,
  client_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  terminal_id TEXT,
  cursor_json TEXT NOT NULL CHECK (json_valid(cursor_json)),
  cursor_hash TEXT NOT NULL
    CHECK (length(cursor_hash) = 64 AND cursor_hash NOT GLOB '*[^0-9a-f]*'),
  received_at INTEGER NOT NULL CHECK (received_at >= 0),
  status TEXT NOT NULL
    CHECK (status IN ('PENDING', 'PROCESSING', 'HANDLED', 'FAILED')),
  lease_owner TEXT,
  lease_expires_at INTEGER,
  revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
  reconciliation_request_id TEXT,
  finished_at INTEGER,
  last_error TEXT,
  created_at INTEGER NOT NULL CHECK (created_at >= 0),
  updated_at INTEGER NOT NULL CHECK (updated_at >= created_at),
  CHECK (
    (status = 'PENDING'
      AND lease_owner IS NULL AND lease_expires_at IS NULL
      AND finished_at IS NULL AND last_error IS NULL)
    OR
    (status = 'PROCESSING'
      AND length(lease_owner) > 0 AND lease_expires_at IS NOT NULL
      AND finished_at IS NULL AND last_error IS NULL)
    OR
    (status = 'HANDLED'
      AND lease_owner IS NULL AND lease_expires_at IS NULL
      AND finished_at IS NOT NULL AND last_error IS NULL)
    OR
    (status = 'FAILED'
      AND lease_owner IS NULL AND lease_expires_at IS NULL
      AND finished_at IS NOT NULL AND length(last_error) > 0)
  )
);

CREATE INDEX idx_session_resume_admissions_claim
ON session_resume_admissions(status, lease_expires_at, received_at, hello_message_id);

ALTER TABLE deadletter_events
ADD COLUMN source TEXT NOT NULL DEFAULT 'legacy';

ALTER TABLE deadletter_events
ADD COLUMN raw_payload_length INTEGER CHECK (
  raw_payload_length IS NULL OR raw_payload_length >= 0
);

ALTER TABLE deadletter_events
ADD COLUMN error_message TEXT NOT NULL DEFAULT '';

CREATE INDEX idx_deadletter_events_reason_time
ON deadletter_events(reason, received_at, deadletter_id);

CREATE INDEX idx_system_events_component_time
ON system_events(component, timestamp, system_event_id);

CREATE TRIGGER trg_inbound_admissions_identity_immutable
BEFORE UPDATE ON inbound_admissions
WHEN OLD.message_id IS NOT NEW.message_id
  OR OLD.session_id IS NOT NEW.session_id
  OR OLD.client_id IS NOT NEW.client_id
  OR OLD.account_id IS NOT NEW.account_id
  OR OLD.terminal_id IS NOT NEW.terminal_id
  OR OLD.message_type IS NOT NEW.message_type
  OR OLD.schema_version IS NOT NEW.schema_version
  OR OLD.sequence IS NOT NEW.sequence
  OR OLD.correlation_id IS NOT NEW.correlation_id
  OR OLD.causation_id IS NOT NEW.causation_id
  OR OLD.envelope_json IS NOT NEW.envelope_json
  OR OLD.envelope_hash IS NOT NEW.envelope_hash
  OR OLD.received_at IS NOT NEW.received_at
  OR OLD.created_at IS NOT NEW.created_at
BEGIN
  SELECT RAISE(ABORT, 'inbound_admissions identity is immutable');
END;

CREATE TRIGGER trg_inbound_admissions_transition_guard
BEFORE UPDATE ON inbound_admissions
WHEN NOT (
  (OLD.status = 'PENDING' AND NEW.status = 'PROCESSING')
  OR
  (OLD.status = 'PROCESSING' AND NEW.status IN ('PROCESSING', 'HANDLED', 'FAILED'))
)
BEGIN
  SELECT RAISE(ABORT, 'invalid inbound_admissions transition');
END;

CREATE TRIGGER trg_inbound_admissions_no_delete
BEFORE DELETE ON inbound_admissions
BEGIN
  SELECT RAISE(ABORT, 'inbound_admissions cannot be deleted');
END;

CREATE TRIGGER trg_inbound_rejections_no_update
BEFORE UPDATE ON inbound_rejections
BEGIN
  SELECT RAISE(ABORT, 'inbound_rejections is append-only');
END;

CREATE TRIGGER trg_inbound_rejections_no_delete
BEFORE DELETE ON inbound_rejections
BEGIN
  SELECT RAISE(ABORT, 'inbound_rejections is append-only');
END;

CREATE TRIGGER trg_session_resume_admissions_identity_immutable
BEFORE UPDATE ON session_resume_admissions
WHEN OLD.hello_message_id IS NOT NEW.hello_message_id
  OR OLD.session_id IS NOT NEW.session_id
  OR OLD.client_id IS NOT NEW.client_id
  OR OLD.account_id IS NOT NEW.account_id
  OR OLD.terminal_id IS NOT NEW.terminal_id
  OR OLD.cursor_json IS NOT NEW.cursor_json
  OR OLD.cursor_hash IS NOT NEW.cursor_hash
  OR OLD.received_at IS NOT NEW.received_at
  OR OLD.created_at IS NOT NEW.created_at
BEGIN
  SELECT RAISE(ABORT, 'session_resume_admissions identity is immutable');
END;

CREATE TRIGGER trg_session_resume_admissions_transition_guard
BEFORE UPDATE ON session_resume_admissions
WHEN NOT (
  (OLD.status = 'PENDING' AND NEW.status = 'PROCESSING')
  OR
  (OLD.status = 'PROCESSING' AND NEW.status IN ('PROCESSING', 'HANDLED', 'FAILED'))
)
BEGIN
  SELECT RAISE(ABORT, 'invalid session_resume_admissions transition');
END;

CREATE TRIGGER trg_session_resume_admissions_no_delete
BEFORE DELETE ON session_resume_admissions
BEGIN
  SELECT RAISE(ABORT, 'session_resume_admissions cannot be deleted');
END;
