CREATE TABLE outbound_delivery_work (
  work_id TEXT PRIMARY KEY,
  command_id TEXT,
  request_id TEXT,
  generation INTEGER NOT NULL DEFAULT 1 CHECK (generation > 0),
  message_id TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL
    CHECK (status IN ('PENDING', 'PROCESSING', 'DELIVERED')),
  delivery_attempts INTEGER NOT NULL DEFAULT 0 CHECK (delivery_attempts >= 0),
  next_attempt_at INTEGER,
  lease_owner TEXT,
  lease_expires_at INTEGER,
  revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
  last_outcome TEXT
    CHECK (last_outcome IS NULL OR last_outcome IN (
      'SENT',
      'UNCONFIRMED',
      'REJECTED',
      'DEFINITELY_NOT_WRITTEN',
      'INFRASTRUCTURE_ERROR',
      'SUPERSEDED',
      'EXPIRED',
      'PERMANENT_REJECTION'
    )),
  last_error TEXT,
  completed_at INTEGER,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  CHECK ((command_id IS NOT NULL AND request_id IS NULL)
      OR (command_id IS NULL AND request_id IS NOT NULL)),
  CHECK (
    (status = 'PENDING'
      AND next_attempt_at IS NOT NULL
      AND lease_owner IS NULL
      AND lease_expires_at IS NULL
      AND completed_at IS NULL)
    OR (status = 'PROCESSING'
      AND next_attempt_at IS NULL
      AND lease_owner IS NOT NULL
      AND length(trim(lease_owner)) > 0
      AND lease_expires_at IS NOT NULL
      AND lease_expires_at > updated_at
      AND completed_at IS NULL)
    OR (status = 'DELIVERED'
      AND next_attempt_at IS NULL
      AND lease_owner IS NULL
      AND lease_expires_at IS NULL
      AND completed_at IS NOT NULL)
  ),
  CHECK (next_attempt_at IS NULL OR next_attempt_at >= created_at),
  CHECK (completed_at IS NULL OR completed_at >= created_at),
  CHECK (updated_at >= created_at),
  CHECK (last_error IS NULL OR length(trim(last_error)) > 0),
  FOREIGN KEY (command_id)
    REFERENCES execution_commands(command_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT,
  FOREIGN KEY (request_id)
    REFERENCES reconciliation_runs(request_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE INDEX idx_outbound_delivery_work_due
ON outbound_delivery_work(status, next_attempt_at, lease_expires_at, created_at, work_id);

CREATE INDEX idx_outbound_delivery_work_command
ON outbound_delivery_work(command_id);

CREATE INDEX idx_outbound_delivery_work_request
ON outbound_delivery_work(request_id);

CREATE TRIGGER trg_outbound_delivery_work_cas
BEFORE UPDATE ON outbound_delivery_work
WHEN NEW.revision <> OLD.revision + 1
  OR NEW.updated_at < OLD.updated_at
  OR NEW.delivery_attempts < OLD.delivery_attempts
  OR NEW.delivery_attempts > OLD.delivery_attempts + 1
  OR NEW.generation < OLD.generation
  OR NEW.generation > OLD.generation + 1
BEGIN
  SELECT RAISE(ABORT, 'outbound delivery work violates revision or monotonicity');
END;

CREATE TRIGGER trg_outbound_delivery_work_identity_immutable
BEFORE UPDATE OF work_id, command_id, request_id, created_at
ON outbound_delivery_work
BEGIN
  SELECT RAISE(ABORT, 'outbound delivery work identity is immutable');
END;

CREATE TRIGGER trg_outbound_delivery_work_terminal
BEFORE UPDATE ON outbound_delivery_work
WHEN OLD.status = 'DELIVERED'
BEGIN
  SELECT RAISE(ABORT, 'delivered outbound work is terminal');
END;

CREATE TRIGGER trg_outbound_delivery_work_generation_message
BEFORE UPDATE ON outbound_delivery_work
WHEN (NEW.generation = OLD.generation AND NEW.message_id <> OLD.message_id)
  OR (NEW.generation = OLD.generation + 1 AND NEW.message_id = OLD.message_id)
BEGIN
  SELECT RAISE(ABORT, 'outbound message identity must change exactly with generation');
END;

CREATE TRIGGER trg_outbound_delivery_work_no_delete
BEFORE DELETE ON outbound_delivery_work
BEGIN
  SELECT RAISE(ABORT, 'outbound delivery work cannot be deleted');
END;
