CREATE TABLE reconciliation_runs (
  request_id TEXT PRIMARY KEY,
  request_event_id TEXT NOT NULL UNIQUE,
  account_id TEXT NOT NULL,
  terminal_id TEXT,
  client_id TEXT,
  reason TEXT NOT NULL
    CHECK (reason IN (
      'DELIVERY_UNCONFIRMED',
      'CONNECTION_RESTORED',
      'MANUAL_REQUEST',
      'STATE_STORE_RESTORED'
    )),
  scope TEXT NOT NULL CHECK (scope IN ('ACCOUNT', 'TARGETED')),
  command_ids_json TEXT CHECK (command_ids_json IS NULL OR json_valid(command_ids_json)),
  command_ids_hash TEXT
    CHECK (command_ids_hash IS NULL OR (
      length(command_ids_hash) = 64 AND command_ids_hash NOT GLOB '*[^0-9a-f]*'
    )),
  since_server_time INTEGER,
  requested_at INTEGER NOT NULL,
  status TEXT NOT NULL
    CHECK (status IN (
      'REQUESTED',
      'PENDING_EVIDENCE',
      'COMPLETED',
      'MANUAL_RECONCILIATION_REQUIRED'
    )),
  request_payload_json TEXT NOT NULL CHECK (json_valid(request_payload_json)),
  request_payload_hash TEXT NOT NULL
    CHECK (length(request_payload_hash) = 64 AND request_payload_hash NOT GLOB '*[^0-9a-f]*'),
  result_event_id TEXT UNIQUE,
  result_observed_at INTEGER,
  result_payload_json TEXT CHECK (result_payload_json IS NULL OR json_valid(result_payload_json)),
  result_payload_hash TEXT
    CHECK (result_payload_hash IS NULL OR (
      length(result_payload_hash) = 64 AND result_payload_hash NOT GLOB '*[^0-9a-f]*'
    )),
  result_evaluation_json TEXT
    CHECK (result_evaluation_json IS NULL OR json_valid(result_evaluation_json)),
  result_evaluation_hash TEXT
    CHECK (result_evaluation_hash IS NULL OR (
      length(result_evaluation_hash) = 64 AND result_evaluation_hash NOT GLOB '*[^0-9a-f]*'
    )),
  completeness_json TEXT CHECK (completeness_json IS NULL OR json_valid(completeness_json)),
  completeness_hash TEXT
    CHECK (completeness_hash IS NULL OR (
      length(completeness_hash) = 64 AND completeness_hash NOT GLOB '*[^0-9a-f]*'
    )),
  symbol_metadata_complete INTEGER
    CHECK (symbol_metadata_complete IS NULL OR symbol_metadata_complete IN (0, 1)),
  command_scope_complete INTEGER
    CHECK (command_scope_complete IS NULL OR command_scope_complete IN (0, 1)),
  manual_evidence_json TEXT
    CHECK (manual_evidence_json IS NULL OR json_valid(manual_evidence_json)),
  manual_evidence_hash TEXT
    CHECK (manual_evidence_hash IS NULL OR (
      length(manual_evidence_hash) = 64 AND manual_evidence_hash NOT GLOB '*[^0-9a-f]*'
    )),
  manual_evaluation_json TEXT
    CHECK (manual_evaluation_json IS NULL OR json_valid(manual_evaluation_json)),
  manual_evaluation_hash TEXT
    CHECK (manual_evaluation_hash IS NULL OR (
      length(manual_evaluation_hash) = 64 AND manual_evaluation_hash NOT GLOB '*[^0-9a-f]*'
    )),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  CHECK ((scope = 'ACCOUNT' AND command_ids_json IS NULL AND command_ids_hash IS NULL)
      OR (scope = 'TARGETED' AND command_ids_json IS NOT NULL AND command_ids_hash IS NOT NULL)),
  CHECK ((result_event_id IS NULL
          AND result_observed_at IS NULL
          AND result_payload_json IS NULL
          AND result_payload_hash IS NULL
          AND result_evaluation_json IS NULL
          AND result_evaluation_hash IS NULL
          AND completeness_json IS NULL
          AND completeness_hash IS NULL
          AND symbol_metadata_complete IS NULL
          AND command_scope_complete IS NULL)
      OR (result_event_id IS NOT NULL
          AND result_observed_at IS NOT NULL
          AND result_payload_json IS NOT NULL
          AND result_payload_hash IS NOT NULL
          AND result_evaluation_json IS NOT NULL
          AND result_evaluation_hash IS NOT NULL
          AND completeness_json IS NOT NULL
          AND completeness_hash IS NOT NULL
          AND symbol_metadata_complete IS NOT NULL
          AND command_scope_complete IS NOT NULL)),
  CHECK ((manual_evidence_json IS NULL
          AND manual_evidence_hash IS NULL
          AND manual_evaluation_json IS NULL
          AND manual_evaluation_hash IS NULL)
      OR (manual_evidence_json IS NOT NULL
          AND manual_evidence_hash IS NOT NULL
          AND manual_evaluation_json IS NOT NULL
          AND manual_evaluation_hash IS NOT NULL)),
  FOREIGN KEY (request_event_id)
    REFERENCES core_events(event_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT,
  FOREIGN KEY (result_event_id)
    REFERENCES core_events(event_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE INDEX idx_reconciliation_runs_account_status_time
ON reconciliation_runs(account_id, status, requested_at);

CREATE TABLE account_reconciliation_checkpoints (
  account_id TEXT PRIMARY KEY,
  source_request_id TEXT NOT NULL,
  result_observed_at INTEGER NOT NULL,
  account_refreshed_at INTEGER,
  positions_observed_at INTEGER NOT NULL,
  positions_set_hash TEXT NOT NULL
    CHECK (length(positions_set_hash) = 64 AND positions_set_hash NOT GLOB '*[^0-9a-f]*'),
  orders_observed_at INTEGER NOT NULL,
  orders_set_hash TEXT NOT NULL
    CHECK (length(orders_set_hash) = 64 AND orders_set_hash NOT GLOB '*[^0-9a-f]*'),
  symbol_metadata_refreshed_at INTEGER,
  pending_commands_reconciled_at INTEGER,
  updated_at INTEGER NOT NULL,
  FOREIGN KEY (source_request_id)
    REFERENCES reconciliation_runs(request_id)
    ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE reconciliation_position_set_members (
  account_id TEXT NOT NULL,
  set_observed_at INTEGER NOT NULL,
  position_id TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  PRIMARY KEY (account_id, position_id)
);

CREATE TABLE reconciliation_order_set_members (
  account_id TEXT NOT NULL,
  set_observed_at INTEGER NOT NULL,
  broker_order_id TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  PRIMARY KEY (account_id, broker_order_id)
);

CREATE TRIGGER trg_reconciliation_runs_definition_no_update
BEFORE UPDATE OF
  request_id,
  request_event_id,
  account_id,
  terminal_id,
  client_id,
  reason,
  scope,
  command_ids_json,
  command_ids_hash,
  since_server_time,
  requested_at,
  request_payload_json,
  request_payload_hash,
  created_at
ON reconciliation_runs
BEGIN
  SELECT RAISE(ABORT, 'reconciliation request definition is immutable');
END;

CREATE TRIGGER trg_reconciliation_runs_no_delete
BEFORE DELETE ON reconciliation_runs
BEGIN
  SELECT RAISE(ABORT, 'reconciliation runs cannot be deleted');
END;
