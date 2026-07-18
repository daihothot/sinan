CREATE TABLE circuit_breaker_snapshots (
  scope TEXT NOT NULL CHECK (scope = 'GLOBAL'),
  state_revision INTEGER NOT NULL CHECK (state_revision > 0),
  schema_version TEXT NOT NULL CHECK (length(trim(schema_version)) > 0),
  status TEXT NOT NULL CHECK (status IN ('CLOSED', 'OPEN', 'HALF_OPEN')),
  recovery_epoch INTEGER NOT NULL CHECK (recovery_epoch >= 0),
  updated_at INTEGER NOT NULL CHECK (updated_at >= 0),
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  PRIMARY KEY (scope, state_revision)
);

CREATE TRIGGER trg_circuit_breaker_snapshots_no_update
BEFORE UPDATE ON circuit_breaker_snapshots
BEGIN
  SELECT RAISE(ABORT, 'circuit_breaker_snapshots is append-only');
END;

CREATE TRIGGER trg_circuit_breaker_snapshots_no_delete
BEFORE DELETE ON circuit_breaker_snapshots
BEGIN
  SELECT RAISE(ABORT, 'circuit_breaker_snapshots is append-only');
END;

CREATE TRIGGER trg_execution_plans_definition_no_update
BEFORE UPDATE OF
  plan_id,
  risk_id,
  intent_id,
  account_id,
  strategy_id,
  mode,
  failure_policy,
  payload_json,
  payload_hash,
  created_at
ON execution_plans
BEGIN
  SELECT RAISE(ABORT, 'execution_plans definition is immutable');
END;

CREATE TRIGGER trg_execution_plans_no_delete
BEFORE DELETE ON execution_plans
BEGIN
  SELECT RAISE(ABORT, 'execution_plans definition cannot be deleted');
END;

CREATE TRIGGER trg_execution_legs_definition_no_update
BEFORE UPDATE OF
  leg_id,
  plan_id,
  symbol,
  action,
  payload_json,
  payload_hash
ON execution_legs
BEGIN
  SELECT RAISE(ABORT, 'execution_legs definition is immutable');
END;

CREATE TRIGGER trg_execution_legs_no_delete
BEFORE DELETE ON execution_legs
BEGIN
  SELECT RAISE(ABORT, 'execution_legs definition cannot be deleted');
END;
