ALTER TABLE trade_intents
ADD COLUMN decision_timestamp INTEGER
  CHECK (decision_timestamp IS NULL OR (
    decision_timestamp >= 0 AND decision_timestamp <= requested_at
  ));

CREATE TRIGGER trg_trade_intents_decision_timestamp_required
BEFORE INSERT ON trade_intents
WHEN NEW.decision_timestamp IS NULL
BEGIN
  SELECT RAISE(ABORT, 'trade_intents.decision_timestamp is required');
END;

CREATE TABLE risk_capacity_snapshots (
  account_id TEXT NOT NULL,
  strategy_id TEXT NOT NULL,
  observed_at INTEGER NOT NULL CHECK (observed_at >= 0),
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  recorded_at INTEGER NOT NULL CHECK (recorded_at >= observed_at),
  PRIMARY KEY (account_id, strategy_id, observed_at)
);

CREATE INDEX idx_risk_capacity_snapshots_account_strategy_time
ON risk_capacity_snapshots(account_id, strategy_id, observed_at DESC);

CREATE TABLE risk_capacity_snapshots_latest (
  account_id TEXT NOT NULL,
  strategy_id TEXT NOT NULL,
  observed_at INTEGER NOT NULL CHECK (observed_at >= 0),
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  recorded_at INTEGER NOT NULL CHECK (recorded_at >= observed_at),
  PRIMARY KEY (account_id, strategy_id),
  FOREIGN KEY (account_id, strategy_id, observed_at)
    REFERENCES risk_capacity_snapshots(account_id, strategy_id, observed_at)
    ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TRIGGER trg_risk_capacity_snapshots_no_update
BEFORE UPDATE ON risk_capacity_snapshots
BEGIN
  SELECT RAISE(ABORT, 'risk_capacity_snapshots is append-only');
END;

CREATE TRIGGER trg_risk_capacity_snapshots_no_delete
BEFORE DELETE ON risk_capacity_snapshots
BEGIN
  SELECT RAISE(ABORT, 'risk_capacity_snapshots is append-only');
END;
