DROP TRIGGER trg_event_stream_log_no_update;

CREATE TABLE event_stream_log_v6 (
  stream_sequence INTEGER PRIMARY KEY AUTOINCREMENT
    CHECK (stream_sequence > 0),
  event_id TEXT NOT NULL UNIQUE,
  topic TEXT NOT NULL
    CHECK (topic IN (
      'market.snapshot',
      'risk.summary',
      'execution.summary',
      'system.event',
      'deadletter.summary'
    )),
  account_id TEXT
    CHECK (
      (account_id IS NOT NULL AND length(account_id) > 0)
      OR (account_id IS NULL AND topic IN ('system.event', 'deadletter.summary'))
    ),
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
  created_at INTEGER NOT NULL
);

INSERT INTO event_stream_log_v6 (
  stream_sequence,
  event_id,
  topic,
  account_id,
  event_type,
  payload_json,
  payload_hash,
  created_at
)
SELECT
  rowid,
  event_id,
  topic,
  account_id,
  event_type,
  payload_json,
  payload_hash,
  created_at
FROM event_stream_log
ORDER BY rowid;

CREATE TABLE outbound_spool_v6 (
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
    REFERENCES event_stream_log_v6(event_id)
    ON UPDATE RESTRICT ON DELETE SET NULL
);

INSERT INTO outbound_spool_v6 (
  spool_id,
  target,
  event_id,
  payload_json,
  payload_hash,
  status,
  attempts,
  next_retry_at,
  created_at,
  updated_at
)
SELECT
  spool_id,
  target,
  event_id,
  payload_json,
  payload_hash,
  status,
  attempts,
  next_retry_at,
  created_at,
  updated_at
FROM outbound_spool;

DROP TABLE outbound_spool;
DROP TABLE event_stream_log;
ALTER TABLE event_stream_log_v6 RENAME TO event_stream_log;
ALTER TABLE outbound_spool_v6 RENAME TO outbound_spool;

CREATE INDEX idx_event_stream_topic_time
ON event_stream_log(topic, created_at, stream_sequence);

CREATE INDEX idx_event_stream_topic_sequence
ON event_stream_log(topic, stream_sequence);

CREATE INDEX idx_event_stream_account_sequence
ON event_stream_log(account_id, stream_sequence);

CREATE INDEX idx_event_stream_created_sequence
ON event_stream_log(created_at, stream_sequence);

CREATE INDEX idx_outbound_spool_due
ON outbound_spool(status, next_retry_at);

CREATE TRIGGER trg_event_stream_log_no_update
BEFORE UPDATE ON event_stream_log
BEGIN
  SELECT RAISE(ABORT, 'event_stream_log entries are immutable');
END;
