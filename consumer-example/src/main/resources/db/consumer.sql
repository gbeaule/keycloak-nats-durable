-- Apply with the application migration identity; runtime needs DML, not schema ownership.
CREATE TABLE IF NOT EXISTS knd_inbox (
  consumer_name VARCHAR(128) NOT NULL,
  event_id UUID NOT NULL,
  payload_hash BYTEA NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (consumer_name, event_id)
);

CREATE TABLE IF NOT EXISTS knd_effects (
  consumer_name VARCHAR(128) NOT NULL,
  event_id UUID NOT NULL,
  event_type TEXT NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS knd_quarantine (
  consumer_name VARCHAR(128) NOT NULL,
  stream_name VARCHAR(128) NOT NULL,
  stream_sequence BIGINT NOT NULL,
  subject TEXT NOT NULL,
  event_id UUID,
  payload_hash BYTEA NOT NULL,
  payload BYTEA,
  headers_json JSONB,
  reason VARCHAR(64) NOT NULL,
  disposition VARCHAR(16) NOT NULL CHECK (disposition IN ('quarantine', 'drop')),
  delivery_count BIGINT NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  replayed_at TIMESTAMPTZ,
  PRIMARY KEY (consumer_name, stream_name, stream_sequence),
  CHECK ((disposition = 'quarantine') = (payload IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS idx_knd_quarantine_pending
  ON knd_quarantine (consumer_name, recorded_at)
  WHERE disposition = 'quarantine' AND replayed_at IS NULL;
