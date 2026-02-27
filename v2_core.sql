-- v2_core.sql (DuckDB)

CREATE TABLE IF NOT EXISTS entity (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  display_name TEXT NOT NULL,
  attributes JSON
);

CREATE TABLE IF NOT EXISTS event (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  occurred_at TIMESTAMP,
  occurred_to TIMESTAMP,
  amount_brl DOUBLE,
  title TEXT,
  attributes JSON
);

CREATE TABLE IF NOT EXISTS edge (
  id TEXT PRIMARY KEY,
  src_entity_id TEXT NOT NULL,
  dst_entity_id TEXT,
  event_id TEXT,
  type TEXT NOT NULL,
  weight DOUBLE DEFAULT 1.0,
  attributes JSON
);

CREATE TABLE IF NOT EXISTS evidence (
  id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  source_kind TEXT NOT NULL,
  captured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  uri TEXT,
  content_sha256 TEXT,
  payload_ref TEXT,     -- path local / key (opcional)
  excerpt JSON,
  pii_redacted BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS insight (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  severity TEXT NOT NULL,
  confidence INTEGER NOT NULL,
  exposure_brl DOUBLE,
  title TEXT NOT NULL,
  description_md TEXT NOT NULL,
  pattern TEXT,
  sources JSON,
  tags JSON,
  sample_n INTEGER DEFAULT 0,
  unit_total DOUBLE DEFAULT 0.0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS insight_link (
  id TEXT PRIMARY KEY,
  insight_id TEXT NOT NULL,
  entity_id TEXT,
  event_id TEXT
);

CREATE TABLE IF NOT EXISTS evidence_link (
  id TEXT PRIMARY KEY,
  evidence_id TEXT NOT NULL,
  insight_id TEXT,
  entity_id TEXT,
  event_id TEXT,
  role TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
