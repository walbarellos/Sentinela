from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import requests


ROOT = Path(__file__).resolve().parents[2]
SOURCE_CACHE_DIR = ROOT / "data" / "ops_source_cache"

PIPELINE_RUN_DDL = """
CREATE TABLE IF NOT EXISTS ops_pipeline_run (
    run_id VARCHAR PRIMARY KEY,
    pipeline VARCHAR NOT NULL,
    trigger_mode VARCHAR,
    actor VARCHAR,
    status VARCHAR NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    duration_ms BIGINT,
    rows_written BIGINT DEFAULT 0,
    artifacts_written BIGINT DEFAULT 0,
    details_json JSON,
    error_text VARCHAR
)
"""

SOURCE_CACHE_DDL = """
CREATE TABLE IF NOT EXISTS ops_source_cache (
    cache_entry_id VARCHAR PRIMARY KEY,
    cache_key VARCHAR NOT NULL,
    source_name VARCHAR NOT NULL,
    resource_url VARCHAR NOT NULL,
    method VARCHAR,
    status_code INTEGER,
    etag VARCHAR,
    last_modified VARCHAR,
    ttl_seconds INTEGER,
    fetched_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP,
    response_sha256 VARCHAR,
    body_path VARCHAR,
    headers_json JSON,
    meta_json JSON
)
"""

PIPELINE_RUN_VIEW = """
CREATE OR REPLACE VIEW v_ops_pipeline_run_latest AS
SELECT *
FROM ops_pipeline_run
ORDER BY started_at DESC, pipeline, run_id
"""

SOURCE_CACHE_VIEW = """
CREATE OR REPLACE VIEW v_ops_source_cache_latest AS
SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY cache_key
            ORDER BY fetched_at DESC, cache_entry_id DESC
        ) AS rn
    FROM ops_source_cache
)
WHERE rn = 1
"""


TRACKED_SOURCES: list[dict[str, Any]] = [
    {
        "cache_key": "rb_transparencia_home",
        "source_name": "Portal Transparencia Rio Branco",
        "resource_url": "https://transparencia.riobranco.ac.gov.br/",
        "ttl_seconds": 21600,
    },
    {
        "cache_key": "ac_transparencia_home",
        "source_name": "Portal Transparencia Acre",
        "resource_url": "https://transparencia.ac.gov.br/",
        "ttl_seconds": 21600,
    },
    {
        "cache_key": "pncp_api_orgaos",
        "source_name": "PNCP Dados Abertos",
        "resource_url": "https://www.gov.br/pncp/pt-br/acesso-a-informacao/dados-abertos",
        "ttl_seconds": 21600,
    },
    {
        "cache_key": "cgu_ceis_download",
        "source_name": "CGU CEIS Download",
        "resource_url": "https://portaldatransparencia.gov.br/download-de-dados/ceis",
        "ttl_seconds": 43200,
    },
    {
        "cache_key": "cgu_cnep_download",
        "source_name": "CGU CNEP Download",
        "resource_url": "https://portaldatransparencia.gov.br/download-de-dados/cnep",
        "ttl_seconds": 43200,
    },
    {
        "cache_key": "cnes_home",
        "source_name": "CNES Datasus",
        "resource_url": "https://cnes.datasus.gov.br/",
        "ttl_seconds": 43200,
    },
    {
        "cache_key": "doe_ac_home",
        "source_name": "Diario Oficial Acre",
        "resource_url": "https://diario.ac.gov.br/",
        "ttl_seconds": 43200,
    },
]


def utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def ensure_ops_runtime(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(PIPELINE_RUN_DDL)
    con.execute(SOURCE_CACHE_DDL)
    con.execute(PIPELINE_RUN_VIEW)
    con.execute(SOURCE_CACHE_VIEW)


def begin_pipeline_run(
    con: duckdb.DuckDBPyConnection,
    pipeline: str,
    *,
    trigger_mode: str = "manual",
    actor: str = "system",
    details: dict[str, Any] | None = None,
) -> str:
    ensure_ops_runtime(con)
    run_id = f"{pipeline}:{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}:{uuid.uuid4().hex[:8]}"
    con.execute(
        """
        INSERT INTO ops_pipeline_run (
            run_id, pipeline, trigger_mode, actor, status, started_at, details_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            pipeline,
            trigger_mode,
            actor,
            "running",
            utcnow_naive(),
            json.dumps(details or {}, ensure_ascii=False),
        ],
    )
    return run_id


def finish_pipeline_run(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    *,
    status: str,
    rows_written: int = 0,
    artifacts_written: int = 0,
    details: dict[str, Any] | None = None,
    error_text: str | None = None,
) -> None:
    finished_at = utcnow_naive()
    started_at = con.execute(
        "SELECT started_at FROM ops_pipeline_run WHERE run_id = ?",
        [run_id],
    ).fetchone()
    duration_ms = None
    if started_at and started_at[0]:
        duration_ms = int((finished_at - started_at[0]).total_seconds() * 1000)
    con.execute(
        """
        UPDATE ops_pipeline_run
        SET status = ?,
            finished_at = ?,
            duration_ms = ?,
            rows_written = ?,
            artifacts_written = ?,
            details_json = ?,
            error_text = ?
        WHERE run_id = ?
        """,
        [
            status,
            finished_at,
            duration_ms,
            rows_written,
            artifacts_written,
            json.dumps(details or {}, ensure_ascii=False),
            error_text,
            run_id,
        ],
    )


def tracked_sources() -> list[dict[str, Any]]:
    return [dict(item) for item in TRACKED_SOURCES]


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _safe_filename(cache_key: str, suffix: str) -> Path:
    SOURCE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    clean = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in cache_key)
    return SOURCE_CACHE_DIR / f"{clean}{suffix}"


def upsert_source_cache(
    con: duckdb.DuckDBPyConnection,
    *,
    cache_key: str,
    source_name: str,
    resource_url: str,
    method: str,
    status_code: int | None,
    ttl_seconds: int,
    etag: str | None,
    last_modified: str | None,
    response_sha256: str | None,
    body_path: str | None,
    headers: dict[str, Any] | None,
    meta: dict[str, Any] | None,
) -> str:
    ensure_ops_runtime(con)
    fetched_at = utcnow_naive()
    expires_at = fetched_at + timedelta(seconds=ttl_seconds)
    cache_entry_id = f"{cache_key}:{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}:{uuid.uuid4().hex[:8]}"
    con.execute(
        """
        INSERT INTO ops_source_cache (
            cache_entry_id, cache_key, source_name, resource_url, method,
            status_code, etag, last_modified, ttl_seconds, fetched_at, expires_at,
            response_sha256, body_path, headers_json, meta_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            cache_entry_id,
            cache_key,
            source_name,
            resource_url,
            method,
            status_code,
            etag,
            last_modified,
            ttl_seconds,
            fetched_at,
            expires_at,
            response_sha256,
            body_path,
            json.dumps(headers or {}, ensure_ascii=False),
            json.dumps(meta or {}, ensure_ascii=False),
        ],
    )
    return cache_entry_id


def probe_source(
    source: dict[str, Any],
    *,
    timeout: int = 20,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    s = session or requests.Session()
    url = source["resource_url"]
    method_used = "HEAD"
    body_path = None
    response_sha256 = None
    body_bytes = b""
    error_text = None
    response = None

    try:
        response = s.head(url, allow_redirects=True, timeout=timeout)
        if response.status_code >= 400 or response.status_code == 405:
            method_used = "GET"
            response = s.get(url, allow_redirects=True, timeout=timeout)
            content_type = response.headers.get("content-type", "").lower()
            if "text" in content_type or "json" in content_type or "html" in content_type:
                body_bytes = response.content
                response_sha256 = sha256_bytes(body_bytes)
                suffix = ".json" if "json" in content_type else ".html" if "html" in content_type else ".txt"
                target = _safe_filename(source["cache_key"], suffix)
                target.write_bytes(body_bytes)
                body_path = str(target.relative_to(ROOT))
    except Exception as exc:
        error_text = str(exc)

    status_code = getattr(response, "status_code", None)
    headers = dict(getattr(response, "headers", {}) or {})
    if response is not None and method_used == "HEAD":
        etag = headers.get("ETag") or headers.get("etag")
        last_modified = headers.get("Last-Modified") or headers.get("last-modified")
    else:
        etag = headers.get("ETag") or headers.get("etag")
        last_modified = headers.get("Last-Modified") or headers.get("last-modified")

    return {
        "cache_key": source["cache_key"],
        "source_name": source["source_name"],
        "resource_url": url,
        "method": method_used,
        "status_code": status_code,
        "ttl_seconds": int(source["ttl_seconds"]),
        "etag": etag,
        "last_modified": last_modified,
        "response_sha256": response_sha256,
        "body_path": body_path,
        "headers": headers,
        "meta": {
            "ok": error_text is None and status_code is not None and status_code < 500,
            "error_text": error_text,
        },
    }


def refresh_source_cache(
    con: duckdb.DuckDBPyConnection,
    *,
    timeout: int = 20,
) -> dict[str, int]:
    ensure_ops_runtime(con)
    session = requests.Session()
    inserted = 0
    ok_count = 0
    for source in tracked_sources():
        result = probe_source(source, timeout=timeout, session=session)
        upsert_source_cache(
            con,
            cache_key=result["cache_key"],
            source_name=result["source_name"],
            resource_url=result["resource_url"],
            method=result["method"],
            status_code=result["status_code"],
            ttl_seconds=result["ttl_seconds"],
            etag=result["etag"],
            last_modified=result["last_modified"],
            response_sha256=result["response_sha256"],
            body_path=result["body_path"],
            headers=result["headers"],
            meta=result["meta"],
        )
        inserted += 1
        if result["meta"].get("ok"):
            ok_count += 1
    return {"sources": inserted, "ok": ok_count}
