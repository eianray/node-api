"""
Simple async job system backed by PostgreSQL.
No Redis, no ARQ. Works fine at Phase 2 scale.

Jobs are created with status=pending, a worker coroutine runs in the background,
and results are stored as bytes in the jobs table. Clients poll GET /jobs/{id}.

Used for: DXF conversion (can be slow on large files).
"""
import asyncio
import uuid
from datetime import datetime, timezone
from typing import Optional, Callable, Any

import psycopg2
from psycopg2.extras import RealDictCursor

from app.config import get_settings

JOB_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id           TEXT        PRIMARY KEY,
    status       TEXT        NOT NULL DEFAULT 'pending',  -- pending|running|done|failed
    operation    TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payer        TEXT,
    tx_hash      TEXT,
    result_bytes BYTEA,
    result_name  TEXT,
    result_mime  TEXT,
    result_meta  JSONB,
    error        TEXT,
    expires_at   TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '24 hours'
);
CREATE INDEX IF NOT EXISTS idx_jobs_status     ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_expires_at ON jobs(expires_at);
"""


def _get_conn():
    return psycopg2.connect(get_settings().database_url, cursor_factory=RealDictCursor)


def extend_db_schema():
    """Called from init_db() to add jobs table."""
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(JOB_SCHEMA)
        conn.commit()


def create_job(operation: str, payer: str, tx_hash: str) -> str:
    """Create a pending job, return job_id."""
    job_id = str(uuid.uuid4())
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO jobs (id, status, operation, payer, tx_hash)
                   VALUES (%s, 'pending', %s, %s, %s)""",
                (job_id, operation, payer, tx_hash)
            )
        conn.commit()
    return job_id


def get_job(job_id: str) -> Optional[dict]:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
            row = cur.fetchone()
    return dict(row) if row else None


def complete_job(job_id: str, result_bytes: bytes, result_name: str,
                 result_mime: str, meta: dict):
    import json
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE jobs SET status='done', result_bytes=%s, result_name=%s,
                   result_mime=%s, result_meta=%s, updated_at=NOW()
                   WHERE id=%s""",
                (result_bytes, result_name, result_mime,
                 json.dumps(meta), job_id)
            )
        conn.commit()


def fail_job(job_id: str, error: str):
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE jobs SET status='failed', error=%s, updated_at=NOW() WHERE id=%s",
                (error, job_id)
            )
        conn.commit()


def mark_running(job_id: str):
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE jobs SET status='running', updated_at=NOW() WHERE id=%s",
                (job_id,)
            )
        conn.commit()
