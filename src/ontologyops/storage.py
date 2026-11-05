"""Runnable local SQLite analogue of the Databricks Bronze/Silver/Gold flow."""
import json
import sqlite3
from pathlib import Path


SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_payment_events (
    event_id TEXT PRIMARY KEY, transaction_id TEXT NOT NULL,
    merchant_id TEXT NOT NULL, merchant_tier TEXT NOT NULL,
    acquirer_id TEXT NOT NULL, corridor TEXT NOT NULL,
    currency TEXT NOT NULL, amount_minor INTEGER NOT NULL,
    status TEXT NOT NULL, event_sequence INTEGER NOT NULL,
    updated_at TEXT NOT NULL, ingested_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_events_tx ON raw_payment_events(transaction_id, updated_at);
CREATE TABLE IF NOT EXISTS silver_transactions (
    transaction_id TEXT PRIMARY KEY, event_id TEXT NOT NULL,
    merchant_id TEXT NOT NULL, merchant_tier TEXT NOT NULL,
    acquirer_id TEXT NOT NULL, corridor TEXT NOT NULL,
    currency TEXT NOT NULL, amount_minor INTEGER NOT NULL,
    status TEXT NOT NULL, event_sequence INTEGER NOT NULL,
    updated_at TEXT NOT NULL, ingested_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS gold_payment_outcomes (
    transaction_id TEXT PRIMARY KEY, business_date TEXT NOT NULL,
    merchant_id TEXT NOT NULL, merchant_tier TEXT NOT NULL,
    acquirer_id TEXT NOT NULL, corridor TEXT NOT NULL,
    currency TEXT NOT NULL, amount_minor INTEGER NOT NULL,
    status TEXT NOT NULL
);
"""
COLUMNS = ["event_id", "transaction_id", "merchant_id", "merchant_tier", "acquirer_id", "corridor", "currency", "amount_minor", "status", "event_sequence", "updated_at", "ingested_at"]


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    return con


def ingest(con: sqlite3.Connection, event_file: Path) -> dict:
    before = con.execute("SELECT COUNT(*) FROM raw_payment_events").fetchone()[0]
    with event_file.open(encoding="utf-8") as f, con:
        for line_number, line in enumerate(f, 1):
            if not line.strip():
                continue
            event = json.loads(line)
            if not all(k in event for k in COLUMNS):
                raise ValueError(f"Missing event fields in line {line_number}")
            if event["amount_minor"] <= 0:
                raise ValueError(f"Invalid amount in line {line_number}")
            if event["currency"] not in {"MXN", "USD"}:
                raise ValueError(f"Invalid currency in line {line_number}")
            con.execute(f"INSERT OR IGNORE INTO raw_payment_events ({','.join(COLUMNS)}) VALUES ({','.join('?' for _ in COLUMNS)})", [event[c] for c in COLUMNS])
    after = con.execute("SELECT COUNT(*) FROM raw_payment_events").fetchone()[0]
    return {"new_events": after - before, "unique_events": after}


def build(con: sqlite3.Connection) -> dict:
    # Snapshot rebuild is atomic and deterministic; a rerun does not double count transactions.
    # In production use a Delta MERGE / Lakeflow AUTO CDC with explicit ordering and constraints.
    with con:
        con.execute("DELETE FROM silver_transactions")
        con.execute("""
            INSERT INTO silver_transactions
            SELECT transaction_id, event_id, merchant_id, merchant_tier, acquirer_id,
                   corridor, currency, amount_minor, status, event_sequence, updated_at, ingested_at
            FROM (
              SELECT e.*, ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY updated_at DESC, event_sequence DESC, ingested_at DESC, event_id DESC
              ) AS rn
              FROM raw_payment_events e
            ) WHERE rn = 1
        """)
        con.execute("DELETE FROM gold_payment_outcomes")
        con.execute("""
          INSERT INTO gold_payment_outcomes
          SELECT transaction_id, SUBSTR(updated_at, 1, 10) AS business_date,
                 merchant_id, merchant_tier, acquirer_id, corridor, currency, amount_minor, status
          FROM silver_transactions
        """)
    return {
        "silver_transactions": con.execute("SELECT COUNT(*) FROM silver_transactions").fetchone()[0],
        "gold_outcomes": con.execute("SELECT COUNT(*) FROM gold_payment_outcomes").fetchone()[0],
        "statuses": dict(con.execute("SELECT status, COUNT(*) FROM gold_payment_outcomes GROUP BY status").fetchall()),
    }


def rates(con: sqlite3.Connection) -> list[dict]:
    rows = con.execute("""
        SELECT business_date, corridor, acquirer_id, currency,
               SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed,
               SUM(CASE WHEN status='SETTLED' THEN 1 ELSE 0 END) AS settled,
               SUM(CASE WHEN status='PENDING' THEN 1 ELSE 0 END) AS pending,
               SUM(CASE WHEN status='FAILED' THEN amount_minor ELSE 0 END) AS failed_amount_minor,
               CAST(SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS REAL)
               / NULLIF(SUM(CASE WHEN status IN ('FAILED','SETTLED') THEN 1 ELSE 0 END), 0) AS failure_rate
        FROM gold_payment_outcomes
        GROUP BY business_date, corridor, acquirer_id, currency
        ORDER BY business_date, corridor, acquirer_id, currency
    """).fetchall()
    return [dict(r) for r in rows]
