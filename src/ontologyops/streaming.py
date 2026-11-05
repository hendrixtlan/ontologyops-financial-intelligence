"""Debezium PostgreSQL change events -> exactly-once *database effects* in SQLite.

Kafka is at-least-once. The SQLite write, its topic/partition offset, and the
latest-state materializations commit in the same database transaction. A Kafka
consumer commits its broker offset only AFTER this commit. This avoids duplicate
metric effects on crash/replay; it is NOT a claim of end-to-end exactly-once.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .storage import COLUMNS

ALLOWED_STATUSES = {"INITIATED", "AUTHORIZED", "PENDING", "SETTLED", "FAILED"}


@dataclass(frozen=True)
class KafkaRecord:
    topic: str
    partition: int
    offset: int
    value: bytes | str | None
    key: bytes | str | None = None


class InvalidEnvelope(ValueError):
    pass


def bootstrap_stream(con: sqlite3.Connection) -> None:
    con.executescript("""
    CREATE TABLE IF NOT EXISTS consumer_offsets (
        topic TEXT NOT NULL, partition_id INTEGER NOT NULL, last_offset INTEGER NOT NULL,
        PRIMARY KEY(topic, partition_id)
    );
    CREATE TABLE IF NOT EXISTS cdc_quarantine (
        topic TEXT NOT NULL, partition_id INTEGER NOT NULL, offset_id INTEGER NOT NULL,
        error TEXT NOT NULL, payload TEXT,
        PRIMARY KEY(topic, partition_id, offset_id)
    );
    """)


def _utc_iso(raw: Any) -> str:
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        # Debezium timestamp representation can vary with source temporal type.
        n = float(raw)
        if abs(n) > 1e14:
            n /= 1_000_000  # microseconds
        elif abs(n) > 1e11:
            n /= 1_000  # milliseconds
        dt = datetime.fromtimestamp(n, timezone.utc)
    elif isinstance(raw, str):
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            raise ValueError("Naive timestamps are not permitted")
    else:
        raise ValueError("Expected ISO-8601 UTC timestamp or epoch timestamp")
    return dt.astimezone(timezone.utc).isoformat()


def _canonical(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise InvalidEnvelope("CDC after must be an object")
    missing = set(COLUMNS) - payload.keys()
    if missing:
        raise InvalidEnvelope(f"Missing fields: {', '.join(sorted(missing))}")
    p = {c: payload[c] for c in COLUMNS}
    for field in ("event_id", "transaction_id", "merchant_id", "merchant_tier", "acquirer_id", "corridor"):
        if not isinstance(p[field], str) or not p[field] or len(p[field]) > 128:
            raise InvalidEnvelope(f"Invalid {field}")
    if p["status"] not in ALLOWED_STATUSES:
        raise InvalidEnvelope("Invalid status")
    if p["currency"] not in ("USD", "MXN"):
        raise InvalidEnvelope("Invalid currency")
    if type(p["amount_minor"]) is not int or p["amount_minor"] <= 0:
        raise InvalidEnvelope("Invalid amount_minor")
    if type(p["event_sequence"]) is not int or p["event_sequence"] <= 0:
        raise InvalidEnvelope("Invalid event_sequence")
    try:
        for field in ("updated_at", "ingested_at"):
            p[field] = _utc_iso(p[field])
    except (ValueError, TypeError, OverflowError) as exc:
        raise InvalidEnvelope(f"Invalid timestamp: {exc}") from exc
    return p


def decode_debezium(value: bytes | str | None) -> tuple[str, dict | None]:
    if value is None:
        return "tombstone", None  # Kafka compaction marker, not a payment update.
    try:
        raw = json.loads(value)
        # Support the standard Debezium JSON envelope, schemas.enable=false.
        envelope = raw.get("payload", raw) if isinstance(raw, dict) else None
        if not isinstance(envelope, dict):
            raise InvalidEnvelope("Envelope must be JSON object")
        op = envelope.get("op")
        if op in ("c", "r", "u"):
            return "event", _canonical(envelope.get("after"))
        if op == "d":
            raise InvalidEnvelope("A DELETE violates immutable payment-event ledger policy")
        raise InvalidEnvelope(f"Unsupported Debezium op: {op}")
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as exc:
        raise InvalidEnvelope("Invalid Debezium JSON") from exc


def _newer(candidate: dict, existing: sqlite3.Row | None) -> bool:
    if existing is None:
        return True
    ordering = ("updated_at", "event_sequence", "ingested_at", "event_id")
    return tuple(candidate[c] for c in ordering) > tuple(existing[c] for c in ordering)


def process_record(con: sqlite3.Connection, record: KafkaRecord) -> str:
    """Apply record, offsets, quarantine and aggregates as one SQLite transaction."""
    if not record.topic or record.partition < 0 or record.offset < 0:
        raise ValueError("Invalid Kafka topic/partition/offset")
    bootstrap_stream(con)
    with con:
        checkpoint = con.execute(
            "SELECT last_offset FROM consumer_offsets WHERE topic=? AND partition_id=?",
            (record.topic, record.partition),
        ).fetchone()
        if checkpoint is not None and record.offset <= checkpoint[0]:
            return "offset_replay"
        try:
            action, event = decode_debezium(record.value)
            if action == "tombstone":
                outcome = "tombstone_ignored"
            else:
                # Event identity is immutable; conflicting duplicates are quarantined.
                existing_event = con.execute(
                    "SELECT * FROM raw_payment_events WHERE event_id=?", (event["event_id"],)
                ).fetchone()
                if existing_event is not None:
                    if any(existing_event[c] != event[c] for c in COLUMNS):
                        raise InvalidEnvelope("Conflicting event_id payload")
                    outcome = "event_replay"
                else:
                    con.execute(
                        f"INSERT INTO raw_payment_events ({','.join(COLUMNS)}) "
                        f"VALUES ({','.join(['?'] * len(COLUMNS))})",
                        [event[c] for c in COLUMNS],
                    )
                    existing_state = con.execute(
                        "SELECT * FROM silver_transactions WHERE transaction_id=?",
                        (event["transaction_id"],),
                    ).fetchone()
                    if _newer(event, existing_state):
                        silver_columns = ["transaction_id", "event_id", "merchant_id", "merchant_tier",
                                          "acquirer_id", "corridor", "currency", "amount_minor", "status",
                                          "event_sequence", "updated_at", "ingested_at"]
                        con.execute(
                            f"INSERT OR REPLACE INTO silver_transactions ({','.join(silver_columns)}) "
                            f"VALUES ({','.join(['?']*len(silver_columns))})",
                            [event[c] for c in silver_columns],
                        )
                        gold = [event["transaction_id"], event["updated_at"][:10],
                                event["merchant_id"], event["merchant_tier"], event["acquirer_id"],
                                event["corridor"], event["currency"], event["amount_minor"], event["status"]]
                        con.execute("""INSERT OR REPLACE INTO gold_payment_outcomes
                                    (transaction_id,business_date,merchant_id,merchant_tier,acquirer_id,
                                     corridor,currency,amount_minor,status) VALUES (?,?,?,?,?,?,?,?,?)""", gold)
                        outcome = "current_state_updated"
                    else:
                        outcome = "late_event_preserved"
        except InvalidEnvelope as exc:
            outcome = "quarantined"
            raw = record.value.decode("utf-8", errors="replace") if isinstance(record.value, bytes) else record.value
            con.execute(
                "INSERT OR IGNORE INTO cdc_quarantine (topic,partition_id,offset_id,error,payload) VALUES (?,?,?,?,?)",
                (record.topic, record.partition, record.offset, str(exc), raw),
            )
        con.execute("""INSERT INTO consumer_offsets(topic, partition_id, last_offset) VALUES (?, ?, ?)
                      ON CONFLICT(topic,partition_id) DO UPDATE SET last_offset=excluded.last_offset""",
                    (record.topic, record.partition, record.offset))
        return outcome


def counts(con: sqlite3.Connection) -> dict:
    return {
        "bronze_events": con.execute("SELECT COUNT(*) FROM raw_payment_events").fetchone()[0],
        "current_transactions": con.execute("SELECT COUNT(*) FROM silver_transactions").fetchone()[0],
        "quarantined": con.execute("SELECT COUNT(*) FROM cdc_quarantine").fetchone()[0],
        "partitions": con.execute("SELECT COUNT(*) FROM consumer_offsets").fetchone()[0],
    }
