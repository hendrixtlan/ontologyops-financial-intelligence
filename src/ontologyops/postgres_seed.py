"""Seed a real PostgreSQL append-only event ledger; Debezium reads its WAL."""
import os
from .generator import generate_events

SQL = """INSERT INTO payment_events
(event_id,transaction_id,merchant_id,merchant_tier,acquirer_id,corridor,currency,
 amount_minor,status,event_sequence,updated_at,ingested_at)
VALUES (%(event_id)s,%(transaction_id)s,%(merchant_id)s,%(merchant_tier)s,
%(acquirer_id)s,%(corridor)s,%(currency)s,%(amount_minor)s,%(status)s,
%(event_sequence)s,%(updated_at)s,%(ingested_at)s) ON CONFLICT (event_id) DO NOTHING"""


def seed_postgres(n: int = 1600) -> dict:
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("Install postgres extra: pip install -e '.[postgres]'") from exc
    dsn = os.environ.get("ONTOLOGYOPS_PG_DSN", "postgresql://postgres:localonly@localhost:5432/payments")
    events = generate_events(n)
    # Replay IDs are intentional and should be ignored by source primary key.
    with psycopg.connect(dsn) as con:
        with con.cursor() as cur:
            cur.executemany(SQL, events)
    return {"attempted_inserts": len(events), "source": "PostgreSQL WAL", "dsn_redacted": True}
