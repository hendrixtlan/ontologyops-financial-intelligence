"""Offline deterministic emulation of the actual Debezium Kafka envelope contract."""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from .generator import generate_events
from .streaming import KafkaRecord, process_record, counts
from .storage import connect, rates
from .evaluation import validate_metric
from .paths import ROOT
from .investigation import generate_report

TOPIC = "ontologyops.public.payment_events"


def run_demo(db_path: Path, transactions: int = 1600) -> dict:
    if db_path.exists():
        db_path.unlink()
    action_counts = Counter()
    with connect(db_path) as con:
        events = generate_events(transactions)
        for offset, event in enumerate(events):
            # The exact value envelope emitted by Debezium JSON converter w/o schemas.
            value = json.dumps({"before": None, "after": event, "op": "c",
                                "source": {"db": "payments", "schema": "public", "table": "payment_events"}})
            result = process_record(con, KafkaRecord(TOPIC, 0, offset, value))
            action_counts[result] += 1
        action_counts[process_record(con, KafkaRecord(TOPIC, 0, len(events), b"bad json"))] += 1
        # Prove no additional database effects when replaying already-committed offset.
        action_counts[process_record(con, KafkaRecord(TOPIC, 0, len(events), b"bad json"))] += 1
        validation = validate_metric(con)
        evidence = generate_report(con, ROOT / "output" / "phase2_investigation.md")
        result = {"mode": "offline_debezium_envelope_replay", "events_seen": len(events) + 2,
                  "effects": dict(action_counts), "counts": counts(con),
                  "semantic_regression": validation, "evidence": evidence,
                  "rates": [x for x in rates(con) if x["corridor"] == "MX-US" and x["acquirer_id"] == "ACQ_B"]}
    out = ROOT / "output" / "phase2_stream_demo.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    return result
