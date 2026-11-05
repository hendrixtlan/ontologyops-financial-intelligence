"""Seeded synthetic payments, including replay, out-of-order arrival and an outage.

No real accounts or payments appear in this dataset. All dates are controlled test dates.
"""
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

MERCHANT_TIERS = ("enterprise", "mid-market", "small-business")


def generate_events(n: int = 1600, seed: int = 2026) -> list[dict]:
    rng = random.Random(seed)
    rows = []
    start = datetime(2026, 11, 2, 0, 0, tzinfo=timezone.utc)
    for index in range(n):
        day = index % 2
        acquirer = "ACQ_B" if index % 4 < 2 else "ACQ_A"
        corridor = "MX-US" if index % 3 else "MX-DOMESTIC"
        currency = "USD" if corridor == "MX-US" else "MXN"
        # A documented synthetic outage: ACQ_B, MX-US, November 3.
        fail_probability = 0.38 if (day == 1 and acquirer == "ACQ_B" and corridor == "MX-US") else 0.055
        final_roll = rng.random()
        status = "FAILED" if final_roll < fail_probability else ("PENDING" if final_roll < fail_probability + 0.12 else "SETTLED")
        created = start + timedelta(days=day, minutes=(index * 13) % 1437)
        amount_minor = rng.randint(900, 80000)
        payload = {
            "transaction_id": f"tx-{index:06d}",
            "merchant_id": f"m-{index % 12:03d}",
            "merchant_tier": MERCHANT_TIERS[(index // 7) % 3],
            "acquirer_id": acquirer,
            "corridor": corridor,
            "currency": currency,
            "amount_minor": amount_minor,
        }
        statuses = ["INITIATED", "AUTHORIZED", status] if status != "PENDING" else ["INITIATED", "PENDING"]
        events = []
        for seq, st in enumerate(statuses, start=1):
            updated = created + timedelta(seconds=seq * 20)
            ingested = created + timedelta(seconds=seq * 25)
            if index % 19 == 0 and seq == 1:
                ingested += timedelta(minutes=20)  # old event delivered late
            events.append({
                **payload,
                "event_id": f"evt-{index:06d}-{seq}",
                "event_sequence": seq,
                "status": st,
                "updated_at": updated.isoformat(),
                "ingested_at": ingested.isoformat(),
            })
        if index % 19 == 0:
            events.reverse()  # arrival order is not business order
        rows.extend(events)
        if index % 17 == 0:
            rows.append(dict(events[-1]))  # replayed event_id should be ignored
    return rows


def write_events(path: Path, n: int = 1600, seed: int = 2026) -> dict:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = generate_events(n=n, seed=seed)
    with path.open("w", encoding="utf-8") as f:
        for event in rows:
            f.write(json.dumps(event, sort_keys=True) + "\n")
    return {"path": str(path), "transactions": n, "events_with_replays": len(rows)}
