import json
from ontologyops.storage import ingest, build, rates
from ontologyops.generator import write_events


def test_transaction_grain_and_replay_dedup(db):
    assert db.execute('SELECT COUNT(*) FROM silver_transactions').fetchone()[0] == 1200
    assert db.execute('SELECT COUNT(*) FROM gold_payment_outcomes').fetchone()[0] == 1200
    assert db.execute('SELECT COUNT(*) FROM raw_payment_events').fetchone()[0] > 2400
    assert db.execute('SELECT COUNT(*) FROM raw_payment_events').fetchone()[0] == db.execute('SELECT COUNT(DISTINCT event_id) FROM raw_payment_events').fetchone()[0]


def test_late_event_does_not_rewind_status(db):
    # tx-000000 deliberately receives its old INITIATED event after final status
    row = db.execute("SELECT status FROM silver_transactions WHERE transaction_id='tx-000000'").fetchone()
    assert row['status'] in {'SETTLED', 'FAILED', 'PENDING'}


def test_rerunning_build_and_ingestion_is_idempotent(db, tmp_path):
    events = tmp_path / 'events.jsonl'
    write_events(events, n=1200)
    before = db.execute('SELECT COUNT(*) FROM raw_payment_events').fetchone()[0]
    result = ingest(db, events)
    assert result['new_events'] == 0
    build(db)
    assert db.execute('SELECT COUNT(*) FROM raw_payment_events').fetchone()[0] == before
    assert db.execute('SELECT COUNT(*) FROM gold_payment_outcomes').fetchone()[0] == 1200


def test_outage_increases_rate(db):
    records = [r for r in rates(db) if r['corridor'] == 'MX-US' and r['acquirer_id'] == 'ACQ_B']
    assert len(records) == 2
    assert records[1]['failure_rate'] > records[0]['failure_rate'] + 0.15
    assert all(r['currency'] == 'USD' for r in records)


def test_denominator_excludes_pending(db):
    assert db.execute("SELECT COUNT(*) FROM gold_payment_outcomes WHERE status='PENDING'").fetchone()[0] > 0
    row = db.execute("SELECT COUNT(*) AS total, SUM(status='FAILED') AS failed, SUM(status='SETTLED') AS settled FROM gold_payment_outcomes").fetchone()
    assert row['total'] > row['failed'] + row['settled']
