import json

import pytest

from ontologyops.generator import generate_events
from ontologyops.storage import connect, rates, build
from ontologyops.streaming import KafkaRecord, process_record, counts, decode_debezium, InvalidEnvelope
from ontologyops.agents import investigate
from ontologyops.genie_bridge import genie_url, _as_payload
from ontologyops.cdc_demo import run_demo

T = "ontologyops.public.payment_events"


def rec(offset, event=None, *, op="c", value=None, partition=0):
    payload = json.dumps({"op": op, "after": event}).encode() if value is None else value
    return KafkaRecord(T, partition, offset, payload)


def test_late_event_does_not_overwrite(tmp_path):
    # Final state before INITIATED intentionally arrives out of order.
    rows = generate_events(1)
    assert rows[0]["event_sequence"] == 2
    with connect(tmp_path / "payments.db") as con:
        assert process_record(con, rec(0, rows[0])) == "current_state_updated"
        assert process_record(con, rec(1, rows[1])) == "late_event_preserved"
        stored = con.execute("SELECT status, event_sequence FROM silver_transactions").fetchone()
        assert (stored["status"], stored["event_sequence"]) == (rows[0]["status"], 2)
        assert counts(con)["bronze_events"] == 2
        assert process_record(con, rec(1, rows[2])) == "offset_replay"
        assert counts(con)["bronze_events"] == 2
        assert process_record(con, rec(2, rows[0])) == "event_replay"
        assert counts(con)["bronze_events"] == 2


def test_conflicting_event_id_and_poison_are_quarantined(tmp_path):
    original = generate_events(1)[0]
    conflicting = {**original, "amount_minor": original["amount_minor"] + 1}
    with connect(tmp_path / "payments.db") as con:
        assert process_record(con, rec(0, original)) == "current_state_updated"
        assert process_record(con, rec(1, conflicting)) == "quarantined"
        assert process_record(con, rec(2, value=b"not-json")) == "quarantined"
        assert process_record(con, rec(3, original, op="d")) == "quarantined"
        assert process_record(con, rec(4, value=None)) == "quarantined"  # malformed fixture arg
        assert process_record(con, KafkaRecord(T, 0, 5, None)) == "tombstone_ignored"
        assert counts(con)["quarantined"] == 4
        assert counts(con)["bronze_events"] == 1
        assert con.execute("SELECT MAX(last_offset) FROM consumer_offsets").fetchone()[0] == 5


def test_offsets_isolated_by_partition(tmp_path):
    events = generate_events(2)
    with connect(tmp_path / "payments.db") as con:
        assert process_record(con, rec(5, events[0], partition=0)) == "current_state_updated"
        assert process_record(con, rec(0, events[3], partition=1)) == "current_state_updated"
        assert counts(con)["partitions"] == 2
        assert process_record(con, rec(3, events[1], partition=0)) == "offset_replay"


def test_envelope_snapshot_and_schema_wrapper():
    event = generate_events(1)[0]
    assert decode_debezium(json.dumps({"payload": {"op": "r", "after": event}}))[0] == "event"
    assert decode_debezium(None) == ("tombstone", None)
    with pytest.raises(InvalidEnvelope):
        decode_debezium(json.dumps({"op": "d", "before": event, "after": None}))
    with pytest.raises(InvalidEnvelope):
        decode_debezium(json.dumps({"op": "c", "after": {**event, "status": "UNKNOWN"}}))


def test_demo_semantic_parity_and_deterministic_agent(tmp_path):
    db = tmp_path / "payments.db"
    result = run_demo(db, 1600)
    assert result["counts"]["current_transactions"] == 1600
    assert result["counts"]["quarantined"] == 1
    assert result["semantic_regression"]["status"] == "pass"
    assert result["evidence"]["difference_percentage_points"] == 41.4
    agent = investigate(db_path=db)
    assert agent["semantic_check"] == "pass"
    assert agent["findings"]["delta_percentage_points"] == 41.4
    assert agent["findings"]["causality"] == "not_established"
    assert agent["write_actions_executed"] == 0
    with pytest.raises(ValueError):
        investigate(db_path=db, genie_host="https://example.azuredatabricks.net", workspace_id="123")


def test_mcp_ga_endpoint_and_payload_normalization():
    assert genie_url("https://adb-test.azuredatabricks.net", "123") == (
        "https://adb-test.azuredatabricks.net/ai-gateway/mcp-services/system.ai.genie_one_mcp?o=123")
    with pytest.raises(ValueError):
        genie_url("http://unsafe", "123")
    with pytest.raises(ValueError):
        genie_url("https://adb-test.azuredatabricks.net/path", "123")
    with pytest.raises(ValueError):
        genie_url("https://attacker.example", "123")
    payload = {"content": [{"type": "text", "text": '{"conversation_id":"a", "response_id":"b", "status":"in_progress"}'}]}
    assert _as_payload(payload)["conversation_id"] == "a"
    assert _as_payload({"structuredContent": {"status": "completed"}})["status"] == "completed"


def test_batch_and_eventwise_cdc_have_identical_certified_rates(tmp_path):
    # Independent reference batch rebuild vs transactional event-by-event processing.
    from ontologyops.storage import ingest
    source = tmp_path / "events.jsonl"
    events = generate_events(120)
    source.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")
    with connect(tmp_path / "snapshot.db") as con:
        ingest(con, source)
        build(con)
        snapshot_rates = rates(con)
    with connect(tmp_path / "cdc.db") as con:
        for index, event in enumerate(events):
            process_record(con, rec(index, event))
        assert rates(con) == snapshot_rates
