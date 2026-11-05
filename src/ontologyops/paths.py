from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB = ROOT / "data" / "generated" / "ontologyops.db"
DEFAULT_EVENTS = ROOT / "data" / "generated" / "raw_payment_events.jsonl"
DEFAULT_METRIC_VIEW = ROOT / "semantics" / "metric_views" / "payment_health.yaml"
DEFAULT_CONTRACT = ROOT / "contracts" / "settlement_failure_rate.json"
