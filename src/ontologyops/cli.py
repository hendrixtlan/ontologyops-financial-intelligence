"""No network or cloud credentials are required for demo, evaluate or drift."""
import argparse
import json
from pathlib import Path

from .generator import write_events
from .storage import connect, ingest, build, rates
from .evaluation import validate_metric
from .semantics import detect_drift, render_metric_ddl
from .investigation import generate_report
from .paths import ROOT, DEFAULT_EVENTS, DEFAULT_DB, DEFAULT_METRIC_VIEW


def output(data):
    print(json.dumps(data, indent=2, sort_keys=True, default=str))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="ontologyops", description="Governed payments semantics: local-first demo + Databricks assets")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    sub = parser.add_subparsers(dest="command", required=True)
    seed = sub.add_parser("seed", help="Generate deterministic JSONL synthetic payment events")
    seed.add_argument("--events", type=Path, default=DEFAULT_EVENTS)
    seed.add_argument("--transactions", type=int, default=1600)
    ingest_p = sub.add_parser("ingest", help="Load event file idempotently into local bronze")
    ingest_p.add_argument("--events", type=Path, default=DEFAULT_EVENTS)
    sub.add_parser("build", help="Build latest-state silver and gold layers")
    sub.add_parser("metrics", help="Show governed rates by date, corridor, acquirer and currency")
    check = sub.add_parser("evaluate", help="Check executed KPI SQL against independent Python oracle")
    check.add_argument("--view", type=Path, default=DEFAULT_METRIC_VIEW)
    drift = sub.add_parser("drift", help="Detect semantic changes and emit approval gate decision")
    drift.add_argument("--candidate", type=Path, required=True)
    sub.add_parser("render-sql", help="Render CREATE METRIC VIEW DDL for Databricks SQL editor")
    sub.add_parser("demo", help="Run reproducible offline demo and produce investigation")
    genie = sub.add_parser("genie-ask", help="REAL remote MCP call. Requires configured workspace and token")
    genie.add_argument("--host", required=True)
    genie.add_argument("--workspace-id", required=True)
    genie.add_argument("--question", required=True)
    stream = sub.add_parser("stream-demo", help="Replay Debezium CDC envelopes through replay-safe SQLite streaming sink")
    stream.add_argument("--transactions", type=int, default=1600)
    worker = sub.add_parser("stream-consume", help="Consume real Debezium Kafka topic from Docker/Redpanda")
    worker.add_argument("--bootstrap", default="localhost:19092")
    worker.add_argument("--topic", default="ontologyops.public.payment_events")
    worker.add_argument("--max-records", type=int, default=0)
    source = sub.add_parser("seed-postgres", help="Insert synthetic ledger events into real local PostgreSQL")
    source.add_argument("--transactions", type=int, default=1600)
    investigation = sub.add_parser("investigate", help="Run guarded investigation nodes (optional LangGraph + Genie)")
    investigation.add_argument("--question", default="Why did MX-US settlement failures rise?")
    investigation.add_argument("--engine", choices=["reference", "langgraph"], default="reference")
    investigation.add_argument("--genie-host")
    investigation.add_argument("--workspace-id")
    args = parser.parse_args(argv)
    if args.command == "seed":
        if args.transactions <= 0:
            parser.error("--transactions must be positive")
        output(write_events(args.events, args.transactions))
    elif args.command == "stream-demo":
        from .cdc_demo import run_demo
        output(run_demo(args.db if args.db != DEFAULT_DB else ROOT / "data" / "generated" / "ontologyops_phase2.db", args.transactions))
    elif args.command == "stream-consume":
        from .stream_worker import run
        output(run(args.bootstrap, args.topic,
                   args.db if args.db != DEFAULT_DB else ROOT / "data" / "generated" / "ontologyops_phase2_live.db",
                   args.max_records))
    elif args.command == "seed-postgres":
        from .postgres_seed import seed_postgres
        output(seed_postgres(args.transactions))
    elif args.command == "investigate":
        from .agents import investigate
        target = args.db
        if target == DEFAULT_DB and (ROOT / "data" / "generated" / "ontologyops_phase2.db").exists():
            target = ROOT / "data" / "generated" / "ontologyops_phase2.db"
        output(investigate(args.question, target, args.engine, args.genie_host, args.workspace_id))
    elif args.command == "drift":
        result = detect_drift(args.candidate)
        output(result)
        return 2 if result["status"] == "blocked" else 0
    elif args.command == "render-sql":
        print(render_metric_ddl())
    elif args.command == "genie-ask":
        from .genie_bridge import run_live
        output(run_live(args.question, args.host, args.workspace_id))
    elif args.command == "demo":
        # A self-contained, deterministic run, never touches data outside the chosen db.
        if args.db.exists():
            args.db.unlink()
        write_events(DEFAULT_EVENTS)
        with connect(args.db) as con:
            raw = ingest(con, DEFAULT_EVENTS)
            layers = build(con)
            verified = validate_metric(con)
            evidence = generate_report(con, ROOT / "output" / "investigation.md")
        drift = detect_drift(ROOT / "examples" / "invalid_payment_health.yaml")
        result = {"bronze": raw, "layers": layers, "semantic_regression": verified,
                  "intentional_drift": drift, "investigation": evidence}
        out = ROOT / "output" / "demo_result.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(result, indent=2), encoding="utf-8")
        output({"report_path": str(ROOT / "output" / "investigation.md"),
                "result_path": str(out), "semantic_regression": verified["status"],
                "intentional_drift_detected": drift["status"] == "blocked", "investigation": evidence})
        return 0 if verified["status"] == "pass" and drift["status"] == "blocked" else 1
    else:
        with connect(args.db) as con:
            if args.command == "ingest":
                output(ingest(con, args.events))
            elif args.command == "build":
                output(build(con))
            elif args.command == "metrics":
                output(rates(con))
            elif args.command == "evaluate":
                result = validate_metric(con, args.view)
                output(result)
                return 0 if result["status"] == "pass" else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
