# OntologyOps — Autonomous Financial Intelligence · V0.2

**A GitHub-ready, evidence-based portfolio implementation of PostgreSQL CDC,
Kafka, Databricks Lakeflow AUTO CDC, certified Genie Ontology semantics,
and a guarded LangGraph investigation workflow.**

This repo extends v0.1 rather than replacing it. V0.1's semantic contracts,
metric view, baseline dataset, drift regression, tests and snapshot notebook
remain intact.

## Quick start — runs without Docker or Databricks

Python 3.10+; from the repository root:

```bash
python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1
# macOS/Linux: source .venv/bin/activate
python -m pip install -e '.[dev]'
ontologyops stream-demo
ontologyops investigate --engine reference
pytest -q
```

The local CDC simulator replays **realistic Debezium JSON envelopes** with
snapshot / create / replay / corrupted-message semantics into a transactional
SQLite implementation. It does not open a network Kafka connection.

The stream demo generates:

- `output/phase2_stream_demo.json` — per-event outcome counts, quarantine, semantic regression and metric evidence.
- `output/phase2_investigation.md` — grounded incident investigation with no causal overclaim.

Sample test results for 1,600 synthetic transactions:

| Verified offline property | Observation |
|---|---:|
| Unique payment events applied | 4,593 |
| Current transactions | 1,600 |
| Exact event-id replays deduplicated | 95 |
| Late events preserved without overwriting newer states | 157 |
| Corrupt messages quarantined | 1 |
| Explicit offset replay ignored | 1 |
| Certified metric regression | PASS |
| Synthetic ACQ_B MX-US Nov 2 failure rate | 7.30% |
| Synthetic ACQ_B MX-US Nov 3 failure rate | 48.70% |

**The simulated failure spike is intentionally injected.** It is not evidence
of an actual external acquirer outage or root cause. Failed transaction value
is a proxy for exposure, not a confirmed loss.

## Real streaming infrastructure — local Docker

`docker-compose.yml` provisions:

- PostgreSQL 16 with `wal_level=logical` and an append-only event ledger.
- Redpanda (Kafka-compatible broker) on localhost:19092.
- Debezium Connect with a PostgreSQL `pgoutput` connector, initial snapshot,
  publication/slot, and the topic `ontologyops.public.payment_events`.
- A Python consumer that atomically commits (event + current state + local
  offset) to SQLite and acknowledges the broker only after the DB commit.

**LOCAL DEV SECURITY ONLY:** compose uses plaintext example credentials and
no TLS; do not expose the services to external networks.

```powershell
python -m pip install -e ".[dev,postgres,kafka]"
docker compose up -d
python scripts/register_connector.py
ontologyops seed-postgres --transactions 1600
ontologyops stream-consume --bootstrap localhost:19092 --max-records 4593
```

See **[Full PowerShell walkthrough](docs/PHASE2_RUNBOOK.md)** for diagnostics,
restart rules and resetting local data.

## Databricks Lakeflow + Genie One MCP

`databricks/pipelines/payments_cdc.py` defines:

```text
Kafka Debezium envelopes
  -> bronze_payment_cdc               (streaming)
  -> parsed_payment_cdc               (temporary view)
  -> cdc_validated + cdc_quarantine   (streaming)
  -> silver_transactions              (Lakeflow AUTO CDC type 1)
  -> gold_payment_outcomes            (materialized view)
  -> Unity Catalog payment_health     (certified metric view 1.1)
  -> Genie One / Genie Ontology       (workspace-managed)
  -> Genie One MCP                    (read-only remote tool)
  -> LangGraph investigation nodes   (optional remote invocation)
```

There are **two distinct environments**: Docker on your PC and an Azure
Databricks workspace. A remote Databricks pipeline cannot consume
`localhost:19092`; configure a network-accessible Kafka endpoint, TLS/SASL,
secrets and suitable workspace permissions. `resources/phase2-pipeline.yml` is
an example Declarative Automation Bundle resource and must be validated in
the target workspace before running.

**The v0.1 snapshot job and the v0.2 pipeline both use
`main.ontologyops.gold_payment_outcomes`. Do not run them concurrently.**

See [Phase 2 architecture](docs/PHASE2_ARCHITECTURE.md) and
[Azure deployment runbook](docs/PHASE2_RUNBOOK.md).

## Investigation agent

Offline, without LangGraph installation:

```bash
ontologyops investigate --engine reference
```

LangGraph (`StateGraph`) execution with the same certified nodes:

```bash
pip install -e '.[agents]'
ontologyops investigate --engine langgraph
```

Opt-in remote Genie One through Unity Gateway:

```bash
pip install -e '.[agents,genie]'
# Set DATABRICKS_TOKEN securely; do not store it in Git.
ontologyops investigate --engine langgraph \
  --genie-host https://YOUR-WORKSPACE.azuredatabricks.net \
  --workspace-id YOUR_NUMERIC_ID \
  --question 'Explain MX-US payment settlement failures on November 3, 2026'
```

The MCP adapter uses the **GA endpoint**
`/ai-gateway/mcp-services/system.ai.genie_one_mcp?o=<workspace-id>`, invokes
`genie_ask` and polls `genie_poll_response` for a completed remote result.
Every MCP answer remains separately attributable and is not automatically
mixed into financial totals. Prefer on-behalf-of-user OAuth (`ai-gateway`
scope) in enterprise integrations; PAT usage is for a short-lived dev test.

## Scope / verification status

| Capability | Status |
| --- | --- |
| V0.1 batch KPI + semantic drift | Tested locally |
| V0.2 Debezium-format event replay | Tested locally |
| Transactional local offset and latest-state sink | Tested locally |
| Malformed-event quarantine, out-of-order and replay tests | Tested locally |
| Deterministic guarded investigation | Tested locally |
| PostgreSQL + Debezium + Redpanda Docker environment | Code provided, not executed here |
| Kafka consumer connecting to live broker | Code provided, not executed here |
| Databricks Lakeflow AUTO CDC pipeline | Code provided, workspace validation outstanding |
| Genie One MCP call with real remote poll | Code provided, credentials/workspace testing outstanding |
| LangGraph compiled StateGraph | Code provided, optional runtime testing outstanding |
| Cross-cloud streaming production benchmark | Not performed |

## Repository layout

```text
src/ontologyops/
  streaming.py           # transactionally replay-safe CDC processing
  cdc_demo.py            # no-Docker event-envelope streaming demo
  stream_worker.py       # actual Kafka consumer (optional confluent-kafka)
  postgres_seed.py       # actual PostgreSQL ledger writer (optional psycopg)
  agents.py              # stateful evidence workflow + LangGraph runtime
  genie_bridge.py        # GA Genie One MCP async ask/poll
infra/postgres/001_schema.sql
infra/debezium/connector.json
docker-compose.yml
scripts/register_connector.py
resources/phase2-pipeline.yml
databricks/pipelines/payments_cdc.py
docs/PHASE2_ARCHITECTURE.md
docs/PHASE2_RUNBOOK.md
semantics/metric_views/payment_health.yaml
contracts/settlement_failure_rate.json
tests/test_streaming.py
```

## Sources and design reference

- [Genie One MCP — GA server](https://docs.databricks.com/aws/en/agents/mcp-tools/genie-mcp)
- [Lakeflow AUTO CDC](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes)
- [Lakeflow Python API](https://docs.databricks.com/aws/en/ldp/developer/python-ref)
- [Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/)
- [Debezium PostgreSQL CDC](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)

## License

MIT. All simulated transactions, merchant identifiers and acquirer identifiers
are synthetic. No live financial transactions are performed.
