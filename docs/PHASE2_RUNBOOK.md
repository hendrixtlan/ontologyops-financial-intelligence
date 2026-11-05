# Phase 2 — Windows PowerShell runbook

## A. Offline proof (no Docker, cloud, API keys or LangGraph dependency)

From repository root, Python 3.10+:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
ontologyops stream-demo
ontologyops investigate --engine reference
pytest -q
```

Outputs: `output/phase2_stream_demo.json` and `output/phase2_investigation.md`.
The offline stream-demo uses realistic Debezium envelope JSON emitted in
Kafka-order into SQLite, NOT a live Kafka connection.

## B. Real local PostgreSQL -> Debezium -> Redpanda -> consumer

Preconditions: Docker Desktop with Linux containers; working Docker Compose;
Python extras installed; host ports 5432, 8083 and 19092 available.

```powershell
python -m pip install -e ".[dev,postgres,kafka]"
docker compose up -d
python scripts/register_connector.py
ontologyops seed-postgres --transactions 1600
ontologyops stream-consume --bootstrap localhost:19092 --max-records 4593
```

The `--max-records` bound is illustrative and should be adjusted for snapshot
and connector versions. Without it, the worker runs until stopped with Ctrl+C.
Do not stop the worker while it is processing a transaction without allowing
its database commit to finish. Rerunning the source seed uses `ON CONFLICT DO NOTHING`;
for fresh stream testing use a clean Docker volume and/or generate new events.

Useful diagnostics:

```powershell
docker compose ps
curl.exe http://localhost:8083/connectors/ontologyops-payments-cdc/status
docker compose logs --tail=100 connect
docker compose exec postgres psql -U postgres -d payments -c "SELECT slot_name, active, restart_lsn FROM pg_replication_slots;"
docker compose exec redpanda rpk topic list
```

**Destructive reset (local test volumes only):** `docker compose down -v`.

## C. Azure Databricks Lakeflow + Genie One

1. Deploy an Azure-reachable broker (e.g. Confluent Cloud) with TLS/SASL and
   a controlled route from Databricks workspace compute. Localhost Docker is
   not an Azure-facing Kafka endpoint.
2. Configure PostgreSQL -> Debezium -> that broker, or a secure replication
   bridge. Validate topic `ontologyops.public.payment_events` contains unwrapped
   JSON-converter Debezium `before/after/op` envelopes.
3. Create `main.ontologyops` and grant a least-privilege job principal appropriate
   `USE` / `CREATE TABLE` / `SELECT` permissions. Set pipeline network policies,
   secret-backed Spark configuration and UTC session timezone.
4. Configure bundle variables `kafka_bootstrap` and `kafka_sasl_jaas` securely.
   Example: use protected CI variables or an approved Databricks secret reference;
   DO NOT store the JAAS string in Git, shell history or a checked-in yml.
5. Disable v0.1 snapshot job (both write the same Gold target). Validate, deploy
   and start the v0.2 pipeline:

   ```powershell
   databricks bundle validate -t dev
   databricks bundle deploy -t dev
   databricks bundle run ontologyops_cdc -t dev
   ```

   Review syntax and compatibility with your installed CLI and your workspace.
   No remote Databricks commands were executed in the supplied environment.
6. Run `ontologyops render-sql` and apply the generated certified metric view;
   test `sql/01_metric_queries.sql`. Publish the business glossary as a Unity
   Catalog Page; confirm Genie Ontology knows the table and certified metrics.
7. Grant caller `EXECUTE` on `system.ai.genie_one_mcp`, UC `USE` and `SELECT`
   on the authorized analytics datasets, and warehouse permissions where needed.
   Prefer per-user OAuth (`ai-gateway` scope); short-lived PAT is local dev only.
8. For a guarded LangGraph investigation, install `.[agents,genie]`, then:

   ```powershell
   $env:DATABRICKS_TOKEN = "<ephemeral developer token from a secure source>"
   ontologyops investigate --engine langgraph --question "Explain MX-US settlement failures on Nov 3, 2026" --genie-host https://adb-example.azuredatabricks.net --workspace-id 123456789
   ```

   The local certified evidence remains from SQLite; the Genie tool queries
   the workspace. This is not a federated or automated reconciliation of both
   sources. Review provenance and compare measured values before conclusions.

## Operational checks to run in a real environment

- Snapshot completeness vs PostgreSQL row count.
- Restart Debezium mid-snapshot; verify no missing source rows.
- Restart Kafka consumer between database commit and broker ack; verify replay.
- Duplicate event IDs, out-of-order states, poison envelopes, deletions.
- Pipeline checkpoint restart, partition expansion, broker retention boundary.
- Databricks semantic metric parity (local vs cloud) by day/corridor/acquirer/currency.
- Genie authorization controls, error handling, and deep link traceability.

## Known gaps

Docker/Redpanda/Databricks/Genie were not remotely executed during generation.
The networked path and bundle need testing in the target environment. Do not
present local envelope replay as evidence of production streaming throughput.
