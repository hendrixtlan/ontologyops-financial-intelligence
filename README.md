# OntologyOps: Autonomous Financial Intelligence · V0.1

[![CI](https://img.shields.io/badge/tests-pytest-lightgrey)](#verify)

**A reproducible portfolio vertical slice for Databricks Genie Ontology, certified metrics and semantic drift.**

This release is intentionally **local-first**: an executable Python + SQLite payment event pipeline, a genuine
Databricks Unity Catalog Metric View YAML artifact, an optional Genie One MCP client, and semantic regression that
blocks incompatible business-definition changes.

## Five-minute quick start

Python 3.10+ required. From repository root:

```bash
python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1
# macOS/Linux: source .venv/bin/activate
python -m pip install -e '.[dev]'
ontologyops demo
pytest -q
```

The demo produces `output/investigation.md` and `output/demo_result.json`.
It runs completely offline, no cloud account required.

## Verify

```bash
ontologyops seed --transactions 1600
ontologyops ingest
ontologyops build
ontologyops metrics
ontologyops evaluate
ontologyops drift --candidate semantics/metric_views/payment_health.yaml
ontologyops drift --candidate examples/invalid_payment_health.yaml  # expected exit code 2
ontologyops render-sql > metric_view.sql
```

The last example is an **intentional broken KPI**. It divides failures by `COUNT(*)`, which incorrectly
includes pending payments. The drift gate catches the modified formula and the independent regression engine
finds numeric mismatches. No semantic modification is ever applied automatically.

## Architecture

```text
Synthetic payment events (JSONL)  --> local bronze (SQLite) --> dedup / latest state
                                                     |
                                                 silver (1 row/tx)
                                                     |
                                        gold_payment_outcomes (1 row/tx)
                                                     |
       Git semantic contract <----------+----------> UC Metric View YAML
                 |                                      |
       Drift + executable regression                 Genie Ontology
                 |                                 (Databricks workspace)
            Review gate                                    |
                                             Genie One MCP / Unity Gateway
```

**Monetary metric warning:** the certified view defines amount measures in minor units but cannot itself prevent
an ad hoc user from removing the `currency` grouping. Always group by currency or use a separate approved FX
conversion measure. Enforcing this restriction across all query surfaces is a next-phase governance task.

**Event semantics:** `event_id` is the replay key; `transaction_id` is the payment grain; latest event is
chosen using `updated_at`, `event_sequence`, `ingested_at`, and `event_id` in descending order. Currency amounts
are represented in integer minor units. Gold includes non-terminal latest states so erroneous denominator
calculations are detectable.

**Business semantics:** settlement failure rate = `FAILED / (FAILED + SETTLED)`; returns NULL if there are
no terminal attempts; pending and authorized states are excluded. Exposure is a proxy, not accounting loss.

**Sample scenario:** on 2026-11-03, synthetic failures increase for ACQ_B on MX-US cross-border transactions.
The investigation is observational, not causal attribution.

## Databricks deployment

Follow [Azure Databricks setup](docs/SETUP_DATABRICKS.md). This includes a PySpark/Delta notebook,
Declarative Automation Bundle configuration, a metric-view DDL renderer, a business glossary for publication
as a Unity Catalog Page, metric queries with `MEASURE()`, and an optional real Genie One MCP `genie_ask` call.

The UC Page and Genie Ontology inference happen **inside** Databricks; the Git content alone is not a
configured ontology. For Phase 2, see [architecture and scope](docs/ARCHITECTURE.md).

## What is implemented vs planned

| Capability | V0.1 status |
| --- | --- |
| Synthetic payment source and deliberate outage | Executable locally |
| Replay-safe latest-state silver and gold | Executable locally |
| Versioned KPI and semantic drift | Executable locally |
| Cross-check against independent reference | Executable locally |
| Databricks Delta batch notebook | Provided; requires workspace validation |
| UC Metric View YAML / generated DDL | Provided; requires workspace execution |
| Genie One MCP client for `genie_ask` | Provided; requires credentials, permissions and follow-up polling |
| Unity Catalog Page | Curated Markdown provided; manual publication required |
| Kafka/Debezium real-time CDC | Phase 2 |
| LangGraph, Neo4j, simulation, action engine | Future phases |

## Technical references

- [Genie Ontology](https://docs.databricks.com/aws/en/genie/genie-ontology)
- [Genie One MCP](https://docs.databricks.com/aws/en/agents/mcp-tools/genie-mcp)
- [Metric View YAML Reference](https://docs.databricks.com/aws/en/uc-semantics/metric-views/yaml-reference)
- [Metric View Querying](https://docs.databricks.com/aws/en/uc-semantics/metric-views/query)
- [Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/settings)

## License

MIT (synthetic datasets only; no real payment or personal data).
