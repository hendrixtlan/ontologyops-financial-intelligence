# V0.2 — CDC + Ontology-grounded investigation

## Data topology and factual claims

```text
  PostgreSQL: public.payment_events (append-only)
       | WAL pgoutput / publication ontologyops_publication
       v
  Debezium Kafka Connect (initial snapshot + WAL change events)
       |   JSON envelopes: { op: c/r/u/d, before, after, source }
       v
  Redpanda (local Kafka API) or managed Kafka (Azure-reachable broker)
       |                               |
       v                               v
  SQLite reference consumer        Lakeflow Spark Declarative Pipelines
  transactional offsets + states   bronze Kafka envelopes (streaming table)
       |                              -> validation / quarantine tables
       v                              -> AUTO CDC silver latest transactions
  local Gold + semantic contract     -> Gold materialized latest outcomes
       |                               |
       +-------------------------------+
                                       v
                             Unity Catalog Metric View 1.1
                                       |
                              Genie Ontology / Genie One
                                       |
                          Unity Gateway Genie One MCP
                                       |
                          LangGraph guarded investigation
                         [ontology check, metrics, findings,
                           optional remote Genie, report]
```

The local and Databricks pipelines are **two independent implementations** of the
same event and semantic contracts. The local consumer does not stream changes into
Databricks. An Azure-reachable Kafka cluster, network connectivity and secret
configuration are needed for the cloud source; **localhost:19092 cannot be used
from Azure Databricks**.

The source is a payment *event ledger* rather than a current-payment table.
Every payment-state transition is a new immutable PostgreSQL row keyed by
`event_id`; the **business entity key** is `transaction_id`. Snapshot events
(`r`) and insert events (`c`) share the same ledger semantics. `u` is supported
for compatibility, but source updates violate immutability and should be
prevented by production database permissions. Deletes are quarantined locally
and filtered from Databricks `cdc_validated`.

## Delivery guarantees and replay

- Debezium performs initial snapshot and streams PostgreSQL WAL, so connection
  disruptions can cause replays. Never assume exactly-once network delivery.
- The SQLite worker stores `(topic, partition, last_offset)` in the same transaction
  as a new event and its current Silver/Gold state. A broker acknowledgment occurs
  only after durable commit. If it fails, a replay is safe.
- The worker enforces `event_id` immutability and uses
  `(updated_at, event_sequence, ingested_at, event_id)` for latest-state order.
- Lakeflow `create_auto_cdc_flow` uses this same ordering as `sequence_by`, with
  `transaction_id` as the key. Spark/Kafka ingest is checkpointed by Lakeflow;
  no custom streaming foreachBatch or append-only Gold is used.
- Kafka partition-order is not payment event order. AUTO CDC resolves late
  business-state transitions; backfill, deletes, restarts and broker retention
  require separate production tests.
- Poison envelopes go into a SQLite quarantine table or a Databricks quarantine
  streaming table. Local quarantine advances the offset only after writing it.

## Semantic guardrails

Certified settlement failure rate = `FAILED / (FAILED + SETTLED)`.
Pending and authorized are excluded; zero denominators return NULL.
`failed_amount_minor` is a financial *exposure proxy* — not a booked loss.
Never sum payments across currencies without an approved FX policy.

Validation: `ontologyops evaluate`, `ontologyops drift`, `pytest`.
Agents do not issue payment routing, transfers, transaction edits or data writes.

## Agent path

1. `verify_certified_ontology`: reject an invalid certified metric before any analysis.
2. `retrieve_operational_evidence`: retrieve dimensioned outcomes.
3. `investigate_safely`: compare explicit baseline and incident segments, without
   asserting causality from correlation.
4. Optionally, `query_genie_one_mcp`: real remote Genie One MCP `genie_ask`
   and bounded polling; its output is kept separate until reviewed.
5. `render_cited_findings`: produce a deterministic, source-grounded statement.

`--engine reference` runs the same node functions without LangGraph, for an
offline deterministic portfolio baseline. `--engine langgraph` requires the
optional LangGraph dependency and uses a real compiled `StateGraph`.

## Security and production gaps

- Local docker development contains **deliberately insecure demo credentials**,
  no TLS, and localhost-bound ports. Use a managed secret store, TLS/SASL,
  network policies, least privilege, and managed Kafka for Azure deployment.
- Databricks secrets must be injected through an approved workspace mechanism;
  `CHANGE_ME_FROM_SECRET_MANAGER` must never be used for an actual connection.
- Monitor Kafka consumer lag, replication slot WAL retention, Debezium health,
  event-to-metric lag, `cdc_quarantine`, pipeline update failures and Genie
  request/response status. Add SLOs and alerts before production adoption.
- The v0.1 notebook and v0.2 Lakeflow pipeline both target `gold_payment_outcomes`;
  **do not run them concurrently**. Disable v0.1 snapshot job before enabling v0.2.
- True multi-agent cross-system execution, authenticated investigator dashboard,
  graph reasoning and policy-constrained actions are not in this phase.

## External documentation

- [Databricks Genie One MCP](https://docs.databricks.com/aws/en/agents/mcp-tools/genie-mcp)
- [Lakeflow AUTO CDC](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes)
- [Lakeflow Python reference](https://docs.databricks.com/aws/en/ldp/developer/python-ref)
- [Debezium PostgreSQL](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Kafka Structured Streaming options](https://docs.databricks.com/aws/en/spark/api-options)
