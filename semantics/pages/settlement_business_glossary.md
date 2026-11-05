# Payments and Settlement — authoritative business glossary

**Owner:** Payments Risk Governance · **Semantic contract:** `payments.settlement_failure_rate` v1.0.0

## Settlement attempt
A payment counts as a *terminal settlement attempt* only when its **latest** known event is `SETTLED` or `FAILED`.
`PENDING` and `AUTHORIZED` are not terminal outcomes and **must not** be counted in the failure-rate denominator.

## Settlement failure rate
`failed_settlements / (failed_settlements + settled_settlements)` at a declared reporting grain. If no terminal
attempts exist, the metric returns `NULL`, not zero. Deduplicate replayed events by `event_id` and select the
latest event for each `transaction_id` by `updated_at`, `event_sequence`, `ingested_at`, and `event_id`.

## Failed settlement exposure
The sum of `amount_minor` for payments whose latest status is `FAILED`. This is an **operational exposure
proxy**, not a recognized accounting loss, fraud amount, or prediction of eventual non-recovery.

## Currencies
Never add amounts in MXN and USD. Group by `currency`, or explicitly apply an auditable FX conversion policy.

## Authority and governance
This repository is the version-controlled source for metrics and semantic contracts. Changes require tests and a
human reviewer. Publish this glossary as a Unity Catalog Page in a Databricks workspace to make it authoritative
Genie context. This Markdown file alone is **not** a published Unity Catalog Page.
