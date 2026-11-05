# OntologyOps — Settlement investigation (synthetic)

**Scenario:** MX-US corridor, acquirer ACQ_B. **Currency:** USD.

| Measure | 2026-11-02 | 2026-11-03 |
|---|---:|---:|
| Certified settlement failure rate | 7.30% | 48.70% |
| Failed terminal attempts | 17 | 112 |
| Settled terminal attempts | 216 | 118 |
| Pending, excluded from denominator | 33 | 37 |

**Change:** +41.40 percentage points. **Failed transaction exposure proxy:** 4,464,788 minor units (USD).

## What is and isn't established

- The change is measurable using the governed contract `payments.settlement_failure_rate`.
- The generated dataset deliberately injects a higher failure probability for this segment on November 3.
- A production investigation would require incident timelines, acquirer response codes, external outage data,
  control groups, and causal checks before naming an actual root cause.
- Exposure is **not** recognized financial loss; no real money moves, and no automated actions are taken.

## Suggested human-reviewed next steps

1. Compare response-code distributions and latency by acquirer and corridor.
2. Check whether the alternative acquirer handled a comparable merchant and transaction mix.
3. Simulate routing adjustments only after reviewing relevant pricing, eligibility, and risk constraints.
