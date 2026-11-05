"""Evidence-only investigation: observation is not causation."""
from pathlib import Path
from .storage import rates


def generate_report(con, markdown_path: Path) -> dict:
    rows = rates(con)
    selected = [r for r in rows if r["corridor"] == "MX-US" and r["acquirer_id"] == "ACQ_B"]
    previous = next((r for r in selected if r["business_date"] == "2026-11-02"), None)
    current = next((r for r in selected if r["business_date"] == "2026-11-03"), None)
    if not previous or not current:
        raise ValueError("Expected controlled MX-US synthetic scenario not present")
    delta_pp = 100 * (current["failure_rate"] - previous["failure_rate"])
    evidence = {
        "scenario": "MX-US settlement failures on ACQ_B",
        "baseline_date": previous["business_date"],
        "incident_date": current["business_date"],
        "baseline_rate": previous["failure_rate"],
        "incident_rate": current["failure_rate"],
        "difference_percentage_points": round(delta_pp, 2),
        "failed_transactions": current["failed"],
        "pending_excluded": current["pending"],
        "failed_amount_minor": current["failed_amount_minor"],
        "currency": current["currency"],
        "severity": "investigate" if delta_pp > 10 else "normal",
        "causal_attribution": "NOT ESTABLISHED — synthetic correlation only",
    }
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(f"""# OntologyOps — Settlement investigation (synthetic)

**Scenario:** MX-US corridor, acquirer ACQ_B. **Currency:** {current['currency']}.

| Measure | {previous['business_date']} | {current['business_date']} |
|---|---:|---:|
| Certified settlement failure rate | {previous['failure_rate']:.2%} | {current['failure_rate']:.2%} |
| Failed terminal attempts | {previous['failed']} | {current['failed']} |
| Settled terminal attempts | {previous['settled']} | {current['settled']} |
| Pending, excluded from denominator | {previous['pending']} | {current['pending']} |

**Change:** {delta_pp:+.2f} percentage points. **Failed transaction exposure proxy:** {current['failed_amount_minor']:,} minor units ({current['currency']}).

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
""", encoding="utf-8")
    return evidence
