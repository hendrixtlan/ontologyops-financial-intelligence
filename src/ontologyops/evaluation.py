"""Executable semantic regression against an independent business-rule oracle."""
import sqlite3
from pathlib import Path
from typing import Any

from .paths import DEFAULT_METRIC_VIEW
from .semantics import read_view, measure

GROUPS = ("business_date", "corridor", "acquirer_id", "currency")


def validate_metric(con: sqlite3.Connection, view_path: Path = DEFAULT_METRIC_VIEW) -> dict[str, Any]:
    view = read_view(view_path)
    metric = measure(view, "settlement_failure_rate")
    if metric is None:
        raise ValueError("Cannot evaluate a missing settlement_failure_rate")
    # Local mirror: evaluation of the metric-view SQL expression over the same
    # transaction-grain gold table; Databricks MEASURE() is tested separately in workspace.
    expr = metric["expr"].replace("source.", "")
    if ";" in expr or "--" in expr or "/*" in expr:
        raise ValueError("Unsafe expression in local regression fixture")
    groups = ", ".join(GROUPS)
    got = con.execute(f"SELECT {groups}, {expr} AS value FROM gold_payment_outcomes GROUP BY {groups}").fetchall()
    actual = {tuple(row[k] for k in GROUPS): row["value"] for row in got}
    # Reference implementation intentionally NOT derived from metric YAML SQL.
    rows = con.execute("SELECT business_date, corridor, acquirer_id, currency, status FROM gold_payment_outcomes").fetchall()
    reference = {}
    for row in rows:
        key = tuple(row[k] for k in GROUPS)
        entry = reference.setdefault(key, {"FAILED": 0, "SETTLED": 0})
        if row["status"] in entry:
            entry[row["status"]] += 1
    mismatches = []
    for key, counts in sorted(reference.items()):
        total = counts["FAILED"] + counts["SETTLED"]
        expected = counts["FAILED"] / total if total else None
        observed = actual.get(key)
        if (expected is None and observed is not None) or (expected is not None and (observed is None or abs(expected - observed) > 1e-10)):
            mismatches.append({"group": dict(zip(GROUPS, key)), "expected": expected, "observed": observed})
    return {
        "metric": "settlement_failure_rate",
        "groups_checked": len(reference),
        "status": "pass" if not mismatches else "fail",
        "mismatches": mismatches,
        "engine": "SQLite executable SQL expression; separate from Databricks engine integration",
    }
