"""Versioned semantics, Databricks metric-view SQL rendering, and drift analysis."""
import json
import re
from pathlib import Path
from typing import Any

import yaml

from .paths import DEFAULT_CONTRACT, DEFAULT_METRIC_VIEW


def read_contract(path: Path = DEFAULT_CONTRACT) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def read_view(path: Path = DEFAULT_METRIC_VIEW) -> dict:
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(doc, dict) or not isinstance(doc.get("measures"), list):
        raise ValueError(f"Invalid metric view: {path}")
    return doc


def normalized(expr: str) -> str:
    # A strict change detector, not a SQL equivalence solver. Only whitespace/case
    # normalization is supported; harmless equivalent SQL may still need review.
    return re.sub(r"\s+", "", expr).lower()


def measure(view: dict, name: str) -> dict | None:
    return next((m for m in view.get("measures", []) if m.get("name") == name), None)


def detect_drift(candidate_path: Path, baseline_path: Path = DEFAULT_METRIC_VIEW,
                 contract_path: Path = DEFAULT_CONTRACT) -> dict[str, Any]:
    candidate = read_view(candidate_path)
    baseline = read_view(baseline_path)
    contract = read_contract(contract_path)
    findings = []

    def finding(code: str, severity: str, detail: str):
        findings.append({"code": code, "severity": severity, "detail": detail})

    if candidate.get("version") != baseline.get("version"):
        finding("SPEC_VERSION_CHANGE", "high", "Metric YAML version changed")
    if candidate.get("source") != contract["source"]:
        finding("SOURCE_CHANGE", "critical", "Authoritative metric source changed")
    canonical = measure(candidate, contract["measure_name"])
    if canonical is None:
        finding("MISSING_CERTIFIED_MEASURE", "critical", "Certified settlement_failure_rate is missing")
    elif normalized(canonical["expr"]) != normalized(contract["expression"]):
        finding("CERTIFIED_FORMULA_DRIFT", "critical", "Certified rate formula differs from governance contract")
    baseline_fields = {f["name"]: f["expr"] for f in baseline.get("fields", [])}
    candidate_fields = {f["name"]: f["expr"] for f in candidate.get("fields", [])}
    missing_fields = sorted(set(baseline_fields) - set(candidate_fields))
    if missing_fields:
        finding("MISSING_DIMENSIONS", "high", f"Missing dimensions: {', '.join(missing_fields)}")
    for name, expr in baseline_fields.items():
        if name in candidate_fields and normalized(expr) != normalized(candidate_fields[name]):
            finding("DIMENSION_FORMULA_DRIFT", "critical", f"Changed dimension expression: {name}")
    baseline_measures = {m["name"]: m["expr"] for m in baseline.get("measures", [])}
    candidate_measures = {m["name"]: m["expr"] for m in candidate.get("measures", [])}
    for name, expression in baseline_measures.items():
        if name not in candidate_measures:
            finding("MISSING_MEASURE", "high", f"Missing measure: {name}")
        elif normalized(candidate_measures[name]) != normalized(expression) and name != contract["measure_name"]:
            finding("UNCERTIFIED_MEASURE_CHANGE", "high", f"Changed measure: {name}")
    return {
        "candidate": str(candidate_path),
        "contract_id": contract["id"],
        "contract_version": contract["version"],
        "status": "blocked" if findings else "pass",
        "findings": findings,
        "approval_required": bool(findings),
        "automatic_changes_applied": False,
    }


def render_metric_ddl(view_path: Path = DEFAULT_METRIC_VIEW, catalog: str = "main", schema: str = "ontologyops") -> str:
    # Strict identifier validation: never interpolate arbitrary SQL identifiers.
    for part in (catalog, schema):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", part):
            raise ValueError("Invalid catalog/schema identifier")
    view = read_view(view_path)
    source = view["source"]
    if source != "main.ontologyops.gold_payment_outcomes":
        raise ValueError("This release expects the reference gold table in main.ontologyops")
    text = view_path.read_text(encoding="utf-8")
    if "$$" in text:
        raise ValueError("Unexpected SQL dollar-quoting delimiter")
    return (f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};\n"
            f"CREATE OR REPLACE VIEW {catalog}.{schema}.payment_health WITH METRICS LANGUAGE YAML AS\n"
            f"$$\n{text.rstrip()}\n$$;\n")
