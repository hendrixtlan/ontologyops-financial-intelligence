"""Evidence-first investigation workflow; optionally materialized as a LangGraph graph.

The offline analyst is deliberately deterministic, and never claims root cause.
Remote Genie is opt-in, read-only, and kept separate from certified local facts.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import TypedDict, Any

from .evaluation import validate_metric
from .storage import connect, rates
from .paths import DEFAULT_DB


class InvestigationState(TypedDict, total=False):
    question: str
    db_path: str
    semantic_check: dict
    metrics: list[dict]
    findings: dict
    genie_evidence: dict
    answer: str
    provenance: list[str]


def verify_contract(state: InvestigationState) -> dict:
    with connect(Path(state["db_path"])) as con:
        result = validate_metric(con)
    if result["status"] != "pass":
        raise RuntimeError("Semantic regression failed: investigation is blocked")
    return {"semantic_check": result, "provenance": ["contracts/settlement_failure_rate.json",
                                                     "semantics/metric_views/payment_health.yaml"]}


def collect_metrics(state: InvestigationState) -> dict:
    with connect(Path(state["db_path"])) as con:
        rows = rates(con)
    return {"metrics": rows}


def establish_findings(state: InvestigationState) -> dict:
    previous = next((x for x in state["metrics"] if x["business_date"] == "2026-11-02"
                     and x["corridor"] == "MX-US" and x["acquirer_id"] == "ACQ_B"), None)
    incident = next((x for x in state["metrics"] if x["business_date"] == "2026-11-03"
                     and x["corridor"] == "MX-US" and x["acquirer_id"] == "ACQ_B"), None)
    if not previous or not incident:
        return {"findings": {"status": "insufficient_data", "causality": "not_established"}}
    return {"findings": {
        "status": "investigate" if incident["failure_rate"] - previous["failure_rate"] > 0.1 else "within_rule",
        "baseline_failure_rate": previous["failure_rate"],
        "incident_failure_rate": incident["failure_rate"],
        "delta_percentage_points": round(100 * (incident["failure_rate"] - previous["failure_rate"]), 2),
        "failed_amount_minor": incident["failed_amount_minor"],
        "currency": incident["currency"],
        "pending_excluded": incident["pending"],
        "causality": "not_established",
        "exposure_is_accounting_loss": False,
    }}


def synthesize(state: InvestigationState) -> dict:
    findings = state["findings"]
    if findings["status"] == "insufficient_data":
        answer = "Insufficient baseline and incident data; do not infer settlement rate change."
    else:
        answer = (f"Certified MX-US/ACQ_B settlement failure rate changed from "
                  f"{findings['baseline_failure_rate']:.2%} to "
                  f"{findings['incident_failure_rate']:.2%} "
                  f"({findings['delta_percentage_points']:+.2f} pp). "
                  f"Pending transactions are excluded. Failed transaction exposure proxy: "
                  f"{findings['failed_amount_minor']:,} minor units ({findings['currency']}). "
                  "No root cause is established. Suggested next step: compare acquirer "
                  "response codes, comparable control groups and incident timelines. "
                  "No routing or settlement action was executed.")
    if state.get("genie_evidence"):
        genie = state["genie_evidence"]
        if genie.get("status") == "completed":
            answer += " Genie One produced separate remote analytic evidence; inspect its source links before combining conclusions."
        else:
            answer += " Remote Genie evidence is incomplete and was not used for numerical conclusions."
    return {"answer": answer}


def _run_reference(state: InvestigationState) -> InvestigationState:
    """Offline state machine with same node contracts; NOT a LangGraph runtime."""
    for fn in (verify_contract, collect_metrics, establish_findings, synthesize):
        state.update(fn(state))
    return state


def query_genie(state: InvestigationState, host: str, workspace_id: str) -> dict:
    """Explicit opt-in, read-only MCP call; remote result stays as separate evidence."""
    from .genie_bridge import run_live
    return {"genie_evidence": run_live(state["question"], host, workspace_id)}


def compile_langgraph(genie_host: str | None = None, workspace_id: str | None = None):
    """Build actual LangGraph StateGraph when optional dependency is installed."""
    try:
        from langgraph.graph import StateGraph, START, END
    except ImportError as exc:
        raise RuntimeError("Install optional dependency: pip install -e '.[agents]'") from exc
    graph = StateGraph(InvestigationState)
    graph.add_node("verify_certified_ontology", verify_contract)
    graph.add_node("retrieve_operational_evidence", collect_metrics)
    graph.add_node("investigate_safely", establish_findings)
    graph.add_node("render_cited_findings", synthesize)
    graph.add_edge(START, "verify_certified_ontology")
    graph.add_edge("verify_certified_ontology", "retrieve_operational_evidence")
    graph.add_edge("retrieve_operational_evidence", "investigate_safely")
    if genie_host and workspace_id:
        graph.add_node("query_genie_one_mcp", lambda state: query_genie(state, genie_host, workspace_id))
        graph.add_edge("investigate_safely", "query_genie_one_mcp")
        graph.add_edge("query_genie_one_mcp", "render_cited_findings")
    else:
        graph.add_edge("investigate_safely", "render_cited_findings")
    graph.add_edge("render_cited_findings", END)
    return graph.compile()


def investigate(question: str = "Why did MX-US settlement failures rise?",
                db_path: Path = DEFAULT_DB, engine: str = "reference",
                genie_host: str | None = None, workspace_id: str | None = None) -> dict:
    if bool(genie_host) != bool(workspace_id):
        raise ValueError("Provide both Genie host and workspace ID")
    if genie_host and engine != "langgraph":
        raise ValueError("Remote Genie calls require engine=langgraph")
    state: InvestigationState = {"question": question, "db_path": str(db_path)}
    if engine == "reference":
        result = _run_reference(state)
    elif engine == "langgraph":
        result = compile_langgraph(genie_host, workspace_id).invoke(state)
    else:
        raise ValueError("engine must be reference or langgraph")
    return {"engine": engine, "question": question, "semantic_check": result["semantic_check"]["status"],
            "findings": result["findings"], "answer": result["answer"],
            "provenance": result.get("provenance", []),
            "remote_genie_attested": result.get("genie_evidence", {}).get("status") == "completed",
            "genie_evidence": result.get("genie_evidence"), "write_actions_executed": 0}
