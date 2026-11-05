"""Optional real Genie One MCP client (Unity Gateway GA endpoint).

Traces/answers are raw remote results, never fabricated. Untrusted MCP text must
not be executed as instructions or be written to transaction tables.
"""
import asyncio
import json
import os
from urllib.parse import urlsplit


def genie_url(host: str, workspace_id: str) -> str:
    parsed = urlsplit(host)
    if (parsed.scheme != "https" or not parsed.hostname or
            not parsed.hostname.endswith(".azuredatabricks.net") or
            parsed.path not in ("", "/") or parsed.query or parsed.fragment or
            parsed.username or parsed.password or parsed.port):
        raise ValueError("Use a bare HTTPS Azure Databricks workspace host ending in .azuredatabricks.net")
    if not workspace_id.isdigit():
        raise ValueError("workspace_id must be numeric")
    return f"{host.rstrip('/')}/ai-gateway/mcp-services/system.ai.genie_one_mcp?o={workspace_id}"


def _as_payload(result) -> dict:
    """MCP SDK CallToolResult -> JSON object, preserving raw envelope if different."""
    if isinstance(result, dict):
        raw = result
    else:
        raw = result.model_dump(mode="json")
    structured = raw.get("structuredContent") or raw.get("structured_content")
    if isinstance(structured, dict):
        return structured.get("result", structured)
    for part in raw.get("content", []):
        if part.get("type") == "text":
            try:
                data = json.loads(part["text"])
                if isinstance(data, dict):
                    return data.get("result", data)
            except (json.JSONDecodeError, TypeError, KeyError):
                pass
    return {"status": "unparsed", "raw": raw}


def _normalize_status(value: dict) -> str:
    status = value.get("status", "unknown")
    return status.lower() if isinstance(status, str) else "unknown"


async def ask_genie(question: str, host: str, workspace_id: str,
                    max_polls: int = 30, poll_interval_seconds: float = 2.0) -> dict:
    token = os.environ.get("DATABRICKS_TOKEN")
    if not token:
        raise RuntimeError("DATABRICKS_TOKEN required; no mock Genie answer will be returned")
    try:
        from mcp import ClientSession
        from mcp.client.streamable_http import streamable_http_client
    except ImportError as exc:
        raise RuntimeError("Install optional dependency: pip install -e '.[genie]'") from exc
    url = genie_url(host, workspace_id)
    async with streamable_http_client(url, headers={"Authorization": f"Bearer {token}"}) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            started = _as_payload(await session.call_tool("genie_ask", {"question": question}))
            status = _normalize_status(started)
            if status in {"completed", "failed", "incomplete"}:
                return {"server": "system.ai.genie_one_mcp", "status": status, "response": started}
            # Poll only if we have IDs from Genie; never guess unknown tool arguments.
            conversation_id = started.get("conversation_id")
            response_id = started.get("response_id")
            if not conversation_id or not response_id:
                return {"server": "system.ai.genie_one_mcp", "status": "unparsed", "response": started}
            result = started
            for _ in range(max_polls):
                await asyncio.sleep(poll_interval_seconds)
                result = _as_payload(await session.call_tool("genie_poll_response", {
                    "conversation_id": conversation_id, "response_id": response_id,
                }))
                status = _normalize_status(result)
                if status in {"completed", "failed", "incomplete"}:
                    break
            else:
                status = "poll_limit_reached"
            return {"server": "system.ai.genie_one_mcp", "status": status,
                    "conversation_id": conversation_id, "response_id": response_id,
                    "response": result}


def run_live(question: str, host: str, workspace_id: str) -> dict:
    return asyncio.run(ask_genie(question, host, workspace_id))
