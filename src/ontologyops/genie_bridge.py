"""Optional REAL Genie One MCP adapter. Never fall back to fabricated AI responses."""
import asyncio
import json
import os
from urllib.parse import urlsplit


def genie_url(host: str, workspace_id: str) -> str:
    parsed = urlsplit(host)
    if parsed.scheme != "https" or not parsed.hostname or parsed.path not in ("", "/"):
        raise ValueError("Use a bare HTTPS Databricks workspace host, e.g. https://adb-...azuredatabricks.net")
    if not workspace_id.isdigit():
        raise ValueError("workspace_id must be numeric")
    return f"{host.rstrip('/')}/ai-gateway/mcp-services/system.ai.genie_one_mcp?o={workspace_id}"


async def ask_genie(question: str, host: str, workspace_id: str) -> dict:
    token = os.environ.get("DATABRICKS_TOKEN")
    if not token:
        raise RuntimeError("DATABRICKS_TOKEN is required for a real Genie MCP call; no mock answer will be returned")
    try:
        from mcp import ClientSession
        from mcp.client.streamable_http import streamable_http_client
    except ImportError as exc:
        raise RuntimeError("Install the optional dependency: pip install -e '.[genie]'") from exc
    url = genie_url(host, workspace_id)
    async with streamable_http_client(url, headers={"Authorization": f"Bearer {token}"}) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool("genie_ask", {"question": question})
            # MCP returns an asynchronous response. The raw envelope is retained
            # so the caller can poll using IDs and the server's documented schema.
            return {
                "server": "system.ai.genie_one_mcp",
                "result": result.model_dump(mode="json"),
                "note": "genie_ask starts an asynchronous answer. Use genie_poll_response with returned conversation_id and response_id to obtain the final answer.",
            }


def run_live(question: str, host: str, workspace_id: str) -> dict:
    return asyncio.run(ask_genie(question, host, workspace_id))
