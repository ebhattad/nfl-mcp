"""Tests for server error payload formatting."""

import asyncio
import json

import nfl_mcp.server as server_module
from nfl_mcp.server import _tool_error_payload, create_app


def test_error_payload_for_unknown_tool():
    payload = _tool_error_payload("bad_tool", ValueError("Unknown tool: bad_tool"))
    assert payload["ok"] is False
    assert payload["error"]["code"] == "UNKNOWN_TOOL"
    assert payload["error"]["tool"] == "bad_tool"


def test_error_payload_for_timeout():
    payload = _tool_error_payload("nfl_query", TimeoutError("Query exceeded timeout"))
    assert payload["error"]["code"] == "TIMEOUT"


def test_error_payload_for_invalid_arguments():
    payload = _tool_error_payload("nfl_query", TypeError("missing required argument"))
    assert payload["error"]["code"] == "INVALID_ARGUMENTS"


def test_list_tools_returns_registered_tools():
    tools = asyncio.run(server_module.list_tools())
    names = {tool.name for tool in tools}
    assert {"nfl_schema", "nfl_query", "nfl_schedule"}.issubset(names)


def test_call_tool_success_with_arguments(monkeypatch):
    monkeypatch.setattr(server_module, "nfl_query", lambda sql: {"rows": [{"sql": sql}]})
    result = asyncio.run(server_module.call_tool("nfl_query", {"sql": "SELECT 1"}))
    payload = json.loads(result[0].text)
    assert payload["rows"][0]["sql"] == "SELECT 1"


def test_call_tool_unknown_name_returns_error_payload():
    result = asyncio.run(server_module.call_tool("no_such_tool", {}))
    payload = json.loads(result[0].text)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "UNKNOWN_TOOL"


def test_call_tool_tool_exception_returns_execution_error(monkeypatch):
    def _boom():
        raise RuntimeError("boom")

    monkeypatch.setattr(server_module, "nfl_status", _boom)
    result = asyncio.run(server_module.call_tool("nfl_status", None))
    payload = json.loads(result[0].text)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "TOOL_EXECUTION_ERROR"
    assert payload["error"]["tool"] == "nfl_status"


def test_create_app_returns_starlette_app():
    from starlette.applications import Starlette
    app = create_app()
    assert isinstance(app, Starlette)
    route_paths = [r.path for r in app.routes]
    assert "/mcp" in route_paths
