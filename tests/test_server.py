"""Tests for server error payload formatting."""

from nfl_mcp.server import _tool_error_payload


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
