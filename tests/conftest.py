"""Pytest marker normalization for unit and integration test selection."""

import pytest


def pytest_collection_modifyitems(items):
    """Auto-tag unmarked tests as unit tests."""
    for item in items:
        if "integration" not in item.keywords:
            item.add_marker(pytest.mark.unit)
