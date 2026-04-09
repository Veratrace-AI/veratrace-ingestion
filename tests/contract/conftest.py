"""
Contract tests run against real vendor sandbox APIs.
Skip unless --contract flag is passed or CI_CONTRACT=true.
"""
import os
import pytest


def pytest_addoption(parser):
    parser.addoption("--contract", action="store_true", default=False, help="Run contract tests against live APIs")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--contract") or os.environ.get("CI_CONTRACT") == "true":
        return  # run all tests
    skip = pytest.mark.skip(reason="Contract tests require --contract flag or CI_CONTRACT=true")
    for item in items:
        if "contract" in str(item.fspath):
            item.add_marker(skip)
