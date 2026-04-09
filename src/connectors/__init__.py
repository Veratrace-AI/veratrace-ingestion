"""
Connector auto-discovery registry.

Each connector package exports CONNECTOR_ID and CONNECTOR_CLASS in its __init__.py.
This module discovers all connectors at import time — no manual registration needed.

To add a new connector:
  1. Create src/connectors/your_platform/__init__.py
  2. Export CONNECTOR_ID = "your-platform" and CONNECTOR_CLASS = YourConnector
  3. That's it — the scheduler and API will find it automatically.
"""
from __future__ import annotations

import importlib
import logging
import os
import pkgutil

logger = logging.getLogger(__name__)

CONNECTOR_MAP = {}

_connectors_dir = os.path.dirname(__file__)
for _, name, is_pkg in pkgutil.iter_modules([_connectors_dir]):
    if not is_pkg or name.startswith("_"):
        continue
    try:
        mod = importlib.import_module(f"src.connectors.{name}")
        if hasattr(mod, "CONNECTOR_ID") and hasattr(mod, "CONNECTOR_CLASS"):
            CONNECTOR_MAP[mod.CONNECTOR_ID] = mod.CONNECTOR_CLASS
            logger.debug("Discovered connector: %s", mod.CONNECTOR_ID)
    except Exception as e:
        logger.warning("Failed to load connector '%s': %s", name, e)
