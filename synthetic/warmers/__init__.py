"""
Warmer auto-discovery registry.

Each warmer module exports WARMER_ID and WARMER_CLASS.
This module discovers all warmers at import time.

To add a new warmer:
  1. Create synthetic/warmers/your_platform.py
  2. Export WARMER_ID = "your-platform" and WARMER_CLASS = YourWarmer
  3. That's it — the CLI will find it automatically.
"""
from __future__ import annotations

import importlib
import logging
import os
import pkgutil

from synthetic.warmers.base import BaseWarmer

logger = logging.getLogger(__name__)

WARMERS = {}

_warmers_dir = os.path.dirname(__file__)
for _, name, _ in pkgutil.iter_modules([_warmers_dir]):
    if name.startswith("_") or name == "base":
        continue
    try:
        mod = importlib.import_module(f"synthetic.warmers.{name}")
        if hasattr(mod, "WARMER_ID") and hasattr(mod, "WARMER_CLASS"):
            WARMERS[mod.WARMER_ID] = mod.WARMER_CLASS
    except Exception as e:
        logger.warning("Failed to load warmer '%s': %s", name, e)
