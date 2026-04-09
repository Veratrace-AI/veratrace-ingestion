"""
Base warmer interface — creates real activity in vendor systems
so the ingestion pipeline has actual data to pull.

Each platform implements create_activity() and verify_activity().
The warm() method handles rate limiting, counting, and logging.
"""
from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class WarmResult:
    """Result of a warming run."""
    created: int = 0
    verified: int = 0
    failed: int = 0
    activity_ids: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


class BaseWarmer(ABC):
    """
    Abstract base for creating real activity in vendor systems.

    Subclasses implement the vendor-specific create/verify logic.
    The warm() method is reusable across all platforms.
    """

    def __init__(self, credentials: dict, external_identity: dict):
        self.credentials = credentials
        self.external_identity = external_identity

    @abstractmethod
    def validate_access(self) -> bool:
        """Verify the credentials have write permissions."""
        ...

    @abstractmethod
    def create_activity(self, scenario_config: dict) -> dict:
        """
        Create one unit of activity in the vendor system.
        Returns {"id": "vendor-native-id", "type": "chat|task|call", ...}
        """
        ...

    @abstractmethod
    def verify_activity(self, activity_id: str) -> bool:
        """
        Confirm the activity produced a record the connector can ingest.
        Called after a delay to allow the vendor to generate the record.
        """
        ...

    def warm(
        self,
        count: int,
        scenario_config: dict | None = None,
        delay_between: float = 2.0,
        verify_delay: float = 15.0,
    ) -> WarmResult:
        """
        Create N activities with rate limiting, then verify they're queryable.

        Args:
            count: Number of activities to create
            scenario_config: Platform-specific config (customer profiles, etc.)
            delay_between: Seconds between creates (rate limiting)
            verify_delay: Seconds to wait before verifying (vendor processing time)
        """
        config = scenario_config or {}
        result = WarmResult()

        logger.info("Warming: creating %d activities...", count)

        # Phase 1: Create activities
        for i in range(count):
            try:
                activity = self.create_activity(config)
                result.activity_ids.append(activity["id"])
                result.created += 1
                logger.info(
                    "  Created %d/%d: %s (%s)",
                    i + 1, count, activity["id"][:12], activity.get("type", "unknown"),
                )
            except Exception as e:
                result.failed += 1
                result.errors.append(str(e)[:200])
                logger.error("  Failed %d/%d: %s", i + 1, count, str(e)[:100])

            if i < count - 1:
                time.sleep(delay_between)

        if not result.activity_ids:
            logger.error("No activities created — nothing to verify")
            return result

        # Phase 2: Wait for vendor processing
        logger.info("Waiting %.0fs for vendor to process records...", verify_delay)
        time.sleep(verify_delay)

        # Phase 3: Verify activities are queryable
        logger.info("Verifying %d activities...", len(result.activity_ids))
        for activity_id in result.activity_ids:
            try:
                if self.verify_activity(activity_id):
                    result.verified += 1
                else:
                    logger.warning("  Not found: %s", activity_id[:12])
            except Exception as e:
                logger.error("  Verify error for %s: %s", activity_id[:12], str(e)[:100])

        logger.info(
            "Warming complete: %d created, %d verified, %d failed",
            result.created, result.verified, result.failed,
        )
        return result
