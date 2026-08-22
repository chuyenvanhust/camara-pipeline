# pipeline/modules/shared/metrics.py
import logging
from typing import Dict

logger = logging.getLogger(__name__)

class ModuleMetrics:
    def __init__(self, name: str):
        self.name = name
        self.counters: Dict[str, int] = {
            "processed": 0,
            "success": 0,
            "ignored": 0,
            "events_detected": 0,
            "notifications_sent": 0,
            "notifications_failed": 0,
            "errors": 0,
        }

    def increment(self, metric: str, amount: int = 1):
        if metric in self.counters:
            self.counters[metric] += amount

    def get(self, metric: str) -> int:
        return self.counters.get(metric, 0)

    def log_summary(self):
        logger.info(f"[{self.name} Metrics Summary] {self.counters}")
