"""Real-time alerting rules evaluation and state machine."""

from __future__ import annotations


class AlertEvaluator:
    """Evaluates rules R1-R4 against aggregate metrics."""

    def __init__(self, alerts_config: dict):
        self.config = alerts_config
