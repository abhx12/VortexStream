"""Data-quality chaos injector: duplicate, late, invalid, and dirty dimensions."""

from __future__ import annotations


class ChaosEngine:
    """Injects synthetic data-quality degradation into event streams."""

    def __init__(self, chaos_config: dict):
        self.config = chaos_config
