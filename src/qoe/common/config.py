"""Configuration loader with environment variable overrides and typed dot-access."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Iterator, Mapping, Optional
import yaml


class ConfigDict(dict):
    """Dictionary subclass providing dot-notation attribute access."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for key, value in list(self.items()):
            self[key] = self._wrap(value)

    @classmethod
    def _wrap(cls, val: Any) -> Any:
        if isinstance(val, dict) and not isinstance(val, ConfigDict):
            return cls(val)
        if isinstance(val, list):
            return [cls._wrap(item) for item in val]
        return val

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(f"'ConfigDict' object has no attribute '{name}'")

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = self._wrap(value)

    def __delattr__(self, name: str) -> None:
        try:
            del self[name]
        except KeyError:
            raise AttributeError(f"'ConfigDict' object has no attribute '{name}'")


def _find_config_dir(custom_path: Optional[str | Path] = None) -> Path:
    """Locate config directory."""
    if custom_path:
        p = Path(custom_path)
        if p.is_dir():
            return p.resolve()
        raise FileNotFoundError(f"Specified config path does not exist: {custom_path}")

    env_path = os.environ.get("CONFIG_DIR")
    if env_path:
        p = Path(env_path)
        if p.is_dir():
            return p.resolve()

    # Search upward from this file
    current = Path(__file__).resolve().parent
    for _ in range(5):
        candidate = current / "config"
        if candidate.is_dir() and (candidate / "config.yaml").is_file():
            return candidate
        if (current / "config.yaml").is_file():
            return current
        current = current.parent

    # Fallback to current working directory
    cwd_candidate = Path.cwd() / "config"
    if cwd_candidate.is_dir():
        return cwd_candidate

    raise FileNotFoundError("Could not find configuration directory containing config.yaml")


def _coerce_env_value(val: str) -> Any:
    """Coerce string environment variable to appropriate Python type."""
    val = val.strip()
    lower = val.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False
    if lower in ("none", "null"):
        return None

    if (val.startswith("{") and val.endswith("}")) or (val.startswith("[") and val.endswith("]")):
        try:
            return json.loads(val)
        except Exception:
            pass

    try:
        return int(val)
    except ValueError:
        pass

    try:
        return float(val)
    except ValueError:
        pass

    return val


def _find_matching_key(curr: Dict[str, Any], part: str) -> str:
    """Find key in dictionary matching case-insensitively or return lowercase."""
    part_lower = part.lower()
    for k in curr.keys():
        if k.lower() == part_lower:
            return k
    return part_lower


def _apply_env_overrides(data: Dict[str, Any], prefix: str = "QOE__") -> None:
    """Apply environment overrides matching QOE__section__key pattern (case-insensitive)."""
    prefix_upper = prefix.upper()
    for env_k, env_v in os.environ.items():
        if not env_k.upper().startswith(prefix_upper):
            continue
        parts = env_k[len(prefix):].split("__")
        if not parts or any(not p for p in parts):
            continue

        coerced = _coerce_env_value(env_v)
        curr = data
        for part in parts[:-1]:
            matched_key = _find_matching_key(curr, part)
            if matched_key not in curr or not isinstance(curr[matched_key], dict):
                curr[matched_key] = {}
            curr = curr[matched_key]

        final_key = _find_matching_key(curr, parts[-1])
        curr[final_key] = coerced



class AppConfig(ConfigDict):
    """Application configuration with validation and typed access."""

    def validate(self) -> None:
        """Validate config consistency."""
        # 1. QoE weights sum to 100
        if "qoe" in self and "weights" in self.qoe:
            weights = self.qoe.weights
            total_weight = sum(weights.values())
            if abs(total_weight - 100.0) > 1e-4:
                raise ValueError(f"QoE weights must sum to 100, got {total_weight} ({dict(weights)})")

        # 2. Thresholds sanity checks
        if "alerts" in self:
            alerts = self.alerts
            if "min_sessions" in alerts and alerts.min_sessions <= 0:
                raise ValueError("alerts.min_sessions must be positive")
            if "rebuffer_ratio_threshold" in alerts:
                if not (0 <= alerts.rebuffer_ratio_threshold <= 1):
                    raise ValueError("alerts.rebuffer_ratio_threshold must be between 0 and 1")
            if "vsf_rate_threshold" in alerts:
                if not (0 <= alerts.vsf_rate_threshold <= 1):
                    raise ValueError("alerts.vsf_rate_threshold must be between 0 and 1")
            if "error_rate_threshold" in alerts:
                if not (0 <= alerts.error_rate_threshold <= 1):
                    raise ValueError("alerts.error_rate_threshold must be between 0 and 1")

        # 3. Replay speed check
        if "replay" in self and "speed" in self.replay:
            if self.replay.speed <= 0:
                raise ValueError("replay.speed must be > 0")


def load_config(config_dir: Optional[str | Path] = None) -> AppConfig:
    """Load configuration from config directory, applying env overrides and validation."""
    c_dir = _find_config_dir(config_dir)

    raw_data: Dict[str, Any] = {}

    main_yaml = c_dir / "config.yaml"
    if main_yaml.is_file():
        with open(main_yaml, "r", encoding="utf-8") as f:
            loaded = yaml.safe_load(f) or {}
            raw_data.update(loaded)

    # Load additional YAMLs under their stem if not already loaded
    for extra_file in c_dir.glob("*.yaml"):
        stem = extra_file.stem
        if stem == "config":
            continue
        with open(extra_file, "r", encoding="utf-8") as f:
            extra_data = yaml.safe_load(f) or {}
            # If root key is already stem or faults, attach accordingly
            raw_data[stem] = extra_data

    # Apply environment overrides
    _apply_env_overrides(raw_data)

    cfg = AppConfig(raw_data)
    cfg.validate()
    return cfg


_CACHED_CONFIG: Optional[AppConfig] = None


def get_config(reload: bool = False, config_dir: Optional[str | Path] = None) -> AppConfig:
    """Return cached AppConfig singleton or reload."""
    global _CACHED_CONFIG
    if _CACHED_CONFIG is None or reload:
        _CACHED_CONFIG = load_config(config_dir)
    return _CACHED_CONFIG


def get_topic(topic_name: str) -> str:
    """Return topic name from configuration."""
    cfg = get_config()
    topics = cfg.get("kafka", {}).get("topics", {})
    return str(topics.get(topic_name, topic_name.replace("_", "-")))

