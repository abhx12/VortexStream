"""Schema loading and JSON validation utilities."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional
import jsonschema


def get_schema_dir() -> Path:
    """Return path to schemas directory."""
    current = Path(__file__).resolve().parent
    for _ in range(5):
        cand = current / "schemas"
        if cand.is_dir():
            return cand
        current = current.parent
    return Path.cwd() / "schemas"


def load_schema(schema_name: str) -> Dict[str, Any]:
    """Load JSON schema by name."""
    s_dir = get_schema_dir()
    p = s_dir / schema_name
    if not p.is_file():
        p = s_dir / f"{schema_name}.schema.json"
    if not p.is_file():
        raise FileNotFoundError(f"Schema not found: {schema_name} in {s_dir}")
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)
