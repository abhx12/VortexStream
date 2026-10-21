"""Test suite configuration and fixtures."""

import sys
from pathlib import Path

# Ensure src and repo root are on sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = REPO_ROOT / "src"

for p in [str(SRC_DIR), str(REPO_ROOT)]:
    if p not in sys.path:
        sys.path.insert(0, p)

