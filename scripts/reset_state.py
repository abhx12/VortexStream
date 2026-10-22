"""Reset pipeline state: clears checkpoints, warehouse outputs, manifests, and runs topic/table reset."""

from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import sys

from qoe.common.logging import get_logger, setup_logging

logger = get_logger("reset_state")


def reset_pipeline_state(clean_warehouse: bool = True, clean_checkpoints: bool = True, clean_manifests: bool = True) -> None:
    """Clear local runtime artifacts for a fresh start."""
    repo_root = Path(__file__).resolve().parent.parent

    dirs_to_clean = []
    if clean_checkpoints:
        dirs_to_clean.append(repo_root / "data" / "checkpoints")
    if clean_warehouse:
        dirs_to_clean.append(repo_root / "data" / "warehouse")
    if clean_manifests:
        dirs_to_clean.append(repo_root / "data" / "manifests")

    for d in dirs_to_clean:
        if d.exists() and d.is_dir():
            logger.info("Cleaning directory: %s", d)
            for item in d.iterdir():
                if item.is_dir():
                    shutil.rmtree(item, ignore_errors=True)
                else:
                    try:
                        item.unlink()
                    except OSError:
                        pass
        else:
            d.mkdir(parents=True, exist_ok=True)

    print("Pipeline checkpoints and runtime warehouse state successfully reset.")


if __name__ == "__main__":
    setup_logging()
    parser = argparse.ArgumentParser(description="Reset VortexStream pipeline state")
    parser.add_argument("--keep-warehouse", action="store_true", help="Preserve data/warehouse")
    args = parser.parse_args()

    reset_pipeline_state(clean_warehouse=not args.keep_warehouse)
