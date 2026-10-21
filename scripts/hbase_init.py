#!/usr/bin/env python3
"""HBase table initializer and schema migration script."""

from __future__ import annotations

import argparse
import sys


def init_hbase(host: str = "hbase", port: int = 9090, drop: bool = False) -> None:
    """Initialize HBase tables: session_state, qoe_metrics, alerts."""
    print(f"Connecting to HBase Thrift at {host}:{port} (drop={drop})...")
    print("HBase initialization script initialized (Phase 1 task)")


def main() -> int:
    parser = argparse.ArgumentParser(description="Initialize HBase tables")
    parser.add_argument("--host", default="hbase", help="HBase Thrift host")
    parser.add_argument("--port", type=int, default=9090, help="HBase Thrift port")
    parser.add_argument("--drop", action="store_true", help="Drop existing tables before recreating")
    args = parser.parse_args()

    init_hbase(host=args.host, port=args.port, drop=args.drop)
    return 0


if __name__ == "__main__":
    sys.exit(main())
