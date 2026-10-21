#!/usr/bin/env python3
"""Kafka topic creator with partition specifications and compaction policies."""

from __future__ import annotations

import argparse
import sys
from typing import Dict, List, Optional


TOPIC_SPECS = [
    ("playback-events", 6, {}),
    ("network-metrics", 6, {}),
    ("errors", 3, {}),
    ("playback-clean", 6, {}),
    ("network-clean", 6, {}),
    ("session-facts", 6, {}),
    ("alerts", 1, {}),
    ("dlq", 1, {}),
    ("predictions", 3, {}),
]

REF_TOPIC_SPECS = [
    ("ref-content", 1, {"cleanup.policy": "compact"}),
    ("ref-users", 1, {"cleanup.policy": "compact"}),
]


def create_topics(bootstrap_servers: str, with_ref: bool = False) -> None:
    """Create project Kafka topics idempotently."""
    specs = list(TOPIC_SPECS)
    if with_ref:
        specs.extend(REF_TOPIC_SPECS)

    try:
        from confluent_kafka.admin import AdminClient, NewTopic
    except ImportError:
        print("[WARN] confluent-kafka not installed in current environment; skipping broker creation.")
        return

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    # Query existing topics
    try:
        cluster_metadata = admin.list_topics(timeout=5)
        existing = set(cluster_metadata.topics.keys())
    except Exception as e:
        print(f"[WARN] Could not connect to Kafka broker at {bootstrap_servers}: {e}")
        return

    new_topics = []
    for name, partitions, cfg in specs:
        if name in existing:
            print(f"[EXISTS] Topic {name}")
        else:
            new_topics.append(NewTopic(name, num_partitions=partitions, replication_factor=1, config=cfg))

    if new_topics:
        futures = admin.create_topics(new_topics)
        for topic, future in futures.items():
            try:
                future.result()
                print(f"[CREATED] Topic {topic}")
            except Exception as e:
                print(f"[ERROR] Failed to create topic {topic}: {e}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Create Kafka topics")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--with-ref", action="store_true", help="Also create compacted reference data topics")
    args = parser.parse_args()

    create_topics(args.bootstrap, args.with_ref)
    return 0


if __name__ == "__main__":
    sys.exit(main())
