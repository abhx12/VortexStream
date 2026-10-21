#!/usr/bin/env bash
# create_topics.sh — creates Kafka topics idempotently
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
python3 scripts/create_topics.py --bootstrap "${BOOTSTRAP}" "$@" || python scripts/create_topics.py --bootstrap "${BOOTSTRAP}" "$@"
