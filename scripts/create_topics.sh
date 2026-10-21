#!/usr/bin/env bash
# create_topics.sh — creates Kafka topics idempotently
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
WITH_REF=false

for arg in "$@"; do
  case $arg in
    --with-ref)
      WITH_REF=true
      shift
      ;;
  esac
done

echo "Creating Kafka topics on ${BOOTSTRAP}..."
echo "Topic creation script initialized (Phase 1 task)"
