"""Unit tests for Phase 1 HBase row key formatting and parsing."""

from __future__ import annotations

import pytest
from qoe.common.hbase_keys import (
    alert_key,
    parse_alert_key,
    parse_qoe_key,
    parse_rev_minute,
    parse_salted_key,
    qoe_key,
    rev_minute,
    salted_key,
)


def test_salted_key_deterministic_and_bounds():
    """Verify salt calculation is deterministic and within [0, 16)."""
    session_id = "test-session-12345-0"
    k1 = salted_key(session_id, salt_buckets=16)
    k2 = salted_key(session_id, salt_buckets=16)
    assert k1 == k2, "Salted key must be deterministic"

    salt, parsed_sid = parse_salted_key(k1)
    assert 0 <= salt < 16, f"Salt {salt} out of bounds [0, 16)"
    assert parsed_sid == session_id

    # Test distribution across different session IDs
    salts = {parse_salted_key(salted_key(f"sess-{i}-0"))[0] for i in range(100)}
    assert len(salts) > 8, f"Expected good hash distribution, got {len(salts)} unique salts"


def test_rev_minute_ordering_and_length():
    """Verify rev_minute is 19 digits and orders newest first lexicographically."""
    m1 = 29876540  # earlier minute
    m2 = 29876550  # later minute

    r1 = rev_minute(m1)
    r2 = rev_minute(m2)

    assert len(r1) == 19, f"Expected 19 digits, got {len(r1)}"
    assert len(r2) == 19, f"Expected 19 digits, got {len(r2)}"

    # Newest (m2) must sort before older (m1) lexicographically
    assert r2 < r1, "Newer timestamp must produce smaller lexicographical string in reversed minute"

    # Round trip
    assert parse_rev_minute(r1) == m1
    assert parse_rev_minute(r2) == m2


def test_qoe_key_round_trip():
    """Verify qoe_key formatting and round-trip parsing."""
    grain = "city_isp"
    dimkey = "Chennai|Airtel"
    epoch_min = 29876543

    key = qoe_key(grain, dimkey, epoch_min)
    assert key.startswith("city_isp#Chennai|Airtel#")

    parsed_grain, parsed_dimkey, parsed_min = parse_qoe_key(key)
    assert parsed_grain == grain
    assert parsed_dimkey == dimkey
    assert parsed_min == epoch_min


def test_alert_key_round_trip():
    """Verify alert_key formatting and round-trip parsing."""
    alert_id = "alt-9876-F1"
    epoch_min = 29876543

    key = alert_key(epoch_min, alert_id)
    parsed_min, parsed_id = parse_alert_key(key)

    assert parsed_min == epoch_min
    assert parsed_id == alert_id
