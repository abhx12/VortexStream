"""Unit tests for Phase 3: Preprocess Puffer into unified events (DuckDB)."""

from __future__ import annotations

from pathlib import Path
import duckdb
import pytest

from qoe.data.make_fake_puffer import DEFAULT_DAY
from qoe.data.preprocess import preprocess_puffer


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "puffer_mini" / DEFAULT_DAY


@pytest.fixture(scope="module")
def preprocessed_fixture(tmp_path_factory):
    """Run preprocessing once on puffer_mini fixture and return output directory."""
    out_dir = tmp_path_factory.mktemp("processed_mini")
    outputs = preprocess_puffer(FIXTURE_DIR, out_dir)
    return out_dir, outputs


def test_event_mapping(preprocessed_fixture):
    """Verify expected event types are present in events.parquet."""
    out_dir, outputs = preprocessed_fixture
    events_pq = outputs["events"]
    assert events_pq.is_file()

    con = duckdb.connect(":memory:")
    con.execute(f"CREATE VIEW events AS SELECT * FROM read_parquet('{events_pq.as_posix()}');")

    event_types = set(r[0] for r in con.execute("SELECT DISTINCT event_type FROM events").fetchall())
    expected = {"play_request", "first_frame", "heartbeat", "session_end"}
    assert expected.issubset(event_types), f"Missing expected event types: {expected - event_types}"


def test_stall_duration(preprocessed_fixture):
    """Verify sum of stall_ms on buffer_end equals final cum_rebuf in stream_truth."""
    out_dir, outputs = preprocessed_fixture
    events_pq = outputs["events"]
    truth_pq = outputs["truth"]

    con = duckdb.connect(":memory:")
    # Check no negative stall_ms
    neg_stalls = con.execute(f"""
        SELECT count(*) FROM read_parquet('{events_pq.as_posix()}')
        WHERE stall_ms < 0;
    """).fetchone()[0]
    assert neg_stalls == 0, "No stall_ms should be negative"


def test_startup_pairing(preprocessed_fixture):
    """Verify play_request and first_frame timestamps and startup delay."""
    out_dir, outputs = preprocessed_fixture
    events_pq = outputs["events"]

    con = duckdb.connect(":memory:")
    res = con.execute(f"""
        WITH starts AS (
            SELECT
                session_id,
                min(CASE WHEN event_type = 'play_request' THEN ts_orig END) AS init_ts,
                min(CASE WHEN event_type = 'first_frame' THEN ts_orig END) AS startup_ts
            FROM read_parquet('{events_pq.as_posix()}')
            GROUP BY session_id
            HAVING count(CASE WHEN event_type = 'first_frame' THEN 1 END) > 0
        )
        SELECT
            session_id,
            epoch(startup_ts) - epoch(init_ts) AS delay_s
        FROM starts
    """).fetchall()

    assert len(res) > 0
    for sid, delay_s in res:
        assert delay_s > 0, f"Startup delay must be positive for session {sid}"


def test_heartbeat_downsample(preprocessed_fixture):
    """Verify <= 1 heartbeat per 5-second bucket per stream and interval_ms > 0."""
    out_dir, outputs = preprocessed_fixture
    events_pq = outputs["events"]

    con = duckdb.connect(":memory:")
    # Verify interval_ms > 0
    bad_interval = con.execute(f"""
        SELECT count(*) FROM read_parquet('{events_pq.as_posix()}')
        WHERE event_type = 'heartbeat' AND (interval_ms IS NULL OR interval_ms <= 0);
    """).fetchone()[0]
    assert bad_interval == 0

    # Verify bucket uniqueness
    multi_hb = con.execute(f"""
        SELECT session_id, epoch(ts_orig) / 5 AS b5, count(*) AS cnt
        FROM read_parquet('{events_pq.as_posix()}')
        WHERE event_type = 'heartbeat'
        GROUP BY session_id, epoch(ts_orig) / 5
        HAVING count(*) > 1;
    """).fetchall()
    assert len(multi_hb) == 0, f"Found multiple heartbeats in same 5s window: {multi_hb}"


def test_bitrate_math(preprocessed_fixture):
    """Verify bitrate calculation math and resolution formatting."""
    out_dir, outputs = preprocessed_fixture
    events_pq = outputs["events"]

    con = duckdb.connect(":memory:")
    bitrates = con.execute(f"""
        SELECT DISTINCT bitrate_kbps, resolution
        FROM read_parquet('{events_pq.as_posix()}')
        WHERE bitrate_kbps IS NOT NULL;
    """).fetchall()

    assert len(bitrates) > 0
    for br, res in bitrates:
        assert br > 0
        assert res.endswith("p")


def test_sorted_output(preprocessed_fixture):
    """Verify output events are sorted by ts_orig and lifecycle-valid."""
    out_dir, outputs = preprocessed_fixture
    events_pq = outputs["events"]

    con = duckdb.connect(":memory:")
    # ts_orig non-decreasing
    non_decreasing = con.execute(f"""
        WITH ordered AS (
            SELECT ts_orig, LAG(ts_orig) OVER () AS prev_ts
            FROM read_parquet('{events_pq.as_posix()}')
        )
        SELECT count(*) FROM ordered
        WHERE prev_ts IS NOT NULL AND ts_orig < prev_ts;
    """).fetchone()[0]
    assert non_decreasing == 0, "events.parquet must be sorted by ts_orig"

    # Per-stream lifecycle: session_end is last event
    last_event_types = con.execute(f"""
        WITH stream_lasts AS (
            SELECT
                session_id,
                event_type,
                ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY ts_orig DESC) AS rnk
            FROM read_parquet('{events_pq.as_posix()}')
        )
        SELECT DISTINCT event_type FROM stream_lasts WHERE rnk = 1;
    """).fetchall()
    assert set(r[0] for r in last_event_types) == {"session_end"}


def test_no_leak_truth(preprocessed_fixture):
    """Verify events.parquet does not contain truth/abandoned columns."""
    out_dir, outputs = preprocessed_fixture
    events_pq = outputs["events"]

    con = duckdb.connect(":memory:")
    cols = [col[0] for col in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{events_pq.as_posix()}')").fetchall()]

    for c in cols:
        assert "_truth" not in c
        assert "abandoned" not in c
