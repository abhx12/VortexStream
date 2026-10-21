"""Live Kafka stream replayer with controlled speed, simulated clock, faults, and chaos."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
from typing import Any, Dict, List, Optional
import uuid
import duckdb
import yaml

from qoe.common.config import get_config, get_topic
from qoe.common.logging import get_logger, setup_logging
from qoe.data.make_fake_puffer import DEFAULT_DAY
from qoe.data.sim_dims import DimensionAssigner, compute_simulated_packet_loss
from qoe.producers.chaos import ChaosEngine
from qoe.producers.faults import FaultEngine
from qoe.producers.manifest import RunManifest

logger = get_logger("replay")


def make_deterministic_event_id(run_id: str, source: str, session_id: str, ts: str, seq: int = 0) -> str:
    """Generate deterministic event_id sha1 hash."""
    payload = f"{run_id}|{source}|{session_id}|{ts}|{seq}".encode("utf-8")
    return "e-" + hashlib.sha1(payload).hexdigest()[:16]


def run_replay(
    day: str = DEFAULT_DAY,
    speed: float = 10.0,
    duration_min: int = 120,
    start_offset_min: int = 0,
    max_sessions: int = 4000,
    faults_config_path: Optional[str] = "config/faults.yaml",
    chaos_config_path: Optional[str] = "config/chaos.yaml",
    seed: int = 42,
    run_id: Optional[str] = None,
    dry_run: bool = False,
    bootstrap_servers: str = "localhost:9092",
    processed_dir: Optional[str | Path] = None,
) -> RunManifest:
    """Execute live replay of processed Puffer dataset."""
    if run_id is None:
        run_id = f"run-{uuid.uuid4().hex[:8]}"

    setup_logging(run_id=run_id)
    logger.info("Initializing stream replay", extra={"run_id": run_id, "speed": speed, "seed": seed})

    if processed_dir is None:
        processed_dir = Path(f"data/processed/{day}")
    else:
        processed_dir = Path(processed_dir)

    events_pq = processed_dir / "events.parquet"
    network_pq = processed_dir / "network.parquet"

    if not events_pq.is_file() or not network_pq.is_file():
        raise FileNotFoundError(f"Missing preprocessed Parquet tables in {processed_dir}")

    con = duckdb.connect(":memory:")

    # Read events and filter to slice
    con.execute(f"CREATE VIEW pe AS SELECT * FROM read_parquet('{events_pq.as_posix()}');")
    con.execute(f"CREATE VIEW nm AS SELECT * FROM read_parquet('{network_pq.as_posix()}');")

    min_ts_result = con.execute("SELECT min(ts_orig) FROM pe").fetchone()
    if not min_ts_result or not min_ts_result[0]:
        raise ValueError("No events found in preprocessed events.parquet")
    slice_base_dt = min_ts_result[0]

    from datetime import timedelta
    slice_start_dt = slice_base_dt + timedelta(minutes=start_offset_min)
    slice_end_dt = slice_start_dt + timedelta(minutes=duration_min)

    # 1. Fetch playback events in time window
    pb_rows = con.execute(f"""
        SELECT * FROM pe
        WHERE ts_orig >= '{slice_start_dt}' AND ts_orig < '{slice_end_dt}'
        ORDER BY ts_orig
        LIMIT {max_sessions * 200};
    """).fetchall()
    pb_cols = [c[0] for c in con.execute("DESCRIBE pe").fetchall()]

    # 2. Fetch network metrics in time window
    net_rows = con.execute(f"""
        SELECT * FROM nm
        WHERE ts_orig >= '{slice_start_dt}' AND ts_orig < '{slice_end_dt}'
        ORDER BY ts_orig
        LIMIT {max_sessions * 100};
    """).fetchall()
    net_cols = [c[0] for c in con.execute("DESCRIBE nm").fetchall()]

    top_playback = get_topic("playback_events")
    top_network = get_topic("network_metrics")
    top_errors = get_topic("errors")

    # Convert to dicts
    all_events: List[Tuple[datetime, str, Dict[str, Any]]] = []
    for r in pb_rows:
        d = dict(zip(pb_cols, r))
        all_events.append((d["ts_orig"], top_playback, d))

    for r in net_rows:
        d = dict(zip(net_cols, r))
        all_events.append((d["ts_orig"], top_network, d))

    # Sort all events chronologically by ts_orig
    all_events.sort(key=lambda item: item[0])

    if not all_events:
        logger.warning("No events found in the requested slice window")

    wall_start_time = time.time()
    slice_start_epoch = slice_start_dt.timestamp() if hasattr(slice_start_dt, "timestamp") else time.time()
    time_shift = wall_start_time - slice_start_epoch

    # Load faults and chaos engines
    faults_list = []
    if faults_config_path and faults_config_path.lower() != "none" and Path(faults_config_path).is_file():
        with open(faults_config_path, "r", encoding="utf-8") as f:
            faults_data = yaml.safe_load(f) or {}
            faults_list = faults_data.get("faults", [])

    chaos_cfg = {}
    if chaos_config_path and chaos_config_path.lower() != "none" and Path(chaos_config_path).is_file():
        with open(chaos_config_path, "r", encoding="utf-8") as f:
            chaos_cfg = yaml.safe_load(f) or {}

    ledger_path = Path("data/ledger/faults.jsonl")
    fault_engine = FaultEngine(
        faults_config=faults_list,
        slice_start_dt=datetime.fromtimestamp(slice_start_epoch, tz=timezone.utc),
        time_shift=time_shift,
        run_id=run_id,
        ledger_path=ledger_path,
        seed=seed,
    )

    chaos_engine = ChaosEngine(chaos_cfg, seed=seed)
    dim_assigner = DimensionAssigner(seed=seed)
    manifest = RunManifest(run_id=run_id, speed=speed, seed=seed)

    # Setup Kafka or Dry-run outputs
    kafka_producer = None
    dry_run_files = {}
    if dry_run:
        dry_dir = Path("data/dry_run") / run_id
        dry_dir.mkdir(parents=True, exist_ok=True)
        dry_run_files[top_playback] = open(dry_dir / f"{top_playback}.jsonl", "w", encoding="utf-8")
        dry_run_files[top_network] = open(dry_dir / f"{top_network}.jsonl", "w", encoding="utf-8")
        dry_run_files[top_errors] = open(dry_dir / f"{top_errors}.jsonl", "w", encoding="utf-8")
    else:
        try:
            from confluent_kafka import Producer
            kafka_producer = Producer({
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "linger.ms": 20,
                "compression.type": "lz4",
            })
        except Exception as e:
            logger.warning(f"Could not initialize Kafka Producer: {e}. Falling back to dry-run file output.")
            dry_dir = Path("data/dry_run") / run_id
            dry_dir.mkdir(parents=True, exist_ok=True)
            dry_run_files[top_playback] = open(dry_dir / f"{top_playback}.jsonl", "w", encoding="utf-8")
            dry_run_files[top_network] = open(dry_dir / f"{top_network}.jsonl", "w", encoding="utf-8")
            dry_run_files[top_errors] = open(dry_dir / f"{top_errors}.jsonl", "w", encoding="utf-8")

    # Emission loop
    for orig_dt, topic, raw_ev in all_events:
        # Pacing
        event_epoch = orig_dt.timestamp() if hasattr(orig_dt, "timestamp") else time.time()
        elapsed_data_s = event_epoch - slice_start_epoch
        target_wall_time = wall_start_time + (elapsed_data_s / speed)
        now = time.time()
        sleep_needed = target_wall_time - now

        if sleep_needed > 0.002:
            time.sleep(min(sleep_needed, 0.05))

        # Attach dimensions
        puffer_sid = raw_ev.get("puffer_session_id", raw_ev.get("session_id", ""))
        dims = dim_assigner.assign_for_session(puffer_sid)
        raw_ev.update(dims)

        # Shift timestamp to simulated UTC
        shifted_epoch = event_epoch + time_shift
        shifted_iso = datetime.fromtimestamp(shifted_epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        raw_ev["ts"] = shifted_iso

        # Deterministic event_id
        if "event_id" not in raw_ev or not raw_ev["event_id"]:
            raw_ev["event_id"] = make_deterministic_event_id(run_id, topic, raw_ev["session_id"], shifted_iso)

        # Apply faults
        events_to_emit: List[Tuple[str, Dict[str, Any]]] = []
        if topic == top_network:
            impaired_net = fault_engine.process_network_event(raw_ev, orig_dt)
            # Add simulated packet loss
            impaired_net["packet_loss_pct"] = compute_simulated_packet_loss(
                impaired_net.get("throughput_kbps", 2000.0) or 2000.0,
                impaired_net.get("rtt_ms", 50.0) or 50.0,
                seed_key=impaired_net.get("event_id", ""),
            )
            events_to_emit.append((topic, impaired_net))
        else:
            main_pb, extra_pb = fault_engine.process_playback_event(raw_ev, orig_dt)
            if main_pb:
                events_to_emit.append((topic, main_pb))
            for extra in extra_pb:
                target_top = top_errors if extra.get("event_type") == "error" else top_playback
                events_to_emit.append((target_top, extra))

        # Apply chaos & publish
        for t_name, ev_payload in events_to_emit:
            ev, extra_ev, is_late, is_invalid, is_dirty = chaos_engine.process_event(ev_payload)

            # Emit main event
            payload_str = json.dumps(ev, default=str)
            manifest.record_event(t_name, ev["ts"], is_duplicate=False, is_late=is_late, is_invalid=is_invalid, is_dirty=is_dirty)

            if kafka_producer:
                kafka_producer.produce(
                    t_name,
                    key=ev.get("session_id", "").encode("utf-8"),
                    value=payload_str.encode("utf-8"),
                    headers=[("run_id", run_id.encode("utf-8")), ("source", "replay".encode("utf-8"))],
                )
            elif t_name in dry_run_files:
                dry_run_files[t_name].write(payload_str + "\n")

            # Emit duplicate if injected
            if extra_ev:
                manifest.record_event(t_name, extra_ev["ts"], is_duplicate=True)
                dup_str = json.dumps(extra_ev, default=str)
                if kafka_producer:
                    kafka_producer.produce(
                        t_name,
                        key=extra_ev.get("session_id", "").encode("utf-8"),
                        value=dup_str.encode("utf-8"),
                    )
                elif t_name in dry_run_files:
                    dry_run_files[t_name].write(dup_str + "\n")

    if kafka_producer:
        kafka_producer.flush()

    for f in dry_run_files.values():
        f.close()

    manifest_path = manifest.save()
    logger.info("Replay completed successfully", extra={"manifest": str(manifest_path)})
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Live Kafka replay producer for Stanford Puffer dataset")
    parser.add_argument("--day", default=DEFAULT_DAY, help="Day identifier")
    parser.add_argument("--speed", type=float, default=10.0, help="Replay speed multiplier")
    parser.add_argument("--duration", type=int, default=120, help="Slice duration in minutes")
    parser.add_argument("--start-offset", type=int, default=0, help="Start offset in minutes")
    parser.add_argument("--max-sessions", type=int, default=4000, help="Max sessions to replay")
    parser.add_argument("--faults", default="config/faults.yaml", help="Path to faults.yaml or 'none'")
    parser.add_argument("--chaos", default="config/chaos.yaml", help="Path to chaos.yaml or 'none'")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--run-id", default=None, help="Custom run identifier")
    parser.add_argument("--dry-run", action="store_true", help="Write JSONL files instead of emitting to Kafka")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    args = parser.parse_args()

    run_replay(
        day=args.day,
        speed=args.speed,
        duration_min=args.duration,
        start_offset_min=args.start_offset,
        max_sessions=args.max_sessions,
        faults_config_path=args.faults,
        chaos_config_path=args.chaos,
        seed=args.seed,
        run_id=args.run_id,
        dry_run=args.dry_run,
        bootstrap_servers=args.bootstrap,
    )
    return 0


if __name__ == "__main__":
    main()
