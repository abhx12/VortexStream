"""Streaming Query Q3: Network metrics window aggregation."""

from __future__ import annotations

from datetime import datetime, timezone
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from qoe.common.config import get_config
from qoe.common.hbase_keys import qoe_key
from qoe.streaming.q2_qoe_aggregator import _format_epoch_s, _parse_ts_to_epoch_s, get_event_grains
from qoe.streaming.sinks_hbase import write_qoe_metrics_to_hbase
from qoe.streaming.sinks_warehouse import write_gold_network_5m


def compute_network_window_aggregates(
    network_records: List[Dict[str, Any]],
    window_s: int = 300,
    slide_s: int = 60,
    config: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """Compute multi-grain windowed network metrics (cf 'n')."""
    if not network_records:
        return []

    buckets: Dict[Tuple[int, str, str], Dict[str, Any]] = {}

    for r in network_records:
        ts_s = _parse_ts_to_epoch_s(r.get("ts"))
        if ts_s <= 0.0:
            continue

        tp = r.get("throughput_kbps")
        rtt = r.get("rtt_ms")
        loss = r.get("packet_loss_pct")

        first_w_end = int(math.floor(ts_s / slide_s) + 1) * slide_s
        window_ends = [
            w for w in range(first_w_end, first_w_end + window_s, slide_s)
            if (w - window_s) <= ts_s < w
        ]

        grains = get_event_grains(r)

        for w_end in window_ends:
            for grain, dimkey in grains:
                bkey = (w_end, grain, dimkey)
                if bkey not in buckets:
                    buckets[bkey] = {
                        "window_end_s": w_end,
                        "grain": grain,
                        "dimkey": dimkey,
                        "tp_sum": 0.0,
                        "tp_count": 0,
                        "rtt_sum": 0.0,
                        "rtt_count": 0,
                        "loss_sum": 0.0,
                        "loss_count": 0,
                        "samples": 0,
                    }

                b = buckets[bkey]
                b["samples"] += 1
                if tp is not None:
                    b["tp_sum"] += float(tp)
                    b["tp_count"] += 1
                if rtt is not None:
                    b["rtt_sum"] += float(rtt)
                    b["rtt_count"] += 1
                if loss is not None:
                    b["loss_sum"] += float(loss)
                    b["loss_count"] += 1

    out_rows: List[Dict[str, Any]] = []
    now_iso = _format_epoch_s(datetime.now(timezone.utc).timestamp())

    for (w_end, grain, dimkey), b in sorted(buckets.items()):
        avg_tp = (b["tp_sum"] / b["tp_count"]) if b["tp_count"] > 0 else 0.0
        avg_rtt = (b["rtt_sum"] / b["rtt_count"]) if b["rtt_count"] > 0 else 0.0
        avg_loss = (b["loss_sum"] / b["loss_count"]) if b["loss_count"] > 0 else 0.0

        w_end_iso = _format_epoch_s(float(w_end))
        w_start_iso = _format_epoch_s(float(w_end - window_s))
        rev_key = qoe_key(grain, dimkey, int(w_end // 60))

        row: Dict[str, Any] = {
            "row_key": rev_key,
            "grain": grain,
            "dimkey": dimkey,
            "window_start": w_start_iso,
            "window_end": w_end_iso,
            "updated_at": now_iso,
            "avg_throughput_kbps": round(avg_tp, 1),
            "avg_rtt_ms": round(avg_rtt, 1),
            "avg_packet_loss_pct": round(avg_loss, 2),
            "net_samples": b["samples"],
        }
        out_rows.append(row)

    return out_rows


def run_q3_network_aggregator(
    input_records: Optional[List[Dict[str, Any]]] = None,
    window_s: int = 300,
    slide_s: int = 60,
    warehouse_dir: Optional[str | Path] = None,
    config: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """Run Q3 network aggregation query in batch or streaming mode."""
    cfg = config or get_config()

    if input_records is not None:
        rows = compute_network_window_aggregates(
            input_records,
            window_s=window_s,
            slide_s=slide_s,
            config=cfg,
        )

        h_cfg = cfg.get("hbase", {})
        write_qoe_metrics_to_hbase(
            rows,
            host=h_cfg.get("host", "hbase"),
            port=int(h_cfg.get("port", 9090)),
            batch_size=int(h_cfg.get("batch_size", 500)),
        )

        if warehouse_dir and rows:
            write_gold_network_5m(rows, warehouse_dir=warehouse_dir)

        return rows

    return []


if __name__ == "__main__":
    run_q3_network_aggregator()
