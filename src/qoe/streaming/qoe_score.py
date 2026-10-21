"""QoE score calculation formula and penalty functions."""

from __future__ import annotations

from typing import Any, Dict, Optional
from qoe.common.config import get_config


def clip(val: float, low: float = 0.0, high: float = 1.0) -> float:
    """Clamp value between low and high."""
    return max(low, min(high, val))


def compute_qoe_score(
    metrics: Dict[str, Any],
    config: Optional[Any] = None,
) -> Dict[str, Any]:
    """Compute aggregate QoE score (0-100) and penalties using configured weights.

    Formula:
      startup_pen = clip((avg_startup_s - startup_good_s) / (startup_bad_s - startup_good_s), 0, 1)
      rebuffer_pen = clip(rebuffer_ratio / rebuffer_full_penalty_ratio, 0, 1)
      switch_pen = clip(switches_per_min / switches_per_min_full_penalty, 0, 1)
      bitrate_pen = 1.0 - hd_pct
      failure_pen = clip((vsf_rate + error_rate) / failure_full_penalty_rate, 0, 1)
      QoE = 100 - sum(weight * penalty)
    """
    cfg = config or get_config()
    qoe_cfg = cfg.get("qoe", {})

    weights = qoe_cfg.get("weights", {
        "startup": 15,
        "rebuffer": 40,
        "switch": 10,
        "bitrate": 15,
        "failure": 20,
    })

    startup_good_s = float(qoe_cfg.get("startup_good_s", 1.0))
    startup_bad_s = float(qoe_cfg.get("startup_bad_s", 8.0))
    rebuffer_full_pen = float(qoe_cfg.get("rebuffer_full_penalty_ratio", 0.10))
    switch_full_pen = float(qoe_cfg.get("switches_per_min_full_penalty", 2.0))
    failure_full_pen = float(qoe_cfg.get("failure_full_penalty_rate", 0.10))

    labels_cfg = qoe_cfg.get("labels", {"good_min": 80, "fair_min": 60})
    good_min = float(labels_cfg.get("good_min", 80))
    fair_min = float(labels_cfg.get("fair_min", 60))

    is_partial = False

    # 1. Startup penalty
    avg_startup_s = metrics.get("avg_startup_s")
    if avg_startup_s is None:
        avg_startup_ms = metrics.get("avg_startup_ms")
        if avg_startup_ms is not None:
            avg_startup_s = float(avg_startup_ms) / 1000.0

    if avg_startup_s is not None and (startup_bad_s > startup_good_s):
        startup_pen = clip(
            (float(avg_startup_s) - startup_good_s) / (startup_bad_s - startup_good_s)
        )
    else:
        startup_pen = 0.0
        if avg_startup_s is None:
            is_partial = True

    # 2. Rebuffering penalty
    rebuf_ratio = metrics.get("rebuffer_ratio")
    if rebuf_ratio is not None and rebuffer_full_pen > 0:
        rebuffer_pen = clip(float(rebuf_ratio) / rebuffer_full_pen)
    else:
        rebuffer_pen = 0.0
        if rebuf_ratio is None:
            is_partial = True

    # 3. Switches penalty
    switches_min = metrics.get("switches_per_min")
    if switches_min is not None and switch_full_pen > 0:
        switch_pen = clip(float(switches_min) / switch_full_pen)
    else:
        switch_pen = 0.0
        if switches_min is None:
            is_partial = True

    # 4. Bitrate / HD penalty
    hd_pct = metrics.get("hd_pct")
    if hd_pct is not None:
        bitrate_pen = clip(1.0 - float(hd_pct))
    else:
        bitrate_pen = 0.0
        is_partial = True

    # 5. Failure penalty (VSF + error rate)
    vsf_rate = float(metrics.get("vsf_rate") or 0.0)
    error_rate = float(metrics.get("error_rate") or 0.0)
    if failure_full_pen > 0:
        failure_pen = clip((vsf_rate + error_rate) / failure_full_pen)
    else:
        failure_pen = 0.0

    total_penalty = (
        float(weights.get("startup", 15)) * startup_pen
        + float(weights.get("rebuffer", 40)) * rebuffer_pen
        + float(weights.get("switch", 10)) * switch_pen
        + float(weights.get("bitrate", 15)) * bitrate_pen
        + float(weights.get("failure", 20)) * failure_pen
    )

    qoe_score = clip(100.0 - total_penalty, 0.0, 100.0)

    if qoe_score >= good_min:
        label = "Good"
    elif qoe_score >= fair_min:
        label = "Fair"
    else:
        label = "Poor"

    return {
        "qoe_score": round(qoe_score, 2),
        "qoe_label": label,
        "qoe_partial": is_partial,
        "startup_pen": round(startup_pen, 4),
        "rebuffer_pen": round(rebuffer_pen, 4),
        "switch_pen": round(switch_pen, 4),
        "bitrate_pen": round(bitrate_pen, 4),
        "failure_pen": round(failure_pen, 4),
    }
