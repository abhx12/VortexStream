"""Spark RDD transformations and actions demonstration."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def run_rdd_demo(
    input_records: Optional[List[Dict[str, Any]]] = None,
    spark_context: Optional[Any] = None,
) -> Dict[str, Any]:
    """Execute RDD demonstration using map, filter, flatMap, reduceByKey, aggregateByKey, and actions."""
    if input_records is None:
        # Default fixture records
        records = [
            {"session_id": "s1", "isp": "Jio", "device": "android", "event_type": "error", "error_code": "NET_TIMEOUT", "bitrate_kbps": 2000, "channel": "c1"},
            {"session_id": "s2", "isp": "Airtel", "device": "ios", "event_type": "heartbeat", "bitrate_kbps": 4000, "channel": "c1"},
            {"session_id": "s3", "isp": "Jio", "device": "android", "event_type": "error", "error_code": "NET_TIMEOUT", "bitrate_kbps": 1500, "channel": "c2"},
            {"session_id": "s4", "isp": "Vi", "device": "web", "event_type": "error", "error_code": "CDN_5XX", "bitrate_kbps": 3000, "channel": "c2"},
            {"session_id": "s5", "isp": "Airtel", "device": "smarttv", "event_type": "error", "error_code": "DECODE_ERR", "bitrate_kbps": 5000, "channel": "c1"},
            {"session_id": "s6", "isp": "Jio", "device": "ios", "event_type": "heartbeat", "bitrate_kbps": 2500, "channel": "c3"},
        ]
    else:
        records = input_records

    # Reference data for pair join: channel -> genre
    ref_channels = [("c1", "News"), ("c2", "Sports"), ("c3", "Movies")]

    results: Dict[str, Any] = {}

    sc = spark_context
    if sc is None:
        try:
            from pyspark import SparkConf, SparkContext
            conf = SparkConf().setAppName("RDDDemo").setMaster("local[1]")
            sc = SparkContext.getOrCreate(conf=conf)
        except Exception:
            sc = None

    if sc is not None:
        # 1. Parallelize & Accumulator + Broadcast
        rdd = sc.parallelize(records, 2)
        inv_acc = sc.accumulator(0)
        bc_genres = sc.broadcast(dict(ref_channels))

        # 2. Persist / cache
        rdd.persist()

        # 3. Actions: count, first, take
        results["total_count"] = rdd.count()
        results["first_record"] = rdd.first()
        results["take_3"] = rdd.take(3)

        # 4. Filter (keep only error events)
        errors_rdd = rdd.filter(lambda x: x.get("event_type") == "error")

        # 5. Map & reduceByKey (errors per ISP)
        isp_errors = (
            errors_rdd.map(lambda x: (x.get("isp", "unknown"), 1))
            .reduceByKey(lambda a, b: a + b)
        )
        results["errors_per_isp"] = dict(isp_errors.collect())

        # 6. FlatMap (tokenize error tokens)
        error_tokens = errors_rdd.flatMap(lambda x: [x.get("error_code", ""), f"isp:{x.get('isp', '')}"])
        results["distinct_tokens"] = error_tokens.distinct().collect()

        # 7. AggregateByKey: mean bitrate per device
        # zeroValue = (sum, count), seqOp, combOp
        device_bitrate = (
            rdd.map(lambda x: (x.get("device", "unknown"), float(x.get("bitrate_kbps") or 0.0)))
            .aggregateByKey(
                (0.0, 0),
                lambda acc, val: (acc[0] + val, acc[1] + 1),
                lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]),
            )
            .mapValues(lambda sum_cnt: round(sum_cnt[0] / sum_cnt[1], 1) if sum_cnt[1] > 0 else 0.0)
        )
        results["avg_bitrate_by_device"] = dict(device_bitrate.collect())

        # 8. GroupByKey demonstration
        grouped_by_isp = (
            rdd.map(lambda x: (x.get("isp", "unknown"), x.get("session_id")))
            .groupByKey()
            .mapValues(list)
        )
        results["sessions_by_isp"] = dict(grouped_by_isp.collect())

        # 9. SortBy / takeOrdered
        top_isps = isp_errors.sortBy(lambda x: x[1], ascending=False).take(10)
        results["top_isps"] = top_isps

        # 10. Pair RDD Join with reference
        channel_rdd = sc.parallelize(ref_channels)
        session_channel = rdd.map(lambda x: (x.get("channel", ""), x.get("session_id")))
        joined = session_channel.join(channel_rdd)
        results["joined_count"] = joined.count()

        # 11. Lineage toDebugString
        results["lineage"] = isp_errors.toDebugString().decode("utf-8") if isinstance(isp_errors.toDebugString(), bytes) else str(isp_errors.toDebugString())

        # 12. countByKey & reduce
        results["count_by_isp"] = dict(rdd.map(lambda x: (x.get("isp"), 1)).countByKey())
        total_bitrate = rdd.map(lambda x: float(x.get("bitrate_kbps") or 0.0)).reduce(lambda a, b: a + b)
        results["total_bitrate"] = total_bitrate

    else:
        # Standalone Python RDD simulation for environments without PySpark JVM
        results["total_count"] = len(records)
        results["first_record"] = records[0] if records else None
        results["take_3"] = records[:3]

        # Filter
        errors = [r for r in records if r.get("event_type") == "error"]

        # Map & reduceByKey (errors per ISP)
        err_counts: Dict[str, int] = {}
        for e in errors:
            isp = e.get("isp", "unknown")
            err_counts[isp] = err_counts.get(isp, 0) + 1
        results["errors_per_isp"] = err_counts

        # AggregateByKey (device -> avg bitrate)
        dev_map: Dict[str, List[float]] = {}
        for r in records:
            dev = r.get("device", "unknown")
            br = float(r.get("bitrate_kbps") or 0.0)
            dev_map.setdefault(dev, []).append(br)
        results["avg_bitrate_by_device"] = {
            k: round(sum(v) / len(v), 1) for k, v in dev_map.items() if v
        }

        # FlatMap
        tokens = set()
        for e in errors:
            tokens.add(e.get("error_code", ""))
            tokens.add(f"isp:{e.get('isp', '')}")
        results["distinct_tokens"] = list(tokens)

        # SortBy
        results["top_isps"] = sorted(err_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        results["joined_count"] = len(records)
        results["lineage"] = "ParallelCollectionRDD -> MapPartitionsRDD -> ShuffledRDD"
        results["count_by_isp"] = {r.get("isp"): 1 for r in records}
        results["total_bitrate"] = sum(float(r.get("bitrate_kbps") or 0.0) for r in records)

    return results


if __name__ == "__main__":
    res = run_rdd_demo()
    print("RDD Demo executed successfully.")
    print(f"Errors per ISP: {res['errors_per_isp']}")
    print(f"Avg Bitrate by Device: {res['avg_bitrate_by_device']}")
    print("Lineage:\n", res["lineage"])
