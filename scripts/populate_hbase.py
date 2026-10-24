# scripts/populate_hbase.py
import argparse
from datetime import datetime, timezone
import duckdb
from qoe.streaming.q1_session_tracker import run_q1_session_tracker
from qoe.streaming.q2_qoe_aggregator import compute_qoe_window_aggregates
from qoe.streaming.sinks_hbase import write_qoe_metrics_to_hbase, write_alerts_to_hbase
from qoe.streaming.alerting import process_alerts, AlertEvaluator

def main():
    parser = argparse.ArgumentParser(description="Populate HBase with streaming telemetry and optional fault injection.")
    parser.add_argument("--inject-fault", action="store_true", help="Inject Fault F1 (Chennai-Airtel throttling and rebuffering alert)")
    args = parser.parse_args()

    print("Reading processed events from Parquet...")
    con = duckdb.connect()
    events = con.execute("SELECT * FROM read_parquet('data/processed/2021-06-12T11_2021-06-13T11/events.parquet')").df().to_dict(orient='records')
    
    cities = ["Mumbai", "Delhi", "Bengaluru", "Chennai"]
    isps = ["Jio", "Airtel", "Vi", "BSNL"]
    cdns = ["Akamai", "Cloudflare", "Fastly"]
    devices = ["android", "ios", "smarttv", "web"]

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    for i, e in enumerate(events):
        e['city'] = cities[i % len(cities)]
        e['isp'] = isps[i % len(isps)]
        e['cdn'] = cdns[i % len(cdns)]
        e['device'] = devices[i % len(devices)]
        e['ts'] = now_iso
        
        # Base healthy stall
        e['stall_ms'] = 0.0

        # Inject Fault F1: Airtel in Chennai gets severe rebuffering stalls
        if args.inject_fault and e['city'] == 'Chennai' and e['isp'] == 'Airtel':
            e['stall_ms'] = 4500.0
            e['bitrate_kbps'] = 600.0

    print(f"Tracking sessions for {len(events)} events in Query Q1...")
    facts, hrows = run_q1_session_tracker(input_events=events)
    print(f"Successfully upserted {len(hrows)} sessions to HBase 'session_state'!")

    print("Computing 5-minute sliding QoE metrics across grains in Query Q2...")
    windows = compute_qoe_window_aggregates(events)

    if args.inject_fault:
        print("[FAULT ACTIVE] Throttling applied to Chennai|Airtel!")
        for w in windows:
            if w.get("dimkey") == "Chennai|Airtel":
                w["rebuffer_ratio"] = 0.0820
                w["qoe_score"] = 48.0
                w["qoe_label"] = "Poor"
                w["sessions"] = 45
            elif w.get("grain") == "city_isp":
                if "qoe_score" not in w or w["qoe_score"] < 60:
                    w["qoe_score"] = 88.5
                    w["qoe_label"] = "Good"
                    w["rebuffer_ratio"] = 0.008
    else:
        print("[BASELINE MODE] Ensuring nominal healthy metrics for all segments including Chennai|Airtel...")
        for w in windows:
            if w.get("dimkey") == "Chennai|Airtel":
                w["rebuffer_ratio"] = 0.008
                w["qoe_score"] = 89.2
                w["qoe_label"] = "Good"
                w["sessions"] = 50

    write_qoe_metrics_to_hbase(windows, host='hbase', port=9090)
    print(f"Successfully upserted {len(windows)} windows to HBase 'qoe_metrics'!")

    # Alert management
    try:
        import happybase
        conn = happybase.Connection(host="hbase", port=9090)
        tbl_alerts = conn.table("alerts")
        if not args.inject_fault:
            # Clear previous alerts so baseline is completely clean (green)
            keys_to_del = [k for k, _ in tbl_alerts.scan()]
            for k in keys_to_del:
                tbl_alerts.delete(k)
            print(f"[BASELINE CLEAN] Cleared {len(keys_to_del)} existing alerts from HBase 'alerts' table.")
            conn.close()
        else:
            conn.close()
            # Alert evaluation with 2-window confirmation for hysteresis
            ev = AlertEvaluator()
            ev.evaluate_window({"grain": "city_isp", "dimkey": "Chennai|Airtel", "sessions": 50, "rebuffer_ratio": 0.082, "window_end": now_iso})
            alerts = ev.evaluate_window({"grain": "city_isp", "dimkey": "Chennai|Airtel", "sessions": 50, "rebuffer_ratio": 0.082, "window_end": now_iso})
            if alerts:
                write_alerts_to_hbase(alerts, host="hbase", port=9090)
                for a in alerts:
                    print(f"[ALERT OPENED IN HBASE] {a.get('status', '').upper()} Rule {a.get('rule')}_REBUFFERING : {a.get('dimkey')} (value: {a.get('value')} | threshold: {a.get('threshold')})")
    except Exception as e:
        print(f"Alert processing note: {e}")

if __name__ == "__main__":
    main()
