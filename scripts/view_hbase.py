# scripts/view_hbase.py
"""
HBase Data Viewer: Inspect raw rows, columns, and timestamps stored in HBase.
Usage:
  python scripts/view_hbase.py [table_name] [--limit N]
Examples:
  python scripts/view_hbase.py qoe_metrics --limit 5
  python scripts/view_hbase.py session_state --limit 5
  python scripts/view_hbase.py alerts
"""

import sys
import argparse
import happybase

def view_table(table_name: str, limit: int = 5):
    try:
        conn = happybase.Connection(host="hbase", port=9090, timeout=5000)
        tables = [t.decode("utf-8") for t in conn.tables()]
        
        if table_name not in tables:
            print(f"❌ Table '{table_name}' does not exist! Available tables: {tables}")
            conn.close()
            return

        table = conn.table(table_name)
        rows = list(table.scan(limit=limit))
        
        print("\n" + "=" * 90)
        print(f"📦 APACHE HBASE STORAGE INSPECTOR: Table '{table_name}'")
        print("=" * 90)
        
        if not rows:
            print(f"Table '{table_name}' is currently empty (0 rows).")
            print("=" * 90 + "\n")
            conn.close()
            return

        print(f"Showing first {len(rows)} row(s) from table '{table_name}':\n")
        
        for idx, (row_key, data) in enumerate(rows, 1):
            rk_str = row_key.decode("utf-8", errors="replace")
            print(f"┌─ Row #{idx} [RowKey: {rk_str}]")
            
            # Group by column family
            families = {}
            for col_bytes, val_bytes in data.items():
                col_str = col_bytes.decode("utf-8", errors="replace")
                val_str = val_bytes.decode("utf-8", errors="replace")
                cf, col = col_str.split(":", 1) if ":" in col_str else ("cf", col_str)
                families.setdefault(cf, []).append((col, val_str))
            
            for cf, cols in families.items():
                print(f"│  ├── Column Family '{cf}':")
                for col, val in sorted(cols):
                    # Truncate very long values for clean terminal viewing
                    display_val = val if len(val) <= 50 else val[:47] + "..."
                    print(f"│  │   • {col:<24} = {display_val}")
            print("└" + "─" * 80 + "\n")

        print("=" * 90)
        print(f"✅ Successfully retrieved {len(rows)} row(s) from HBase RegionServer.")
        print("=" * 90 + "\n")
        conn.close()

    except Exception as e:
        print(f"❌ Error connecting to HBase: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="View actual rows stored in HBase tables.")
    parser.add_argument("table", nargs="?", default="qoe_metrics", choices=["qoe_metrics", "session_state", "alerts"], help="Table to inspect")
    parser.add_argument("--limit", type=int, default=3, help="Max rows to display (default: 3)")
    args = parser.parse_args()

    view_table(args.table, limit=args.limit)
