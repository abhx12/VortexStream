"""Streamlit Real-Time OTT Operations Dashboard."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd

from qoe.common.config import get_config
from qoe.dashboard.data_access import DashboardDataAccess


def create_dashboard_app(data_access: Optional[DashboardDataAccess] = None):
    """Main dashboard application renderer using Streamlit."""
    try:
        import streamlit as st
        import plotly.express as px
        import plotly.graph_objects as go
    except ImportError:
        # Fallback when running in headless unit test environment without streamlit
        return {"status": "streamlit_not_installed", "message": "Streamlit required for web UI"}

    st.set_page_config(
        page_title="VortexStream | OTT Video QoE Engine",
        page_icon="📺",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    da = data_access or DashboardDataAccess()

    st.title("📺 VortexStream — Real-Time OTT Video QoE & Operations Engine")
    st.markdown("Live operational monitoring, wide-column HBase telemetry, and ML abandonment scoring.")

    # Data freshness banner
    freshness_s = da.get_data_freshness_seconds()
    if freshness_s > 60.0:
        st.warning(f"⚠️ Telemetry stream paused or delayed: last window updated {int(freshness_s)}s ago.")

    # Tabs
    tab_live, tab_hist, tab_ml = st.tabs(["🔴 Live Operations", "📊 Historical Warehouse (DuckDB)", "🤖 ML Abandonment & Anomaly"])

    with tab_live:
        st.subheader("Current Telemetry & KPI Cards")
        glob = da.latest_segment("global", "ALL") or {}

        qoe_score = float(glob.get("qoe_score") or 92.5)
        qoe_label = str(glob.get("qoe_label") or "Good")
        rebuf = float(glob.get("rebuffer_ratio") or 0.008)
        bitrate = float(glob.get("avg_bitrate_kbps") or 3450.0)
        vsf = float(glob.get("vsf_rate") or 0.012)
        sessions = int(glob.get("sessions") or 1420)

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("QoE Score", f"{qoe_score:.1f}", delta=qoe_label)
        c2.metric("Rebuffer Ratio", f"{rebuf * 100:.2f}%")
        c3.metric("Avg Bitrate", f"{bitrate:.0f} kbps")
        c4.metric("VSF Rate", f"{vsf * 100:.2f}%")
        c5.metric("Active Sessions", f"{sessions:,}")

        st.markdown("---")
        col_left, col_right = st.columns([2, 1])

        with col_left:
            st.subheader("Segment Performance Trends")
            grain_select = st.selectbox("Select Grain", ["city_isp", "cdn", "device"], index=0)
            segments = da.latest_all(grain_select)
            keys = [s.get("dimkey", "") for s in segments if s.get("dimkey")]
            chosen_key = st.selectbox("Select Dimension Key", keys if keys else ["Chennai|Airtel", "Mumbai|Jio"])

            hist = da.segment_history(grain_select, chosen_key, minutes=60)
            if hist:
                h_df = pd.DataFrame(hist)
                fig_trend = px.line(h_df, x="window_end", y="qoe_score", title=f"QoE Score Trend: {chosen_key}", markers=True)
                fig_trend.update_yaxes(range=[0, 100])
                st.plotly_chart(fig_trend, use_container_width=True)
            else:
                st.info("Waiting for multi-window history to populate...")

        with col_right:
            st.subheader("Active & Recent Alerts")
            alerts = da.recent_alerts(limit=8)
            if alerts:
                for a in alerts:
                    sev = a.get("severity", "warn")
                    status = a.get("status", "open")
                    color = "red" if str(sev).lower() == "critical" else "orange"
                    st.markdown(
                        f"**:{color}[{status.upper()}]** `Rule {a.get('rule')}`: **{a.get('dimkey')}** "
                        f"(value: {a.get('metric_value') or a.get('value')} | threshold: {a.get('threshold')})"
                    )
            else:
                st.success("No active alerts. All regional segments within nominal baseline thresholds.")

    with tab_hist:
        st.subheader("Historical Analytics via DuckDB Warehouse Views")
        worst_df = da.history_worst_segments(limit=10)
        if not worst_df.empty:
            st.dataframe(worst_df, use_container_width=True)
        else:
            st.info("Warehouse views compiling...")

    with tab_ml:
        st.subheader("Predictive Abandonment & Churn Risk")
        m_df = da.model_risk_distribution()
        if not m_df.empty:
            fig_risk = px.bar(m_df, x="predicted_label", y="count_sessions", title="Live Predicted Abandonment Count")
            st.plotly_chart(fig_risk, use_container_width=True)
        else:
            st.info("Q4 ML scoring stream active. Early session evaluations will appear here.")

    # Footer
    st.markdown("---")
    st.caption(
        "📌 **Real data**: Stanford Puffer (one day, replayed). "
        "**Simulated**: city, ISP, CDN, device, app version, packet loss, errors, users, injected faults. "
        "Engine: Apache Spark, Kafka, HBase, DuckDB."
    )
    return {"status": "ok"}


def main():
    """CLI entrypoint for streamlit run."""
    create_dashboard_app()


if __name__ == "__main__":
    main()
