"""Dimension standardization and alias mapping."""

from __future__ import annotations

import re
from typing import Any, Dict, Optional
import pandas as pd
import yaml

from qoe.common.config import get_config


def _clean_str(val: Any) -> str:
    if val is None or pd.isna(val):
        return ""
    return str(val).strip().casefold()


def standardize_dimensions(
    df: pd.DataFrame,
    standardization_config: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """Normalize device, ISP, city, country, and resolution columns in-place or copy."""
    if df.empty:
        return df.copy()

    if standardization_config is None:
        cfg = get_config()
        standardization_config = cfg.get("standardization", {})

    out_df = df.copy()
    unknown_val = standardization_config.get("unknown_value", "unknown")

    device_map = {str(k).casefold(): v for k, v in standardization_config.get("device", {}).items()}
    isp_map = {str(k).casefold(): v for k, v in standardization_config.get("isp", {}).items()}
    city_map = {str(k).casefold(): v for k, v in standardization_config.get("city", {}).items()}
    country_map = {str(k).casefold(): v for k, v in standardization_config.get("country", {}).items()}

    # Standardize device
    if "device" in out_df.columns:
        out_df["device"] = out_df["device"].apply(lambda v: device_map.get(_clean_str(v), unknown_val if _clean_str(v) else unknown_val))

    # Standardize ISP
    if "isp" in out_df.columns:
        out_df["isp"] = out_df["isp"].apply(lambda v: isp_map.get(_clean_str(v), unknown_val if _clean_str(v) else unknown_val))

    # Standardize City
    if "city" in out_df.columns:
        out_df["city"] = out_df["city"].apply(lambda v: city_map.get(_clean_str(v), unknown_val if _clean_str(v) else unknown_val))

    # Standardize Country
    if "country" in out_df.columns:
        out_df["country"] = out_df["country"].apply(lambda v: country_map.get(_clean_str(v), unknown_val if _clean_str(v) else unknown_val))

    # Normalize Resolution to <H>p
    if "resolution" in out_df.columns:
        def norm_res(res: Any) -> Optional[str]:
            if res is None or pd.isna(res):
                return None
            s = str(res).strip()
            # Already matches \d{3,4}p
            if re.match(r"^\d{3,4}p$", s):
                return s
            # Matches WxH or WxH-CRF
            m = re.search(r"(\d+)x(\d+)", s)
            if m:
                return f"{m.group(2)}p"
            return s

        out_df["resolution"] = out_df["resolution"].apply(norm_res)

    return out_df
