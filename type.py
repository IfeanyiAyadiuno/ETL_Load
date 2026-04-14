"""
Legacy helpers and thin wrappers. Type curves load into dbo.PCE_TC via type_curves_import.
"""

import numpy as np
import pandas as pd

import log_format as lf
from type_curves_import import append_typecurves_from_excel


def safe_float(value):
    """Safely convert any value to float or return None - handles N/A"""
    if value is None or pd.isna(value):
        return None
    try:
        if isinstance(value, str):
            value = value.strip().replace(",", "")
            if value.lower() in ["", "nan", "null", "none", "-", "n/a", "na"]:
                return None
        result = float(value)
        return None if np.isinf(result) or np.isnan(result) else round(result, 4)
    except (ValueError, TypeError):
        return None


def get_float_value(val):
    """Safely convert to float or None, handling numpy NaN"""
    if val is None:
        return None
    if isinstance(val, float) and np.isnan(val):
        return None
    return float(val)


def get_string_value(val):
    """Safely convert to string or None, handling NaN and N/A"""
    if val is None or pd.isna(val):
        return None
    if isinstance(val, float) and np.isnan(val):
        return None
    s = str(val).strip()
    if s.lower() in ["", "nan", "null", "none", "-", "n/a", "na"]:
        return None
    return s


def import_typecurves(
    excel_path,
    log_callback=None,
    progress_callback=None,
    selected_production_names=None,
    cancel_event=None,
):
    """
    Import type curves from Excel into dbo.PCE_TC only (not PCE_Production).

    selected_production_names: optional list of mapped production well names without the
      `` - TC`` suffix. None or empty list imports every well present in the file after WM mapping.
    """
    r = append_typecurves_from_excel(
        excel_path,
        log_callback=log_callback,
        progress_callback=progress_callback,
        selected_production_names=selected_production_names,
        cancel_event=cancel_event,
    )
    if not r.get("ok"):
        if log_callback and r.get("unmatched") and not r.get("rows_inserted"):
            log_callback(
                lf.warn(
                    "No rows were written. Fix WM coverage or column headers, "
                    "then check unmatched_type_curve_wells_*.csv if produced."
                )
            )
        return False
    return True
