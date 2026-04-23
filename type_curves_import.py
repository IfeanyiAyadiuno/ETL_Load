"""
Type curves: Excel (sheet 1, header row 1) -> dbo.PCE_TC, then materialize into PCE_Production.

Vincent conversion constants (imperial -> metric):
  m³/d from bbl/d: divide by M3_PER_BBL
  e³m³/d from mcf/d: divide mcf/d by E3M3_PER_MCF
  bcf cumulative -> e³m³: 1 bcf = 1_000_000 mcf; e³m³ = mcf / E3M3_PER_MCF
  Mbbl cumulative -> m³: Mbbl * 1000 bbl, then m³ = bbl / M3_PER_BBL
  Gas S2 cum (mmcf column): (mmcf * 1000) / E3M3_PER_MCF -> e³m³ (legacy YE convention)
"""

from __future__ import annotations

import csv
import os
import re
from datetime import date, datetime
from typing import Callable, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

import log_format as lf
from db_connection import get_sql_conn

_MIN_HYPHEN_PARTS_FOR_TAIL_STRIP = 6

TC_SUFFIX = " - TC"
TC_PAD_PREFIX = "PCE-TC-"

M3_PER_BBL = 6.29287017808823
E3M3_PER_MCF = 35.4937299999999

INSERT_SQL = """
INSERT INTO dbo.PCE_TC (
    [Well Name], [ImportDate],
    [Gas S2 Production (10³m³)], [Gas Sales Production (10³m³)],
    [Condensate Sales (m³/d)], [Sales CGR (m³/e³m³)],
    [Gas WH Production (e³m³/d)], [Condensate WH (m³/d)],
    [Cum Gas (e³m³)], [Cum Condy (m³)],
    [Layer Producer], [Pad Name], [SourceFileName],
    [Formation Producer], [Fault Block], [Remarks],
    [Lateral Length], [On Production Year], [Orientation]
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def _tc_clean_well_string(name) -> Optional[str]:
    if name is None or (isinstance(name, float) and np.isnan(name)) or pd.isna(name):
        return None
    if not isinstance(name, str):
        name = str(name).strip()
    cleaned = name.strip()
    if not cleaned:
        return None
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = re.sub(r"\s*-\s*", "-", cleaned)
    return cleaned


def _excel_well_cell_cleaned(file_well_cell: object) -> Optional[str]:
    """Normalized Excel well cell text (same rules as historical cleaner)."""
    if file_well_cell is None or (isinstance(file_well_cell, float) and np.isnan(file_well_cell)):
        return None
    raw = str(file_well_cell).strip()
    if not raw or raw.lower() == "null":
        return None
    return _tc_clean_well_string(raw)


def _tc_storage_base_name(excel_cleaned: Optional[str], wm_pname: Optional[str]) -> str:
    """
    Stored base before ``TC_SUFFIX``: longer of Excel vs WM canonical name; tie -> WM.
    """
    ex = (excel_cleaned or "").strip()
    wm = (wm_pname or "").strip()
    if not ex:
        return wm
    if not wm:
        return ex
    if len(ex) > len(wm):
        return ex
    if len(wm) > len(ex):
        return wm
    return wm


def _tc_well_match_key(name) -> str:
    if name is None or (isinstance(name, float) and np.isnan(name)) or pd.isna(name):
        return ""
    if not isinstance(name, str):
        name = str(name).strip()
    cleaned = _tc_clean_well_string(name)
    if not cleaned:
        return ""
    s = cleaned.casefold()
    s = s.replace("\\", "-").replace("/", "-")
    s = re.sub(r"\s*-\s*", "-", s)
    s = re.sub(r"-+", "-", s)
    s = re.sub(r"\s+", " ", s).strip()

    def _dig(m: re.Match) -> str:
        try:
            return str(int(m.group(0), 10))
        except ValueError:
            return m.group(0)

    s = re.sub(r"\d+", _dig, s)
    return re.sub(r"(?i)(w\d+)m$", r"\1", s)


def safe_float(value) -> Optional[float]:
    if value is None or (isinstance(value, float) and np.isnan(value)) or pd.isna(value):
        return None
    try:
        if isinstance(value, str):
            value = value.strip().replace(",", "")
            if value.lower() in ("", "nan", "null", "none", "-", "n/a", "na"):
                return None
        result = float(value)
        if np.isinf(result) or np.isnan(result):
            return None
        return round(result, 6)
    except (ValueError, TypeError):
        return None


def get_string_value(val) -> Optional[str]:
    if val is None or pd.isna(val):
        return None
    if isinstance(val, float) and np.isnan(val):
        return None
    s = str(val).strip()
    if s.lower() in ("", "nan", "null", "none", "-", "n/a", "na"):
        return None
    return s


def normalize_header(h) -> str:
    if h is None or (isinstance(h, float) and pd.isna(h)):
        return ""
    s = str(h).replace("\n", " ").replace("\r", " ")
    s = re.sub(r"\s+", " ", s).strip().lower()
    s = s.replace("³", "3").replace("²", "2")
    return s


def with_tc_suffix(mapped_production_name: str) -> str:
    b = str(mapped_production_name).rstrip()
    if b.endswith(TC_SUFFIX):
        return b
    return b + TC_SUFFIX


def _is_ye2_family_bulk_name(cleaned: str) -> bool:
    """
    YE2 / YE23 (and similar) type-curve rows are stored verbatim — no `` - TC`` suffix.
    Matches the same ``LIKE 'YE2%'`` guard used on ``PCE_Production`` (``YE23`` starts with ``YE2``).
    """
    s = str(cleaned).strip().casefold()
    return bool(s) and s.startswith("ye2")


def stored_well_name_file_only(cleaned: str) -> str:
    """Stored ``[Well Name]`` for a file-only (no WM) row."""
    if _is_ye2_family_bulk_name(cleaned):
        return str(cleaned).strip()
    return with_tc_suffix(str(cleaned).strip())


def strip_tc_suffix(stored_name: str) -> str:
    s = str(stored_name).rstrip()
    if s.endswith(TC_SUFFIX):
        return s[: len(s) - len(TC_SUFFIX)].rstrip()
    return s


def _bcf_to_cum_e3m3(bcf: Optional[float]) -> Optional[float]:
    if bcf is None:
        return None
    mcf_total = float(bcf) * 1_000_000.0
    return mcf_total / E3M3_PER_MCF


def _mbbl_to_cum_m3(mbbl: Optional[float]) -> Optional[float]:
    if mbbl is None:
        return None
    bbl_total = float(mbbl) * 1000.0
    return bbl_total / M3_PER_BBL


def _mcf_d_to_e3m3_d(mcf_d: Optional[float]) -> Optional[float]:
    if mcf_d is None:
        return None
    return float(mcf_d) / E3M3_PER_MCF


def _bbl_d_to_m3_d(bbl_d: Optional[float]) -> Optional[float]:
    if bbl_d is None:
        return None
    return float(bbl_d) / M3_PER_BBL


def _mmcf_cum_tab_to_e3m3(mmcf: Optional[float]) -> Optional[float]:
    """Interpret workbook *mmcf* cumulative like legacy YE: (mmcf * 1000) mcf-equivalent / divisor."""
    if mmcf is None:
        return None
    return float(mmcf) * 1000.0 / E3M3_PER_MCF


def _tc_pad_name_from_excel(pad_raw: Optional[str]) -> Optional[str]:
    if not pad_raw:
        return None
    s = str(pad_raw).strip()
    if not s:
        return None
    # Already normalized (re-import / sync); do not double-prefix.
    if s.casefold().startswith(TC_PAD_PREFIX.casefold()):
        return s
    s = re.sub(r"\s+", " ", s)
    tail = re.sub(r"[^\w\-]+", "-", s, flags=re.UNICODE)
    tail = re.sub(r"-+", "-", tail).strip("-")
    if not tail:
        return None
    return TC_PAD_PREFIX + tail


def _assign_column_roles(columns: List) -> Dict[str, int]:
    norms = [(i, normalize_header(c)) for i, c in enumerate(columns)]
    used: Set[int] = set()
    roles: Dict[str, int] = {}

    def take(match_fn):
        for i, n in norms:
            if i in used or not n:
                continue
            if match_fn(n):
                used.add(i)
                return i
        return None

    idx = take(
        lambda n: "well" in n
        and "name" in n
        and "tc" not in n
        and "production" not in n
    )
    if idx is not None:
        roles["well"] = idx

    idx = take(lambda n: "cum" in n and "gas" in n and "bcf" in n)
    if idx is not None:
        roles["cum_gas_bcf"] = idx

    idx = take(
        lambda n: "gas" in n
        and "s2" in n
        and "mcf" in n
        and "cum" not in n
        and "bcf" not in n
    )
    if idx is not None:
        roles["gas_s2_mcf_d"] = idx

    idx = take(
        lambda n: "gas" in n
        and "s2" in n
        and "cum" in n
        and ("mmcf" in n or "mmscf" in n)
    )
    if idx is not None:
        roles["gas_s2_cum_mmcf"] = idx

    idx = take(
        lambda n: "condensate" in n
        and "sales" in n
        and "bbl" in n
        and "cum" not in n
        and "m3" not in n
    )
    if idx is not None:
        roles["cond_sales_bbl_d"] = idx

    # Assign before cum_cond_mbbl: "cond" matches inside "condensate" and would steal the wrong column.
    idx = take(
        lambda n: "condensate" in n
        and "sales" in n
        and "cum" in n
        and ("mbbl" in n or "bbl" in n)
    )
    if idx is not None:
        roles["cond_sales_cum_mbbl"] = idx

    # TC workbook "Cum Condy (Mbbl)" — require "condy" so "Condensate Sales Cum …" does not match.
    idx = take(
        lambda n: "cum" in n
        and "condy" in n
        and ("mbbl" in n or "bbl" in n)
        and "bcf" not in n
        and "sales" not in n
    )
    if idx is not None:
        roles["cum_cond_mbbl"] = idx

    idx = take(
        lambda n: "gas" in n
        and ("s1" in n or "s2" in n)
        and "sales" not in n
        and "wh" not in n
        and "cum" not in n
        and "mcf" not in n
    )
    if idx is not None:
        roles["gas_s2_10e3"] = idx

    idx = take(lambda n: "gas" in n and "sales" in n and "cum" not in n)
    if idx is not None:
        roles["gas_sales_10e3"] = idx

    idx = take(
        lambda n: "condensate" in n
        and "sales" in n
        and "cum" not in n
        and "cgr" not in n
        and "bbl" not in n
    )
    if idx is not None:
        roles["cond_sales"] = idx

    idx = take(lambda n: "cgr" in n and "sales" in n)
    if idx is None:
        idx = take(lambda n: "cgr" in n)
    if idx is not None:
        roles["sales_cgr"] = idx

    idx = take(
        lambda n: "gas" in n and "wh" in n and "mcf" in n and "cum" not in n
    )
    if idx is not None:
        roles["gas_wh_mcf"] = idx

    idx = take(
        lambda n: "condensate" in n
        and "wh" in n
        and "bbl" in n
        and "cum" not in n
    )
    if idx is not None:
        roles["cond_wh_bbl"] = idx

    idx = take(
        lambda n: "formation" in n
        and "producer" in n
    )
    if idx is not None:
        roles["formation_producer"] = idx
    if "formation_producer" not in roles:
        idx = take(lambda n: n == "formation")
        if idx is not None:
            roles["formation_producer"] = idx

    idx = take(lambda n: "fault" in n and "block" in n)
    if idx is not None:
        roles["fault_block"] = idx

    idx = take(lambda n: "remark" in n)
    if idx is not None:
        roles["remarks"] = idx

    idx = take(lambda n: "lateral" in n)
    if idx is not None:
        roles["lateral_length"] = idx

    idx = take(
        lambda n: ("on" in n and "prod" in n and "year" in n)
        or ("reserves" in n and "year" in n)
    )
    if idx is not None:
        roles["on_production_year"] = idx

    idx = take(lambda n: "orient" in n)
    if idx is not None:
        roles["orientation"] = idx

    # Prefer "Pad Name" explicitly. Avoid matching "Padding" — substring "pad" is inside "padding".
    idx = take(lambda n: "pad name" in n)
    if idx is None:
        idx = take(lambda n: "pad" in n and "padding" not in n)
    if idx is not None:
        roles["pad"] = idx

    idx = take(
        lambda n: "layer" in n
        and "pad" not in n
        and "sales" not in n
        and "wh" not in n
        and "cum" not in n
        and "formation" not in n
    )
    if idx is not None:
        roles["layer"] = idx

    return roles


def read_typecurve_excel(excel_path: str) -> pd.DataFrame:
    return pd.read_excel(excel_path, sheet_name=0, header=0, dtype=object)


def _excel_base_for_wm_match(cleaned: str) -> str:
    s = str(cleaned).strip()
    if not s:
        return s
    parts = s.split("-")
    if len(parts) >= _MIN_HYPHEN_PARTS_FOR_TAIL_STRIP:
        return "-".join(parts[:-2])
    return s


def _build_wm_well_name_key_map() -> Dict[str, str]:
    query = """
        SELECT [Well Name]
        FROM PCE_WM
        WHERE [Well Name] IS NOT NULL
          AND LTRIM(RTRIM([Well Name])) <> ''
          AND ([Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N')
    """
    with get_sql_conn() as conn:
        df = pd.read_sql(query, conn)
    key_to_wn: Dict[str, str] = {}
    for _, row in df.iterrows():
        wn = get_string_value(row.get("Well Name"))
        if not wn:
            continue
        k = _tc_well_match_key(wn)
        if k:
            key_to_wn[k] = wn
    return key_to_wn


def _wm_fault_by_well_name() -> Dict[str, Optional[str]]:
    query = """
        SELECT [Well Name], [Fault Block]
        FROM PCE_WM
        WHERE [Well Name] IS NOT NULL
          AND LTRIM(RTRIM([Well Name])) <> ''
          AND ([Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N')
    """
    with get_sql_conn() as conn:
        df = pd.read_sql(query, conn)
    out: Dict[str, Optional[str]] = {}
    for _, row in df.iterrows():
        wn = get_string_value(row.get("Well Name"))
        if not wn:
            continue
        fb = get_string_value(row.get("Fault Block"))
        out[wn] = fb
    return out


def resolve_file_well_to_wm_well_name(
    file_well_cell: object,
    wm_key_to_well_name: Dict[str, str],
) -> Optional[str]:
    cleaned = _excel_well_cell_cleaned(file_well_cell)
    if not cleaned:
        return None
    base = _excel_base_for_wm_match(cleaned)
    k = _tc_well_match_key(base)
    if not k:
        return None
    return wm_key_to_well_name.get(k)


def _dataframe_from_excel(excel_path: str, log: Callable[[str], None]) -> Tuple[pd.DataFrame, Dict[str, int]]:
    df = read_typecurve_excel(excel_path)
    roles = _assign_column_roles(list(df.columns))
    if "well" not in roles:
        raise ValueError(
            "Could not find a Well Name column (header row 1). Check the Excel headers."
        )
    log(lf.detail(f"Column mapping: {roles}"))
    return df, roles


def scan_typecurve_wells(
    excel_path: str,
) -> Tuple[List[Dict[str, str]], List[str]]:
    """
    Return (append_row_descriptors, unmatched_file_well_texts).

    Each descriptor has: ``kind`` (``wm`` | ``file``), ``display_label``, ``stored_key``
    (``PCE_TC.[Well Name]`` as stored: WM-backed rows end with `` - TC``; file-only YE2/YE23-style
    names are verbatim with no suffix).
    """

    def log(_):
        pass

    df, roles = _dataframe_from_excel(excel_path, log)
    col_well = roles["well"]
    wm_map = _build_wm_well_name_key_map()
    descriptors: List[Dict[str, str]] = []
    seen_key: Set[str] = set()
    unmatched: List[str] = []
    seen_unmatched: Set[str] = set()

    for _, row in df.iterrows():
        cell = row.iloc[col_well]
        cleaned = _excel_well_cell_cleaned(cell)
        pname = resolve_file_well_to_wm_well_name(cell, wm_map)
        if pname:
            base = _tc_storage_base_name(cleaned, pname)
            sk = with_tc_suffix(base)
            if sk not in seen_key:
                seen_key.add(sk)
                descriptors.append(
                    {
                        "kind": "wm",
                        "display_label": f"[WM] {sk}",
                        "stored_key": sk,
                    }
                )
        else:
            u = get_string_value(cell)
            if u and u not in seen_unmatched:
                seen_unmatched.add(u)
                unmatched.append(u)
            if not cleaned:
                continue
            sk = stored_well_name_file_only(cleaned)
            if sk not in seen_key:
                seen_key.add(sk)
                descriptors.append(
                    {
                        "kind": "file",
                        "display_label": f"[File] {sk}",
                        "stored_key": sk,
                    }
                )

    return descriptors, unmatched


def _gas_s2_metric(
    row,
    roles: Dict[str, int],
    col_fn,
) -> Optional[float]:
    mcf_d = col_fn("gas_s2_mcf_d", row)
    if mcf_d is not None:
        return _mcf_d_to_e3m3_d(mcf_d)
    v = col_fn("gas_s2_10e3", row)
    if v is not None:
        return v
    return None


def _cum_gas_e3(
    row,
    roles: Dict[str, int],
    col_fn,
) -> Optional[float]:
    mmcf = col_fn("gas_s2_cum_mmcf", row)
    if mmcf is not None:
        return _mmcf_cum_tab_to_e3m3(mmcf)
    return _bcf_to_cum_e3m3(col_fn("cum_gas_bcf", row))


def _delete_production_for_tc_well_names(cursor, names: List[str]) -> int:
    if not names:
        return 0
    total = 0
    batch_size = 50
    for i in range(0, len(names), batch_size):
        chunk = names[i : i + batch_size]
        ph = ",".join("?" * len(chunk))
        cursor.execute(
            f"DELETE FROM dbo.PCE_Production WHERE [Well Name] IN ({ph})",
            chunk,
        )
        total += cursor.rowcount or 0
    return total


def append_typecurves_from_excel(
    excel_path: str,
    log_callback: Optional[Callable[[str], None]] = None,
    progress_callback: Optional[Callable[[int], None]] = None,
    selected_production_names: Optional[List[str]] = None,
    selected_stored_keys: Optional[List[str]] = None,
    cancel_event: Optional[object] = None,
) -> dict:
    """
    For each target well: DELETE PCE_TC rows for that stored key, then INSERT from file.

    ``selected_stored_keys``: full ``[Well Name]`` values as stored (WM rows end with `` - TC``;
      YE2/YE23-style file rows do not).

    ``selected_production_names``: legacy — WM ``[Well Name]`` without `` - TC`` suffix.
      Used only when ``selected_stored_keys`` is None.
    """

    def log(msg: str):
        if log_callback:
            log_callback(msg)
        else:
            print(msg)

    def progress(p: int):
        if progress_callback:
            progress_callback(p)

    import_date = date.today()
    source_name = os.path.basename(excel_path)

    def aborted() -> bool:
        if cancel_event is None:
            return False
        fn = getattr(cancel_event, "is_set", None)
        return bool(fn()) if callable(fn) else False

    result: Dict[str, object] = {
        "ok": False,
        "wells_updated": 0,
        "rows_inserted": 0,
        "wells_skipped_no_file_rows": 0,
        "wells_skipped_selection_not_in_file": 0,
        "unmatched": [],
    }

    log(lf.step("Reading type curve Excel (sheet 1, header row 1)"))
    progress(5)
    df, roles = _dataframe_from_excel(excel_path, log)
    wm_map = _build_wm_well_name_key_map()
    wm_fault = _wm_fault_by_well_name()
    progress(15)

    col_well = roles["well"]

    def col(role: str, row) -> Optional[float]:
        if role not in roles:
            return None
        return safe_float(row.iloc[roles[role]])

    def col_str(role: str, row) -> Optional[str]:
        if role not in roles:
            return None
        return get_string_value(row.iloc[roles[role]])

    rows_out: List[Tuple] = []
    unmatched: List[str] = []
    seen_um: Set[str] = set()

    for _, row in df.iterrows():
        cell = row.iloc[col_well]
        cleaned = _excel_well_cell_cleaned(cell)
        pname = resolve_file_well_to_wm_well_name(cell, wm_map)
        raw_well = get_string_value(cell)

        fault_wm = wm_fault.get(pname) if pname else None
        fault_excel = col_str("fault_block", row)

        gas_s2_metric = _gas_s2_metric(row, roles, col)
        gas_sales = col("gas_sales_10e3", row)
        cond_sales = col("cond_sales", row)
        if cond_sales is None:
            cond_sales = _bbl_d_to_m3_d(col("cond_sales_bbl_d", row))
        sales_cgr = col("sales_cgr", row)
        gas_wh_e3 = _mcf_d_to_e3m3_d(col("gas_wh_mcf", row))
        cond_wh_m3 = _bbl_d_to_m3_d(col("cond_wh_bbl", row))
        cum_gas_e3 = _cum_gas_e3(row, roles, col)
        cum_cond_mbbl_raw = col("cum_cond_mbbl", row)
        if cum_cond_mbbl_raw is not None:
            cum_cond_m3 = _mbbl_to_cum_m3(cum_cond_mbbl_raw)
        else:
            cum_cond_m3 = _mbbl_to_cum_m3(col("cond_sales_cum_mbbl", row))

        layer = col_str("layer", row)
        pad_raw = col_str("pad", row)
        pad = _tc_pad_name_from_excel(pad_raw) if pad_raw else None
        formation = col_str("formation_producer", row)
        remarks = col_str("remarks", row)
        lateral = col("lateral_length", row)
        on_year = col("on_production_year", row)
        orientation = col_str("orientation", row)

        if pname:
            storage_base = _tc_storage_base_name(cleaned, pname)
            w_tc = with_tc_suffix(storage_base)
            fault = fault_wm if fault_wm is not None else fault_excel
        else:
            if not cleaned:
                if raw_well and raw_well not in seen_um:
                    seen_um.add(raw_well)
                    unmatched.append(raw_well)
                continue
            w_tc = stored_well_name_file_only(cleaned)
            fault = fault_excel

        if not pname and raw_well and raw_well not in seen_um:
            seen_um.add(raw_well)
            unmatched.append(raw_well)

        rows_out.append(
            (
                w_tc,
                import_date,
                gas_s2_metric,
                gas_sales,
                cond_sales,
                sales_cgr,
                gas_wh_e3,
                cond_wh_m3,
                cum_gas_e3,
                cum_cond_m3,
                layer,
                pad,
                source_name,
                formation,
                fault,
                remarks,
                lateral,
                on_year,
                orientation,
            )
        )

    result["unmatched"] = unmatched
    if unmatched:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_csv = os.path.join(os.path.dirname(excel_path) or ".", f"unmatched_type_curve_wells_{ts}.csv")
        try:
            with open(out_csv, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["Well Name (file)"])
                for u in unmatched:
                    w.writerow([u])
            log(lf.warn(f"Unmatched wells written to: {out_csv}"))
        except OSError as e:
            log(lf.warn(f"Could not write unmatched CSV: {e}"))

    if not rows_out:
        log(lf.warn("No rows to import from file."))
        return result

    by_well: Dict[str, List[Tuple]] = {}
    for tup in rows_out:
        by_well.setdefault(tup[0], []).append(tup)

    all_wells_in_file = list(by_well.keys())
    in_file_tc = set(all_wells_in_file)

    want: Optional[Set[str]] = None
    if selected_stored_keys is not None:
        want = {str(x).strip() for x in selected_stored_keys if str(x).strip()}
    elif selected_production_names:
        bases = {strip_tc_suffix(str(n).strip()) for n in selected_production_names if str(n).strip()}
        want = {with_tc_suffix(b) for b in bases}

    if want is not None and len(want) == 0:
        want = None

    if want is not None:
        targets = [w for w in want if w in in_file_tc]
        missing = want - in_file_tc
        result["wells_skipped_selection_not_in_file"] = len(missing)
        if missing:
            log(
                lf.detail(
                    f"{lf.num(len(missing))} selected well(s) have no rows in this file (skipped)."
                )
            )
    else:
        targets = all_wells_in_file

    if not targets:
        log(lf.warn("No wells to apply after selection filter."))
        return result

    log(lf.step(f"Writing to PCE_TC for {lf.num(len(targets))} well(s)"))
    progress(40)

    from sync_typecurves_to_production import sync_tc_to_production

    with get_sql_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor()
        cur.fast_executemany = True
        total_inserted = 0
        n = len(targets)
        for i, w_tc in enumerate(targets):
            if aborted():
                conn.rollback()
                log(lf.warn("Import cancelled before completion."))
                return result
            batch = by_well.get(w_tc, [])
            if not batch:
                result["wells_skipped_no_file_rows"] += 1
                continue
            cur.execute("DELETE FROM dbo.PCE_TC WHERE [Well Name] = ?", w_tc)
            cur.executemany(INSERT_SQL, batch)
            total_inserted += len(batch)
            result["wells_updated"] += 1
            progress(40 + int((i + 1) / max(n, 1) * 55))
        conn.commit()

    progress(95)
    try:
        sync_tc_to_production(log_callback=log)
    except Exception as e:
        log(lf.warn(f"PCE_Production sync after TC import: {e}"))
    progress(100)
    result["ok"] = True
    result["rows_inserted"] = total_inserted
    log(
        lf.success(
            f"PCE_TC import complete: {lf.num(result['wells_updated'])} wells, "
            f"{lf.num(total_inserted)} row(s), import date {import_date}"
        )
    )
    return result


def delete_typecurves_from_tc(
    stored_well_names_with_suffix: List[str],
    log_callback: Optional[Callable[[str], None]] = None,
) -> int:
    def log(msg: str):
        if log_callback:
            log_callback(msg)
        else:
            print(msg)

    if not stored_well_names_with_suffix:
        return 0
    names = list({str(n).strip() for n in stored_well_names_with_suffix if str(n).strip()})
    if not names:
        return 0
    total = 0
    with get_sql_conn() as conn:
        cur = conn.cursor()
        batch_size = 50
        for i in range(0, len(names), batch_size):
            chunk = names[i : i + batch_size]
            ph = ",".join("?" * len(chunk))
            cur.execute(f"DELETE FROM dbo.PCE_TC WHERE [Well Name] IN ({ph})", chunk)
            total += cur.rowcount or 0
        _delete_production_for_tc_well_names(cur, names)
        conn.commit()
    log(lf.detail(f"Deleted {lf.num(total)} PCE_TC row(s) for {lf.num(len(names))} well key(s)"))
    return total


def fetch_distinct_tc_well_names() -> List[str]:
    # GROUP BY (not DISTINCT + ORDER BY CASE): SQL Server requires ORDER BY
    # expressions to appear in the select list when DISTINCT is used (error 145).
    q = """
        SELECT [Well Name]
        FROM dbo.PCE_TC
        GROUP BY [Well Name]
        ORDER BY
            CASE
                WHEN [Well Name] LIKE 'YE2%' THEN 1
                WHEN [Well Name] LIKE '% - TC' THEN 2
                ELSE 0
            END,
            [Well Name]
    """
    with get_sql_conn() as conn:
        df = pd.read_sql(q, conn)
    if df.empty:
        return []
    return [str(x) for x in df["Well Name"].tolist() if pd.notna(x)]


def ye2_append_rows_to_pce_tc(
    excel_path: str,
    log_callback: Optional[Callable[[str], None]] = None,
) -> dict:
    """
    Bulk YE2/YE23-style load: Excel well text is stored verbatim (no WM match, no `` - TC``).
    Same metric columns as GUI import; pad uses ``PCE-TC-`` prefix when a pad column exists.
    """

    def log(msg: str):
        if log_callback:
            log_callback(msg)
        else:
            print(msg)

    import_date = date.today()
    source_name = os.path.basename(excel_path)
    df, roles = _dataframe_from_excel(excel_path, log)
    col_well = roles["well"]

    def col(role: str, row) -> Optional[float]:
        if role not in roles:
            return None
        return safe_float(row.iloc[roles[role]])

    def col_str(role: str, row) -> Optional[str]:
        if role not in roles:
            return None
        return get_string_value(row.iloc[roles[role]])

    rows_out: List[Tuple] = []
    for _, row in df.iterrows():
        wn = _excel_well_cell_cleaned(row.iloc[col_well])
        if not wn:
            continue
        gas_s2_metric = _gas_s2_metric(row, roles, col)
        gas_sales = col("gas_sales_10e3", row)
        cond_sales = col("cond_sales", row)
        if cond_sales is None:
            cond_sales = _bbl_d_to_m3_d(col("cond_sales_bbl_d", row))
        sales_cgr = col("sales_cgr", row)
        gas_wh_e3 = _mcf_d_to_e3m3_d(col("gas_wh_mcf", row))
        cond_wh_m3 = _bbl_d_to_m3_d(col("cond_wh_bbl", row))
        cum_gas_e3 = _cum_gas_e3(row, roles, col)
        cum_cond_mbbl_raw = col("cum_cond_mbbl", row)
        if cum_cond_mbbl_raw is not None:
            cum_cond_m3 = _mbbl_to_cum_m3(cum_cond_mbbl_raw)
        else:
            cum_cond_m3 = _mbbl_to_cum_m3(col("cond_sales_cum_mbbl", row))
        layer = col_str("layer", row)
        pad_raw = col_str("pad", row)
        pad = _tc_pad_name_from_excel(pad_raw) if pad_raw else None
        formation = col_str("formation_producer", row)
        fault = col_str("fault_block", row)
        remarks = col_str("remarks", row)
        lateral = col("lateral_length", row)
        on_year = col("on_production_year", row)
        orientation = col_str("orientation", row)

        rows_out.append(
            (
                wn,
                import_date,
                gas_s2_metric,
                gas_sales,
                cond_sales,
                sales_cgr,
                gas_wh_e3,
                cond_wh_m3,
                cum_gas_e3,
                cum_cond_m3,
                layer,
                pad,
                source_name,
                formation,
                fault,
                remarks,
                lateral,
                on_year,
                orientation,
            )
        )

    if not rows_out:
        log(lf.warn("YE2 load: no rows after well-name filter."))
        return {"ok": False, "rows_inserted": 0}

    with get_sql_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor()
        cur.fast_executemany = True
        cur.executemany(INSERT_SQL, rows_out)
        conn.commit()

    from sync_typecurves_to_production import sync_tc_to_production

    try:
        sync_tc_to_production(log_callback=log)
    except Exception as e:
        log(lf.warn(f"PCE_Production sync after YE2 load: {e}"))

    log(lf.success(f"YE2 PCE_TC insert: {lf.num(len(rows_out))} row(s)."))
    return {"ok": True, "rows_inserted": len(rows_out)}
