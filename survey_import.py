import csv
import pandas as pd
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple
import os
import sys
import traceback
import re
import log_format as lf
from db_connection import get_sql_conn

# Logical column keys for directional mapping (values are 0-based Excel column indices or None)
DIRECTIONAL_FIELD_KEYS = [
    "Measured Depth",
    "Inclination",
    "Azimuth Angle",
    "Subsea Elevation",
    "True Vertical Depth",
    "Longitude",
    "Latitude",
    "East",
    "North",
]


def is_survey_csv_path(path: str) -> bool:
    return str(path).lower().endswith(".csv")


_CSV_ENCODINGS_TRY = ("utf-8-sig", "utf-8", "cp1252", "latin-1")


def _infer_csv_delimiter(sample: str) -> str:
    """Prefer comma; fall back to semicolon, tab, or pipe (Excel / vendor exports)."""
    if not sample or not sample.strip():
        return ","
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        if dialect.delimiter in ",;\t|":
            return dialect.delimiter
    except csv.Error:
        pass
    first = sample.splitlines()[0] if sample else ""
    scores = {
        ",": first.count(","),
        ";": first.count(";"),
        "\t": first.count("\t"),
        "|": first.count("|"),
    }
    mx = max(scores.values())
    if mx == 0:
        return ","
    # On a tie, prefer comma (US-style) then semicolon, tab, pipe
    for d in (",", ";", "\t", "|"):
        if scores[d] == mx:
            return d
    return ","


def _read_csv_rows(path: str) -> List[List[str]]:
    """Read CSV with csv.reader (RFC 4180): quoted fields, ragged rows, delimiter sniffing."""
    last_err: Optional[Exception] = None
    for enc in _CSV_ENCODINGS_TRY:
        try:
            with open(path, "r", encoding=enc, newline="") as f:
                sample = f.read(16384)
                f.seek(0)
                delim = _infer_csv_delimiter(sample)
                return list(csv.reader(f, delimiter=delim))
        except UnicodeDecodeError as e:
            last_err = e
    if last_err:
        raise last_err
    raise OSError(f"Could not decode CSV: {path}")


def _pad_csv_rows_to_grid(rows: List[List[str]]) -> pd.DataFrame:
    """Pad ragged rows to equal width so line 2 can have more columns than line 1."""
    if not rows:
        return pd.DataFrame()
    max_c = max(len(r) for r in rows)
    padded: List[List[Optional[str]]] = []
    for r in rows:
        row = list(r)
        if len(row) < max_c:
            row.extend([None] * (max_c - len(row)))
        padded.append(row)
    return pd.DataFrame(padded, dtype=object)


def read_survey_raw_grid(path: str, sheet_index: int = 0) -> pd.DataFrame:
    """
    Raw cell grid for directional mapping: Excel sheet or comma-separated CSV (single table).
    For CSV, sheet_index is ignored. Uses csv.reader (not pandas C parser) so ragged rows
    and quoted fields with commas do not fail with "Expected N fields, saw M".
    """
    if is_survey_csv_path(path):
        return _pad_csv_rows_to_grid(_read_csv_rows(path))
    return pd.read_excel(path, sheet_name=sheet_index, header=None, dtype=object)


def read_legacy_flat_survey_file(path: str) -> pd.DataFrame:
    """First row = headers: Excel workbook or CSV for bulk / Settings survey import."""
    if is_survey_csv_path(path):
        rows = _read_csv_rows(path)
        if not rows:
            return pd.DataFrame()
        max_c = max(len(r) for r in rows)
        header = list(rows[0])
        while len(header) < max_c:
            header.append(f"Unnamed_{len(header)}")
        body: List[List[Optional[str]]] = []
        for r in rows[1:]:
            row = list(r)
            if len(row) < max_c:
                row.extend([None] * (max_c - len(row)))
            body.append(row[:max_c])
        return pd.DataFrame(body, columns=header[:max_c], dtype=object)
    return pd.read_excel(path)


def clean_well_name(name):
    """Clean well name by removing extra spaces and normalizing"""
    if pd.isna(name) or not isinstance(name, str):
        return name

    cleaned = name.strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = re.sub(r"\s*-\s*", "-", cleaned)
    return cleaned


def _well_name_lookup_trim_candidates(text: str) -> List[str]:
    """
    Strings to try against PCE_WM [Well Name], in order.

    Survey cells often prefix the asset with operator/area text (e.g. \"Pacific … Altares\")
    while Well Master stores the trailing well id. We try the full cell first (longest match
    wins), then the same text with 1, 2, … leading whitespace-separated words removed.
    """
    raw = str(text).strip()
    if not raw:
        return []
    tokens = re.split(r"\s+", raw)
    out: List[str] = []
    for i in range(len(tokens)):
        part = " ".join(tokens[i:]).strip()
        if part:
            out.append(part)
    return out


def survey_well_name_matches_wm_keys(well_name_cell: Any, valid_wm_keys: set) -> bool:
    """True if any leading-word-trim variant matches a key in valid_wm_keys (legacy bulk)."""
    if well_name_cell is None or (isinstance(well_name_cell, float) and pd.isna(well_name_cell)):
        return False
    text = str(well_name_cell).strip()
    if not text:
        return False
    for cand in _well_name_lookup_trim_candidates(text):
        k = well_name_match_key(cand)
        if k and k in valid_wm_keys:
            return True
    return False


def _collapse_digit_runs_leading_zeros(s: str) -> str:
    """Each contiguous digit block is normalized via int() so 094→94, 05→5, 00→0."""

    def norm_digits(m: re.Match) -> str:
        block = m.group(0)
        try:
            return str(int(block, 10))
        except ValueError:
            return block

    return re.sub(r"\d+", norm_digits, s)


def well_name_match_key(name) -> str:
    """
    Normalized key for matching survey/file text to PCE_WM [Well Name].

    Vendors often differ on casing and on slash vs hyphen in lateral IDs (e.g. D/94 vs D-94).
    Contiguous digits are compared with leading zeros removed (e.g. V098 vs V98, …-094-… vs …-94-…).
    This does not change the stored display name; use clean_well_name for that.
    """
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return ""
    if not isinstance(name, str):
        name = str(name).strip()
    cleaned = clean_well_name(name)
    if not isinstance(cleaned, str) or not cleaned:
        return ""
    s = cleaned.casefold()
    s = s.replace("\\", "-").replace("/", "-")
    s = re.sub(r"\s*-\s*", "-", s)
    s = re.sub(r"-+", "-", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = _collapse_digit_runs_leading_zeros(s)
    return s


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Rename Excel columns to canonical names using case-insensitive match."""
    canonical_by_fold = {
        "well name": "Well Name",
        "well unique identifier": "UWI",
        "subsea elevation": "Subsea Elevation",
        "inclination": "Inclination",
        "azimuth angle": "Azimuth Angle",
        "measured depth": "Measured Depth",
        "true vertical depth": "True Vertical Depth",
        "longitude": "Longitude",
        "latitude": "Latitude",
        "offset in ew": "Longitude",
        "offset in ns": "Latitude",
        "east": "East",
        "north": "North",
        "pad": "PAD",
    }
    rename = {}
    for c in df.columns:
        if not isinstance(c, str):
            continue
        key = str(c).strip().casefold()
        if key in canonical_by_fold:
            rename[c] = canonical_by_fold[key]
    if rename:
        df = df.rename(columns=rename)
    return df


@dataclass
class DirectionalSurveyMappingSpec:
    """User-defined layout for directional / single-well survey Excel files."""

    sheet_index: int = 0
    header_row: int = 42  # 0-based row index of MD, INCL, ... header row
    data_start_row: Optional[int] = None  # default: header_row + 1
    well_name_row: int = 5  # 0-based (e.g. row 6 in Excel)
    well_name_col: int = 0
    columns: Dict[str, Optional[int]] = field(default_factory=dict)

    def resolved_data_start_row(self) -> int:
        if self.data_start_row is not None:
            return self.data_start_row
        return self.header_row + 1

    def to_json_dict(self) -> dict:
        d = asdict(self)
        return d

    @classmethod
    def from_json_dict(cls, d: dict) -> "DirectionalSurveyMappingSpec":
        cols = dict(d.get("columns") or {})
        # Presets saved before Longitude/Latitude replaced Offset in EW / NS
        if "Longitude" not in cols and "Offset in EW" in cols:
            cols["Longitude"] = cols.pop("Offset in EW", None)
        if "Latitude" not in cols and "Offset in NS" in cols:
            cols["Latitude"] = cols.pop("Offset in NS", None)
        return cls(
            sheet_index=int(d.get("sheet_index", 0)),
            header_row=int(d.get("header_row", 0)),
            data_start_row=d.get("data_start_row"),
            well_name_row=int(d.get("well_name_row", 0)),
            well_name_col=int(d.get("well_name_col", 0)),
            columns={k: (int(v) if v is not None else None) for k, v in cols.items()},
        )


def _db_rows_to_dataframe(rows, columns):
    if not rows:
        return pd.DataFrame(columns=columns)
    normalized = [tuple(r) for r in rows]
    ncols = len(columns)
    for i, t in enumerate(normalized):
        if len(t) != ncols:
            raise ValueError(
                f"Row {i}: query returned {len(t)} field(s), expected {ncols} columns {tuple(columns)}"
            )
    return pd.DataFrame(normalized, columns=columns)


INSERT_SQL = """
        INSERT INTO PCE_Surveys (
            [UWI], [Well Name],
            [Subsea Elevation],
            [Inclination], [Azimuth Angle],
            [Measured Depth], [True Vertical Depth],
            [Longitude], [Latitude],
            [East], [North],
            [PAD], [SourceFile]
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

INSERT_COLS = [
    "UWI",
    "Well Name Cleaned",
    "Subsea Elevation",
    "Inclination",
    "Azimuth Angle",
    "Measured Depth",
    "True Vertical Depth",
    "Longitude",
    "Latitude",
    "East",
    "North",
    "PAD",
]


def lookup_wm_uwi_pad_for_directional(
    well_name_from_file: str,
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Match well name text to PCE_WM [Well Name] using well_name_match_key
    (case-insensitive; slashes normalized to hyphens). Tries the full cell text first,
    then drops leading words one at a time so prefixed vendor strings still resolve.
    Returns (uwi, pad, wm_well_name, error_message). On success error_message is None
    and wm_well_name is the exact PCE_WM.[Well Name] to store in PCE_Surveys.
    """
    conn = get_sql_conn()
    try:
        df = pd.read_sql(
            """
            SELECT [Well Name], [Value Navigator UWI], [Pad Name]
            FROM PCE_WM
            WHERE [Well Name] IS NOT NULL
              AND LTRIM(RTRIM([Well Name])) <> ''
              AND ([Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N')
            """,
            conn,
        )
    finally:
        conn.close()

    if df.empty:
        return None, None, None, "No wells found in PCE_WM."

    df = df.copy()
    df["_key"] = df["Well Name"].apply(well_name_match_key)

    m = pd.DataFrame()
    for cand in _well_name_lookup_trim_candidates(well_name_from_file):
        k = well_name_match_key(cand)
        if not k:
            continue
        m = df[df["_key"] == k]
        if len(m) == 1:
            break
        # 0 rows: try next candidate; >1 rows: try next (shorter tail may disambiguate)
        m = pd.DataFrame()

    if len(m) != 1:
        disp = clean_well_name(well_name_from_file)
        disp_s = disp if isinstance(disp, str) else str(well_name_from_file)
        return None, None, None, (
            f"No unique PCE_WM row matches this well name after trying the full cell and "
            f"dropping leading words (compare to [Well Name] in Well Master): '{disp_s}'"
        )
    uwi = m.iloc[0]["Value Navigator UWI"]
    pad = m.iloc[0]["Pad Name"]
    wm_wn = m.iloc[0]["Well Name"]
    if pd.isna(uwi) or str(uwi).strip() == "":
        return None, None, None, "Value Navigator UWI is missing in PCE_WM for this well."
    uwi_s = str(uwi).strip()
    pad_s = "" if pd.isna(pad) or pad is None else str(pad).strip()
    wm_wn_s = "" if pd.isna(wm_wn) or wm_wn is None else str(wm_wn).strip()
    if not wm_wn_s:
        return None, None, None, "[Well Name] is missing in PCE_WM for the matched row."
    return uwi_s, pad_s, wm_wn_s, None


def _apply_append_or_overwrite(
    conn,
    cursor,
    matched_df: pd.DataFrame,
    import_mode: str,
    log: Callable[[str], None],
) -> Tuple[pd.DataFrame, int]:
    """
    Apply overwrite deletes or append anti-join. Returns (matched_df, skipped_count for append).
    skipped_count is 0 for overwrite.
    """
    skipped_count = 0
    original_matched_count = len(matched_df)

    if import_mode == "overwrite" or import_mode == "rewrite":
        uwis = matched_df["UWI"].unique().tolist()
        total_deleted = 0
        batch_size = 500
        for i in range(0, len(uwis), batch_size):
            batch = uwis[i : i + batch_size]
            placeholders = ",".join(["?"] * len(batch))
            cursor.execute(
                f"DELETE FROM PCE_Surveys WHERE UWI IN ({placeholders})", batch
            )
            total_deleted += cursor.rowcount
        conn.commit()
        log(lf.detail(f"Deleted {lf.num(total_deleted)} existing records for {lf.num(len(uwis))} wells"))
        return matched_df, 0

    if import_mode == "append":
        log(lf.detail("Checking for existing records..."))
        uwis_to_check = matched_df["UWI"].dropna().unique().tolist()
        if not uwis_to_check:
            return matched_df, 0

        placeholders = ",".join(["?"] * len(uwis_to_check))
        existing_query = f"""
                    SELECT [UWI], [Measured Depth]
                    FROM PCE_Surveys
                    WHERE [UWI] IN ({placeholders})
                """
        cursor.execute(existing_query, uwis_to_check)
        existing_records = cursor.fetchall()

        if existing_records:
            existing_df = _db_rows_to_dataframe(
                existing_records, ["_ex_uwi", "_ex_depth"]
            )
            existing_df["_ex_uwi"] = existing_df["_ex_uwi"].astype(str).str.strip()
            existing_df["_ex_depth"] = pd.to_numeric(
                existing_df["_ex_depth"], errors="coerce"
            )
            log(lf.detail(f"Found {lf.num(len(existing_df))} existing records in database"))

            matched_df = matched_df.copy()
            matched_df["_uwi_key"] = matched_df["UWI"].astype(str).str.strip()
            matched_df["_depth_key"] = pd.to_numeric(
                matched_df["Measured Depth"], errors="coerce"
            )
            before_count = len(matched_df)
            merged = matched_df.merge(
                existing_df,
                left_on=["_uwi_key", "_depth_key"],
                right_on=["_ex_uwi", "_ex_depth"],
                how="left",
                indicator=True,
            )
            matched_df = merged[merged["_merge"] == "left_only"].drop(
                columns=["_ex_uwi", "_ex_depth", "_merge", "_uwi_key", "_depth_key"],
            ).copy()
            skipped_count = before_count - len(matched_df)
        return matched_df, skipped_count

    return matched_df, 0


def _batch_insert_surveys(
    cursor,
    conn,
    matched_df: pd.DataFrame,
    source_basename: str,
    log: Callable[[str], None],
    progress: Callable[[int], None],
    progress_lo: int,
    progress_hi: int,
) -> Tuple[int, int, int]:
    """Insert rows from matched_df. Returns (total_inserted, duplicate_skipped, error_count)."""
    error_count = 0
    duplicate_skipped = 0

    sub = matched_df[INSERT_COLS].astype(object)
    sub[sub.isna()] = None
    rows_to_insert = [
        tuple(row) + (source_basename,)
        for row in sub.itertuples(index=False, name=None)
    ]

    cursor.fast_executemany = True
    batch_size = 5000
    total_inserted = 0
    total_rows = len(rows_to_insert)
    span = max(progress_hi - progress_lo, 1)

    for i in range(0, total_rows, batch_size):
        batch = rows_to_insert[i : i + batch_size]
        try:
            cursor.executemany(INSERT_SQL, batch)
            total_inserted += len(batch)
        except Exception:
            for row in batch:
                try:
                    cursor.execute(INSERT_SQL, row)
                    total_inserted += 1
                except Exception as e:
                    if "Violation of UNIQUE KEY" in str(e):
                        duplicate_skipped += 1
                    else:
                        error_count += 1
                        log(lf.error(f"{str(e)[:100]}"))
        conn.commit()

        if total_rows:
            pct = min(i + len(batch), total_rows) / total_rows
            progress(progress_lo + int(pct * span))

        if (i + len(batch)) % 5000 == 0 or (i + len(batch)) >= total_rows:
            log(
                lf.detail(
                    f"Progress: {lf.num(min(i + len(batch), total_rows))}/{lf.num(total_rows)} rows"
                )
            )

    return total_inserted, duplicate_skipped, error_count


def _finalize_survey_summary(
    import_mode: str,
    total_rows: int,
    matched_final: int,
    unmatched: int,
    matched_df: pd.DataFrame,
    skipped_count: int,
    duplicate_skipped: int,
    total_inserted: int,
    error_count: int,
    log: Callable[[str], None],
) -> dict:
    # Align with legacy: append uses pre-insert anti-join skip count + insert-time unique violations
    if import_mode == "append":
        duplicates_final = skipped_count + duplicate_skipped
        matched_for_summary = matched_final
    else:
        duplicates_final = duplicate_skipped
        matched_for_summary = (
            len(matched_df) + duplicate_skipped if duplicate_skipped > 0 else len(matched_df)
        )

    summary = {
        "total_rows": total_rows,
        "matched": matched_for_summary,
        "unmatched": unmatched,
        "inserted": total_inserted,
        "duplicates": duplicates_final,
        "errors": error_count,
    }

    log(
        lf.summary(
            "IMPORT COMPLETE",
            {
                "Total rows in file": summary["total_rows"],
                "Rows matched to wells": summary["matched"],
                "Rows unmatched": summary["unmatched"],
                "Rows inserted": summary["inserted"],
                "Duplicates skipped": summary["duplicates"],
                "Errors": summary["errors"],
            },
        )
    )
    return summary


def import_surveys(excel_path, import_mode="append", progress_callback=None, log_callback=None):
    """
    Legacy flat Excel import (headers in row 1, columns from Settings-style template).
    """
    def log(message):
        if log_callback:
            log_callback(message)
        else:
            print(message)

    def progress(value):
        if progress_callback:
            progress_callback(value)

    conn = None
    try:
        log(
            lf.header(
                "SURVEY DATA IMPORT",
                File=os.path.basename(excel_path),
                Mode=import_mode,
            )
        )
        log(lf.step("Reading survey file"))
        df = read_legacy_flat_survey_file(excel_path)
        log(lf.detail(f"Read {lf.num(len(df))} rows"))
        progress(10)

        log(lf.step("Mapping columns"))
        df = _normalize_column_names(df)
        log(lf.detail("Normalized column names (case-insensitive)"))
        progress(20)

        log(lf.step("Cleaning data"))
        if "Well Name" not in df.columns:
            return {"error": "Missing required column: Well Name (check spelling)."}
        df["Well Name Cleaned"] = df["Well Name"].apply(clean_well_name)
        log(lf.detail("Cleaned Well Name column"))
        sample_df = df[["Well Name", "Well Name Cleaned"]].head(3)
        for _, row in sample_df.iterrows():
            log(lf.item(f"'{row['Well Name']}' → '{row['Well Name Cleaned']}'"))
        progress(30)

        log(lf.step("Validating data"))
        required_cols = [
            "Well Name",
            "UWI",
            "Subsea Elevation",
            "Inclination",
            "Azimuth Angle",
            "Measured Depth",
            "True Vertical Depth",
            "Longitude",
            "Latitude",
            "East",
            "North",
            "PAD",
        ]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            error_msg = f"Missing required columns: {missing_cols}"
            log(lf.error(error_msg))
            return {"error": error_msg}
        log(lf.detail("All required columns present"))

        null_counts = df[required_cols].isnull().sum()
        if null_counts.sum() > 0:
            log(lf.warn("Null values found in required columns:"))
            for col in required_cols:
                if null_counts[col] > 0:
                    log(lf.item(f"{col}: {lf.num(int(null_counts[col]))} nulls"))
        progress(40)

        log(lf.step("Matching wells to database"))
        conn = get_sql_conn()
        valid_wells_df = pd.read_sql(
            """
            SELECT DISTINCT [Well Name]
            FROM PCE_WM
            WHERE [Well Name] IS NOT NULL
              AND LTRIM(RTRIM([Well Name])) <> ''
              AND ([Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N')
        """,
            conn,
        )
        valid_wells_df["Match Key"] = valid_wells_df["Well Name"].apply(
            well_name_match_key
        )
        valid_wells = {k for k in valid_wells_df["Match Key"].tolist() if k}
        log(lf.detail(f"Found {lf.num(len(valid_wells))} valid wells in database"))
        db_samples = list(valid_wells)[:3]
        log(lf.detail(f"Sample DB match keys (normalized): {db_samples}"))

        df["Well Found"] = df["Well Name"].apply(
            lambda w: survey_well_name_matches_wm_keys(w, valid_wells)
        )
        matched_df = df[df["Well Found"]].copy()
        unmatched_df = df[~df["Well Found"]].copy()
        log(lf.success(f"{lf.num(len(matched_df))} rows matched to database wells"))
        log(lf.warn(f"{lf.num(len(unmatched_df))} rows did not match"))

        if not unmatched_df.empty:
            log(lf.detail("Sample unmatched wells (first 10):"))
            for name in unmatched_df["Well Name Cleaned"].dropna().unique()[:10]:
                log(lf.item(f"'{name}'"))
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            unmatched_file = f"unmatched_survey_wells_{timestamp}.csv"
            unmatched_df[["Well Name", "Well Name Cleaned", "UWI"]].drop_duplicates().to_csv(
                unmatched_file, index=False
            )
            log(lf.detail(f"Unmatched wells saved to: {unmatched_file}"))

        progress(50)
        if matched_df.empty:
            log(lf.error("No matching wells to import"))
            return {
                "total_rows": len(df),
                "matched": 0,
                "unmatched": len(unmatched_df),
                "inserted": 0,
                "duplicates": 0,
                "errors": 0,
            }

        log(lf.step(f"Processing with mode: {import_mode}"))
        cursor = conn.cursor()
        original_matched_count = len(matched_df)
        matched_df, skipped_precheck = _apply_append_or_overwrite(
            conn, cursor, matched_df, import_mode, log
        )

        if import_mode == "append":
            skipped_count = skipped_precheck
        else:
            skipped_count = 0

        if matched_df.empty:
            log(lf.detail("No new records to insert. All records already exist in database."))
            return {
                "total_rows": len(df),
                "matched": original_matched_count,
                "unmatched": len(unmatched_df),
                "inserted": 0,
                "duplicates": skipped_count,
                "errors": 0,
            }

        progress(60)
        log(lf.step("Inserting data into database"))
        source_file = os.path.basename(excel_path)
        total_inserted, duplicate_skipped, ins_err = _batch_insert_surveys(
            cursor, conn, matched_df, source_file, log, progress, 60, 100
        )
        progress(100)

        summary = _finalize_survey_summary(
            import_mode,
            len(df),
            original_matched_count,
            len(unmatched_df),
            matched_df,
            skipped_count if import_mode == "append" else 0,
            duplicate_skipped,
            total_inserted,
            ins_err,
            log,
        )
        return summary

    except Exception as e:
        log(lf.error(str(e)))
        traceback.print_exc()
        return {"error": str(e)}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _read_cell_raw(df: pd.DataFrame, r: int, c: int) -> Any:
    if r < 0 or r >= len(df) or c < 0 or c >= len(df.columns):
        return None
    return df.iat[r, c]


def import_directional_survey_with_mapping(
    excel_path: str,
    mapping_spec: DirectionalSurveyMappingSpec,
    import_mode: str = "append",
    progress_callback=None,
    log_callback=None,
) -> dict:
    """
    Import a single-well directional survey workbook using user-defined row/column mapping.
    UWI, PAD, and the stored [Well Name] on each row come from the matched PCE_WM record
    (Value Navigator UWI, Pad Name, Well Name). The survey file cell is only used to find that row.
    """
    def log(message):
        if log_callback:
            log_callback(message)
        else:
            print(message)

    def progress(value):
        if progress_callback:
            progress_callback(value)

    conn = None
    try:
        log(
            lf.header(
                "SURVEY DATA IMPORT (directional)",
                File=os.path.basename(excel_path),
                Mode=import_mode,
            )
        )

        md_col = mapping_spec.columns.get("Measured Depth")
        if md_col is None:
            return {"error": "Measured Depth column is not mapped."}

        log(lf.step("Reading survey file (raw layout)"))
        df_raw = read_survey_raw_grid(excel_path, mapping_spec.sheet_index)
        log(lf.detail(f"Grid shape: {lf.num(len(df_raw))} rows × {lf.num(len(df_raw.columns))} cols"))
        progress(10)

        wnr, wnc = mapping_spec.well_name_row, mapping_spec.well_name_col
        raw_name = _read_cell_raw(df_raw, wnr, wnc)
        if raw_name is None or (isinstance(raw_name, float) and pd.isna(raw_name)):
            return {"error": f"No well name at row {wnr + 1}, column {wnc + 1}."}
        well_name = str(raw_name).strip()
        if not well_name:
            return {"error": "Well name cell is empty."}

        cleaned = clean_well_name(well_name)
        if not cleaned:
            return {"error": "Well name is invalid after cleaning."}

        log(lf.step("Resolving UWI, PAD, and Well Name from PCE_WM"))
        uwi, pad, wm_well_name, err = lookup_wm_uwi_pad_for_directional(well_name)
        if err:
            log(lf.error(err))
            return {"error": err}
        log(lf.detail(f"UWI: {uwi}"))
        log(lf.detail(f"PAD: {pad or '(empty)'}"))
        log(lf.detail(f"Well Name (from PCE_WM): {wm_well_name}"))
        progress(25)

        data_start = mapping_spec.resolved_data_start_row()
        rows_out: List[dict] = []
        max_r = len(df_raw)

        def col_val(r: int, field: str) -> Any:
            ci = mapping_spec.columns.get(field)
            if ci is None:
                return None
            v = _read_cell_raw(df_raw, r, ci)
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            return v

        for r in range(data_start, max_r):
            md = col_val(r, "Measured Depth")
            if md is None or (isinstance(md, float) and pd.isna(md)):
                break
            if isinstance(md, str) and not md.strip():
                break
            try:
                float(md)
            except (TypeError, ValueError):
                break

            row_dict = {
                "UWI": uwi,
                "Well Name Cleaned": wm_well_name,
                "Subsea Elevation": col_val(r, "Subsea Elevation"),
                "Inclination": col_val(r, "Inclination"),
                "Azimuth Angle": col_val(r, "Azimuth Angle"),
                "Measured Depth": md,
                "True Vertical Depth": col_val(r, "True Vertical Depth"),
                "Longitude": col_val(r, "Longitude"),
                "Latitude": col_val(r, "Latitude"),
                "East": col_val(r, "East"),
                "North": col_val(r, "North"),
                "PAD": pad,
            }
            rows_out.append(row_dict)

        if not rows_out:
            return {"error": "No survey data rows found below the header row (check mapping)."}

        log(lf.detail(f"Parsed {lf.num(len(rows_out))} survey station rows"))
        progress(40)

        matched_df = pd.DataFrame(rows_out)
        for col in INSERT_COLS:
            if col not in matched_df.columns:
                matched_df[col] = None

        conn = get_sql_conn()
        cursor = conn.cursor()
        log(lf.step(f"Processing with mode: {import_mode}"))
        original_matched_count = len(matched_df)
        matched_df, skipped_precheck = _apply_append_or_overwrite(
            conn, cursor, matched_df, import_mode, log
        )
        skipped_count = skipped_precheck if import_mode == "append" else 0

        if matched_df.empty:
            log(lf.detail("No new records to insert."))
            return {
                "total_rows": len(rows_out),
                "matched": original_matched_count,
                "unmatched": 0,
                "inserted": 0,
                "duplicates": skipped_count,
                "errors": 0,
            }

        progress(60)
        log(lf.step("Inserting data into database"))
        source_file = os.path.basename(excel_path)
        total_inserted, duplicate_skipped, ins_err = _batch_insert_surveys(
            cursor, conn, matched_df, source_file, log, progress, 60, 100
        )
        progress(100)

        summary = _finalize_survey_summary(
            import_mode,
            len(rows_out),
            original_matched_count,
            0,
            matched_df,
            skipped_count if import_mode == "append" else 0,
            duplicate_skipped,
            total_inserted,
            ins_err,
            log,
        )
        return summary

    except Exception as e:
        log(lf.error(str(e)))
        traceback.print_exc()
        return {"error": str(e)}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def main():
    if len(sys.argv) < 2:
        print("Usage: python survey_import.py <excel_file_path> [mode]")
        print("Modes: append (default), overwrite, merge")
        return

    excel_path = sys.argv[1]
    mode = sys.argv[2] if len(sys.argv) > 2 else "append"

    import_surveys(excel_path, mode)


if __name__ == "__main__":
    main()
