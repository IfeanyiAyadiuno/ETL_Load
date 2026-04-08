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
    "Offset in EW",
    "Offset in NS",
    "East",
    "North",
]


def clean_well_name(name):
    """Clean well name by removing extra spaces and normalizing"""
    if pd.isna(name) or not isinstance(name, str):
        return name

    cleaned = name.strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = re.sub(r"\s*-\s*", "-", cleaned)
    return cleaned


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
        "offset in ew": "Offset in EW",
        "offset in ns": "Offset in NS",
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
        cols = d.get("columns") or {}
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
            [Offset in EW], [Offset in NS],
            [East], [North],
            <redacted_PAD>, [SourceFile]
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
    "Offset in EW",
    "Offset in NS",
    "East",
    "North",
    "PAD",
]


def lookup_wm_uwi_pad_for_directional(
    cleaned_well_name: str,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Match cleaned well name to PCE_WM [Base Composite Name] (cleaned).
    Returns (uwi, pad, error_message). error_message set if 0 or multiple matches.
    """
    conn = get_sql_conn()
    try:
        df = pd.read_sql(
            """
            SELECT [Base Composite Name], [Value Navigator UWI], [Pad Name]
            FROM PCE_WM
            WHERE [Base Composite Name] IS NOT NULL
              AND ([Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N')
            """,
            conn,
        )
    finally:
        conn.close()

    if df.empty:
        return None, None, "No wells found in PCE_WM."

    df = df.copy()
    df["_clean"] = df["Base Composite Name"].apply(
        lambda x: clean_well_name(x) if pd.notna(x) and isinstance(x, str) else ""
    )
    m = df[df["_clean"] == cleaned_well_name]
    if len(m) == 0:
        return None, None, (
            f"No PCE_WM row matches well name after cleaning: '{cleaned_well_name}'"
        )
    if len(m) > 1:
        return None, None, (
            f"Multiple PCE_WM rows ({len(m)}) match well name '{cleaned_well_name}'. "
            "Resolve duplicates in Well Master."
        )
    uwi = m.iloc[0]["Value Navigator UWI"]
    pad = m.iloc[0]["Pad Name"]
    if pd.isna(uwi) or str(uwi).strip() == "":
        return None, None, "Value Navigator UWI is missing in PCE_WM for this well."
    uwi_s = str(uwi).strip()
    pad_s = "" if pd.isna(pad) or pad is None else str(pad).strip()
    return uwi_s, pad_s, None


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

        placeholders = ",".join(["?"] for _ in uwis_to_check)
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
        log(lf.step("Reading Excel file"))
        df = pd.read_excel(excel_path)
        log(lf.detail(f"Read {lf.num(len(df))} rows from Excel"))
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
            "Offset in EW",
            "Offset in NS",
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
            SELECT DISTINCT [Base Composite Name]
            FROM PCE_WM
            WHERE [Base Composite Name] IS NOT NULL
              AND ([Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N')
        """,
            conn,
        )
        valid_wells_df["Cleaned Name"] = valid_wells_df["Base Composite Name"].apply(
            clean_well_name
        )
        valid_wells = set(valid_wells_df["Cleaned Name"].tolist())
        log(lf.detail(f"Found {lf.num(len(valid_wells))} valid wells in database"))
        db_samples = list(valid_wells)[:3]
        log(lf.detail(f"Sample DB names: {db_samples}"))

        df["Well Found"] = df["Well Name Cleaned"].isin(valid_wells)
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
    UWI and PAD come from PCE_WM (Value Navigator UWI, Pad Name) via Base Composite Name match.
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

        log(lf.step("Reading Excel (raw layout)"))
        df_raw = pd.read_excel(
            excel_path, sheet_name=mapping_spec.sheet_index, header=None, dtype=object
        )
        log(lf.detail(f"Sheet shape: {lf.num(len(df_raw))} rows × {lf.num(len(df_raw.columns))} cols"))
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

        log(lf.step("Resolving UWI and PAD from PCE_WM"))
        uwi, pad, err = lookup_wm_uwi_pad_for_directional(cleaned)
        if err:
            log(lf.error(err))
            return {"error": err}
        log(lf.detail(f"UWI: {uwi}"))
        log(lf.detail(f"PAD: {pad or '(empty)'}"))
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
                "Well Name Cleaned": cleaned,
                "Subsea Elevation": col_val(r, "Subsea Elevation"),
                "Inclination": col_val(r, "Inclination"),
                "Azimuth Angle": col_val(r, "Azimuth Angle"),
                "Measured Depth": md,
                "True Vertical Depth": col_val(r, "True Vertical Depth"),
                "Offset in EW": col_val(r, "Offset in EW"),
                "Offset in NS": col_val(r, "Offset in NS"),
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
