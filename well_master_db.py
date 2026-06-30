# well_master_db.py — PCE_WM database operations for Well Master

from datetime import date, datetime

import log_format as lf

# Python key -> SQL column for Well Master "Additional Fields" dialog
ADDITIONAL_FIELD_COLUMNS = (
    ("bottom_hole_latitude", "Bottom Hole Latitude", "float"),
    ("bottom_hole_longitude", "Bottom Hole Longitude", "float"),
    ("bottom_hole_utm_easting_m", "Bottom Hole UTM Easting (m)", "float"),
    ("bottom_hole_utm_northing_m", "Bottom Hole UTM Northing (m)", "float"),
    ("bottom_hole_utm_zone", "Bottom Hole UTM Zone", "int"),
    ("surface_hole_latitude", "Surface Hole Latitude", "float"),
    ("surface_hole_longitude", "Surface Hole Longitude", "float"),
    ("surface_hole_utm_easting_m", "Surface Hole UTM Easting (m)", "float"),
    ("surface_hole_utm_northing_m", "Surface Hole UTM Northing (m)", "float"),
    ("surface_hole_utm_zone", "Surface Hole UTM Zone", "int"),
    ("kb_elevation_m", "KB Elevation (m)", "float"),
    ("ground_elevation_m", "Ground Elevation (m)", "float"),
    ("max_true_vertical_depth_m", "Max True Vertical Depth (m)", "float"),
    ("total_depth_m", "Total Depth (m)", "float"),
    ("spud_date", "Spud Date", "date"),
    ("rig_release_date", "Rig Release Date", "date"),
    ("outside_diameter_mm", "Outside Diameter (mm)", "float"),
    ("tubing_strength_mpa", "Tubing Strength (MPa)", "float"),
    ("tubing_linear_weight_kg_m", "Tubing Linear Weight (kg/m)", "float"),
    ("fluid_pumped_m3", "Fluid Pumped (m³)", "float"),
    ("proppant_pumped_t", "Proppant Pumped (t)", "float"),
    ("initial_flow_date", "Initial flow date", "date"),
)

_ADDITIONAL_FIELD_SQL_LIST = ", ".join(
    f"[{sql_name}]" for _key, sql_name, _typ in ADDITIONAL_FIELD_COLUMNS
)

# Main-grid / core PCE_WM columns that must never appear in the Additional Fields
# dialog. Everything else on the table that is not a built-in additional field is
# auto-discovered as a custom additional field.
_CORE_PCE_WM_COLUMNS = frozenset(
    {
        "Well Name",
        "GasIDREC",
        "PressuresIDREC",
        "Formation Producer",
        "Layer Producer",
        "Fault Block",
        "Pad Name",
        "Completions Technology",
        "Lateral Length",
        "Value Navigator UWI",
        "Orient",
        "On Production Year",
        "Composite Name",
        "Horizontal Distance Right",
        "Horizontal Distance Left",
        "Vertical Distance Above",
        "Vertical Distance Below",
        "Exception",
        "Bounded",
    }
)

# User-selectable custom column types -> (SQL type for ALTER TABLE, coercion type)
CUSTOM_FIELD_TYPES = {
    "text": ("NVARCHAR(255)", "text"),
    "float": ("FLOAT", "float"),
    "int": ("INT", "int"),
    "date": ("DATE", "date"),
}


def _coercion_type_from_sql(data_type) -> str:
    """Map a SQL Server DATA_TYPE to one of: float / int / date / text."""
    dt = (data_type or "").strip().lower()
    if dt in ("float", "real", "decimal", "numeric", "money", "smallmoney"):
        return "float"
    if dt in ("int", "bigint", "smallint", "tinyint"):
        return "int"
    if dt in ("date", "datetime", "datetime2", "smalldatetime", "datetimeoffset"):
        return "date"
    return "text"


class WellMasterDB:
    """Handles all database operations for Well Master List"""

    @staticmethod
    def get_all_wells():
        """Load all wells from PCE_WM"""
        from db_connection import sql_connection

        try:
            with sql_connection() as conn:
                cursor = conn.cursor()

                query = """
            SELECT 
                [Well Name],
                [GasIDREC],
                [PressuresIDREC],
                [Formation Producer],
                [Layer Producer],
                [Fault Block],
                [Pad Name],
                [Completions Technology],
                [Lateral Length],
                [Value Navigator UWI],
                [Orient],
                [On Production Year],
                [Composite Name],
                [Horizontal Distance Right],
                [Horizontal Distance Left],
                [Vertical Distance Above],
                [Vertical Distance Below],
                [Exception],
                [Bounded]
            FROM PCE_WM
            WHERE [Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N'
            ORDER BY [Well Name]
            """

                cursor.execute(query)
                rows = cursor.fetchall()

            # Convert to list of dicts
            wells = []
            for row in rows:
                # Map SQL columns to dictionary keys
                exception_val = row[17]
                if exception_val is None or str(exception_val).strip() == "":
                    exception_val = "N"
                else:
                    exception_val = str(exception_val).strip().upper()

                bounded_val = row[18]
                bounded_val = str(bounded_val).strip() if bounded_val is not None else ""

                well = {
                    'well_name': row[0],
                    'gas_idrec': row[1],
                    'pressures_idrec': row[2],
                    'formation': row[3],
                    'layer': row[4],
                    'fault_block': row[5],
                    'pad_name': row[6],
                    'completions_tech': row[7],
                    'lateral_length': row[8],
                    'value_nav_uwi': row[9],
                    'orient': row[10],
                    'on_production_year': row[11],
                    'composite_name': row[12],
                    'horizontal_right': row[13],
                    'horizontal_left': row[14],
                    'vertical_above': row[15],
                    'vertical_below': row[16],
                    'exception': exception_val,
                    'bounded': bounded_val,
                }
                wells.append(well)

            return wells

        except Exception as e:
            print(lf.error(f"Error loading wells: {e}"))
            return []

    @staticmethod
    def get_dropdown_options():
        """Get unique values for dropdown fields"""
        from db_connection import sql_connection

        options = {}
        fields = [
            'Formation Producer',
            'Layer Producer',
            'Fault Block',
            'Completions Technology',
            'Orient'
        ]

        try:
            with sql_connection() as conn:
                cursor = conn.cursor()

                for field in fields:
                    query = f"""
                SELECT DISTINCT [{field}] 
                FROM PCE_WM 
                WHERE [{field}] IS NOT NULL AND [{field}] != ''
                ORDER BY [{field}]
                """
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    options[field] = [row[0] for row in rows]

            return options

        except Exception as e:
            print(lf.error(f"Error loading dropdown options: {e}"))
            return {}

    @staticmethod
    def is_pending(well):
        """Check if a well is pending (has IDs but missing other fields)"""
        # Has required IDs and Well Name
        if not well.get('well_name') or not well.get('gas_idrec') or not well.get('pressures_idrec'):
            return False

        # Check if other fields are all NULL/empty/0
        # Lateral length of 0 is considered "missing" or "not set"
        other_fields = [
            well.get('formation'),
            well.get('layer'),
            well.get('fault_block'),
            well.get('pad_name'),
            well.get('completions_tech'),
            well.get('value_nav_uwi'),
            well.get('orient'),
            well.get('composite_name')
        ]

        # Check lateral length separately (0 means missing)
        lateral = well.get('lateral_length')
        has_lateral = lateral is not None and str(lateral).strip() != '' and float(lateral) != 0

        # If all other fields are empty AND lateral is 0/missing, it's pending
        all_others_empty = all(field is None or str(field).strip() == '' for field in other_fields)

        return all_others_empty and not has_lateral

    @staticmethod
    def compose_name(well_name, layer, tech, orient):
        """Generate composite name from components"""
        w = (well_name or "").strip()
        l = (layer or "").strip()
        t = (tech or "").strip()
        o = (orient or "").strip()

        if not (w and l and t and o):
            return None
        return f"{w} - {l} - {t} - {o}"

    @staticmethod
    def _normalize_composite_value(value):
        if value is None:
            return None
        s = str(value).strip()
        return s if s else None

    @staticmethod
    def sanitize_text_field(value):
        """Trim leading/trailing whitespace; blank becomes None."""
        if value is None:
            return None
        s = str(value).strip()
        return s if s else None

    @staticmethod
    def sanitize_well_update(update):
        """Trim text fields on a well update dict before writing to PCE_WM."""
        text_keys = (
            "well_name",
            "formation",
            "layer",
            "fault_block",
            "pad_name",
            "completions_tech",
            "value_nav_uwi",
            "orient",
            "composite_name",
            "bounded",
        )
        out = dict(update)
        for key in text_keys:
            if key in out:
                out[key] = WellMasterDB.sanitize_text_field(out[key])
        if "exception" in out and out["exception"] is not None:
            exc = str(out["exception"]).strip().upper()
            out["exception"] = exc if exc else "N"
        return out

    @staticmethod
    def sync_composite_names_from_parts():
        """
        Recompute ``[Composite Name]`` from Well Name, Layer Producer,
        Completions Technology, and Orient; persist when out of sync.
        Uses a single set-based UPDATE instead of per-row round trips.
        """
        from db_connection import sql_connection

        sql = """
        UPDATE wm
        SET [Composite Name] = calc.new_composite
        FROM PCE_WM AS wm
        INNER JOIN (
            SELECT
                [Well Name],
                CASE
                    WHEN LTRIM(RTRIM(ISNULL([Well Name], ''))) <> ''
                     AND LTRIM(RTRIM(ISNULL([Layer Producer], ''))) <> ''
                     AND LTRIM(RTRIM(ISNULL([Completions Technology], ''))) <> ''
                     AND LTRIM(RTRIM(ISNULL([Orient], ''))) <> ''
                    THEN
                        LTRIM(RTRIM([Well Name])) + ' - ' +
                        LTRIM(RTRIM([Layer Producer])) + ' - ' +
                        LTRIM(RTRIM([Completions Technology])) + ' - ' +
                        LTRIM(RTRIM([Orient]))
                    ELSE NULL
                END AS new_composite,
                NULLIF(LTRIM(RTRIM(ISNULL([Composite Name], ''))), '') AS old_composite
            FROM PCE_WM
            WHERE [Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N'
        ) AS calc ON calc.[Well Name] = wm.[Well Name]
        WHERE
            (calc.new_composite IS NULL AND calc.old_composite IS NOT NULL)
            OR (calc.new_composite IS NOT NULL AND calc.old_composite IS NULL)
            OR (calc.new_composite <> calc.old_composite)
        """

        try:
            with sql_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                updated = cursor.rowcount or 0
                conn.commit()
                return updated
        except Exception as e:
            print(lf.error(f"Error syncing composite names: {e}"))
            return 0

    @staticmethod
    def backfill_shared_nad83_location_columns(cursor):
        """Copy shared NAD83 surface/bottom coordinates from another PCE_WM row onto rows with any NULL."""
        sql = """
UPDATE tgt
SET
  tgt.[Surface Location Latitude (NAD83)] = src.[Surface Location Latitude (NAD83)],
  tgt.[Surface Location Longitude (NAD83)] = src.[Surface Location Longitude (NAD83)],
  tgt.[Surface Location Easting (NAD83)] = src.[Surface Location Easting (NAD83)],
  tgt.[Surface Location Northing (NAD83)] = src.[Surface Location Northing (NAD83)],
  tgt.[Bottom Location Latitude (NAD83)] = src.[Bottom Location Latitude (NAD83)],
  tgt.[Bottom Location Longitude (NAD83)] = src.[Bottom Location Longitude (NAD83)],
  tgt.[Bottom Location Easting (NAD83)] = src.[Bottom Location Easting (NAD83)],
  tgt.[Bottom Location Northing (NAD83)] = src.[Bottom Location Northing (NAD83)]
FROM PCE_WM AS tgt
CROSS APPLY (
  SELECT TOP (1)
    d.[Surface Location Latitude (NAD83)],
    d.[Surface Location Longitude (NAD83)],
    d.[Surface Location Easting (NAD83)],
    d.[Surface Location Northing (NAD83)],
    d.[Bottom Location Latitude (NAD83)],
    d.[Bottom Location Longitude (NAD83)],
    d.[Bottom Location Easting (NAD83)],
    d.[Bottom Location Northing (NAD83)]
  FROM PCE_WM AS d
  WHERE d.[Well Name] <> tgt.[Well Name]
    AND d.[Surface Location Latitude (NAD83)] IS NOT NULL
  ORDER BY
    CASE WHEN
      d.[Surface Location Latitude (NAD83)] IS NOT NULL
      AND d.[Surface Location Longitude (NAD83)] IS NOT NULL
      AND d.[Surface Location Easting (NAD83)] IS NOT NULL
      AND d.[Surface Location Northing (NAD83)] IS NOT NULL
      AND d.[Bottom Location Latitude (NAD83)] IS NOT NULL
      AND d.[Bottom Location Longitude (NAD83)] IS NOT NULL
      AND d.[Bottom Location Easting (NAD83)] IS NOT NULL
      AND d.[Bottom Location Northing (NAD83)] IS NOT NULL
    THEN 0 ELSE 1 END,
    d.[Well Name]
) AS src
WHERE
  tgt.[Surface Location Latitude (NAD83)] IS NULL
  OR tgt.[Surface Location Longitude (NAD83)] IS NULL
  OR tgt.[Surface Location Easting (NAD83)] IS NULL
  OR tgt.[Surface Location Northing (NAD83)] IS NULL
  OR tgt.[Bottom Location Latitude (NAD83)] IS NULL
  OR tgt.[Bottom Location Longitude (NAD83)] IS NULL
  OR tgt.[Bottom Location Easting (NAD83)] IS NULL
  OR tgt.[Bottom Location Northing (NAD83)] IS NULL
"""
        cursor.execute(sql)

    @staticmethod
    def delete_well(well_name):
        """Permanently delete a well from PCE_WM by Well Name.

        Removes dependent rows first (PCE_CDA, PCE_Production, Allocation_Factors,
        PCE_Surveys) so foreign keys such as FK_PCE_CDA_PCE_WM do not block the delete.
        """
        from db_connection import get_sql_conn
        from purge_exception_wells import delete_dependent_rows_for_well_master

        conn = None
        try:
            conn = get_sql_conn()
            cursor = conn.cursor()
            delete_dependent_rows_for_well_master(cursor, [well_name])
            cursor.execute("DELETE FROM PCE_WM WHERE [Well Name] = ?", (well_name,))
            conn.commit()
            return True, None
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            return False, str(e)
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    @staticmethod
    def save_well_updates(updates):
        """Save multiple well updates to database"""
        from db_connection import get_sql_conn
        from purge_exception_wells import purge_wells

        if not updates:
            return 0, ["No updates provided"]

        conn = None
        try:
            conn = get_sql_conn()
            cursor = conn.cursor()
            updated = 0
            errors = []
            wells_to_purge = set()

            for raw_update in updates:
                update = WellMasterDB.sanitize_well_update(raw_update)
                well_name = update.get("well_name")
                if not well_name:
                    errors.append("Missing well name")
                    continue

                # Determine if Exception is changing from N -> Y for this well
                new_exception = update.get('exception')
                if new_exception is not None:
                    new_exception_norm = str(new_exception).strip().upper() or "N"
                    # Fetch current exception from DB
                    cursor.execute(
                        "SELECT [Exception] FROM PCE_WM WHERE [Well Name] = ?", well_name
                    )
                    row = cursor.fetchone()
                    if row is not None:
                        current_exception = row[0]
                        if current_exception is None or str(current_exception).strip() == "":
                            current_exception_norm = "N"
                        else:
                            current_exception_norm = str(current_exception).strip().upper()

                        # Mark for purge only on transition N -> Y
                        if current_exception_norm != "Y" and new_exception_norm == "Y":
                            wells_to_purge.add(well_name)

                # Build update query dynamically based on provided fields
                set_clauses = []
                params = []

                field_mapping = {
                    'formation': '[Formation Producer]',
                    'layer': '[Layer Producer]',
                    'fault_block': '[Fault Block]',
                    'pad_name': '[Pad Name]',
                    'completions_tech': '[Completions Technology]',
                    'lateral_length': '[Lateral Length]',
                    'on_production_year': '[On Production Year]',
                    'horizontal_distance_right': '[Horizontal Distance Right]',
                    'horizontal_distance_left': '[Horizontal Distance Left]',
                    'vertical_distance_above': '[Vertical Distance Above]',
                    'vertical_distance_below': '[Vertical Distance Below]',
                    'value_nav_uwi': '[Value Navigator UWI]',
                    'orient': '[Orient]',
                    'composite_name': '[Composite Name]',
                    'exception': '[Exception]',
                    'bounded': '[Bounded]',
                }

                for key, db_field in field_mapping.items():
                    if key in update and update[key] is not None:
                        set_clauses.append(f"{db_field} = ?")
                        params.append(update[key])

                if not set_clauses:
                    errors.append(f"No fields to update for {well_name}")
                    continue

                params.append(well_name)
                query = f"""
                UPDATE PCE_WM 
                SET {', '.join(set_clauses)}
                WHERE [Well Name] = ?
                """

                cursor.execute(query, params)
                if cursor.rowcount > 0:
                    updated += 1
                else:
                    errors.append(f"Well not found: {well_name}")

            WellMasterDB.backfill_shared_nad83_location_columns(cursor)

            conn.commit()

            # After WM updates are committed, purge data for any wells
            # whose Exception flag was changed from N -> Y during this save.
            if wells_to_purge:
                purge_wells(list(wells_to_purge))

            return updated, errors

        except Exception as e:
            if conn:
                conn.rollback()
            return 0, [str(e)]
        finally:
            if conn:
                conn.close()

    @staticmethod
    def _coerce_additional_float(value):
        if value is None:
            return None
        s = str(value).strip().replace(",", "")
        if not s or s.lower() in ("-", "nan", "none", "n/a", "na"):
            return None
        try:
            return float(s)
        except ValueError:
            return None

    @staticmethod
    def _coerce_additional_int(value):
        if value is None:
            return None
        s = str(value).strip().replace(",", "")
        if not s or s.lower() in ("-", "nan", "none", "n/a", "na"):
            return None
        try:
            return int(float(s))
        except ValueError:
            return None

    @staticmethod
    def _coerce_additional_date(value):
        if value is None:
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        s = str(value).strip()
        if not s or s.lower() in ("-", "nan", "none", "n/a", "na"):
            return None
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        return None

    # Tokens treated as "no value" (saved as NULL) rather than invalid input.
    _ADDITIONAL_BLANK_TOKENS = ("", "-", "nan", "none", "n/a", "na")

    @staticmethod
    def validate_additional_value(raw, field_type):
        """Coerce one additional-field value, reporting type errors.

        Returns ``(value, error)`` where ``error`` is ``None`` on success.
        Blank/placeholder input is valid and yields ``(None, None)``. Non-blank
        input that cannot be parsed to ``field_type`` yields ``(None, message)``.
        """
        if field_type not in ("float", "int", "date"):
            return WellMasterDB.sanitize_text_field(raw), None

        if raw is None:
            return None, None
        if field_type == "date" and isinstance(raw, (date, datetime)):
            return WellMasterDB._coerce_additional_date(raw), None

        s = str(raw).strip()
        if s.lower() in WellMasterDB._ADDITIONAL_BLANK_TOKENS:
            return None, None

        if field_type == "float":
            val = WellMasterDB._coerce_additional_float(s)
            if val is None:
                return None, f"'{s}' is not a valid number"
            return val, None
        if field_type == "int":
            cleaned = s.replace(",", "")
            try:
                val = int(float(cleaned))
            except (TypeError, ValueError):
                return None, f"'{s}' is not a valid whole number"
            return val, None
        # date
        val = WellMasterDB._coerce_additional_date(s)
        if val is None:
            return None, f"'{s}' is not a valid date (use YYYY-MM-DD)"
        return val, None

    @staticmethod
    def _format_additional_value_for_ui(value, field_type):
        if value is None:
            return ""
        if field_type == "date":
            if isinstance(value, datetime):
                return value.date().isoformat()
            if isinstance(value, date):
                return value.isoformat()
            return str(value).strip()
        if field_type == "int":
            try:
                return str(int(value))
            except (TypeError, ValueError):
                return str(value).strip()
        return str(value).strip()

    @staticmethod
    def get_additional_fields(well_name):
        """Load additional-field columns for one PCE_WM row keyed by [Well Name]."""
        from db_connection import sql_connection

        columns = WellMasterDB.all_additional_field_columns()
        wn = WellMasterDB.sanitize_text_field(well_name)
        if not wn:
            return {key: "" for key, _sql, _typ in columns}

        try:
            select_list = ", ".join(f"[{sql_name}]" for _key, sql_name, _typ in columns)
            with sql_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f"""
                SELECT {select_list}
                FROM PCE_WM
                WHERE [Well Name] = ?
                """,
                    wn,
                )
                row = cursor.fetchone()
            if not row:
                return {key: "" for key, _sql, _typ in columns}

            out = {}
            for i, (key, _sql, typ) in enumerate(columns):
                out[key] = WellMasterDB._format_additional_value_for_ui(row[i], typ)
            return out
        except Exception as e:
            print(lf.error(f"Error loading additional fields: {e}"))
            return {key: "" for key, _sql, _typ in columns}

    @staticmethod
    def save_additional_fields(well_name, fields_dict):
        """Persist additional-field columns for one well. Blank values -> NULL."""
        from db_connection import get_sql_conn

        wn = WellMasterDB.sanitize_text_field(well_name)
        if not wn:
            return False, "Missing well name"

        set_parts = []
        params = []
        for key, sql_name, typ in WellMasterDB.all_additional_field_columns():
            if key not in fields_dict:
                continue
            raw = fields_dict[key]
            if typ == "float":
                val = WellMasterDB._coerce_additional_float(raw)
            elif typ == "int":
                val = WellMasterDB._coerce_additional_int(raw)
            elif typ == "date":
                val = WellMasterDB._coerce_additional_date(raw)
            else:
                val = WellMasterDB.sanitize_text_field(raw)
            set_parts.append(f"[{sql_name}] = ?")
            params.append(val)

        if not set_parts:
            return False, "No fields to save"

        conn = None
        try:
            conn = get_sql_conn()
            cursor = conn.cursor()
            params.append(wn)
            cursor.execute(
                f"""
                UPDATE PCE_WM
                SET {', '.join(set_parts)}
                WHERE [Well Name] = ?
                """,
                params,
            )
            if cursor.rowcount <= 0:
                conn.rollback()
                return False, f"Well not found: {wn}"
            conn.commit()
            return True, None
        except Exception as e:
            if conn:
                conn.rollback()
            return False, str(e)
        finally:
            if conn:
                conn.close()

    @staticmethod
    def get_custom_field_columns():
        """Auto-discover custom additional-field columns from the PCE_WM schema.

        Returns a list of (key, sql_name, coercion_type) for every PCE_WM column
        that is neither a core/main-grid column nor a built-in additional field.
        The python key for a custom field is its SQL column name.
        """
        from db_connection import sql_connection

        builtin_sql_names = {sql_name for _key, sql_name, _typ in ADDITIONAL_FIELD_COLUMNS}
        try:
            with sql_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT COLUMN_NAME, DATA_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = 'PCE_WM'
                    ORDER BY ORDINAL_POSITION
                    """
                )
                rows = cursor.fetchall()
            extras = []
            for col_name, data_type in rows:
                name = (col_name or "").strip()
                if not name:
                    continue
                if name in _CORE_PCE_WM_COLUMNS or name in builtin_sql_names:
                    continue
                extras.append((name, name, _coercion_type_from_sql(data_type)))
            return extras
        except Exception as e:
            print(lf.error(f"Error discovering custom fields: {e}"))
            return []

    @staticmethod
    def all_additional_field_columns():
        """Built-in additional fields followed by auto-discovered custom columns."""
        columns = list(ADDITIONAL_FIELD_COLUMNS)
        seen = {sql_name for _key, sql_name, _typ in columns}
        for key, sql_name, typ in WellMasterDB.get_custom_field_columns():
            if sql_name in seen:
                continue
            seen.add(sql_name)
            columns.append((key, sql_name, typ))
        return columns

    @staticmethod
    def validate_custom_field_name(name):
        """Return (clean_name, error). Clean name is safe to use as a SQL identifier."""
        import re

        clean = (name or "").strip()
        if not clean:
            return None, "Column name cannot be empty."
        if len(clean) > 100:
            return None, "Column name must be 100 characters or fewer."
        if not re.match(r"^[A-Za-z]", clean):
            return None, "Column name must start with a letter."
        # Brackets break bracket-quoted identifiers; disallow other risky characters.
        if not re.match(r"^[A-Za-z0-9 _()/%³.\-]+$", clean):
            return None, (
                "Column name may only contain letters, numbers, spaces, and "
                "_ ( ) / % . - characters."
            )
        return clean, None

    @staticmethod
    def add_custom_field(name, field_type):
        """Create a new column on PCE_WM via ALTER TABLE. Returns (ok, error)."""
        from db_connection import get_sql_conn

        clean, err = WellMasterDB.validate_custom_field_name(name)
        if err:
            return False, err

        type_entry = CUSTOM_FIELD_TYPES.get(field_type)
        if not type_entry:
            return False, f"Unsupported field type: {field_type!r}"
        sql_type, _coercion = type_entry

        if clean in _CORE_PCE_WM_COLUMNS:
            return False, f"'{clean}' is a core Well Master column."
        if clean in {sql_name for _key, sql_name, _typ in ADDITIONAL_FIELD_COLUMNS}:
            return False, f"'{clean}' is already a built-in additional field."

        conn = None
        try:
            conn = get_sql_conn()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT 1
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = 'PCE_WM' AND COLUMN_NAME = ?
                """,
                clean,
            )
            if cursor.fetchone():
                return False, f"Column '{clean}' already exists in PCE_WM."

            cursor.execute(f"ALTER TABLE PCE_WM ADD [{clean}] {sql_type} NULL")
            conn.commit()
            return True, None
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            return False, str(e)
        finally:
            if conn:
                conn.close()
