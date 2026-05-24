# well_master_db.py — PCE_WM database operations for Well Master

import log_format as lf


class WellMasterDB:
    """Handles all database operations for Well Master List"""

    @staticmethod
    def get_all_wells():
        """Load all wells from PCE_WM"""
        from db_connection import get_sql_conn

        try:
            conn = get_sql_conn()
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
                [Composite Name],
                [Horizontal Distance Right],
                [Horizontal Distance Left],
                [Vertical Distance Above],
                [Vertical Distance Below],
                [Exception]
            FROM PCE_WM
            WHERE [Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N'
            ORDER BY [Well Name]
            """

            cursor.execute(query)
            rows = cursor.fetchall()
            conn.close()

            # Convert to list of dicts
            wells = []
            for row in rows:
                # Map SQL columns to dictionary keys
                exception_val = row[16]
                if exception_val is None or str(exception_val).strip() == "":
                    exception_val = "N"
                else:
                    exception_val = str(exception_val).strip().upper()

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
                    'composite_name': row[11],
                    'horizontal_right': row[12],
                    'horizontal_left': row[13],
                    'vertical_above': row[14],
                    'vertical_below': row[15],
                    'exception': exception_val,
                }
                wells.append(well)

            return wells

        except Exception as e:
            print(lf.error(f"Error loading wells: {e}"))
            return []

    @staticmethod
    def get_dropdown_options():
        """Get unique values for dropdown fields"""
        from db_connection import get_sql_conn

        options = {}
        fields = [
            'Formation Producer',
            'Layer Producer',
            'Fault Block',
            'Completions Technology',
            'Orient'
        ]

        try:
            conn = get_sql_conn()
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

            conn.close()
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

            for update in updates:
                well_name = update.get('well_name')
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
                    'horizontal_distance_right': '[Horizontal Distance Right]',
                    'horizontal_distance_left': '[Horizontal Distance Left]',
                    'vertical_distance_above': '[Vertical Distance Above]',
                    'vertical_distance_below': '[Vertical Distance Below]',
                    'value_nav_uwi': '[Value Navigator UWI]',
                    'orient': '[Orient]',
                    'composite_name': '[Composite Name]',
                    'exception': '[Exception]',
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
