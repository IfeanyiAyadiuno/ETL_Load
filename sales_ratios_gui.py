# sales_ratios_gui.py
import os
import time
from datetime import datetime, timedelta

import log_format as lf
from db_connection import get_sql_conn


def preflight_sales_ratios_range(start_month, end_month):
    """
    Check Allocation_Factors and PCE_CDA coverage for the same month range as the update.

    Returns:
        dict with allocation_month_count, cda_row_count; or {"error": str} on parse failure.
    """
    try:
        start_date = datetime.strptime(start_month, "%b %Y")
        end_date = datetime.strptime(end_month, "%b %Y")
    except ValueError as e:
        return {"error": f"Invalid month format: {e}"}

    if start_date > end_date:
        return {"error": "Start month must be before end month"}

    cda_start = start_date.replace(day=1).date()
    if end_date.month == 12:
        end_last = datetime(end_date.year + 1, 1, 1) - timedelta(days=1)
    else:
        end_last = datetime(end_date.year, end_date.month + 1, 1) - timedelta(days=1)
    cda_end = end_last.date()

    conn = get_sql_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT MonthStartDate
            FROM Allocation_Factors
            WHERE MonthStartDate BETWEEN ? AND ?
            """,
            start_date,
            end_date,
        )
        allocation_month_count = len(cursor.fetchall())

        cursor.execute(
            """
            SELECT COUNT(*) FROM PCE_CDA
            WHERE ProdDate BETWEEN ? AND ?
            """,
            cda_start,
            cda_end,
        )
        cda_row_count = cursor.fetchone()[0]

        return {
            "allocation_month_count": allocation_month_count,
            "cda_row_count": cda_row_count,
        }
    finally:
        conn.close()


def run_sales_ratios_update(
    start_month,
    end_month,
    progress_callback=None,
    log_callback=None,
    cancelled=None,
    accumap_path=None,
):
    """
    Update sales ratios in PCE_CDA and PCE_Production for a range of months.
    Merges Accumap public sales gas into Allocation_Factors per month, then applies
    gas sales and CGR on PCE_CDA and a four-column sync from CDA to PCE_Production
    (see sales_allocation_updates).

    Args:
        start_month: Start month in format "MMM YYYY" (e.g., "Jan 2020")
        end_month: End month in format "MMM YYYY" (e.g., "Dec 2025")
        progress_callback: Function to call with progress percentage (0-100)
        log_callback: Function to call with log messages
        cancelled: Optional callable returning True if the run should stop (checked between months).
        accumap_path: Path to Public Data Accumap Excel (required).

    Returns:
        dict: Summary statistics; includes cancelled=True if stopped early
    """

    def log(message):
        if log_callback:
            log_callback(message)
        else:
            print(message)
    
    def progress(value):
        if progress_callback:
            progress_callback(value)

    def is_cancelled():
        return cancelled is not None and cancelled()

    log(lf.header("SALES RATIOS UPDATE", Range=f"{start_month} to {end_month}"))

    from sales_allocation_updates import (
        merge_accumap_into_allocation_factors,
        apply_full_sales_ratios_for_month,
    )

    if not accumap_path or not os.path.isfile(accumap_path):
        error_msg = f"Accumap file not found or not configured: {accumap_path!r}"
        log(lf.error(error_msg))
        return {"error": error_msg}
    
    total_start = time.time()
    
    try:
        # Parse months
        start_date = datetime.strptime(start_month, "%b %Y")
        end_date = datetime.strptime(end_month, "%b %Y")
        
        # Ensure start is before end
        if start_date > end_date:
            error_msg = "Start month must be before end month"
            log(lf.error(error_msg))
            return {"error": error_msg}
        
        # Connect to database (always closed in inner finally)
        conn = get_sql_conn()
        try:
            cursor = conn.cursor()
            
            # Get all months in range
            cursor.execute("""
                SELECT DISTINCT MonthStartDate 
                FROM Allocation_Factors 
                WHERE MonthStartDate BETWEEN ? AND ?
                ORDER BY MonthStartDate
            """, start_date, end_date)
            
            all_months = cursor.fetchall()
            log(lf.detail(f"Processing {lf.num(len(all_months))} months"))
            
            if len(all_months) == 0:
                log(lf.detail("No allocation factors found in selected range"))
                return {
                    'months_processed': 0,
                    'wells_updated': 0,
                    'cda_records': 0,
                    'production_records': 0,
                    'duration': 0
                }
            
            total_months = len(all_months)
            months_processed = 0
            total_wells_updated = 0
            total_cda_records = 0
            total_production_records = 0

            for month_idx, month_row in enumerate(all_months):
                if is_cancelled():
                    log(lf.warn(
                        f"Cancelled by user after {lf.num(months_processed)} month(s) completed."
                    ))
                    total_time = time.time() - total_start
                    return {
                        "months_processed": months_processed,
                        "wells_updated": total_wells_updated,
                        "cda_records": total_cda_records,
                        "production_records": total_production_records,
                        "duration": total_time,
                        "cancelled": True,
                    }

                month_start = month_row[0]
                month_name = month_start.strftime('%B %Y')
                log(lf.step(f"Processing {month_name}"))

                merge_result = merge_accumap_into_allocation_factors(
                    conn, month_start, accumap_path, log=log
                )
                if "error" in merge_result:
                    log(lf.error(merge_result["error"]))
                    return {"error": merge_result["error"]}

                cda_rows_updated, production_updated, month_wells_updated = (
                    apply_full_sales_ratios_for_month(conn, month_start, log=log)
                )

                progress(int(((month_idx + 1) / total_months) * 100))

                months_processed += 1
                total_wells_updated += month_wells_updated
                total_cda_records += cda_rows_updated
                total_production_records += max(0, production_updated)

                log(lf.detail(
                    f"Updated {lf.num(month_wells_updated)} wells / "
                    f"{lf.num(cda_rows_updated)} CDA rows, "
                    f"{lf.num(production_updated)} Production records"
                ))
            
            total_time = time.time() - total_start
            
            summary = {
                'months_processed': months_processed,
                'wells_updated': total_wells_updated,
                'cda_records': total_cda_records,
                'production_records': total_production_records,
                'duration': total_time
            }
            
            log(lf.summary("COMPLETE", {
                "Months processed": months_processed,
                "Wells updated": total_wells_updated,
                "PCE_CDA records": total_cda_records,
                "PCE_Production records": total_production_records,
                "Duration": lf.elapsed(total_time),
            }))
            
            return summary
        finally:
            conn.close()
        
    except Exception as e:
        error_msg = f"ERROR: {str(e)}"
        log(lf.error(str(e)))
        import traceback
        for line in traceback.format_exc().strip().split("\n"):
            log(lf.detail(line))
        return {"error": error_msg}