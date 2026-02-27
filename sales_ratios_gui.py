# sales_ratios_gui.py
import time
from datetime import datetime, timedelta
from db_connection import get_sql_conn

def run_sales_ratios_update(start_month, end_month, progress_callback=None, log_callback=None):
    """
    Update sales ratios in PCE_CDA and PCE_Production for a range of months
    
    Args:
        start_month: Start month in format "MMM YYYY" (e.g., "Jan 2020")
        end_month: End month in format "MMM YYYY" (e.g., "Dec 2025")
        progress_callback: Function to call with progress percentage (0-100)
        log_callback: Function to call with log messages
    
    Returns:
        dict: Summary statistics
    """
    
    def log(message):
        if log_callback:
            log_callback(message)
        else:
            print(message)
    
    def progress(value):
        if progress_callback:
            progress_callback(value)
    
    log("\n" + "="*80)
    log("SALES RATIOS UPDATE")
    log("="*80)
    log(f"Range: {start_month} to {end_month}")
    
    total_start = time.time()
    
    try:
        # Parse months
        start_date = datetime.strptime(start_month, "%b %Y")
        end_date = datetime.strptime(end_month, "%b %Y")
        
        # Ensure start is before end
        if start_date > end_date:
            error_msg = "Start month must be before end month"
            log(f"ERROR: {error_msg}")
            return {"error": error_msg}
        
        # Connect to database
        log("\nConnecting to SQL Server...")
        conn = get_sql_conn()
        cursor = conn.cursor()
        log("✅ Database connected")
        
        # Get all months in range
        cursor.execute("""
            SELECT DISTINCT MonthStartDate 
            FROM Allocation_Factors 
            WHERE MonthStartDate BETWEEN ? AND ?
            ORDER BY MonthStartDate
        """, start_date, end_date)
        
        all_months = cursor.fetchall()
        log(f"Found {len(all_months)} months to process")
        
        if len(all_months) == 0:
            log("No allocation factors found in selected range")
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
        
        for month_idx, month_row in enumerate(all_months):
            month_start = month_row[0]
            month_name = month_start.strftime('%B %Y')
            
            # Calculate month end
            if month_start.month == 12:
                month_end = datetime(month_start.year + 1, 1, 1) - timedelta(days=1)
            else:
                month_end = datetime(month_start.year, month_start.month + 1, 1) - timedelta(days=1)
            
            month_start_date = month_start
            month_end_date = month_end.date()
            days_in_month = (month_end_date - month_start_date).days + 1
            
            log(f"\n{'='*60}")
            log(f"Processing {month_name}...")
            log(f"Range: {month_start_date} to {month_end_date}")
            
            # Get allocation factors for this month
            cursor.execute("""
                SELECT [Well Name], 
                       WH_to_S2_AllocFactor,
                       WH_to_Sales_AllocFactor,
                       WH_to_Sales_Cond_AllocFactor,
                       Sales_Gas
                FROM Allocation_Factors 
                WHERE MonthStartDate = ?
            """, month_start)
            
            alloc_rows = cursor.fetchall()
            
            if len(alloc_rows) == 0:
                log(f"  No allocation factors for {month_name}, skipping")
                continue
            
            month_wells_updated = 0
            month_cda_records = 0
            
            # First, count how many CDA records exist for this month
            cursor.execute("""
                SELECT COUNT(*) FROM PCE_CDA 
                WHERE ProdDate BETWEEN ? AND ?
            """, month_start_date, month_end_date)
            month_cda_records = cursor.fetchone()[0]
            
            for well_name, wh_to_s2, wh_to_sales, wh_to_sales_cond, sales_gas in alloc_rows:
                try:
                    # Convert to float with defaults
                    wh_to_s2_val = float(wh_to_s2) if wh_to_s2 is not None else 1.0
                    wh_to_sales_val = float(wh_to_sales) if wh_to_sales is not None else 1.0
                    wh_to_sales_cond_val = float(wh_to_sales_cond) if wh_to_sales_cond is not None else 1.0
                    monthly_sales_gas_val = float(sales_gas) if sales_gas is not None else 0
                    
                    # -----------------------------------------------------------------
                    # UPDATE PCE_CDA
                    # -----------------------------------------------------------------
                    
                    # Update 1: Gas - S2 Production
                    cursor.execute("""
                        UPDATE PCE_CDA 
                        SET [Gas - S2 Production] = ? * [GasWH_Production]
                        WHERE [Well Name] = ? 
                        AND ProdDate BETWEEN ? AND ?
                    """, wh_to_s2_val, well_name, month_start_date, month_end_date)
                    
                    # Update 2: Gas - Sales Production
                    if monthly_sales_gas_val > 0:
                        cursor.execute("""
                            UPDATE PCE_CDA 
                            SET [Gas - Sales Production] = ? * [GasWH_Production]
                            WHERE [Well Name] = ? 
                            AND ProdDate BETWEEN ? AND ?
                        """, wh_to_sales_val, well_name, month_start_date, month_end_date)
                    else:
                        daily_sales_gas = monthly_sales_gas_val / days_in_month
                        cursor.execute("""
                            UPDATE PCE_CDA 
                            SET [Gas - Sales Production] = ?
                            WHERE [Well Name] = ? 
                            AND ProdDate BETWEEN ? AND ?
                        """, daily_sales_gas, well_name, month_start_date, month_end_date)
                    
                    # Update 3: Condensate - Sales Production
                    cursor.execute("""
                        UPDATE PCE_CDA 
                        SET [Condensate - Sales Production] = ? * [Condensate_WH_Production]
                        WHERE [Well Name] = ? 
                        AND ProdDate BETWEEN ? AND ?
                    """, wh_to_sales_cond_val, well_name, month_start_date, month_end_date)
                    
                    # Update 4: Sales CGR Ratio
                    cursor.execute("""
                        UPDATE PCE_CDA 
                        SET [Sales CGR Ratio] = 
                            IIF([Gas - Sales Production] > 0, 
                                [Condensate - Sales Production] / [Gas - Sales Production], 
                                0)
                        WHERE [Well Name] = ? 
                        AND ProdDate BETWEEN ? AND ?
                    """, well_name, month_start_date, month_end_date)
                    
                    month_wells_updated += 1
                    
                except Exception as e:
                    log(f"  Error updating well '{well_name}': {str(e)[:100]}")
            
            # Commit CDA updates
            conn.commit()
            
            # -----------------------------------------------------------------
            # UPDATE PCE_PRODUCTION
            # -----------------------------------------------------------------
            log(f"  Updating PCE_Production for {month_name}...")

            # First, get the mapping between Well Name and Composite Name
            cursor.execute("""
                SELECT [Well Name], [Composite Name]
                FROM PCE_WM
                WHERE [Composite Name] IS NOT NULL
            """)
            well_mapping = dict(cursor.fetchall())
            log(f"  Loaded {len(well_mapping)} well name mappings")

            # Update PCE_Production by joining through PCE_WM
            cursor.execute("""
                UPDATE p
                SET 
                    p.[Gas S2 Production (10³m³)] = c.[Gas - S2 Production],
                    p.[Gas Sales Production (10³m³)] = c.[Gas - Sales Production],
                    p.[Condensate Sales (m³/d)] = c.[Condensate - Sales Production],
                    p.[Sales CGR (m³/e³m³)] = c.[Sales CGR Ratio]
                FROM PCE_Production p
                INNER JOIN PCE_WM w ON p.[Well Name] = w.[Composite Name]
                INNER JOIN PCE_CDA c ON w.[Well Name] = c.[Well Name] AND p.[Date] = c.ProdDate
                WHERE c.ProdDate BETWEEN ? AND ?
            """, month_start_date, month_end_date)

            production_updated = cursor.rowcount
            conn.commit()

            log(f"  ✅ Updated {production_updated} records in PCE_Production")
            log(f"  ✅ Updated {month_wells_updated} wells in PCE_CDA")
            
            
            # Update progress
            progress_percent = int((month_idx + 1) / total_months * 100)
            progress(progress_percent)
        
        conn.close()
        
        total_time = time.time() - total_start
        
        summary = {
            'months_processed': months_processed,
            'wells_updated': total_wells_updated,
            'cda_records': total_cda_records,
            'production_records': total_cda_records,  # Should be same as CDA records
            'duration': total_time
        }
        
        log("\n" + "="*80)
        log("UPDATE COMPLETE!")
        log("="*80)
        
        return summary
        
    except Exception as e:
        error_msg = f"ERROR: {str(e)}"
        log(error_msg)
        import traceback
        log(traceback.format_exc())
        return {"error": error_msg}