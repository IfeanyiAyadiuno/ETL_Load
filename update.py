import time
from datetime import datetime, timedelta
import traceback

import log_format as lf
from db_connection import get_sql_conn, SQL_DATABASE, SQL_SERVER

def update_all_cda_fields():
    """
    Update ALL historical records in PCE_CDA using only GasWH_Production
    - Gas - S2 Production = WH_to_S2_AllocFactor × GasWH_Production
    - Gas - Sales Production = WH_to_Sales_AllocFactor × GasWH_Production
    - Condensate - Sales Production = WH_to_Sales_Cond_AllocFactor × Condensate_WH_Production
    - Sales CGR Ratio = Condensate_Sales_Production / Gas_Sales_Production
    """
    
    print(lf.header(
        "PCE_CDA historical update (simplified)",
        Logic="GasWH_Production only (no Gathered_Gas fallback)",
        Database=f"{SQL_SERVER}.{SQL_DATABASE}.PCE_CDA",
    ))
    print(lf.warn("Close any applications connected to SQL Server before continuing."))
    
    confirm = input("\nStart historical update? (Type 'GO' to confirm): ")
    if confirm.upper() != 'GO':
        print(lf.detail("Update cancelled."))
        return
    
    total_start = time.time()
    
    try:
        # Connect to SQL Server
        print(lf.step("Connecting to SQL Server..."))
        conn = get_sql_conn()
        cursor = conn.cursor()
        print(lf.success("Database connected."))
        
        # -----------------------------------------------------------------
        # STEP 1: Get all months from Allocation_Factors
        # -----------------------------------------------------------------
        print(lf.step("Step 1: Finding all months with allocation factors"))
        
        cursor.execute("""
            SELECT DISTINCT MonthStartDate 
            FROM Allocation_Factors 
            ORDER BY MonthStartDate
        """)
        
        all_months = cursor.fetchall()
        print(lf.detail(f"Found {lf.num(len(all_months))} months"))
        
        if len(all_months) == 0:
            print(lf.warn("No allocation factors found. Exiting."))
            return
        
        total_updated = 0
        months_processed = 0
        total_errors = 0
        
        # -----------------------------------------------------------------
        # STEP 2: Process each month
        # -----------------------------------------------------------------
        for month_row in all_months:
            month_start = month_row[0]
            month_name = month_start.strftime('%B %Y')
            
            # Calculate month end
            if month_start.month == 12:
                month_end = datetime(month_start.year + 1, 1, 1) - timedelta(days=1)
            else:
                month_end = datetime(month_start.year, month_start.month + 1, 1) - timedelta(days=1)

            # Convert to date objects for SQL query
            month_start_date = month_start  # Already a date
            month_end_date = month_end.date()  # Convert datetime to date

            print(lf.step(f"Processing {month_name}..."))
            print(lf.detail(f"Date range: {month_start_date} to {month_end_date}"))

            # Get all allocation factors for this month
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
                print(lf.detail(f"No allocation factors for {month_name}, skipping"))
                continue

            month_updated = 0
            days_in_month = (month_end_date - month_start_date).days + 1

            for well_name, wh_to_s2, wh_to_sales, wh_to_sales_cond, sales_gas in alloc_rows:
                try:
                    # Convert to float with defaults
                    wh_to_s2_val = float(wh_to_s2) if wh_to_s2 is not None else 1.0
                    wh_to_sales_val = float(wh_to_sales) if wh_to_sales is not None else 1.0
                    wh_to_sales_cond_val = float(wh_to_sales_cond) if wh_to_sales_cond is not None else 1.0
                    monthly_sales_gas_val = float(sales_gas) if sales_gas is not None else 0
                    
                    # -----------------------------------------------------------------
                    # Update 1: Gas - S2 Production (using ONLY GasWH_Production)
                    # -----------------------------------------------------------------
                    cursor.execute("""
                        UPDATE PCE_CDA 
                        SET [Gas - S2 Production] = ? * [GasWH_Production]
                        WHERE [Well Name] = ? 
                        AND ProdDate BETWEEN ? AND ?
                    """, wh_to_s2_val, well_name, month_start_date, month_end_date)
                    
                    # -----------------------------------------------------------------
                    # Update 2: Gas - Sales Production (using ONLY GasWH_Production)
                    # -----------------------------------------------------------------
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
                    
                    # -----------------------------------------------------------------
                    # Update 3: Condensate - Sales Production (using Condensate_WH_Production)
                    # -----------------------------------------------------------------
                    cursor.execute("""
                        UPDATE PCE_CDA 
                        SET [Condensate - Sales Production] = ? * [Condensate_WH_Production]
                        WHERE [Well Name] = ? 
                        AND ProdDate BETWEEN ? AND ?
                    """, wh_to_sales_cond_val, well_name, month_start_date, month_end_date)
                    
                    # -----------------------------------------------------------------
                    # Update 4: Sales CGR Ratio (unchanged)
                    # -----------------------------------------------------------------
                    cursor.execute("""
                        UPDATE PCE_CDA 
                        SET [Sales CGR Ratio] = 
                            IIF([Gas - Sales Production] > 0, 
                                [Condensate - Sales Production] / [Gas - Sales Production], 
                                0)
                        WHERE [Well Name] = ? 
                        AND ProdDate BETWEEN ? AND ?
                    """, well_name, month_start_date, month_end_date)
                    
                    month_updated += 1
                    
                except Exception as e:
                    total_errors += 1
                    print(lf.error(f"Well '{well_name}': {str(e)}"))
            
            conn.commit()
            total_updated += month_updated
            months_processed += 1
            print(lf.detail(f"Updated {lf.num(month_updated)} wells for {month_name}"))
        
        # -----------------------------------------------------------------
        # SUMMARY
        # -----------------------------------------------------------------
        total_time = time.time() - total_start
        
        wells_per_month = (total_updated / months_processed) if months_processed else 0
        print(lf.summary("Update complete", {
            "SQL Server": f"{SQL_SERVER}.{SQL_DATABASE}.PCE_CDA",
            "Months processed": months_processed,
            "Wells updated (total passes)": total_updated,
            "~Wells per month": f"{wells_per_month:.0f}",
            "Est. record touches": total_updated * 30,
            "Errors": total_errors,
            "Total time": lf.elapsed(total_time),
        }))
        print(lf.subheader("Calculation logic"))
        print(lf.item("Gas - S2 Production = WH_to_S2 × GasWH_Production"))
        print(lf.item("Gas - Sales Production = WH_to_Sales × GasWH_Production (or monthly/days if no sales)"))
        print(lf.item("Condensate - Sales Production = WH_to_Sales_Cond × Condensate_WH_Production"))
        print(lf.item("Sales CGR Ratio = Condensate_Sales / Gas_Sales"))
        
        if total_errors > 0:
            print(lf.warn(f"{lf.num(total_errors)} errors occurred during update"))
        
        conn.close()
        
    except Exception as e:
        print(lf.error(str(e)))
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    update_all_cda_fields()
    print(lf.detail("Press Enter to exit..."))
    input()