import pyodbc
import pandas as pd
from datetime import datetime
import os

def generate_ratio_exceptions_report():
    """
    Generate a report of all allocation ratios that are greater than 1 (>100%)
    from the Allocation_Factors table.
    
    Ratios checked:
    - WH_to_S2_AllocFactor (S2_Gas / Prodview_WH_Gas)
    - WH_to_Sales_AllocFactor (Sales_Gas / Prodview_WH_Gas)
    - WH_to_Sales_Cond_AllocFactor (Sales_Condensate / Prodview_WH_Cond)
    - Gathered_to_S2_Gas (S2_Gas / Gathered_Gas_Production)
    - Gathered_to_Sales (Sales_Gas / Gathered_Gas_Production)
    - Gathered_to_Sales_Condensate (Sales_Condensate / Gathered_Condensate_Production)
    """
    
    print("\n" + "="*80)
    print("RATIO EXCEPTIONS REPORT - VALUES > 1")
    print("="*80)
    
    # Database path
    db_path = r"I:\ResEng\Tools\Programmers Paradise\GUI_WM\PCE_WM1.accdb"
    
    print(f"\nDatabase: {db_path}")
    
    # Create output filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"ratio_exceptions_{timestamp}.xlsx"
    
    try:
        # Connect to database
        print("\nConnecting to database...")
        conn_str = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            r'DBQ=' + db_path + ';'
        )
        conn = pyodbc.connect(conn_str)
        
        # Query for all ratio exceptions
        print("\nQuerying for ratio exceptions > 1...")
        
        query = """
            SELECT 
                MonthStartDate,
                WellName,
                Prodview_WH_Gas,
                S2_Gas,
                WH_to_S2_AllocFactor,
                Sales_Gas,
                WH_to_Sales_AllocFactor,
                Prodview_WH_Cond,
                Sales_Condensate,
                WH_to_Sales_Cond_AllocFactor,
                Gathered_Gas_Production,
                Gathered_Condensate_Production,
                Gathered_to_S2_Gas,
                Gathered_to_Sales,
                Gathered_to_Sales_Condensate
            FROM Allocation_Factors
            WHERE 
                WH_to_S2_AllocFactor > 1 OR
                WH_to_Sales_AllocFactor > 1 OR
                WH_to_Sales_Cond_AllocFactor > 1 OR
                CAST(Gathered_to_S2_Gas AS FLOAT) > 1 OR
                CAST(Gathered_to_Sales AS FLOAT) > 1 OR
                CAST(Gathered_to_Sales_Condensate AS FLOAT) > 1
            ORDER BY MonthStartDate DESC, WellName
        """
        
        # Read data into pandas DataFrame
        df = pd.read_sql(query, conn)
        
        if len(df) == 0:
            print("\n✅ No ratio exceptions found (all values ≤ 1)")
            return
        
        print(f"\nFound {len(df)} records with ratio exceptions")
        
        # -----------------------------------------------------------------
        # Create separate DataFrames for each ratio type
        # -----------------------------------------------------------------
        
        # WH_to_S2_AllocFactor exceptions
        df_wh_to_s2 = df[df['WH_to_S2_AllocFactor'] > 1].copy()
        df_wh_to_s2 = df_wh_to_s2[['MonthStartDate', 'WellName', 
                                    'Prodview_WH_Gas', 'S2_Gas', 
                                    'WH_to_S2_AllocFactor']].sort_values('WH_to_S2_AllocFactor', ascending=False)
        
        # WH_to_Sales_AllocFactor exceptions
        df_wh_to_sales = df[df['WH_to_Sales_AllocFactor'] > 1].copy()
        df_wh_to_sales = df_wh_to_sales[['MonthStartDate', 'WellName', 
                                          'Prodview_WH_Gas', 'Sales_Gas', 
                                          'WH_to_Sales_AllocFactor']].sort_values('WH_to_Sales_AllocFactor', ascending=False)
        
        # WH_to_Sales_Cond_AllocFactor exceptions
        df_wh_to_sales_cond = df[df['WH_to_Sales_Cond_AllocFactor'] > 1].copy()
        df_wh_to_sales_cond = df_wh_to_sales_cond[['MonthStartDate', 'WellName', 
                                                    'Prodview_WH_Cond', 'Sales_Condensate', 
                                                    'WH_to_Sales_Cond_AllocFactor']].sort_values('WH_to_Sales_Cond_AllocFactor', ascending=False)
        
        # Gathered_to_S2_Gas exceptions (convert string to float for comparison)
        df['Gathered_to_S2_Gas_num'] = pd.to_numeric(df['Gathered_to_S2_Gas'], errors='coerce')
        df_gathered_to_s2 = df[df['Gathered_to_S2_Gas_num'] > 1].copy()
        df_gathered_to_s2 = df_gathered_to_s2[['MonthStartDate', 'WellName', 
                                                'Gathered_Gas_Production', 'S2_Gas', 
                                                'Gathered_to_S2_Gas']].sort_values('Gathered_to_S2_Gas', ascending=False)
        
        # Gathered_to_Sales exceptions
        df['Gathered_to_Sales_num'] = pd.to_numeric(df['Gathered_to_Sales'], errors='coerce')
        df_gathered_to_sales = df[df['Gathered_to_Sales_num'] > 1].copy()
        df_gathered_to_sales = df_gathered_to_sales[['MonthStartDate', 'WellName', 
                                                      'Gathered_Gas_Production', 'Sales_Gas', 
                                                      'Gathered_to_Sales']].sort_values('Gathered_to_Sales', ascending=False)
        
        # Gathered_to_Sales_Condensate exceptions
        df['Gathered_to_Sales_Condensate_num'] = pd.to_numeric(df['Gathered_to_Sales_Condensate'], errors='coerce')
        df_gathered_to_sales_cond = df[df['Gathered_to_Sales_Condensate_num'] > 1].copy()
        df_gathered_to_sales_cond = df_gathered_to_sales_cond[['MonthStartDate', 'WellName', 
                                                                'Gathered_Condensate_Production', 'Sales_Condensate', 
                                                                'Gathered_to_Sales_Condensate']].sort_values('Gathered_to_Sales_Condensate', ascending=False)
        
        # -----------------------------------------------------------------
        # Create summary statistics
        # -----------------------------------------------------------------
        summary_data = {
            'Ratio Type': [
                'WH_to_S2_AllocFactor',
                'WH_to_Sales_AllocFactor', 
                'WH_to_Sales_Cond_AllocFactor',
                'Gathered_to_S2_Gas',
                'Gathered_to_Sales',
                'Gathered_to_Sales_Condensate'
            ],
            'Count > 1': [
                len(df_wh_to_s2),
                len(df_wh_to_sales),
                len(df_wh_to_sales_cond),
                len(df_gathered_to_s2),
                len(df_gathered_to_sales),
                len(df_gathered_to_sales_cond)
            ],
            'Max Value': [
                df_wh_to_s2['WH_to_S2_AllocFactor'].max() if len(df_wh_to_s2) > 0 else 0,
                df_wh_to_sales['WH_to_Sales_AllocFactor'].max() if len(df_wh_to_sales) > 0 else 0,
                df_wh_to_sales_cond['WH_to_Sales_Cond_AllocFactor'].max() if len(df_wh_to_sales_cond) > 0 else 0,
                df_gathered_to_s2['Gathered_to_S2_Gas'].max() if len(df_gathered_to_s2) > 0 else 0,
                df_gathered_to_sales['Gathered_to_Sales'].max() if len(df_gathered_to_sales) > 0 else 0,
                df_gathered_to_sales_cond['Gathered_to_Sales_Condensate'].max() if len(df_gathered_to_sales_cond) > 0 else 0
            ]
        }
        df_summary = pd.DataFrame(summary_data)
        
        # -----------------------------------------------------------------
        # Write to Excel with multiple sheets
        # -----------------------------------------------------------------
        print(f"\nWriting report to {output_file}...")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Summary sheet
            df_summary.to_excel(writer, sheet_name='Summary', index=False)
            
            # Individual ratio sheets
            if len(df_wh_to_s2) > 0:
                df_wh_to_s2.to_excel(writer, sheet_name='WH_to_S2 > 1', index=False)
            
            if len(df_wh_to_sales) > 0:
                df_wh_to_sales.to_excel(writer, sheet_name='WH_to_Sales > 1', index=False)
            
            if len(df_wh_to_sales_cond) > 0:
                df_wh_to_sales_cond.to_excel(writer, sheet_name='WH_to_Sales_Cond > 1', index=False)
            
            if len(df_gathered_to_s2) > 0:
                df_gathered_to_s2.to_excel(writer, sheet_name='Gathered_to_S2 > 1', index=False)
            
            if len(df_gathered_to_sales) > 0:
                df_gathered_to_sales.to_excel(writer, sheet_name='Gathered_to_Sales > 1', index=False)
            
            if len(df_gathered_to_sales_cond) > 0:
                df_gathered_to_sales_cond.to_excel(writer, sheet_name='Gathered_to_Sales_Cond > 1', index=False)
            
            # Also include a sheet with all exceptions together
            df.to_excel(writer, sheet_name='All Exceptions', index=False)
            
            # Auto-adjust column widths
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
        
        # -----------------------------------------------------------------
        # Print summary to console
        # -----------------------------------------------------------------
        print("\n" + "="*80)
        print("RATIO EXCEPTIONS SUMMARY")
        print("="*80)
        print(f"\nTotal records with exceptions: {len(df)}")
        print("\nBreakdown by ratio type:")
        print(df_summary.to_string(index=False))
        
        print(f"\n\nReport saved to: {output_file}")
        print("\n" + "="*80)
        
        conn.close()
        
    except Exception as e:
        print(f"\nERROR: {e}")
        traceback.print_exc()
        return False
    
    return True

def generate_well_specific_report(well_name=None):
    """
    Generate a report for a specific well showing all ratios over time
    """
    if not well_name:
        well_name = input("\nEnter well name to generate report for: ").strip()
    
    print(f"\nGenerating report for well: {well_name}")
    
    # Database path
    db_path = r"I:\ResEng\Tools\Programmers Paradise\GUI_WM\PCE_WM1.accdb"
    
    try:
        conn_str = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            r'DBQ=' + db_path + ';'
        )
        conn = pyodbc.connect(conn_str)
        
        query = """
            SELECT 
                MonthStartDate,
                Prodview_WH_Gas,
                S2_Gas,
                WH_to_S2_AllocFactor,
                Sales_Gas,
                WH_to_Sales_AllocFactor,
                Prodview_WH_Cond,
                Sales_Condensate,
                WH_to_Sales_Cond_AllocFactor,
                Gathered_Gas_Production,
                Gathered_Condensate_Production,
                Gathered_to_S2_Gas,
                Gathered_to_Sales,
                Gathered_to_Sales_Condensate
            FROM Allocation_Factors
            WHERE WellName = ?
            ORDER BY MonthStartDate DESC
        """
        
        df = pd.read_sql(query, conn, params=[well_name])
        
        if len(df) == 0:
            print(f"No data found for well: {well_name}")
            return
        
        # Mark exceptions
        df['S2_Exception'] = df['WH_to_S2_AllocFactor'] > 1
        df['Sales_Gas_Exception'] = df['WH_to_Sales_AllocFactor'] > 1
        df['Sales_Cond_Exception'] = df['WH_to_Sales_Cond_AllocFactor'] > 1
        
        # Save to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{well_name}_ratios_{timestamp}.xlsx"
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='All Ratios', index=False)
            
            # Filter to just exceptions
            df_exceptions = df[df['S2_Exception'] | df['Sales_Gas_Exception'] | df['Sales_Cond_Exception']]
            if len(df_exceptions) > 0:
                df_exceptions.to_excel(writer, sheet_name='Exceptions Only', index=False)
        
        print(f"Report saved to: {filename}")
        conn.close()
        
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    print("\nRATIO EXCEPTIONS REPORTING TOOL")
    print("="*80)
    print("\nOptions:")
    print("  1. Generate full report (all wells, all ratios > 1)")
    print("  2. Generate report for specific well")
    print("  3. Exit")
    
    choice = input("\nEnter your choice (1-3): ").strip()
    
    if choice == '1':
        generate_ratio_exceptions_report()
    elif choice == '2':
        well = input("Enter well name: ").strip()
        generate_well_specific_report(well)
    elif choice == '3':
        print("Exiting...")
    else:
        print("Invalid choice")
    
    print("\nPress Enter to exit...")
    input()