import pandas as pd

excel_path = r"I:\ResEng\Tools\Programmers Paradise\mvp_cda_load\PCE_TCs_MTHLY.xlsx"

# Read just the first few rows to see structure
df_preview = pd.read_excel(excel_path, nrows=5, header=None)

print("=" * 80)
print("EXCEL FILE STRUCTURE")
print("=" * 80)

# Show first 5 rows with column indices
print("\n📋 FIRST 5 ROWS (with column indices):")
for i in range(min(5, len(df_preview))):
    print(f"\nRow {i+1}:")
    # Show first 20 columns or until we see empty data
    for j in range(min(20, len(df_preview.columns))):
        val = df_preview.iloc[i, j]
        if pd.notna(val) and str(val).strip() != '':
            print(f"   Col {j} ({chr(65 + j) if j < 26 else f'Col{j}'}): '{val}'")

# Show all column headers (row 1)
print("\n📋 POTENTIAL HEADER ROW (Row 1):")
for j in range(min(32, len(df_preview.columns))):
    val = df_preview.iloc[0, j]
    if pd.notna(val) and str(val).strip() != '':
        col_letter = chr(65 + j) if j < 26 else f"Col{j}"
        print(f"   {col_letter} (col {j}): '{val}'")