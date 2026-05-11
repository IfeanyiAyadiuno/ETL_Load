# Code Optimization Opportunities for Snowflake Daily Retrieval

## Priority 1: High Impact, Easy to Implement

### 1. Enable `fast_executemany` for SQL Server Inserts
**Current Issue:** Only `type.py` uses `fast_executemany=True`. This can speed up inserts by 10-50x.

**Files to modify:**
- `cda.py` - `insert_pce_cda_rows()` function
- `prodview_update_gui.py` - Insert operations in `run_quick_update()`

**Change:**
```python
# Before:
cursor = conn.cursor()

# After:
cursor = conn.cursor()
cursor.fast_executemany = True
```

**Expected Impact:** 10-50x faster SQL Server inserts

---

### 2. Reuse Snowflake Connection
**Current Issue:** Each query creates/closes a connection (7 times per month).

**File:** `prodview_update_gui.py` - `run_quick_update()` function

**Change:**
```python
# Before (lines 788-881):
sf = SnowflakeConnector()
try:
    ecf_df = sf.query(ecf_query)
    # ... more queries
finally:
    sf.close()

# After:
sf = SnowflakeConnector()
try:
    # Reuse connection for all queries
    ecf_df = sf.query(ecf_query)
    gaswh_df = sf.query(gaswh_query)
    # ... all 7 queries
finally:
    sf.close()  # Close once at the end
```

**Expected Impact:** 20-30% faster Snowflake queries (eliminates connection overhead)

---

### 3. Optimize Spine Building with Pandas
**Current Issue:** Nested loops (wells × dates) are very slow.

**File:** `prodview_update_gui.py` - `run_quick_update()` function (lines 915-936)

**Change:**
```python
# Before:
all_rows = []
date_range = pd.date_range(start=month_start_date, end=month_end_date, freq='D').date
for well in mapping:
    for date in date_range:
        all_rows.append({...})
spine_df = pd.DataFrame(all_rows)

# After:
# Create date range once
date_range = pd.date_range(start=month_start_date, end=month_end_date, freq='D').date

# Create mapping DataFrame
mapping_df = pd.DataFrame(mapping)

# Use pandas cross join (Cartesian product)
mapping_df['key'] = 1
dates_df = pd.DataFrame({'ProdDate': date_range, 'key': 1})
spine_df = mapping_df.merge(dates_df, on='key').drop('key', axis=1)

# Rename columns to match expected format
spine_df = spine_df.rename(columns={
    'gas_idrec': 'GasIDREC',
    'pressures_idrec': 'PressuresIDREC',
    'well_name': 'Well Name',
    'formation': 'Formation Producer',
    'layer': 'Layer Producer',
    'fault_block': 'Fault Block',
    'pad_name': 'Pad Name',
    'lateral_length': 'Lateral Length',
    'orient': 'Orient'
})
```

**Expected Impact:** 50-100x faster spine building (from seconds to milliseconds)

---

### 4. Replace `iterrows()` with `itertuples()`
**Current Issue:** `iterrows()` is extremely slow for large DataFrames.

**File:** `prodview_update_gui.py` - `run_quick_update()` function (lines 1086-1112)

**Change:**
```python
# Before:
for row_idx, row in enumerate(result_df.iterrows()):
    _, row = row
    rows_batch.append((row.get('GasIDREC'), ...))

# After:
for row in result_df.itertuples(index=False):
    rows_batch.append((
        row.GasIDREC if hasattr(row, 'GasIDREC') else None,
        row.PressuresIDREC if hasattr(row, 'PressuresIDREC') else None,
        # ... etc
    ))
```

**Or better yet, use vectorized operations:**
```python
# Convert DataFrame to list of tuples directly
rows_batch = result_df.values.tolist()
# Then process in batches
```

**Expected Impact:** 5-10x faster row preparation

---

## Priority 2: Medium Impact, Moderate Effort

### 5. Increase Batch Size for SQL Inserts
**Current Issue:** Batch size is 1000 rows.

**Files:** `cda.py`, `prodview_update_gui.py`

**Change:**
```python
# Before:
batch_size = 1000

# After:
batch_size = 5000  # Test with 5000 first, can go up to 10000
```

**Expected Impact:** 20-30% faster inserts (fewer round trips to database)

**Note:** Test first to ensure memory is sufficient.

---

### 6. Parallel Snowflake Queries
**Current Issue:** 7 queries run sequentially.

**File:** `prodview_update_gui.py` - `run_quick_update()` function

**Change:**
```python
import concurrent.futures
from functools import partial

# Create a helper function
def run_query(sf, query):
    return sf.query(query)

# Run queries in parallel
sf = SnowflakeConnector()
try:
    queries = {
        'ecf': ecf_query,
        'gaswh': gaswh_query,
        'cgr': cgr_query,
        'wgr': wgr_query,
        'pressures': pressures_query,
        'alloc': alloc_query,
        'water': water_query
    }
    
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
        future_to_name = {
            executor.submit(sf.query, query): name 
            for name, query in queries.items()
        }
        
        for future in concurrent.futures.as_completed(future_to_name):
            name = future_to_name[future]
            try:
                results[name] = future.result()
            except Exception as e:
                log(f"❌ Error in {name} query: {e}")
                raise
    
    ecf_df = results['ecf']
    gaswh_df = results['gaswh']
    # ... etc
finally:
    sf.close()
```

**Expected Impact:** 3-5x faster Snowflake data retrieval (if network allows)

**Note:** Snowflake connection must be thread-safe. Test first.

---

### 7. Optimize DataFrame Merges
**Current Issue:** 7 sequential merges on same DataFrame.

**File:** `prodview_update_gui.py` - `run_quick_update()` function (lines 967-1058)

**Change:**
```python
# Instead of merging one at a time, prepare all DataFrames first
# Then merge in a single pass or reduce number of merges

# Prepare all data sources first
prepared_dfs = {}
if not ecf_df.empty:
    prepared_dfs['ecf'] = prepare_df(ecf_df, 'GASIDREC', 'PRODDATE', ['ECF_Ratio'])
# ... prepare all

# Merge all at once using reduce
from functools import reduce

merge_keys = {
    'ecf': ['GasIDREC', 'ProdDate'],
    'gaswh': ['GasIDREC', 'ProdDate'],
    'cgr': ['PressuresIDREC', 'ProdDate'],
    # ... etc
}

# Merge all prepared DataFrames
dfs_to_merge = [spine_df]
for name, df in prepared_dfs.items():
    if not df.empty:
        keys = merge_keys[name]
        dfs_to_merge.append(df.set_index(keys))

result_df = reduce(lambda left, right: left.join(right, how='left'), dfs_to_merge)
result_df = result_df.reset_index()
```

**Expected Impact:** 20-30% faster merges

---

## Priority 3: Lower Impact, Advanced

### 8. Use Bulk Insert for SQL Server
**Current Issue:** Using `executemany()` which is slower than bulk insert.

**Alternative:** Use `pandas.to_sql()` with `method='multi'` or SQL Server's `BULK INSERT`.

**Change:**
```python
from sqlalchemy import create_engine

# Create SQLAlchemy engine
engine = create_engine(f'mssql+pyodbc://{server}/{database}?driver={driver}')

# Use pandas to_sql with bulk insert
df_clean.to_sql(
    'PCE_CDA',
    engine,
    if_exists='append',
    index=False,
    method='multi',
    chunksize=5000
)
```

**Expected Impact:** 2-3x faster than executemany

**Note:** Requires SQLAlchemy dependency.

---

### 9. Optimize Data Type Conversions
**Current Issue:** Multiple type conversions in loops.

**Fix:** Do all conversions upfront using vectorized pandas operations.

---

### 10. Memory Optimization for Large Date Ranges
**Current Issue:** Loading all data into memory at once.

**Fix:** Process data in chunks for very large date ranges.

---

## Implementation Priority

1. **Start with Priority 1 items** - These give the biggest performance gains with minimal risk:
   - Enable `fast_executemany` (5 minutes)
   - Optimize spine building (15 minutes)
   - Replace `iterrows()` (10 minutes)
   - Reuse Snowflake connection (5 minutes)

2. **Then Priority 2** - Test thoroughly:
   - Increase batch size (5 minutes + testing)
   - Parallel queries (30 minutes + testing)

3. **Priority 3** - Only if needed:
   - Bulk insert (requires dependency)
   - Memory optimization (only for very large datasets)

---

## Expected Overall Performance Improvement

- **Priority 1 changes:** 2-5x faster overall
- **Priority 1 + 2 changes:** 5-10x faster overall
- **All changes:** 10-20x faster overall

**Estimated time savings:**
- Current: 5-10 minutes per month
- After Priority 1: 1-2 minutes per month
- After Priority 1+2: 30-60 seconds per month
