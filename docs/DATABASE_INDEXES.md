# Recommended database indexes

These indexes support the heaviest ETL paths: Prodview CDA refresh, production rebuild, allocation-factor month loops, and forecast rebuild joins.

Run during a maintenance window. Script is idempotent: `scripts/recommended_indexes.sql`.

## PCE_CDA

| Index | Columns | Used by |
|-------|---------|---------|
| `IX_PCE_CDA_ProdDate` | `ProdDate` | Rolling-window delete/insert, `fetch_cda_data` range filters |
| `IX_PCE_CDA_Well_ProdDate` | `[Well Name]`, `ProdDate` | Per-well history loads, window well queries |

## PCE_Production

| Index | Columns | Used by |
|-------|---------|---------|
| `IX_PCE_Production_Date` | `[Date]` | Future trim, NGL month updates |
| `IX_PCE_Production_Well_Date` | `[Well Name]`, `[Date]` | Per-well delete/insert, TC materialization |

## Allocation_Factors

| Index | Columns | Used by |
|-------|---------|---------|
| `IX_Allocation_Factors_Month` | `MonthStartDate` | Sales/NGL month loops |
| `IX_Allocation_Factors_Well_Month` | `[Well Name]`, `MonthStartDate` | Month-scoped UPDATE joins |

## Notes

- Verify existing indexes on your server before creating duplicates.
- After adding indexes, update statistics on large tables (`UPDATE STATISTICS`).
