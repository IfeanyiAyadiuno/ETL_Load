-- Recommended non-clustered indexes for ETL_Load hot paths (idempotent).
-- Review on staging before production; adjust names if conflicts exist.

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_PCE_CDA_ProdDate' AND object_id = OBJECT_ID(N'dbo.PCE_CDA')
)
CREATE NONCLUSTERED INDEX IX_PCE_CDA_ProdDate
    ON dbo.PCE_CDA (ProdDate);

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_PCE_CDA_Well_ProdDate' AND object_id = OBJECT_ID(N'dbo.PCE_CDA')
)
CREATE NONCLUSTERED INDEX IX_PCE_CDA_Well_ProdDate
    ON dbo.PCE_CDA ([Well Name], ProdDate);

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_PCE_Production_Date' AND object_id = OBJECT_ID(N'dbo.PCE_Production')
)
CREATE NONCLUSTERED INDEX IX_PCE_Production_Date
    ON dbo.PCE_Production ([Date]);

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_PCE_Production_Well_Date' AND object_id = OBJECT_ID(N'dbo.PCE_Production')
)
CREATE NONCLUSTERED INDEX IX_PCE_Production_Well_Date
    ON dbo.PCE_Production ([Well Name], [Date]);

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_Allocation_Factors_Month' AND object_id = OBJECT_ID(N'dbo.Allocation_Factors')
)
CREATE NONCLUSTERED INDEX IX_Allocation_Factors_Month
    ON dbo.Allocation_Factors (MonthStartDate);

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_Allocation_Factors_Well_Month' AND object_id = OBJECT_ID(N'dbo.Allocation_Factors')
)
CREATE NONCLUSTERED INDEX IX_Allocation_Factors_Well_Month
    ON dbo.Allocation_Factors ([Well Name], MonthStartDate);
