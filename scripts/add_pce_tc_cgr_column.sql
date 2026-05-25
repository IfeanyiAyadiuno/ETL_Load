-- Add non–Sales PCE_TC CGR column (Excel "CGR Ratio" / production-style naming).
-- Unicode superscripts in the column name match dbo.PCE_Production / other PCE columns.
-- Idempotent: safe to re-run against the same database.

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dbo.PCE_TC', N'U')
      AND name = N'CGR (m³/e³m³)')
BEGIN
    ALTER TABLE dbo.PCE_TC ADD [CGR (m³/e³m³)] FLOAT NULL;
END
GO
