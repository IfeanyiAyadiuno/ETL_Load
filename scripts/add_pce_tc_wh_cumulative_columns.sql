-- Add PCE_TC columns that mirror cumulative gas and condensate (same values as Cum Gas / Cum Condy).
-- Unit labels use Unicode superscripts (10³m³, m³) to match other PCE_TC columns.
-- Run against the target database before deploying app code that INSERTs these columns.

-- If you already created columns with ASCII names, rename once (then skip on re-run).
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dbo.PCE_TC', N'U')
      AND name = N'Gas WH Cumulative Production (103m3)')
BEGIN
    EXEC sp_rename N'dbo.PCE_TC.[Gas WH Cumulative Production (103m3)]',
                    N'Gas WH Cumulative Production (10³m³)', N'COLUMN';
END
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dbo.PCE_TC', N'U')
      AND name = N'Condensate WH Cumulative Production (m3)')
BEGIN
    EXEC sp_rename N'dbo.PCE_TC.[Condensate WH Cumulative Production (m3)]',
                    N'Condensate WH Cumulative Production (m³)', N'COLUMN';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dbo.PCE_TC', N'U')
      AND name = N'Gas WH Cumulative Production (10³m³)')
BEGIN
    ALTER TABLE dbo.PCE_TC ADD [Gas WH Cumulative Production (10³m³)] FLOAT NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dbo.PCE_TC', N'U')
      AND name = N'Condensate WH Cumulative Production (m³)')
BEGIN
    ALTER TABLE dbo.PCE_TC ADD [Condensate WH Cumulative Production (m³)] FLOAT NULL;
END
GO

-- Backfill: mirror cumulative gas / condy into the WH cumulative columns.
UPDATE dbo.PCE_TC
SET
    [Gas WH Cumulative Production (10³m³)] = [Cum Gas (e³m³)],
    [Condensate WH Cumulative Production (m³)] = [Cum Condy (m³)];
GO
