-- Mirror Gas WH into Gathered Gas on dbo.PCE_TC (type curves).
-- Run once on SQL Server before deploying updated type-curve import/sync code.

IF COL_LENGTH('dbo.PCE_TC', 'Gathered Gas (e³m³/d)') IS NULL
    ALTER TABLE dbo.PCE_TC ADD [Gathered Gas (e³m³/d)] FLOAT NULL;

IF COL_LENGTH('dbo.PCE_TC', 'Gas Gathered Cumulative (e³m³)') IS NULL
    ALTER TABLE dbo.PCE_TC ADD [Gas Gathered Cumulative (e³m³)] FLOAT NULL;

UPDATE dbo.PCE_TC
SET [Gathered Gas (e³m³/d)] = [Gas WH Production (e³m³/d)],
    [Gas Gathered Cumulative (e³m³)] = [Gas WH Cumulative Production (10³m³)]
WHERE [Gathered Gas (e³m³/d)] IS NULL
   OR [Gas Gathered Cumulative (e³m³)] IS NULL;
