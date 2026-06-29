-- Mirror gathered/condensate values into the condensate gathered columns on dbo.PCE_TC (type curves).
-- Run once on SQL Server before deploying updated type-curve import/sync code.
-- Requires add_pce_tc_gathered_gas_columns.sql to have run first ([Gathered Gas (e³m³/d)] must exist).

IF COL_LENGTH('dbo.PCE_TC', 'Gathered Condensate (m³/d)') IS NULL
    ALTER TABLE dbo.PCE_TC ADD [Gathered Condensate (m³/d)] FLOAT NULL;

IF COL_LENGTH('dbo.PCE_TC', 'Condensate Gathered Cumulative (m³)') IS NULL
    ALTER TABLE dbo.PCE_TC ADD [Condensate Gathered Cumulative (m³)] FLOAT NULL;

UPDATE dbo.PCE_TC
SET [Gathered Condensate (m³/d)] = [Gathered Gas (e³m³/d)],
    [Condensate Gathered Cumulative (m³)] = [Condensate WH Cumulative Production (m³)]
WHERE [Gathered Condensate (m³/d)] IS NULL
   OR [Condensate Gathered Cumulative (m³)] IS NULL;
