-- Add Whitson+ attribute and completion columns to dbo.PCE_WM.
-- Run once on SQL Server before deploying the updated Whitson push code.
-- Fluid Pumped / Proppant Pumped hold raw metric values (m³, tonnes);
-- conversion to imperial happens at push time via whitson_imperial.ini.
-- Bounded and Initial flow date are PCE_WM-only (not pushed to Whitson+).

IF COL_LENGTH('dbo.PCE_WM', 'Fluid Pumped (m³)') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Fluid Pumped (m³)] FLOAT NULL;

IF COL_LENGTH('dbo.PCE_WM', 'Proppant Pumped (t)') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Proppant Pumped (t)] FLOAT NULL;

IF COL_LENGTH('dbo.PCE_WM', 'Bounded') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Bounded] NVARCHAR(20) NULL
        CONSTRAINT CK_PCE_WM_Bounded CHECK ([Bounded] IN (N'Bounded', N'Unbounded'));

IF COL_LENGTH('dbo.PCE_WM', 'Initial flow date') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Initial flow date] DATE NULL;
