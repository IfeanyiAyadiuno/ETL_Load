/*
  dbo.PCE_Monthly_Forecasts — unique business row (Date, UWI, CDGR_Mcf_d)

  Adds PERSISTED computed columns for stable comparison, then a UNIQUE index.
  Matches Python import normalization: calendar date + trimmed UWI + CDGR_Mcf_d (FLOAT).

  Run after removing duplicate triples from the live table — index creation fails
  if duplicates exist.

  Prerequisites: dbo.PCE_Monthly_Forecasts exists.

  DROP INDEX / ALTER DROP COLUMN manually if redesigning keys.
*/

SET NOCOUNT ON;

IF OBJECT_ID(N'dbo.PCE_Monthly_Forecasts', N'U') IS NULL
BEGIN
    RAISERROR(' dbo.PCE_Monthly_Forecasts does not exist.', 16, 1);
    RETURN;
END;
GO

-- --- Normalized triple (persisted) -----------------------------------------

IF COL_LENGTH(N'dbo.PCE_Monthly_Forecasts', N'ForecastDateKey') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_Monthly_Forecasts
        ADD ForecastDateKey AS (CONVERT(DATE, [Date])) PERSISTED;
END;
GO

IF COL_LENGTH(N'dbo.PCE_Monthly_Forecasts', N'UWIKey') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_Monthly_Forecasts
        ADD UWIKey AS (
            CAST(LTRIM(RTRIM(CAST([UWI] AS NVARCHAR(4000)))) AS NVARCHAR(512))
        ) PERSISTED;
END;
GO

-- UNIQUE: one row per (date, trimmed UWI, CDGR rate)
IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes AS i
    INNER JOIN sys.tables AS t
        ON t.object_id = i.object_id
    WHERE i.name = N'UX_PCE_Monthly_Forecasts_Date_UWI_CDGR'
      AND SCHEMA_NAME(t.schema_id) = N'dbo'
      AND t.name = N'PCE_Monthly_Forecasts'
      AND i.is_hypothetical = 0
)
BEGIN
    CREATE UNIQUE INDEX UX_PCE_Monthly_Forecasts_Date_UWI_CDGR
        ON dbo.PCE_Monthly_Forecasts (ForecastDateKey, UWIKey, CDGR_Mcf_d);
END;
GO
