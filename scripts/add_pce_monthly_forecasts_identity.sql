/*
  dbo.PCE_Monthly_Forecasts — surrogate key for targeted DELETE / UPDATE
  
  Adds INT IDENTITY(1,1). Existing rows are numbered automatically when the column
  is created (physical order depends on allocation; newest business keys like Date/UWI
  should still identify rows you care about).
  
  The Python Monthly Forecast import does NOT need this column — INSERT omits it and
  SQL Server generates the next value.
  
  Run once in SSMS against the proper database (Adjust schema if not dbo.)
*/

SET NOCOUNT ON;

IF OBJECT_ID(N'dbo.PCE_Monthly_Forecasts', N'U') IS NULL
BEGIN
    RAISERROR('Table dbo.PCE_Monthly_Forecasts does not exist.', 16, 1);
    RETURN;
END;

IF COL_LENGTH('dbo.PCE_Monthly_Forecasts', 'MonthlyForecast_Id') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_Monthly_Forecasts
        ADD MonthlyForecast_Id INT NOT NULL IDENTITY(1, 1);
END;
GO

/*
  Optional: clustered primary key ONLY if nothing else is the clustered PK/heap policy.
  If this fails (“Cannot create clustered index…” / duplicate constraint), skip this
  block and keep the column alone (IDENTITY alone is fine for lookups).
*/

-- Uncomment if your table has no other PRIMARY KEY clustered index:
--
-- IF NOT EXISTS (
--     SELECT 1
--     FROM sys.key_constraints
--     WHERE parent_object_id = OBJECT_ID(N'dbo.PCE_Monthly_Forecasts')
--       AND type = 'PK'
-- )
-- BEGIN
--     ALTER TABLE dbo.PCE_Monthly_Forecasts
--     ADD CONSTRAINT PK_PCE_Monthly_Forecasts
--         PRIMARY KEY CLUSTERED (MonthlyForecast_Id);
-- END;

/*
  --- Find IDs for rows you want ---

SELECT MonthlyForecast_Id, [Date], [UWI],
       CDGR_Mcf_d, CD_Cond_bbl_d, CD_Water_bbl_d, [Month], [Pad], Fault_Block,
       [Enersight Well Name]
FROM dbo.PCE_Monthly_Forecasts
WHERE LTRIM(RTRIM([UWI])) = N'100/16-28-084-25W6/0'
  AND CAST([Date] AS DATE) = DATEFROMPARTS(2030, 11, 1);

  --- Delete only those IDs (exact list from SELECT) ---

DELETE FROM dbo.PCE_Monthly_Forecasts
WHERE MonthlyForecast_Id IN (9991, 9992, 9993);   -- replace with your IDs

  --- Or safer: TRANSACTION + ROWCOUNT ---
*/
