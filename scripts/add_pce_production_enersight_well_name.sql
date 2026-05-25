/*
  dbo.PCE_Production — [Enersight Well Name]

  1. Adds nullable [Enersight Well Name] (matches PCE_WM / monthly forecasts naming).

  2. Backfills from dbo.PCE_WM.[Enersight Well Name] using the same match rules as
     production_update.sync_production_pad_names_from_wm_sql and the in-app function
     sync_production_enersight_well_names_from_wm_sql (runs after each production rebuild).
       - wm.[Well Name] = p.[Well Name], OR
       - wm.[Composite Name] = p.[Well Name] (when composite is populated)
       - excludes WM exception rows unless [Exception] is NULL / '' / 'N'
     Type-curve / YE2 synthetic keys are skipped (same as pad sync).

  Run in SSMS against the correct database. Re-run UPDATE after refreshing PCE_WM.Enersight.
*/

SET NOCOUNT ON;

IF OBJECT_ID(N'dbo.PCE_Production', N'U') IS NULL
BEGIN
    RAISERROR('Table dbo.PCE_Production does not exist.', 16, 1);
    RETURN;
END;
GO

-- --- Part 1: add column (idempotent)
IF COL_LENGTH(N'dbo.PCE_Production', N'Enersight Well Name') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_Production ADD [Enersight Well Name] NVARCHAR(4000) NULL;
END;
GO

-- --- Part 2: populate from Well Master ---
UPDATE p
SET p.[Enersight Well Name] = ca.[Enersight Well Name]
FROM dbo.PCE_Production AS p
CROSS APPLY (
    SELECT TOP 1
           wm.[Enersight Well Name]
    FROM dbo.PCE_WM AS wm
    WHERE (
              wm.[Well Name] = p.[Well Name]
           OR (
                  NULLIF(RTRIM(CAST(wm.[Composite Name] AS NVARCHAR(4000))), N'') IS NOT NULL
              AND wm.[Composite Name] = p.[Well Name]
              )
          )
      AND (wm.[Exception] IS NULL OR wm.[Exception] = N'' OR wm.[Exception] = N'N')
      AND NULLIF(RTRIM(CAST(wm.[Enersight Well Name] AS NVARCHAR(4000))), N'') IS NOT NULL
) AS ca
WHERE p.[Well Name] NOT LIKE N'% - TC'
  AND p.[Well Name] NOT LIKE N'YE2%';

/*
  Rows with [Well Name] like '% - TC' or 'YE2%' are left unchanged here (typically no WM row).

  Narrow by date window (optional):

UPDATE p
SET p.[Enersight Well Name] = ca.[Enersight Well Name]
FROM dbo.PCE_Production AS p
CROSS APPLY (...) AS ca
WHERE p.[Well Name] NOT LIKE N'% - TC'
  AND p.[Well Name] NOT LIKE N'YE2%'
  AND p.[Date] BETWEEN '20240101' AND '20261231';

  Clear labels before reload (optional):

UPDATE dbo.PCE_Production SET [Enersight Well Name] = NULL;
*/
