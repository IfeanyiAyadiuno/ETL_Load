/*
  dbo.PCE_WM — [On Production Year]

  1. Adds nullable [On Production Year] (INT; calendar year well came on production).

  2. Backfills from dbo.PCE_Production.[On Production Year] using Well Master match rules:
       - wm.[Well Name] = production.[Well Name], OR
       - wm.[Composite Name] = production.[Well Name] (when composite is populated)
       - excludes WM exception rows unless [Exception] is NULL / '' / 'N'

  Run in SSMS against the correct database. Re-run UPDATE after production rebuilds.
*/

SET NOCOUNT ON;

IF OBJECT_ID(N'dbo.PCE_WM', N'U') IS NULL
BEGIN
    RAISERROR('Table dbo.PCE_WM does not exist.', 16, 1);
    RETURN;
END;
GO

IF COL_LENGTH(N'dbo.PCE_WM', N'On Production Year') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_WM ADD [On Production Year] INT NULL;
END;
GO

UPDATE wm
SET wm.[On Production Year] = CAST(src.[On Production Year] AS INT)
FROM dbo.PCE_WM AS wm
INNER JOIN (
    SELECT
        p.[Well Name],
        MIN(p.[On Production Year]) AS [On Production Year]
    FROM dbo.PCE_Production AS p
    WHERE p.[On Production Year] IS NOT NULL
    GROUP BY p.[Well Name]
) AS src
    ON wm.[Well Name] = src.[Well Name]
    OR (
           NULLIF(RTRIM(CAST(wm.[Composite Name] AS NVARCHAR(4000))), N'') IS NOT NULL
       AND wm.[Composite Name] = src.[Well Name]
       )
WHERE wm.[On Production Year] IS NULL
  AND (wm.[Exception] IS NULL OR wm.[Exception] = N'' OR wm.[Exception] = N'N');

/*
  Optional: derive year from earliest production date when [On Production Year] is still NULL:

UPDATE wm
SET wm.[On Production Year] = src.prod_year
FROM dbo.PCE_WM AS wm
INNER JOIN (
    SELECT
        p.[Well Name],
        MIN(YEAR(p.[Date])) AS prod_year
    FROM dbo.PCE_Production AS p
    WHERE p.[Well Name] NOT LIKE N'% - TC'
      AND p.[Well Name] NOT LIKE N'YE2%'
    GROUP BY p.[Well Name]
) AS src
    ON wm.[Well Name] = src.[Well Name]
    OR (
           NULLIF(RTRIM(CAST(wm.[Composite Name] AS NVARCHAR(4000))), N'') IS NOT NULL
       AND wm.[Composite Name] = src.[Well Name]
       )
WHERE wm.[On Production Year] IS NULL
  AND (wm.[Exception] IS NULL OR wm.[Exception] = N'' OR wm.[Exception] = N'N');

  Clear before reload (optional):

UPDATE dbo.PCE_WM SET [On Production Year] = NULL;
*/
