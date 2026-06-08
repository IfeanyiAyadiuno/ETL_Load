/*
  dbo.PCE_Production — [UWI] and monthly NGL Ratio (_R) columns

  1. Adds [UWI] and five nullable FLOAT Ratio columns for ValNav monthly NGL update.
  2. Backfills [UWI] from PCE_WM.[Value Navigator UWI] using production [Well Name]
     matched to WM [Composite Name] or [Well Name] (same rules as Enersight sync).

  Run in SSMS once before using ValNav Monthly Update (Sales + NGL) in the GUI.
  Also run scripts/create_pce_ngl_staging.sql once (bulk staging table).

  Legacy Fraction (_F) columns: if present from an earlier trial, drop them after
  confirming no downstream reports depend on them:

    ALTER TABLE dbo.PCE_Production DROP COLUMN
          [NGL-C2_F], [NGL-C3_F], [NGL-C4_F], [NGL-C5_F], [PA_NGLs_F];

  If dbo.PCE_NGL_Daily_Staging still has _F columns, DROP and recreate using
  scripts/create_pce_ngl_staging.sql (see that script for DROP/recreate notes).
*/

SET NOCOUNT ON;

IF OBJECT_ID(N'dbo.PCE_Production', N'U') IS NULL
BEGIN
    RAISERROR('Table dbo.PCE_Production does not exist.', 16, 1);
    RETURN;
END;
GO

-- --- Part 1: add columns (idempotent)
IF COL_LENGTH(N'dbo.PCE_Production', N'UWI') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_Production ADD [UWI] NVARCHAR(4000) NULL;
END;
GO

IF COL_LENGTH(N'dbo.PCE_Production', N'NGL-C2_R') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_Production ADD [NGL-C2_R] FLOAT NULL;
END;
GO

IF COL_LENGTH(N'dbo.PCE_Production', N'NGL-C3_R') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_Production ADD [NGL-C3_R] FLOAT NULL;
END;
GO

IF COL_LENGTH(N'dbo.PCE_Production', N'NGL-C4_R') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_Production ADD [NGL-C4_R] FLOAT NULL;
END;
GO

IF COL_LENGTH(N'dbo.PCE_Production', N'NGL-C5_R') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_Production ADD [NGL-C5_R] FLOAT NULL;
END;
GO

IF COL_LENGTH(N'dbo.PCE_Production', N'PA_NGLs_R') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_Production ADD [PA_NGLs_R] FLOAT NULL;
END;
GO

-- --- Part 2: populate [UWI] from Well Master ---
UPDATE p
SET p.[UWI] = LTRIM(RTRIM(CAST(ca.[Value Navigator UWI] AS NVARCHAR(4000))))
FROM dbo.PCE_Production AS p
CROSS APPLY (
    SELECT TOP 1
           wm.[Value Navigator UWI]
    FROM dbo.PCE_WM AS wm
    WHERE (
              wm.[Well Name] = p.[Well Name]
           OR (
                  NULLIF(RTRIM(CAST(wm.[Composite Name] AS NVARCHAR(4000))), N'') IS NOT NULL
              AND wm.[Composite Name] = p.[Well Name]
              )
          )
      AND (wm.[Exception] IS NULL OR wm.[Exception] = N'' OR wm.[Exception] = N'N')
      AND NULLIF(LTRIM(RTRIM(CAST(wm.[Value Navigator UWI] AS NVARCHAR(4000)))), N'') IS NOT NULL
) AS ca
WHERE p.[Well Name] NOT LIKE N'% - TC'
  AND p.[Well Name] NOT LIKE N'YE2%';
GO

/*
  Clear UWI before re-backfill (optional):

UPDATE dbo.PCE_Production SET [UWI] = NULL;

  Clear NGL ratio columns for a month re-run (optional; the monthly loader also
  NULLs ratio columns for matched wells in the selected month before write):

UPDATE dbo.PCE_Production
SET
      [NGL-C2_R] = NULL, [NGL-C3_R] = NULL, [NGL-C4_R] = NULL,
      [NGL-C5_R] = NULL, [PA_NGLs_R] = NULL;
*/
