/*
  dbo.PCE_Production — [UWI] and trial NGL daily columns (Ratio _R / Fraction _F)

  1. Adds [UWI] and ten nullable FLOAT columns for NGL compare plotting.
  2. Backfills [UWI] from PCE_WM.[Value Navigator UWI] using production [Well Name]
     matched to WM [Composite Name] or [Well Name] (same rules as Enersight sync).

  Run in SSMS before: python scripts/ngl_daily_compare.py --excel "…"
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

IF COL_LENGTH(N'dbo.PCE_Production', N'NGL-C2_F') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_Production ADD [NGL-C2_F] FLOAT NULL;
END;
GO

IF COL_LENGTH(N'dbo.PCE_Production', N'NGL-C3_F') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_Production ADD [NGL-C3_F] FLOAT NULL;
END;
GO

IF COL_LENGTH(N'dbo.PCE_Production', N'NGL-C4_F') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_Production ADD [NGL-C4_F] FLOAT NULL;
END;
GO

IF COL_LENGTH(N'dbo.PCE_Production', N'NGL-C5_F') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_Production ADD [NGL-C5_F] FLOAT NULL;
END;
GO

IF COL_LENGTH(N'dbo.PCE_Production', N'PA_NGLs_F') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_Production ADD [PA_NGLs_F] FLOAT NULL;
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

  Clear trial NGL columns before re-run of ngl_daily_compare.py (optional):

UPDATE dbo.PCE_Production
SET
      [NGL-C2_R] = NULL, [NGL-C3_R] = NULL, [NGL-C4_R] = NULL,
      [NGL-C5_R] = NULL, [PA_NGLs_R] = NULL,
      [NGL-C2_F] = NULL, [NGL-C3_F] = NULL, [NGL-C4_F] = NULL,
      [NGL-C5_F] = NULL, [PA_NGLs_F] = NULL;
*/
