/*
  dbo.Allocation_Factors — UWI and monthly NGL volume columns

  1. Adds [UWI] and five nullable FLOAT columns for monthly NGL volumes
     (bulk-loaded from NGL Excel via scripts/ngl_allocation_load.py).
  2. Backfills [UWI] from PCE_WM.[Value Navigator UWI] on existing AF rows
     matched by [Well Name].

  Run in SSMS once before bulk NGL load or ValNav Monthly Update (Sales + NGL).
  PCE_Production NGL ratio columns: scripts/add_pce_ngl_columns.sql
  Staging table: scripts/create_pce_ngl_staging.sql
*/

SET NOCOUNT ON;

IF OBJECT_ID(N'dbo.Allocation_Factors', N'U') IS NULL
BEGIN
    RAISERROR('Table dbo.Allocation_Factors does not exist.', 16, 1);
    RETURN;
END;
GO

-- --- Part 1: add columns (idempotent)
IF COL_LENGTH(N'dbo.Allocation_Factors', N'UWI') IS NULL
BEGIN
    ALTER TABLE dbo.Allocation_Factors ADD [UWI] NVARCHAR(4000) NULL;
END;
GO

IF COL_LENGTH(N'dbo.Allocation_Factors', N'NGL_C2') IS NULL
BEGIN
    ALTER TABLE dbo.Allocation_Factors ADD [NGL_C2] FLOAT NULL;
END;
GO

IF COL_LENGTH(N'dbo.Allocation_Factors', N'NGL_C3') IS NULL
BEGIN
    ALTER TABLE dbo.Allocation_Factors ADD [NGL_C3] FLOAT NULL;
END;
GO

IF COL_LENGTH(N'dbo.Allocation_Factors', N'NGL_C4') IS NULL
BEGIN
    ALTER TABLE dbo.Allocation_Factors ADD [NGL_C4] FLOAT NULL;
END;
GO

IF COL_LENGTH(N'dbo.Allocation_Factors', N'NGL_C5') IS NULL
BEGIN
    ALTER TABLE dbo.Allocation_Factors ADD [NGL_C5] FLOAT NULL;
END;
GO

IF COL_LENGTH(N'dbo.Allocation_Factors', N'PA_NGLs') IS NULL
BEGIN
    ALTER TABLE dbo.Allocation_Factors ADD [PA_NGLs] FLOAT NULL;
END;
GO

-- --- Part 2: populate [UWI] from Well Master ---
UPDATE a
SET a.[UWI] = LTRIM(RTRIM(CAST(w.[Value Navigator UWI] AS NVARCHAR(4000))))
FROM dbo.Allocation_Factors AS a
INNER JOIN dbo.PCE_WM AS w
    ON a.[Well Name] = w.[Well Name]
WHERE a.[UWI] IS NULL
  AND (w.[Exception] IS NULL OR w.[Exception] = N'' OR w.[Exception] = N'N')
  AND NULLIF(LTRIM(RTRIM(CAST(w.[Value Navigator UWI] AS NVARCHAR(4000)))), N'') IS NOT NULL;
GO
