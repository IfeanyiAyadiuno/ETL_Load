/*
  dbo.PCE_NGL_Daily_Staging — bulk load target for NGL daily compare

  Python loads computed daily NGL values here, then one UPDATE … JOIN copies
  them into dbo.PCE_Production (by UWI + Date).

  Prerequisites:
    scripts/add_pce_ngl_columns.sql (UWI + 10 NGL columns on PCE_Production)

  Run once in SSMS before:
    python scripts/ngl_daily_compare.py --excel "…"
*/

SET NOCOUNT ON;

IF OBJECT_ID(N'dbo.PCE_NGL_Daily_Staging', N'U') IS NOT NULL
BEGIN
    PRINT 'dbo.PCE_NGL_Daily_Staging already exists — skipped.';
    RETURN;
END;
GO

CREATE TABLE dbo.PCE_NGL_Daily_Staging (
      UwiRaw      NVARCHAR(4000) NOT NULL
    , ProdDate    DATE           NOT NULL
    , [NGL-C2_R]  FLOAT          NULL
    , [NGL-C3_R]  FLOAT          NULL
    , [NGL-C4_R]  FLOAT          NULL
    , [NGL-C5_R]  FLOAT          NULL
    , [PA_NGLs_R] FLOAT          NULL
    , [NGL-C2_F]  FLOAT          NULL
    , [NGL-C3_F]  FLOAT          NULL
    , [NGL-C4_F]  FLOAT          NULL
    , [NGL-C5_F]  FLOAT          NULL
    , [PA_NGLs_F] FLOAT          NULL
);
GO

PRINT 'Created dbo.PCE_NGL_Daily_Staging.';
GO
