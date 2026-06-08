/*
  dbo.PCE_NGL_Daily_Staging — bulk load target for ValNav monthly NGL update

  Python loads computed daily NGL Ratio values here, then one UPDATE … JOIN copies
  them into dbo.PCE_Production (by UWI + Date).

  Prerequisites:
    scripts/add_pce_ngl_columns.sql (UWI + five Ratio _R columns on PCE_Production)

  Run once in SSMS before the first ValNav Monthly Update (Sales + NGL) run.

  If an older staging table exists with Fraction (_F) columns, drop and recreate:

    DROP TABLE dbo.PCE_NGL_Daily_Staging;
    -- then run this script again
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
);
GO

PRINT 'Created dbo.PCE_NGL_Daily_Staging.';
GO
