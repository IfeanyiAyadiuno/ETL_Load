/*
  dbo.PCE_FRCST_PRD — combined monthly forecasts + appended gathered production rows

  Run once per database in SSMS before using the rebuild in the Python app.

  Shapes identical business columns from dbo.PCE_Monthly_Forecasts (excludes
  MonthlyForecast_Id if present) so dtypes match forecasts.

  Rows are wiped and
  repopulated by pce_frcst_prd_rebuild.rebuild_pce_frcst_prd() when:
    - Monthly forecast Excel import completes, or
    - Production is rebuilt / Prodview refreshes Production + TC sync, etc.

  Gathered-append rows convert production metrics into CDGR_Mcf_d / CD_Cond_bbl_d /
  CD_Water_bbl_d (e³m³/d×35.49373, cond m³/d×6.29287, water m³/d×6.29010); [Month] =
  N'Gath PRD {Enersight}' (e.g. N'Gath PRD L-16') distinguishes them from forecast uploads.

  See plan: PCE_FRCST_PRD table.
*/

SET NOCOUNT ON;

IF OBJECT_ID(N'dbo.PCE_Monthly_Forecasts', N'U') IS NULL
BEGIN
    RAISERROR(' dbo.PCE_Monthly_Forecasts must exist before cloning structure.', 16, 1);
    RETURN;
END;
GO

IF OBJECT_ID(N'dbo.PCE_FRCST_PRD', N'U') IS NOT NULL
    DROP TABLE dbo.PCE_FRCST_PRD;
GO

SELECT TOP (0)
      mf.[Date]
    , mf.[UWI]
    , mf.[CDGR_Mcf_d]
    , mf.[CD_Cond_bbl_d]
    , mf.[CD_Water_bbl_d]
    , mf.[Month]
    , mf.[Pad]
    , mf.[Fault_Block]
    , mf.[Enersight Well Name]
INTO dbo.PCE_FRCST_PRD
FROM dbo.PCE_Monthly_Forecasts AS mf;
GO

/*
  Verify (optional):

SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = N'dbo' AND TABLE_NAME = N'PCE_FRCST_PRD'
ORDER BY ORDINAL_POSITION;
*/
