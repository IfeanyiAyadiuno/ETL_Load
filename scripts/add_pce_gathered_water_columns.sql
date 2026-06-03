/*
  Gathered water — PCE_CDA + PCE_Production

  Run in SSMS against Re_Main_Production before the next Prodview/Snowflake refresh.

  PCE_CDA: Snowflake VOLPRODGATHWATER lands in Gathered_Water_Production
  PCE_Production: daily rate, cumulative, and calendar-month average (computed on rebuild)
*/

SET NOCOUNT ON;

IF OBJECT_ID(N'dbo.PCE_CDA', N'U') IS NULL
BEGIN
    RAISERROR('Table dbo.PCE_CDA does not exist.', 16, 1);
    RETURN;
END;

IF OBJECT_ID(N'dbo.PCE_Production', N'U') IS NULL
BEGIN
    RAISERROR('Table dbo.PCE_Production does not exist.', 16, 1);
    RETURN;
END;
GO

IF COL_LENGTH(N'dbo.PCE_CDA', N'Gathered_Water_Production') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_CDA ADD Gathered_Water_Production FLOAT NULL;
END;
GO

IF COL_LENGTH(N'dbo.PCE_Production', N'Gath. Water Rate (m³/d)') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_Production ADD [Gath. Water Rate (m³/d)] FLOAT NULL;
END;
GO

IF COL_LENGTH(N'dbo.PCE_Production', N'Gath. Water Cumulative (m³)') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_Production ADD [Gath. Water Cumulative (m³)] FLOAT NULL;
END;
GO

IF COL_LENGTH(N'dbo.PCE_Production', N'Gath. Water Avg (m³/d)') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_Production ADD [Gath. Water Avg (m³/d)] FLOAT NULL;
END;
GO
