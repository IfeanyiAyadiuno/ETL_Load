-- dbo.PCE_Production — [Month] (gathered production source tag)
-- Idempotent ADD. Backfill NULL/blank with the app default (see production_update.PCE_PRODUCTION_MONTH_LABEL).

SET NOCOUNT ON;

IF OBJECT_ID(N'dbo.PCE_Production', N'U') IS NULL
BEGIN
    RAISERROR('Table dbo.PCE_Production does not exist.', 16, 1);
    RETURN;
END;
GO

IF COL_LENGTH(N'dbo.PCE_Production', N'Month') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_Production ADD [Month] NVARCHAR(64) NULL;
END;
GO

UPDATE dbo.PCE_Production
SET [Month] = N'Gathered PRD'
WHERE [Month] IS NULL OR LTRIM(RTRIM([Month])) = N'';
GO
