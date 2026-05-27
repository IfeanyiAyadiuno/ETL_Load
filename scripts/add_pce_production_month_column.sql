-- dbo.PCE_Production — [Month] (gathered production source tag)
-- Idempotent ADD. Backfill NULL/blank with Gath PRD + Enersight name (see production_update.gathered_prd_month_label).

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

UPDATE p
SET p.[Month] = CASE
        WHEN NULLIF(LTRIM(RTRIM(CAST(p.[Enersight Well Name] AS NVARCHAR(4000)))), N'') IS NULL
            THEN N'Gath PRD'
        WHEN LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(
                LTRIM(RTRIM(CAST(p.[Enersight Well Name] AS NVARCHAR(4000)))),
                N' Well', N''), N' well', N''), N' WELL', N''))) = N''
            THEN N'Gath PRD'
        ELSE N'Gath PRD ' + LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(
                LTRIM(RTRIM(CAST(p.[Enersight Well Name] AS NVARCHAR(4000)))),
                N' Well', N''), N' well', N''), N' WELL', N'')))
    END
FROM dbo.PCE_Production AS p
WHERE p.[Month] IS NULL
   OR LTRIM(RTRIM(p.[Month])) = N''
   OR p.[Month] = N'Gathered PRD';
GO
