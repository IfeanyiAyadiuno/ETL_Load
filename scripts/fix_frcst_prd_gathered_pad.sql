/*
  One-time fix: sync gathered production pads in PCE_FRCST_PRD from PCE_Production.

  Gathered rows: [Month] LIKE N'Gath PRD%'
  Forecast rows (PCE_Monthly_Forecasts) are unchanged.
*/

SET NOCOUNT ON;

-- Sync Pad from PCE_Production (already has " PRD" suffix)
UPDATE f
SET f.[Pad] = LTRIM(RTRIM(CAST(p.[Pad Name] AS NVARCHAR(4000))))
FROM dbo.PCE_FRCST_PRD AS f
INNER JOIN dbo.PCE_Production AS p
    ON CAST(p.[Date] AS DATE) = CAST(f.[Date] AS DATE)
   AND LTRIM(RTRIM(CAST(p.[UWI] AS NVARCHAR(4000)))) =
       LTRIM(RTRIM(CAST(f.[UWI] AS NVARCHAR(4000))))
WHERE f.[Month] LIKE N'Gath PRD%'
  AND NULLIF(LTRIM(RTRIM(CAST(p.[Pad Name] AS NVARCHAR(4000)))), N'') IS NOT NULL;

-- Fallback: append " PRD" where production join missed
UPDATE f
SET f.[Pad] = LTRIM(RTRIM(CAST(f.[Pad] AS NVARCHAR(4000)))) + N' PRD'
FROM dbo.PCE_FRCST_PRD AS f
WHERE f.[Month] LIKE N'Gath PRD%'
  AND NULLIF(LTRIM(RTRIM(CAST(f.[Pad] AS NVARCHAR(4000)))), N'') IS NOT NULL
  AND RIGHT(LTRIM(RTRIM(CAST(f.[Pad] AS NVARCHAR(4000)))), 4) <> N' PRD';
