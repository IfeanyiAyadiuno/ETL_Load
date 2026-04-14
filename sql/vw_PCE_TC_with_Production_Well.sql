-- Helper view: each PCE_TC row with the physical well key (strip trailing N' - TC').
-- PCE_TC is keyed by PCE_WM.[Well Name] + ' - TC', which may differ from PCE_Production.[Well Name]
-- when production uses composite names; join via Well Master or composite mapping as needed.

IF OBJECT_ID(N'dbo.vw_PCE_TC_with_Production_Well', N'V') IS NOT NULL
    DROP VIEW dbo.vw_PCE_TC_with_Production_Well;
GO

CREATE VIEW dbo.vw_PCE_TC_with_Production_Well
AS
    SELECT
        t.PCE_TCId,
        t.[Well Name] AS [TypeCurve Well Name],
        CASE
            WHEN RIGHT(RTRIM(t.[Well Name]), 5) = N' - TC' THEN
                LEFT(RTRIM(t.[Well Name]), LEN(RTRIM(t.[Well Name])) - 5)
            ELSE RTRIM(t.[Well Name])
        END AS [Production Well Name],
        t.[ImportDate],
        t.[Gas S2 Production (10³m³)],
        t.[Gas Sales Production (10³m³)],
        t.[Condensate Sales (m³/d)],
        t.[Sales CGR (m³/e³m³)],
        t.[Gas WH Production (e³m³/d)],
        t.[Condensate WH (m³/d)],
        t.[Cum Gas (e³m³)],
        t.[Cum Condy (m³)],
        t.[Layer Producer],
        t.[Pad Name],
        t.[SourceFileName]
    FROM dbo.PCE_TC AS t;
GO

/*
Example: attach latest type-curve batch (by ImportDate) to production rows.
If a well has multiple TC rows (curve points), you still get one TC row per join key;
adjust with ROW_NUMBER / aggregates as needed.

SELECT
    p.[Well Name],
    p.[Date],
    tc.*
FROM dbo.PCE_Production AS p
LEFT JOIN (
    SELECT v.*,
           ROW_NUMBER() OVER (
               PARTITION BY v.[Production Well Name]
               ORDER BY v.[ImportDate] DESC, v.PCE_TCId DESC
           ) AS rn
    FROM dbo.vw_PCE_TC_with_Production_Well AS v
) AS tc
    ON tc.[Production Well Name] = p.[Well Name]
   AND tc.rn = 1;
*/
