/*
  Wells in PCE_WM (non-exception) with a populated Value Navigator UWI that have NO
  matching rows in PCE_Surveys on UWI.

  Duplicate Val Nav UWIs across multiple WM wells will list each WM row separately.
  UWIs compared after trim only; extend with LOWER() or ValNav-format variants if needed.

  A second commented query uses a simple heuristic for “directional-style” gaps:
  UWI appears in PCE_Surveys but with fewer than two rows (often one surface shot only).
*/

-- 1) No survey rows at all for this UWI
SELECT
    wm.[Well Name],
    wm.[Composite Name],
    wm.[Value Navigator UWI] AS WM_UWI,
    wm.[Pad Name],
    wm.[Fault Block]
FROM dbo.PCE_WM AS wm
WHERE (wm.[Exception] IS NULL OR wm.[Exception] = '' OR wm.[Exception] = 'N')
  AND wm.[Value Navigator UWI] IS NOT NULL
  AND LTRIM(RTRIM(CAST(wm.[Value Navigator UWI] AS NVARCHAR(MAX)))) <> N''
  AND NOT EXISTS (
        SELECT 1
        FROM dbo.PCE_Surveys AS s
        WHERE LTRIM(RTRIM(CAST(s.[UWI] AS NVARCHAR(MAX)))) =
              LTRIM(RTRIM(CAST(wm.[Value Navigator UWI] AS NVARCHAR(MAX))))
      )
ORDER BY wm.[Well Name];
GO


-- 2) Optional: UWIs present in surveys but fewer than two rows (possible “no directional”)
/*
WITH survey_counts AS (
    SELECT
        LTRIM(RTRIM(CAST([UWI] AS NVARCHAR(MAX)))) AS UWI_key,
        COUNT_BIG(*) AS point_count
    FROM dbo.PCE_Surveys
    GROUP BY LTRIM(RTRIM(CAST([UWI] AS NVARCHAR(MAX))))
)
SELECT
    wm.[Well Name],
    wm.[Composite Name],
    wm.[Value Navigator UWI] AS WM_UWI,
    ISNULL(sc.point_count, 0) AS survey_row_count_for_uwi,
    wm.[Pad Name],
    wm.[Fault Block]
FROM dbo.PCE_WM AS wm
LEFT JOIN survey_counts AS sc
  ON sc.UWI_key = LTRIM(RTRIM(CAST(wm.[Value Navigator UWI] AS NVARCHAR(MAX))))
WHERE (wm.[Exception] IS NULL OR wm.[Exception] = '' OR wm.[Exception] = 'N')
  AND wm.[Value Navigator UWI] IS NOT NULL
  AND LTRIM(RTRIM(CAST(wm.[Value Navigator UWI] AS NVARCHAR(MAX)))) <> N''
  AND (sc.point_count IS NULL OR sc.point_count < 2)
ORDER BY survey_row_count_for_uwi DESC, wm.[Well Name];
*/
