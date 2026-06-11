/*
  Post-rebuild verification: WM UWI/pad propagation to Production, AF, and FRCST_PRD.

  Run after a full or quick Prodview update when checking corrected B-* wells or
  any Well Master UWI/pad edits.
*/

-- 1) Well Master canonical values (six B-* wells example)
SELECT [Well Name], [Value Navigator UWI], [Pad Name]
FROM dbo.PCE_WM
WHERE [Well Name] IN (
    N'B-K095-H/094-B-08', N'B-I095-H/094-B-08', N'B-L095-H/094-B-08',
    N'B-M095-H/094-B-08', N'B-H095-H/094-B-08', N'B-J095-H/094-B-08'
);

-- 2) Production: UWI populated + pad has PRD suffix
SELECT DISTINCT p.[Well Name], p.[UWI], p.[Pad Name]
FROM dbo.PCE_Production AS p
WHERE p.[Well Name] IN (
    N'B-K095-H/094-B-08', N'B-I095-H/094-B-08', N'B-L095-H/094-B-08',
    N'B-M095-H/094-B-08', N'B-H095-H/094-B-08', N'B-J095-H/094-B-08'
);

-- 3) Allocation_Factors UWI aligned to WM
SELECT DISTINCT a.[Well Name], a.[UWI]
FROM dbo.Allocation_Factors AS a
WHERE a.[Well Name] IN (
    N'B-K095-H/094-B-08', N'B-I095-H/094-B-08', N'B-L095-H/094-B-08',
    N'B-M095-H/094-B-08', N'B-H095-H/094-B-08', N'B-J095-H/094-B-08'
);

-- 4) FRCST_PRD gathered rows: pad matches production (with PRD), month is Gath PRD*
SELECT DISTINCT p.[Well Name], f.[Pad], f.[Month]
FROM dbo.PCE_FRCST_PRD AS f
INNER JOIN dbo.PCE_Production AS p
    ON p.[Date] = f.[Date]
   AND LTRIM(RTRIM(CAST(p.[UWI] AS NVARCHAR(4000)))) =
       LTRIM(RTRIM(CAST(f.[UWI] AS NVARCHAR(4000))))
WHERE p.[Well Name] IN (
    N'B-K095-H/094-B-08', N'B-I095-H/094-B-08', N'B-L095-H/094-B-08',
    N'B-M095-H/094-B-08', N'B-H095-H/094-B-08', N'B-J095-H/094-B-08'
)
  AND f.[Month] LIKE N'Gath PRD%';

-- 5) Spot check: production rows missing UWI (should be zero for active WM wells)
SELECT COUNT(*) AS rows_missing_uwi
FROM dbo.PCE_Production AS p
INNER JOIN dbo.PCE_WM AS w
    ON p.[Well Name] = w.[Well Name]
   AND (w.[Exception] IS NULL OR w.[Exception] = N'' OR w.[Exception] = N'N')
WHERE p.[Well Name] NOT LIKE N'% - TC'
  AND p.[Well Name] NOT LIKE N'YE2%'
  AND NULLIF(LTRIM(RTRIM(CAST(w.[Value Navigator UWI] AS NVARCHAR(4000)))), N'') IS NOT NULL
  AND NULLIF(LTRIM(RTRIM(CAST(p.[UWI] AS NVARCHAR(4000)))), N'') IS NULL;
