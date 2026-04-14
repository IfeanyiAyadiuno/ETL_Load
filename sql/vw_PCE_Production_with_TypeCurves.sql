-- Stacked reporting view: same column list as dbo.PCE_Production (same order as app INSERT).
--   Part 1: all rows from PCE_Production.
--   Part 2: every row from dbo.vw_PCE_TC_with_Production_Well mapped into those columns
--   (TC-only fields; other columns NULL). TC rows appear after production rows.
--
-- Mapping notes (TC vs production naming/units):
--   • [Date] <- TC [ImportDate]; [Days Seq] / [Day Seq UPRT] = 0.
--   • [Well Name] <- full stored TC key (helper [TypeCurve Well Name] = PCE_TC.[Well Name],
--     including literal N' - TC' suffix). Stripped physical name is not used in this UNION.
--   • TC [Gas WH Production (e³m³/d)] -> [Gas WH Production (10³m³)] (same numeric rate slot
--     as used elsewhere in this project; adjust if your DB uses a strict unit split).
--   • TC [Cum Gas (e³m³)] -> [Gas Gathered Cumulative (e³m³)] (exact unit match).
--   • TC [Cum Condy (m³)] -> [Condensate WH Cumulative Production (m³)].
--   • Name-identical rates (S2, sales gas, sales cond, WH cond, sales CGR) copy across.
--
-- If dbo.PCE_Production has extra columns (e.g. identity) not listed here, add them to both
-- SELECT lists so the view can be created.

IF OBJECT_ID(N'dbo.vw_PCE_Production_with_TypeCurves', N'V') IS NOT NULL
    DROP VIEW dbo.vw_PCE_Production_with_TypeCurves;
GO

CREATE VIEW dbo.vw_PCE_Production_with_TypeCurves
AS
    SELECT
        p.[Date],
        p.[Days Seq],
        p.[Day Seq UPRT],
        p.[Well Name],
        p.[Gas WH Production (10³m³)],
        p.[Condensate WH (m³/d)],
        p.[Gas S2 Production (10³m³)],
        p.[Gas Sales Production (10³m³)],
        p.[Condensate Sales (m³/d)],
        p.[Gathered Gas (e³m³/d)],
        p.[Gathered Condensate (m³/d)],
        p.[Sales CGR (m³/e³m³)],
        p.[CGR (m³/e³m³)],
        p.[WGR (m³/e³m³)],
        p.[ECF],
        p.[Hours On],
        p.[Tubing Pressure (kPa)],
        p.[Casing Pressure (kPa)],
        p.[Choke Size],
        p.[Gas WH Cumulative Production (10³m³)],
        p.[Gas S2 Cumulative Production (10³m³)],
        p.[Gas Sales Cumulative Production (10³m³)],
        p.[Condensate Sales Cumulative Production (m³)],
        p.[Condensate WH Cumulative Production (m³)],
        p.[Gas Gathered Cumulative (e³m³)],
        p.[Condensate Gathered Cumulative (m³)],
        p.[Formation Producer],
        p.[Layer Producer],
        p.[Fault Block],
        p.[Pad Name],
        p.[Lateral Length],
        p.[Orientation],
        p.[On Production Year],
        p.[Alloc. Water Rate (m³)],
        p.[NGL (m³)],
        p.[Gas WH Avg (10³m³)],
        p.[Gas S2 Avg (10³m³)],
        p.[Gas Gathered Avg (e³m³/d)],
        p.[Condensate Gathered Avg (m³/d)]
    FROM dbo.PCE_Production AS p

    UNION ALL

    SELECT
        v.[ImportDate] AS [Date],
        CAST(0 AS INT) AS [Days Seq],
        CAST(0 AS INT) AS [Day Seq UPRT],
        v.[TypeCurve Well Name] AS [Well Name],
        CAST(v.[Gas WH Production (e³m³/d)] AS FLOAT) AS [Gas WH Production (10³m³)],
        v.[Condensate WH (m³/d)],
        v.[Gas S2 Production (10³m³)],
        v.[Gas Sales Production (10³m³)],
        v.[Condensate Sales (m³/d)],
        CAST(NULL AS FLOAT) AS [Gathered Gas (e³m³/d)],
        CAST(NULL AS FLOAT) AS [Gathered Condensate (m³/d)],
        v.[Sales CGR (m³/e³m³)],
        CAST(NULL AS FLOAT) AS [CGR (m³/e³m³)],
        CAST(NULL AS FLOAT) AS [WGR (m³/e³m³)],
        CAST(NULL AS FLOAT) AS [ECF],
        CAST(NULL AS FLOAT) AS [Hours On],
        CAST(NULL AS FLOAT) AS [Tubing Pressure (kPa)],
        CAST(NULL AS FLOAT) AS [Casing Pressure (kPa)],
        CAST(NULL AS FLOAT) AS [Choke Size],
        CAST(NULL AS FLOAT) AS [Gas WH Cumulative Production (10³m³)],
        CAST(NULL AS FLOAT) AS [Gas S2 Cumulative Production (10³m³)],
        CAST(NULL AS FLOAT) AS [Gas Sales Cumulative Production (10³m³)],
        CAST(NULL AS FLOAT) AS [Condensate Sales Cumulative Production (m³)],
        v.[Cum Condy (m³)] AS [Condensate WH Cumulative Production (m³)],
        v.[Cum Gas (e³m³)] AS [Gas Gathered Cumulative (e³m³)],
        CAST(NULL AS FLOAT) AS [Condensate Gathered Cumulative (m³)],
        CAST(NULL AS NVARCHAR(500)) AS [Formation Producer],
        v.[Layer Producer],
        CAST(NULL AS NVARCHAR(500)) AS [Fault Block],
        v.[Pad Name],
        CAST(NULL AS FLOAT) AS [Lateral Length],
        CAST(NULL AS NVARCHAR(200)) AS [Orientation],
        CAST(YEAR(v.[ImportDate]) AS INT) AS [On Production Year],
        CAST(NULL AS FLOAT) AS [Alloc. Water Rate (m³)],
        CAST(NULL AS FLOAT) AS [NGL (m³)],
        CAST(NULL AS FLOAT) AS [Gas WH Avg (10³m³)],
        CAST(NULL AS FLOAT) AS [Gas S2 Avg (10³m³)],
        CAST(NULL AS FLOAT) AS [Gas Gathered Avg (e³m³/d)],
        CAST(NULL AS FLOAT) AS [Condensate Gathered Avg (m³/d)]
    FROM dbo.vw_PCE_TC_with_Production_Well AS v;
GO
