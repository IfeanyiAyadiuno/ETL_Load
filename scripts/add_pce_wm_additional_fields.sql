/*
  dbo.PCE_WM — additional well metadata fields (coordinates, elevations, tubing, dates)

  Adds 19 nullable columns for Well Master "Additional Fields" UI and Whitson coordinate sync.
  Run once in SSMS against the correct database. Idempotent — safe to re-run.
*/

SET NOCOUNT ON;

IF OBJECT_ID(N'dbo.PCE_WM', N'U') IS NULL
BEGIN
    RAISERROR('Table dbo.PCE_WM does not exist.', 16, 1);
    RETURN;
END;
GO

IF COL_LENGTH(N'dbo.PCE_WM', N'Bottom Hole Latitude') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Bottom Hole Latitude] FLOAT NULL;
GO

IF COL_LENGTH(N'dbo.PCE_WM', N'Bottom Hole Longitude') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Bottom Hole Longitude] FLOAT NULL;
GO

IF COL_LENGTH(N'dbo.PCE_WM', N'Bottom Hole UTM Easting (m)') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Bottom Hole UTM Easting (m)] FLOAT NULL;
GO

IF COL_LENGTH(N'dbo.PCE_WM', N'Bottom Hole UTM Northing (m)') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Bottom Hole UTM Northing (m)] FLOAT NULL;
GO

IF COL_LENGTH(N'dbo.PCE_WM', N'Bottom Hole UTM Zone') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Bottom Hole UTM Zone] INT NULL;
GO

IF COL_LENGTH(N'dbo.PCE_WM', N'Surface Hole Latitude') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Surface Hole Latitude] FLOAT NULL;
GO

IF COL_LENGTH(N'dbo.PCE_WM', N'Surface Hole Longitude') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Surface Hole Longitude] FLOAT NULL;
GO

IF COL_LENGTH(N'dbo.PCE_WM', N'Surface Hole UTM Easting (m)') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Surface Hole UTM Easting (m)] FLOAT NULL;
GO

IF COL_LENGTH(N'dbo.PCE_WM', N'Surface Hole UTM Northing (m)') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Surface Hole UTM Northing (m)] FLOAT NULL;
GO

IF COL_LENGTH(N'dbo.PCE_WM', N'Surface Hole UTM Zone') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Surface Hole UTM Zone] INT NULL;
GO

IF COL_LENGTH(N'dbo.PCE_WM', N'KB Elevation (m)') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [KB Elevation (m)] FLOAT NULL;
GO

IF COL_LENGTH(N'dbo.PCE_WM', N'Ground Elevation (m)') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Ground Elevation (m)] FLOAT NULL;
GO

IF COL_LENGTH(N'dbo.PCE_WM', N'Max True Vertical Depth (m)') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Max True Vertical Depth (m)] FLOAT NULL;
GO

IF COL_LENGTH(N'dbo.PCE_WM', N'Total Depth (m)') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Total Depth (m)] FLOAT NULL;
GO

IF COL_LENGTH(N'dbo.PCE_WM', N'Spud Date') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Spud Date] DATE NULL;
GO

IF COL_LENGTH(N'dbo.PCE_WM', N'Rig Release Date') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Rig Release Date] DATE NULL;
GO

IF COL_LENGTH(N'dbo.PCE_WM', N'Outside Diameter (mm)') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Outside Diameter (mm)] FLOAT NULL;
GO

IF COL_LENGTH(N'dbo.PCE_WM', N'Tubing Strength (MPa)') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Tubing Strength (MPa)] FLOAT NULL;
GO

IF COL_LENGTH(N'dbo.PCE_WM', N'Tubing Linear Weight (kg/m)') IS NULL
    ALTER TABLE dbo.PCE_WM ADD [Tubing Linear Weight (kg/m)] FLOAT NULL;
GO

PRINT N'PCE_WM additional field columns are present.';
GO
