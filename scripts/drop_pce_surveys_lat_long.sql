-- Drop deprecated Latitude / Longitude columns from PCE_Surveys.
-- Run once per environment before deploying survey import builds that no longer reference these columns.

IF COL_LENGTH('dbo.PCE_Surveys', 'Latitude') IS NOT NULL
    ALTER TABLE dbo.PCE_Surveys DROP COLUMN [Latitude];

IF COL_LENGTH('dbo.PCE_Surveys', 'Longitude') IS NOT NULL
    ALTER TABLE dbo.PCE_Surveys DROP COLUMN [Longitude];
