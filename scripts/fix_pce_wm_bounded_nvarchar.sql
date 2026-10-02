-- Ensure PCE_WM.[Bounded] stores 'Bounded' / 'Unbounded' text (not FLOAT).
-- Run once if Well Master save fails with:
--   Error converting data type nvarchar to float (8114)

IF COL_LENGTH('dbo.PCE_WM', 'Bounded') IS NULL
BEGIN
    ALTER TABLE dbo.PCE_WM ADD [Bounded] NVARCHAR(20) NULL;
END
GO

IF EXISTS (
    SELECT 1
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dbo'
      AND TABLE_NAME = 'PCE_WM'
      AND COLUMN_NAME = 'Bounded'
      AND DATA_TYPE IN ('float', 'real', 'decimal', 'numeric', 'int', 'bigint', 'smallint', 'tinyint')
)
BEGIN
    IF EXISTS (
        SELECT 1 FROM sys.check_constraints
        WHERE name = 'CK_PCE_WM_Bounded'
          AND parent_object_id = OBJECT_ID('dbo.PCE_WM')
    )
        ALTER TABLE dbo.PCE_WM DROP CONSTRAINT CK_PCE_WM_Bounded;

    ALTER TABLE dbo.PCE_WM ALTER COLUMN [Bounded] NVARCHAR(20) NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.check_constraints
    WHERE name = 'CK_PCE_WM_Bounded'
      AND parent_object_id = OBJECT_ID('dbo.PCE_WM')
)
BEGIN
    ALTER TABLE dbo.PCE_WM ADD CONSTRAINT CK_PCE_WM_Bounded
        CHECK ([Bounded] IS NULL OR [Bounded] IN (N'Bounded', N'Unbounded'));
END
GO
