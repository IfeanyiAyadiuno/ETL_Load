-- Type curves storage (PCE_TC). Run once on the target database.
-- Well rows use mapped production-style [Well Name] + suffix N' - TC' (applied in app).

('IF OBJECT_ID(N'dbo.PCE_TC', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.PCE_TC (
        PCE_TCId BIGINT IDENTITY(1, 1) NOT NULL
            CONSTRAINT PK_PCE_TC PRIMARY KEY,
        [Well Name] NVARCHAR(400) NOT NULL,
        [ImportDate] DATE NOT NULL,
        [Gas S2 Production (10³m³)] FLOAT NULL,
        [Gas Sales Production (10³m³)] FLOAT NULL,
        [Condensate Sales (m³/d)] FLOAT NULL,
        [Sales CGR (m³/e³m³)] FLOAT NULL,
        [Gas WH Production (e³m³/d)] FLOAT NULL,
        [Condensate WH (m³/d)] FLOAT NULL,
        [Cum Gas (e³m³)] FLOAT NULL,
        [Cum Condy (m³)] FLOAT NULL,
        [Layer Producer] NVARCHAR(500) NULL,
        [Pad Name] NVARCHAR(500) NULL,
        [SourceFileName] NVARCHAR(400) NULL
    );

    CREATE NONCLUSTERED INDEX IX_PCE_TC_WellName
        ON dbo.PCE_TC ([Well Name]);
END
GO')
