SET NOCOUNT ON;
GO

RESTORE VERIFYONLY
FROM DISK = N'/var/opt/mssql/backup/Northwind_DW_1404_08_14_Full.bak';
GO

IF DB_ID(N'Northwind_BI_1404_05_DW') IS NULL
BEGIN
    RESTORE DATABASE [Northwind_BI_1404_05_DW]
    FROM DISK = N'/var/opt/mssql/backup/Northwind_DW_1404_08_14_Full.bak'
    WITH
        MOVE N'Northwind_BI_1404_05_DW'
            TO N'/var/opt/mssql/data/Northwind_BI_1404_05_DW.mdf',
        MOVE N'Northwind_BI_1404_05_DW_log'
            TO N'/var/opt/mssql/data/Northwind_BI_1404_05_DW_log.ldf',
        RECOVERY,
        STATS = 5;

    PRINT N'Restore completed successfully.';
END
ELSE
BEGIN
    PRINT N'Database already exists. Restore skipped.';
END;
GO
