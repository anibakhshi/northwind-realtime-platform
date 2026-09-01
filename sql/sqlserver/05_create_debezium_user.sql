USE [master];
GO

SET NOCOUNT ON;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.server_principals
    WHERE name = N'debezium'
)
BEGIN
    DECLARE @password nvarchar(128) = N'$(DEBEZIUM_PASSWORD)';
    DECLARE @sql nvarchar(max);

    SET @sql =
        N'CREATE LOGIN [debezium] WITH PASSWORD = N'''
        + REPLACE(@password, N'''', N'''''')
        + N''', CHECK_POLICY = ON, CHECK_EXPIRATION = OFF;';

    EXEC sys.sp_executesql @sql;
    PRINT N'Debezium login created.';
END
ELSE
BEGIN
    PRINT N'Debezium login already exists.';
END
GO

GRANT VIEW SERVER STATE TO [debezium];
GO

USE [Northwind];
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.database_principals
    WHERE name = N'debezium'
)
BEGIN
    CREATE USER [debezium] FOR LOGIN [debezium];
    PRINT N'Debezium database user created.';
END
ELSE
BEGIN
    PRINT N'Debezium database user already exists.';
END
GO

IF ISNULL(IS_ROLEMEMBER(N'db_datareader', N'debezium'), 0) <> 1
BEGIN
    ALTER ROLE [db_datareader] ADD MEMBER [debezium];
END
GO

IF ISNULL(IS_ROLEMEMBER(N'cdc_reader', N'debezium'), 0) <> 1
BEGIN
    ALTER ROLE [cdc_reader] ADD MEMBER [debezium];
END
GO

GRANT VIEW DATABASE STATE TO [debezium];
GRANT VIEW DEFINITION TO [debezium];
GO

PRINT N'Debezium permissions configured successfully.';
GO
