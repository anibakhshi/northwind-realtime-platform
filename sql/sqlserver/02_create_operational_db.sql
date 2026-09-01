SET NOCOUNT ON;
GO

IF DB_ID(N'Northwind') IS NULL
BEGIN
    CREATE DATABASE [Northwind];
    PRINT N'Northwind database created successfully.';
END
ELSE
BEGIN
    PRINT N'Northwind database already exists.';
END
GO

ALTER DATABASE [Northwind] SET RECOVERY FULL;
GO
