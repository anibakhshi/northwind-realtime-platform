USE [Northwind];
GO

SET NOCOUNT ON;
GO

IF EXISTS
(
    SELECT 1
    FROM sys.databases
    WHERE name = N'Northwind'
      AND is_cdc_enabled = 0
)
BEGIN
    EXEC sys.sp_cdc_enable_db;
    PRINT N'CDC enabled for Northwind database.';
END
ELSE
BEGIN
    PRINT N'CDC is already enabled for Northwind database.';
END
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.tables
    WHERE object_id = OBJECT_ID(N'dbo.Customers')
      AND is_tracked_by_cdc = 1
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'Customers',
        @role_name = N'cdc_reader',
        @capture_instance = N'dbo_Customers',
        @supports_net_changes = 0;

    PRINT N'CDC enabled for dbo.Customers.';
END
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.tables
    WHERE object_id = OBJECT_ID(N'dbo.Products')
      AND is_tracked_by_cdc = 1
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'Products',
        @role_name = N'cdc_reader',
        @capture_instance = N'dbo_Products',
        @supports_net_changes = 0;

    PRINT N'CDC enabled for dbo.Products.';
END
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.tables
    WHERE object_id = OBJECT_ID(N'dbo.Orders')
      AND is_tracked_by_cdc = 1
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'Orders',
        @role_name = N'cdc_reader',
        @capture_instance = N'dbo_Orders',
        @supports_net_changes = 0;

    PRINT N'CDC enabled for dbo.Orders.';
END
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.tables
    WHERE object_id = OBJECT_ID(N'dbo.[Order Details]')
      AND is_tracked_by_cdc = 1
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'Order Details',
        @role_name = N'cdc_reader',
        @capture_instance = N'dbo_Order_Details',
        @supports_net_changes = 0;

    PRINT N'CDC enabled for dbo.Order Details.';
END
GO
