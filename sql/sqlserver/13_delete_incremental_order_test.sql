USE Northwind;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

BEGIN TRANSACTION;

DELETE FROM dbo.[Order Details]
WHERE OrderID = 12000;

DELETE FROM dbo.Orders
WHERE OrderID = 12000;

COMMIT TRANSACTION;

SELECT
    (
        SELECT COUNT(*)
        FROM dbo.Orders
        WHERE OrderID = 12000
    ) AS remaining_orders,
    (
        SELECT COUNT(*)
        FROM dbo.[Order Details]
        WHERE OrderID = 12000
    ) AS remaining_order_details;
GO
