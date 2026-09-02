USE Northwind;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

DELETE FROM dbo.Products
WHERE ProductID = 9999;

SELECT COUNT(*) AS remaining_test_rows
FROM dbo.Products
WHERE ProductID = 9999;
GO
