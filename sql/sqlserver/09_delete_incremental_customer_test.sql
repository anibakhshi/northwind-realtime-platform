USE Northwind;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

DELETE FROM dbo.Customers
WHERE CustomerID = N'TST02';

SELECT COUNT(*) AS remaining_test_rows
FROM dbo.Customers
WHERE CustomerID = N'TST02';
GO
