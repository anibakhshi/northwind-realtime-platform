USE [Northwind];
GO

SET NOCOUNT ON;
GO

IF EXISTS
(
    SELECT 1
    FROM dbo.Customers
    WHERE CustomerID = N'TST01'
)
BEGIN
    THROW 51000, 'Test customer TST01 already exists.', 1;
END
GO

INSERT INTO dbo.Customers
(
    CustomerID,
    CompanyName,
    ContactName,
    ContactTitle,
    City,
    Country
)
VALUES
(
    N'TST01',
    N'Northwind CDC Test',
    N'Initial Contact',
    N'Data Engineer',
    N'Tehran',
    N'Iran'
);
GO

WAITFOR DELAY '00:00:01';
GO

UPDATE dbo.Customers
SET
    ContactName = N'Updated Contact',
    City = N'Tabriz'
WHERE CustomerID = N'TST01';
GO

WAITFOR DELAY '00:00:01';
GO

DELETE FROM dbo.Customers
WHERE CustomerID = N'TST01';
GO

PRINT N'CDC smoke test completed successfully.';
GO
