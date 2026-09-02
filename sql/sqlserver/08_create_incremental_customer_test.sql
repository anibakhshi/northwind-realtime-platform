USE Northwind;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

BEGIN TRANSACTION;

IF EXISTS
(
    SELECT 1
    FROM dbo.Customers
    WHERE CustomerID = N'TST02'
)
BEGIN
    DELETE FROM dbo.Customers
    WHERE CustomerID = N'TST02';
END;

INSERT INTO dbo.Customers
(
    CustomerID,
    CompanyName,
    ContactName,
    ContactTitle,
    Address,
    City,
    Region,
    PostalCode,
    Country,
    Phone,
    Fax
)
VALUES
(
    N'TST02',
    N'Northwind Incremental Test',
    N'Initial Contact',
    N'Data Engineer',
    N'Initial Test Address',
    N'Tehran',
    NULL,
    N'12345',
    N'Iran',
    N'02100000000',
    NULL
);

UPDATE dbo.Customers
SET
    ContactName = N'Updated Contact',
    Address = N'Updated Test Address',
    City = N'Tabriz'
WHERE CustomerID = N'TST02';

COMMIT TRANSACTION;

SELECT
    CustomerID,
    CompanyName,
    ContactName,
    Address,
    City,
    Country
FROM dbo.Customers
WHERE CustomerID = N'TST02';
GO
