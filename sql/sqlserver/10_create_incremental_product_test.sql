USE Northwind;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

BEGIN TRANSACTION;

IF EXISTS
(
    SELECT 1
    FROM dbo.Products
    WHERE ProductID = 9999
)
BEGIN
    DELETE FROM dbo.Products
    WHERE ProductID = 9999;
END;

SET IDENTITY_INSERT dbo.Products ON;

INSERT INTO dbo.Products
(
    ProductID,
    ProductName,
    SupplierID,
    CategoryID,
    QuantityPerUnit,
    UnitPrice,
    UnitsInStock,
    UnitsOnOrder,
    ReorderLevel,
    Discontinued
)
VALUES
(
    9999,
    N'Northwind Incremental Product',
    1,
    1,
    N'10 test boxes',
    25.5000,
    20,
    0,
    5,
    0
);

SET IDENTITY_INSERT dbo.Products OFF;

UPDATE dbo.Products
SET
    ProductName = N'Northwind Updated Product',
    UnitPrice = 27.7500,
    UnitsInStock = 25
WHERE ProductID = 9999;

COMMIT TRANSACTION;

SELECT
    ProductID,
    ProductName,
    SupplierID,
    CategoryID,
    UnitPrice,
    UnitsInStock
FROM dbo.Products
WHERE ProductID = 9999;
GO
