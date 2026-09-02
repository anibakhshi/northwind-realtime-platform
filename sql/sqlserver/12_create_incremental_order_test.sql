USE Northwind;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

BEGIN TRANSACTION;

IF EXISTS
(
    SELECT 1
    FROM dbo.[Order Details]
    WHERE OrderID = 12000
)
BEGIN
    DELETE FROM dbo.[Order Details]
    WHERE OrderID = 12000;
END;

IF EXISTS
(
    SELECT 1
    FROM dbo.Orders
    WHERE OrderID = 12000
)
BEGIN
    DELETE FROM dbo.Orders
    WHERE OrderID = 12000;
END;

SET IDENTITY_INSERT dbo.Orders ON;

INSERT INTO dbo.Orders
(
    OrderID,
    CustomerID,
    EmployeeID,
    OrderDate,
    RequiredDate,
    ShippedDate,
    ShipVia,
    Freight,
    ShipName,
    ShipAddress,
    ShipCity,
    ShipRegion,
    ShipPostalCode,
    ShipCountry
)
VALUES
(
    12000,
    N'ALFKI',
    1,
    '1998-05-01',
    '1998-05-15',
    NULL,
    1,
    15.5000,
    N'Northwind Incremental Order',
    N'Initial Shipping Address',
    N'Berlin',
    NULL,
    N'12209',
    N'Germany'
);

SET IDENTITY_INSERT dbo.Orders OFF;

INSERT INTO dbo.[Order Details]
(
    OrderID,
    ProductID,
    UnitPrice,
    Quantity,
    Discount
)
VALUES
(
    12000,
    1,
    18.0000,
    5,
    0.0500
);

UPDATE dbo.Orders
SET
    Freight = 17.7500,
    ShipAddress = N'Updated Shipping Address'
WHERE OrderID = 12000;

UPDATE dbo.[Order Details]
SET
    Quantity = 7,
    Discount = 0.1000
WHERE OrderID = 12000
  AND ProductID = 1;

COMMIT TRANSACTION;

SELECT
    orders.OrderID,
    orders.CustomerID,
    orders.Freight,
    orders.ShipAddress,
    details.ProductID,
    details.UnitPrice,
    details.Quantity,
    details.Discount
FROM dbo.Orders AS orders
INNER JOIN dbo.[Order Details] AS details
    ON orders.OrderID = details.OrderID
WHERE orders.OrderID = 12000;
GO
