CREATE OR REPLACE VIEW staging.v_customers_current AS
WITH latest_events AS
(
    SELECT DISTINCT ON
    (
        COALESCE(
            after_data ->> 'CustomerID',
            before_data ->> 'CustomerID'
        )
    )
        event_id,
        operation,
        source_timestamp,
        after_data
    FROM staging.cdc_events
    WHERE source_table = 'Customers'
    ORDER BY
        COALESCE(
            after_data ->> 'CustomerID',
            before_data ->> 'CustomerID'
        ),
        event_id DESC
)
SELECT
    event_id AS source_event_id,
    operation AS source_operation,
    source_timestamp,
    after_data ->> 'CustomerID' AS customer_id,
    after_data ->> 'CompanyName' AS company_name,
    after_data ->> 'ContactName' AS contact_name,
    after_data ->> 'ContactTitle' AS contact_title,
    after_data ->> 'Address' AS address,
    after_data ->> 'City' AS city,
    after_data ->> 'Region' AS region,
    after_data ->> 'PostalCode' AS postal_code,
    after_data ->> 'Country' AS country,
    after_data ->> 'Phone' AS phone,
    after_data ->> 'Fax' AS fax
FROM latest_events
WHERE after_data IS NOT NULL
  AND operation <> 'd';

CREATE OR REPLACE VIEW staging.v_products_current AS
WITH latest_events AS
(
    SELECT DISTINCT ON
    (
        COALESCE(
            after_data ->> 'ProductID',
            before_data ->> 'ProductID'
        )
    )
        event_id,
        operation,
        source_timestamp,
        after_data
    FROM staging.cdc_events
    WHERE source_table = 'Products'
    ORDER BY
        COALESCE(
            after_data ->> 'ProductID',
            before_data ->> 'ProductID'
        ),
        event_id DESC
)
SELECT
    event_id AS source_event_id,
    operation AS source_operation,
    source_timestamp,
    (after_data ->> 'ProductID')::integer AS product_id,
    after_data ->> 'ProductName' AS product_name,
    (after_data ->> 'SupplierID')::integer AS supplier_id,
    (after_data ->> 'CategoryID')::integer AS category_id,
    after_data ->> 'QuantityPerUnit' AS quantity_per_unit,
    (after_data ->> 'UnitPrice')::numeric(19, 4) AS unit_price,
    (after_data ->> 'UnitsInStock')::smallint AS units_in_stock,
    (after_data ->> 'UnitsOnOrder')::smallint AS units_on_order,
    (after_data ->> 'ReorderLevel')::smallint AS reorder_level,
    (after_data ->> 'Discontinued')::boolean AS discontinued
FROM latest_events
WHERE after_data IS NOT NULL
  AND operation <> 'd';

CREATE OR REPLACE VIEW staging.v_orders_current AS
WITH latest_events AS
(
    SELECT DISTINCT ON
    (
        COALESCE(
            after_data ->> 'OrderID',
            before_data ->> 'OrderID'
        )
    )
        event_id,
        operation,
        source_timestamp,
        after_data
    FROM staging.cdc_events
    WHERE source_table = 'Orders'
    ORDER BY
        COALESCE(
            after_data ->> 'OrderID',
            before_data ->> 'OrderID'
        ),
        event_id DESC
)
SELECT
    event_id AS source_event_id,
    operation AS source_operation,
    source_timestamp,
    (after_data ->> 'OrderID')::integer AS order_id,
    after_data ->> 'CustomerID' AS customer_id,
    (after_data ->> 'EmployeeID')::integer AS employee_id,
    CASE
        WHEN after_data ->> 'OrderDate' IS NOT NULL
        THEN to_timestamp(
            (after_data ->> 'OrderDate')::double precision / 1000.0
        )
    END AS order_date,
    CASE
        WHEN after_data ->> 'RequiredDate' IS NOT NULL
        THEN to_timestamp(
            (after_data ->> 'RequiredDate')::double precision / 1000.0
        )
    END AS required_date,
    CASE
        WHEN after_data ->> 'ShippedDate' IS NOT NULL
        THEN to_timestamp(
            (after_data ->> 'ShippedDate')::double precision / 1000.0
        )
    END AS shipped_date,
    (after_data ->> 'ShipVia')::integer AS ship_via,
    (after_data ->> 'Freight')::numeric(19, 4) AS freight,
    after_data ->> 'ShipName' AS ship_name,
    after_data ->> 'ShipAddress' AS ship_address,
    after_data ->> 'ShipCity' AS ship_city,
    after_data ->> 'ShipRegion' AS ship_region,
    after_data ->> 'ShipPostalCode' AS ship_postal_code,
    after_data ->> 'ShipCountry' AS ship_country
FROM latest_events
WHERE after_data IS NOT NULL
  AND operation <> 'd';

CREATE OR REPLACE VIEW staging.v_order_details_current AS
WITH latest_events AS
(
    SELECT DISTINCT ON
    (
        COALESCE(
            after_data ->> 'OrderID',
            before_data ->> 'OrderID'
        ),
        COALESCE(
            after_data ->> 'ProductID',
            before_data ->> 'ProductID'
        )
    )
        event_id,
        operation,
        source_timestamp,
        after_data
    FROM staging.cdc_events
    WHERE source_table = 'Order Details'
    ORDER BY
        COALESCE(
            after_data ->> 'OrderID',
            before_data ->> 'OrderID'
        ),
        COALESCE(
            after_data ->> 'ProductID',
            before_data ->> 'ProductID'
        ),
        event_id DESC
)
SELECT
    event_id AS source_event_id,
    operation AS source_operation,
    source_timestamp,
    (after_data ->> 'OrderID')::integer AS order_id,
    (after_data ->> 'ProductID')::integer AS product_id,
    (after_data ->> 'UnitPrice')::numeric(19, 4) AS unit_price,
    (after_data ->> 'Quantity')::smallint AS quantity,
    (after_data ->> 'Discount')::real AS discount
FROM latest_events
WHERE after_data IS NOT NULL
  AND operation <> 'd';
