SELECT
    table_name,
    actual_rows,
    expected_rows,
    actual_rows = expected_rows AS is_valid
FROM
(
    SELECT 'dim_customer' AS table_name, count() AS actual_rows, 91 AS expected_rows
    FROM northwind_dw.dim_customer

    UNION ALL
    SELECT 'dim_date', count(), 4017
    FROM northwind_dw.dim_date

    UNION ALL
    SELECT 'dim_employees', count(), 9
    FROM northwind_dw.dim_employees

    UNION ALL
    SELECT 'dim_geography', count(), 150
    FROM northwind_dw.dim_geography

    UNION ALL
    SELECT 'dim_products', count(), 81
    FROM northwind_dw.dim_products

    UNION ALL
    SELECT 'dim_shippers', count(), 3
    FROM northwind_dw.dim_shippers

    UNION ALL
    SELECT 'dim_suppliers', count(), 33
    FROM northwind_dw.dim_suppliers

    UNION ALL
    SELECT 'dim_territories', count(), 53
    FROM northwind_dw.dim_territories

    UNION ALL
    SELECT 'fact_employee_territories', count(), 49
    FROM northwind_dw.fact_employee_territories

    UNION ALL
    SELECT 'fact_orders', count(), 2154
    FROM northwind_dw.fact_orders
)
ORDER BY table_name;

SELECT throwIf(
    count() != uniqExact(tuple(order_id, product_key)),
    'Duplicate keys detected in fact_orders'
)
FROM northwind_dw.fact_orders;

SELECT throwIf(
    count() != uniqExact(tuple(employee_key, territory_key)),
    'Duplicate keys detected in fact_employee_territories'
)
FROM northwind_dw.fact_employee_territories;

SELECT throwIf(
    (
        countIf(p.product_key = 0)
        + countIf(isNotNull(f.customer_key) AND c.customer_key = 0)
        + countIf(isNotNull(f.employee_key) AND e.employee_key = 0)
        + countIf(isNotNull(f.shipper_key) AND s.shipper_key = 0)
        + countIf(isNotNull(f.geography_key) AND g.geography_key = 0)
        + countIf(isNotNull(f.order_date_key) AND od.date_key = 0)
        + countIf(isNotNull(f.required_date_key) AND rd.date_key = 0)
        + countIf(isNotNull(f.shipped_date_key) AND sd.date_key = 0)
    ) > 0,
    'Orphan dimension keys detected in fact_orders'
)
FROM northwind_dw.fact_orders AS f
LEFT JOIN northwind_dw.dim_products AS p
    ON f.product_key = p.product_key
LEFT JOIN northwind_dw.dim_customer AS c
    ON f.customer_key = c.customer_key
LEFT JOIN northwind_dw.dim_employees AS e
    ON f.employee_key = e.employee_key
LEFT JOIN northwind_dw.dim_shippers AS s
    ON f.shipper_key = s.shipper_key
LEFT JOIN northwind_dw.dim_geography AS g
    ON f.geography_key = g.geography_key
LEFT JOIN northwind_dw.dim_date AS od
    ON f.order_date_key = od.date_key
LEFT JOIN northwind_dw.dim_date AS rd
    ON f.required_date_key = rd.date_key
LEFT JOIN northwind_dw.dim_date AS sd
    ON f.shipped_date_key = sd.date_key;

SELECT
    'INITIAL LOAD VALIDATION PASSED' AS result,
    6640 AS expected_total_rows;
