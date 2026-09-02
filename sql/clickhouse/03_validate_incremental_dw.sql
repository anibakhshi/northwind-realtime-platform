SELECT throwIf(
    count() != uniqExact(tuple(order_id, product_key)),
    'Duplicate active keys detected in fact_orders'
)
FROM
(
    SELECT order_id, product_key
    FROM northwind_dw.fact_orders FINAL
    WHERE is_deleted = 0
);

SELECT throwIf(
    count() != uniqExact(customer_alternate_key),
    'Duplicate active alternate keys detected in dim_customer'
)
FROM
(
    SELECT customer_alternate_key
    FROM northwind_dw.dim_customer FINAL
    WHERE is_deleted = 0
);

SELECT throwIf(
    count() != uniqExact(product_alternate_key),
    'Duplicate active alternate keys detected in dim_products'
)
FROM
(
    SELECT product_alternate_key
    FROM northwind_dw.dim_products FINAL
    WHERE is_deleted = 0
      AND end_date IS NULL
);

SELECT throwIf(
    (
        countIf(products.product_key = 0)
        + countIf(
            isNotNull(facts.customer_key)
            AND customers.customer_key = 0
        )
        + countIf(
            isNotNull(facts.employee_key)
            AND employees.employee_key = 0
        )
        + countIf(
            isNotNull(facts.shipper_key)
            AND shippers.shipper_key = 0
        )
        + countIf(
            isNotNull(facts.geography_key)
            AND geographies.geography_key = 0
        )
    ) > 0,
    'Orphan dimension keys detected in active fact_orders'
)
FROM
(
    SELECT *
    FROM northwind_dw.fact_orders FINAL
    WHERE is_deleted = 0
) AS facts
LEFT JOIN
(
    SELECT * FROM northwind_dw.dim_products FINAL
) AS products
    ON facts.product_key = products.product_key
LEFT JOIN
(
    SELECT * FROM northwind_dw.dim_customer FINAL
) AS customers
    ON facts.customer_key = customers.customer_key
LEFT JOIN
(
    SELECT * FROM northwind_dw.dim_employees FINAL
) AS employees
    ON facts.employee_key = employees.employee_key
LEFT JOIN
(
    SELECT * FROM northwind_dw.dim_shippers FINAL
) AS shippers
    ON facts.shipper_key = shippers.shipper_key
LEFT JOIN
(
    SELECT * FROM northwind_dw.dim_geography FINAL
) AS geographies
    ON facts.geography_key = geographies.geography_key;

SELECT
    'INCREMENTAL DW VALIDATION PASSED' AS result,
    countIf(is_deleted = 0) AS active_fact_rows,
    countIf(is_deleted = 1) AS deleted_fact_rows
FROM northwind_dw.fact_orders FINAL;
