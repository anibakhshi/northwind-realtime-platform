CREATE DATABASE IF NOT EXISTS northwind_dw;

CREATE TABLE IF NOT EXISTS northwind_dw.dim_date
(
    date_key UInt32,
    full_date Date,
    calendar_year UInt16,
    calendar_season UInt8,
    season_name String,
    month_number UInt8,
    month_name String,
    day_number UInt8,
    day_of_week UInt8,
    day_of_week_name String
)
ENGINE = MergeTree
ORDER BY date_key;

CREATE TABLE IF NOT EXISTS northwind_dw.dim_geography
(
    geography_key UInt64,
    country Nullable(String),
    region Nullable(String),
    city Nullable(String),
    postal_code Nullable(String),
    address Nullable(String),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3),
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY geography_key;

CREATE TABLE IF NOT EXISTS northwind_dw.dim_customer
(
    customer_key UInt64,
    customer_alternate_key String,
    geography_key Nullable(UInt64),
    company_name Nullable(String),
    contact_name Nullable(String),
    contact_title Nullable(String),
    phone Nullable(String),
    fax Nullable(String),
    start_date DateTime64(3, 'UTC'),
    end_date Nullable(DateTime64(3, 'UTC')),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3),
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY customer_key;

CREATE TABLE IF NOT EXISTS northwind_dw.dim_employees
(
    employee_key UInt64,
    parent_employee_key Nullable(UInt64),
    employee_alternate_key Nullable(UInt32),
    reports_to Nullable(UInt32),
    geography_key Nullable(UInt64),
    first_name Nullable(String),
    last_name Nullable(String),
    title Nullable(String),
    title_of_courtesy Nullable(String),
    birth_date Nullable(DateTime64(3, 'UTC')),
    hire_date Nullable(DateTime64(3, 'UTC')),
    home_phone Nullable(String),
    extension Nullable(String),
    notes Nullable(String),
    photo_path Nullable(String),
    start_date DateTime64(3, 'UTC'),
    end_date Nullable(DateTime64(3, 'UTC')),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3),
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY employee_key;

CREATE TABLE IF NOT EXISTS northwind_dw.dim_suppliers
(
    supplier_key UInt64,
    supplier_alternate_key Nullable(UInt32),
    geography_key Nullable(UInt64),
    company_name Nullable(String),
    contact_name Nullable(String),
    contact_title Nullable(String),
    phone Nullable(String),
    fax Nullable(String),
    home_page Nullable(String),
    start_date Nullable(DateTime64(3, 'UTC')),
    end_date Nullable(DateTime64(3, 'UTC')),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3),
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY supplier_key;

CREATE TABLE IF NOT EXISTS northwind_dw.dim_products
(
    product_key UInt64,
    product_alternate_key UInt32,
    supplier_key Nullable(UInt64),
    product_name Nullable(String),
    category_name Nullable(String),
    quantity_per_unit Nullable(String),
    unit_price Nullable(Decimal(19, 4)),
    units_in_stock Nullable(Int16),
    units_on_order Nullable(Int16),
    reorder_level Nullable(Int16),
    discontinued Nullable(UInt8),
    start_date DateTime64(3, 'UTC'),
    end_date Nullable(DateTime64(3, 'UTC')),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3),
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY product_key;

CREATE TABLE IF NOT EXISTS northwind_dw.dim_shippers
(
    shipper_key UInt64,
    shipper_alternate_key Nullable(UInt32),
    company_name Nullable(String),
    phone Nullable(String),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3),
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY shipper_key;

CREATE TABLE IF NOT EXISTS northwind_dw.dim_territories
(
    territory_key UInt64,
    territory_alternate_key Nullable(String),
    region_description Nullable(String),
    territory_description Nullable(String),
    start_date DateTime64(3, 'UTC'),
    end_date Nullable(DateTime64(3, 'UTC')),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3),
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY territory_key;

CREATE TABLE IF NOT EXISTS northwind_dw.fact_employee_territories
(
    employee_key UInt64,
    territory_key UInt64,
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3),
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (employee_key, territory_key);

CREATE TABLE IF NOT EXISTS northwind_dw.fact_orders
(
    order_id UInt32,
    geography_key Nullable(UInt64),
    product_key UInt64,
    customer_key Nullable(UInt64),
    employee_key Nullable(UInt64),
    shipper_key Nullable(UInt64),
    order_date_key Nullable(UInt32),
    required_date_key Nullable(UInt32),
    shipped_date_key Nullable(UInt32),
    freight Nullable(Decimal(19, 4)),
    ship_name Nullable(String),
    unit_price Nullable(Decimal(19, 4)),
    quantity Nullable(UInt16),
    discount Nullable(Float32),
    order_date Nullable(DateTime64(3, 'UTC')),
    shipped_date Nullable(DateTime64(3, 'UTC')),
    required_date Nullable(DateTime64(3, 'UTC')),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3),
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(ifNull(order_date, toDateTime64('1970-01-01 00:00:00', 3, 'UTC')))
ORDER BY (order_id, product_key);
