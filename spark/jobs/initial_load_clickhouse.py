import base64
import os
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass

from pyspark.sql import SparkSession


@dataclass(frozen=True)
class TableLoad:
    source_query: str
    target_table: str
    expected_rows: int


TABLE_LOADS = [
    TableLoad(
        """
        SELECT
            DateKey AS date_key,
            FullDateAlternateKey AS full_date,
            CalendarYear AS calendar_year,
            CalendarSeason AS calendar_season,
            SeasonName AS season_name,
            MonthNumberOfYear AS month_number,
            MonthName AS month_name,
            DayNumberOfMonth AS day_number,
            DayOfWeek AS day_of_week,
            DayOfWeekName AS day_of_week_name
        FROM dbo.DimDate
        """,
        "dim_date",
        4017,
    ),
    TableLoad(
        """
        SELECT
            GeographyKey AS geography_key,
            Country AS country,
            Region AS region,
            City AS city,
            PostalCode AS postal_code,
            Address AS address,
            SYSUTCDATETIME() AS updated_at,
            CAST(0 AS tinyint) AS is_deleted
        FROM dbo.DimGeography
        """,
        "dim_geography",
        150,
    ),
    TableLoad(
        """
        SELECT
            CustomerKey AS customer_key,
            CustomerAlternateKey AS customer_alternate_key,
            GeographyKey AS geography_key,
            CompanyName AS company_name,
            ContactName AS contact_name,
            ContactTitle AS contact_title,
            Phone AS phone,
            Fax AS fax,
            Startdate AS start_date,
            Enddate AS end_date,
            SYSUTCDATETIME() AS updated_at,
            CAST(0 AS tinyint) AS is_deleted
        FROM dbo.DimCustomer
        """,
        "dim_customer",
        91,
    ),
    TableLoad(
        """
        SELECT
            EmployeeKey AS employee_key,
            ParentEmployeeKey AS parent_employee_key,
            EmployeeAlternateKey AS employee_alternate_key,
            ReportsTo AS reports_to,
            GeographyKey AS geography_key,
            FirstName AS first_name,
            LastName AS last_name,
            Title AS title,
            TitleOfCourtesy AS title_of_courtesy,
            BirthDate AS birth_date,
            HireDate AS hire_date,
            HomePhone AS home_phone,
            Extension AS extension,
            Notes AS notes,
            PhotoPath AS photo_path,
            Startdate AS start_date,
            Enddate AS end_date,
            SYSUTCDATETIME() AS updated_at,
            CAST(0 AS tinyint) AS is_deleted
        FROM dbo.DimEmployees
        """,
        "dim_employees",
        9,
    ),
    TableLoad(
        """
        SELECT
            SupplierKey AS supplier_key,
            SupplierAlternateKey AS supplier_alternate_key,
            GeographyKey AS geography_key,
            CompanyName AS company_name,
            ContactName AS contact_name,
            ContactTitle AS contact_title,
            Phone AS phone,
            Fax AS fax,
            HomePage AS home_page,
            Startdate AS start_date,
            Enddate AS end_date,
            SYSUTCDATETIME() AS updated_at,
            CAST(0 AS tinyint) AS is_deleted
        FROM dbo.DimSuppliers
        """,
        "dim_suppliers",
        33,
    ),
    TableLoad(
        """
        SELECT
            ProductKey AS product_key,
            ProductAlternateKey AS product_alternate_key,
            SupplierKey AS supplier_key,
            ProductName AS product_name,
            CategoryName AS category_name,
            QuantityPerUnit AS quantity_per_unit,
            UnitPrice AS unit_price,
            UnitsInStock AS units_in_stock,
            UnitsOnOrder AS units_on_order,
            ReorderLevel AS reorder_level,
            CAST(Discontinued AS tinyint) AS discontinued,
            Startdate AS start_date,
            Enddate AS end_date,
            SYSUTCDATETIME() AS updated_at,
            CAST(0 AS tinyint) AS is_deleted
        FROM dbo.DimProducts
        """,
        "dim_products",
        81,
    ),
    TableLoad(
        """
        SELECT
            ShipperKey AS shipper_key,
            ShipperAlternateKey AS shipper_alternate_key,
            CompanyName AS company_name,
            Phone AS phone,
            SYSUTCDATETIME() AS updated_at,
            CAST(0 AS tinyint) AS is_deleted
        FROM dbo.DimShippers
        """,
        "dim_shippers",
        3,
    ),
    TableLoad(
        """
        SELECT
            TerritoryKey AS territory_key,
            TerritoryAlternateKey AS territory_alternate_key,
            RegionDescription AS region_description,
            TerritoryDescription AS territory_description,
            Startdate AS start_date,
            Enddate AS end_date,
            SYSUTCDATETIME() AS updated_at,
            CAST(0 AS tinyint) AS is_deleted
        FROM dbo.DimTerritories
        """,
        "dim_territories",
        53,
    ),
    TableLoad(
        """
        SELECT
            EmployeeKey AS employee_key,
            TerritoryKey AS territory_key,
            SYSUTCDATETIME() AS updated_at,
            CAST(0 AS tinyint) AS is_deleted
        FROM dbo.FactEmployeeTerritories
        """,
        "fact_employee_territories",
        49,
    ),
    TableLoad(
        """
        SELECT
            OrderID AS order_id,
            GeographyKey AS geography_key,
            ProductKey AS product_key,
            CustomerKey AS customer_key,
            EmployeeKey AS employee_key,
            ShipperKey AS shipper_key,
            OrderdateKey AS order_date_key,
            RequiredDateKey AS required_date_key,
            ShippedDateKey AS shipped_date_key,
            Freight AS freight,
            ShipName AS ship_name,
            UnitPrice AS unit_price,
            Quantity AS quantity,
            Discount AS discount,
            OrderDate AS order_date,
            ShippedDate AS shipped_date,
            RequiredDate AS required_date,
            SYSUTCDATETIME() AS updated_at,
            CAST(0 AS tinyint) AS is_deleted
        FROM dbo.FactOrders
        """,
        "fact_orders",
        2154,
    ),
]


def required_environment(name):
    value = os.environ.get(name)

    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")

    return value


def clickhouse_request(query, body=None):
    host = required_environment("CLICKHOUSE_HOST")
    port = required_environment("CLICKHOUSE_PORT")
    user = required_environment("CLICKHOUSE_USER")
    password = required_environment("CLICKHOUSE_PASSWORD")

    parameters = {
        "query": query,
        "date_time_input_format": "best_effort",
        "input_format_defaults_for_omitted_fields": "1",
    }

    url = f"http://{host}:{port}/?{urllib.parse.urlencode(parameters)}"
    credentials = base64.b64encode(
        f"{user}:{password}".encode("utf-8")
    ).decode("ascii")

    request = urllib.request.Request(
        url=url,
        data=body.encode("utf-8") if body is not None else None,
        method="POST",
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/json",
        },
    )

    with urllib.request.urlopen(request, timeout=120) as response:
        return response.read().decode("utf-8").strip()


def read_source_table(spark, table_load):
    host = required_environment("SQLSERVER_HOST")
    port = required_environment("SQLSERVER_PORT")
    database = required_environment("SQLSERVER_DATABASE")
    user = required_environment("SQLSERVER_USER")
    password = required_environment("SQLSERVER_PASSWORD")

    jdbc_url = (
        f"jdbc:sqlserver://{host}:{port};"
        f"databaseName={database};"
        "encrypt=true;"
        "trustServerCertificate=true;"
    )

    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("user", user)
        .option("password", password)
        .option("dbtable", f"({table_load.source_query}) AS source_data")
        .option("fetchsize", "1000")
        .load()
    )


def load_table(spark, database, table_load):
    dataframe = read_source_table(spark, table_load)
    source_count = dataframe.count()

    if source_count != table_load.expected_rows:
        raise RuntimeError(
            f"{table_load.target_table}: expected "
            f"{table_load.expected_rows} source rows, found {source_count}"
        )

    json_rows = dataframe.toJSON().collect()
    payload = "\n".join(json_rows)

    clickhouse_request(
        f"TRUNCATE TABLE {database}.{table_load.target_table}"
    )

    if payload:
        clickhouse_request(
            f"INSERT INTO {database}.{table_load.target_table} "
            "FORMAT JSONEachRow",
            payload,
        )

    target_count_text = clickhouse_request(
        f"SELECT count() FROM {database}.{table_load.target_table}"
    )
    target_count = int(target_count_text)

    if target_count != source_count:
        raise RuntimeError(
            f"{table_load.target_table}: source={source_count}, "
            f"target={target_count}"
        )

    print(
        f"SUCCESS table={table_load.target_table} "
        f"source={source_count} target={target_count}",
        flush=True,
    )


def main():
    database = required_environment("CLICKHOUSE_DATABASE")

    spark = (
        SparkSession.builder
        .appName("NorthwindInitialLoadToClickHouse")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        for table_load in TABLE_LOADS:
            load_table(spark, database, table_load)

        print(
            f"INITIAL LOAD COMPLETED: {len(TABLE_LOADS)} tables",
            flush=True,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"INITIAL LOAD FAILED: {error}", file=sys.stderr, flush=True)
        raise
