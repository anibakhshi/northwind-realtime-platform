import base64
import hashlib
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal

import psycopg2
from psycopg2.extras import RealDictCursor


PIPELINE_NAME = "postgres_to_clickhouse_incremental"
SUPPORTED_TABLES = {"Customers", "Products"}
DEFAULT_BATCH_SIZE = 1000

NORTHWIND_CATEGORIES = {
    1: "Beverages",
    2: "Condiments",
    3: "Confections",
    4: "Dairy Products",
    5: "Grains/Cereals",
    6: "Meat/Poultry",
    7: "Produce",
    8: "Seafood",
}


def required_environment(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def postgres_connection():
    return psycopg2.connect(
        host=required_environment("POSTGRES_HOST"),
        port=required_environment("POSTGRES_PORT"),
        dbname=required_environment("POSTGRES_DB"),
        user=required_environment("POSTGRES_USER"),
        password=required_environment("POSTGRES_PASSWORD"),
    )


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

    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return response.read().decode("utf-8").strip()
    except urllib.error.HTTPError as error:
        error_body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"ClickHouse request failed: {error.code} {error_body}"
        ) from error


def clickhouse_select(query):
    response = clickhouse_request(f"{query} FORMAT JSONEachRow")
    if not response:
        return []
    return [
        json.loads(line)
        for line in response.splitlines()
        if line.strip()
    ]


def json_default(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return utc_timestamp(value)
    raise TypeError(f"Unsupported JSON value: {type(value).__name__}")


def clickhouse_insert(database, table, rows):
    if not rows:
        return 0
    payload = "\n".join(
        json.dumps(
            row,
            ensure_ascii=False,
            separators=(",", ":"),
            default=json_default,
        )
        for row in rows
    )
    clickhouse_request(
        f"INSERT INTO {database}.{table} FORMAT JSONEachRow",
        payload,
    )
    return len(rows)


def stable_uint64(namespace, value):
    source = f"{namespace}:{value}".encode("utf-8")
    digest = hashlib.blake2b(source, digest_size=8).digest()
    generated_value = int.from_bytes(digest, byteorder="big")
    return generated_value | (1 << 63)


def normalized_geography_key(row):
    values = [
        row.get("country"),
        row.get("region"),
        row.get("city"),
        row.get("postal_code"),
        row.get("address"),
    ]
    return tuple(
        str(value).strip().casefold() if value is not None else ""
        for value in values
    )


def utc_timestamp(value=None):
    timestamp = value or datetime.now(timezone.utc)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc).isoformat(
        timespec="milliseconds"
    )


def fetch_watermark(cursor):
    cursor.execute(
        """
        SELECT last_event_id
        FROM staging.pipeline_watermarks
        WHERE pipeline_name = %s
        FOR UPDATE
        """,
        (PIPELINE_NAME,),
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError(
            f"Pipeline watermark does not exist: {PIPELINE_NAME}"
        )
    return row["last_event_id"]


def fetch_pending_events(cursor, watermark, batch_size):
    cursor.execute(
        """
        SELECT
            event_id,
            source_table,
            operation,
            source_timestamp,
            before_data,
            after_data
        FROM staging.cdc_events
        WHERE event_id > %s
          AND processed = false
        ORDER BY event_id
        LIMIT %s
        """,
        (watermark, batch_size),
    )
    return cursor.fetchall()


def identifiers_from_events(events, table_name, field_name, converter=str):
    identifiers = set()
    for event in events:
        if event["source_table"] != table_name:
            continue
        for data_name in ("before_data", "after_data"):
            data = event.get(data_name)
            if data and data.get(field_name) is not None:
                identifiers.add(converter(data[field_name]))
    return sorted(identifiers)


def fetch_current_customers(cursor, customer_ids):
    if not customer_ids:
        return {}
    cursor.execute(
        """
        SELECT
            source_event_id,
            source_operation,
            source_timestamp,
            customer_id,
            company_name,
            contact_name,
            contact_title,
            address,
            city,
            region,
            postal_code,
            country,
            phone,
            fax
        FROM staging.v_customers_current
        WHERE customer_id = ANY(%s)
        """,
        (customer_ids,),
    )
    return {row["customer_id"]: row for row in cursor.fetchall()}


def fetch_current_products(cursor, product_ids):
    if not product_ids:
        return {}
    cursor.execute(
        """
        SELECT
            source_event_id,
            source_operation,
            source_timestamp,
            product_id,
            product_name,
            supplier_id,
            category_id,
            quantity_per_unit,
            unit_price,
            units_in_stock,
            units_on_order,
            reorder_level,
            discontinued
        FROM staging.v_products_current
        WHERE product_id = ANY(%s)
        """,
        (product_ids,),
    )
    return {row["product_id"]: row for row in cursor.fetchall()}


def fetch_clickhouse_customers(database):
    rows = clickhouse_select(
        f"""
        SELECT
            customer_key,
            customer_alternate_key,
            geography_key,
            company_name,
            contact_name,
            contact_title,
            phone,
            fax,
            start_date,
            end_date,
            updated_at,
            is_deleted
        FROM {database}.dim_customer FINAL
        ORDER BY customer_alternate_key, updated_at
        """
    )
    result = {}
    for row in rows:
        result[row["customer_alternate_key"]] = row
    return result


def fetch_clickhouse_geographies(database):
    rows = clickhouse_select(
        f"""
        SELECT
            geography_key,
            country,
            region,
            city,
            postal_code,
            address,
            updated_at,
            is_deleted
        FROM {database}.dim_geography FINAL
        WHERE is_deleted = 0
        """
    )
    return {normalized_geography_key(row): row for row in rows}


def fetch_clickhouse_products(database):
    rows = clickhouse_select(
        f"""
        SELECT
            product_key,
            product_alternate_key,
            supplier_key,
            product_name,
            category_name,
            quantity_per_unit,
            unit_price,
            units_in_stock,
            units_on_order,
            reorder_level,
            discontinued,
            start_date,
            end_date,
            updated_at,
            is_deleted
        FROM {database}.dim_products FINAL
        ORDER BY product_alternate_key, updated_at
        """
    )
    result = {}
    for row in rows:
        alternate_key = int(row["product_alternate_key"])
        existing = result.get(alternate_key)
        if existing is None:
            result[alternate_key] = row
        elif existing.get("end_date") is not None and row.get("end_date") is None:
            result[alternate_key] = row
        elif row["updated_at"] >= existing["updated_at"]:
            result[alternate_key] = row
    return result


def fetch_clickhouse_suppliers(database):
    rows = clickhouse_select(
        f"""
        SELECT
            supplier_key,
            supplier_alternate_key,
            updated_at,
            is_deleted
        FROM {database}.dim_suppliers FINAL
        WHERE supplier_alternate_key IS NOT NULL
        ORDER BY supplier_alternate_key, updated_at
        """
    )
    result = {}
    for row in rows:
        result[int(row["supplier_alternate_key"])] = row["supplier_key"]
    return result


def build_customer_changes(
    customer_ids,
    current_customers,
    existing_customers,
    existing_geographies,
):
    geography_rows = []
    customer_rows = []

    for customer_id in customer_ids:
        current = current_customers.get(customer_id)
        existing = existing_customers.get(customer_id)

        if current is None:
            if existing is None:
                continue
            deleted_at = utc_timestamp()
            deleted_row = dict(existing)
            deleted_row["updated_at"] = deleted_at
            deleted_row["end_date"] = deleted_at
            deleted_row["is_deleted"] = 1
            customer_rows.append(deleted_row)
            continue

        event_timestamp = utc_timestamp(current.get("source_timestamp"))
        geography_source = {
            "country": current.get("country"),
            "region": current.get("region"),
            "city": current.get("city"),
            "postal_code": current.get("postal_code"),
            "address": current.get("address"),
        }
        geography_identity = normalized_geography_key(geography_source)
        existing_geography = existing_geographies.get(geography_identity)

        if existing_geography:
            geography_key = existing_geography["geography_key"]
        else:
            geography_key = stable_uint64(
                "geography", "|".join(geography_identity)
            )
            geography_row = {
                "geography_key": geography_key,
                **geography_source,
                "updated_at": event_timestamp,
                "is_deleted": 0,
            }
            geography_rows.append(geography_row)
            existing_geographies[geography_identity] = geography_row

        if existing:
            customer_key = existing["customer_key"]
            start_date = existing["start_date"]
        else:
            customer_key = stable_uint64("customer", customer_id)
            start_date = event_timestamp

        customer_rows.append(
            {
                "customer_key": customer_key,
                "customer_alternate_key": customer_id,
                "geography_key": geography_key,
                "company_name": current.get("company_name"),
                "contact_name": current.get("contact_name"),
                "contact_title": current.get("contact_title"),
                "phone": current.get("phone"),
                "fax": current.get("fax"),
                "start_date": start_date,
                "end_date": None,
                "updated_at": event_timestamp,
                "is_deleted": 0,
            }
        )

    return geography_rows, customer_rows


def build_product_changes(
    product_ids,
    current_products,
    existing_products,
    supplier_keys,
):
    product_rows = []

    for product_id in product_ids:
        current = current_products.get(product_id)
        existing = existing_products.get(product_id)

        if current is None:
            if existing is None:
                continue
            deleted_at = utc_timestamp()
            deleted_row = dict(existing)
            deleted_row["updated_at"] = deleted_at
            deleted_row["end_date"] = deleted_at
            deleted_row["is_deleted"] = 1
            product_rows.append(deleted_row)
            continue

        event_timestamp = utc_timestamp(current.get("source_timestamp"))
        supplier_id = current.get("supplier_id")
        supplier_key = supplier_keys.get(supplier_id)

        if supplier_id is not None and supplier_key is None:
            raise RuntimeError(
                f"Missing supplier dimension for SupplierID={supplier_id}"
            )

        if existing:
            product_key = existing["product_key"]
            start_date = existing["start_date"]
            fallback_category = existing.get("category_name")
        else:
            product_key = stable_uint64("product", product_id)
            start_date = event_timestamp
            fallback_category = None

        category_id = current.get("category_id")
        category_name = NORTHWIND_CATEGORIES.get(
            category_id, fallback_category
        )

        product_rows.append(
            {
                "product_key": product_key,
                "product_alternate_key": product_id,
                "supplier_key": supplier_key,
                "product_name": current.get("product_name"),
                "category_name": category_name,
                "quantity_per_unit": current.get("quantity_per_unit"),
                "unit_price": current.get("unit_price"),
                "units_in_stock": current.get("units_in_stock"),
                "units_on_order": current.get("units_on_order"),
                "reorder_level": current.get("reorder_level"),
                "discontinued": (
                    int(current["discontinued"])
                    if current.get("discontinued") is not None
                    else None
                ),
                "start_date": start_date,
                "end_date": None,
                "updated_at": event_timestamp,
                "is_deleted": 0,
            }
        )

    return product_rows


def complete_batch(cursor, old_watermark, new_watermark):
    cursor.execute(
        """
        UPDATE staging.cdc_events
        SET
            processed = true,
            processed_at = CURRENT_TIMESTAMP
        WHERE event_id > %s
          AND event_id <= %s
          AND processed = false
        """,
        (old_watermark, new_watermark),
    )
    processed_count = cursor.rowcount
    cursor.execute(
        """
        UPDATE staging.pipeline_watermarks
        SET
            last_event_id = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE pipeline_name = %s
        """,
        (new_watermark, PIPELINE_NAME),
    )
    if cursor.rowcount != 1:
        raise RuntimeError("Pipeline watermark update failed")
    return processed_count


def main():
    database = required_environment("CLICKHOUSE_DATABASE")
    batch_size = int(
        os.environ.get("INCREMENTAL_BATCH_SIZE", DEFAULT_BATCH_SIZE)
    )
    connection = postgres_connection()
    connection.autocommit = False

    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            watermark = fetch_watermark(cursor)
            events = fetch_pending_events(cursor, watermark, batch_size)

            if not events:
                connection.rollback()
                print(f"NO PENDING EVENTS watermark={watermark}", flush=True)
                return

            unsupported_tables = sorted(
                {event["source_table"] for event in events}
                - SUPPORTED_TABLES
            )
            if unsupported_tables:
                raise RuntimeError(
                    "Unsupported pending source tables: "
                    + ", ".join(unsupported_tables)
                )

            customer_ids = identifiers_from_events(
                events, "Customers", "CustomerID", str
            )
            product_ids = identifiers_from_events(
                events, "Products", "ProductID", int
            )

            current_customers = fetch_current_customers(
                cursor, customer_ids
            )
            current_products = fetch_current_products(cursor, product_ids)
            existing_customers = fetch_clickhouse_customers(database)
            existing_geographies = fetch_clickhouse_geographies(database)
            existing_products = fetch_clickhouse_products(database)
            supplier_keys = fetch_clickhouse_suppliers(database)

            geography_rows, customer_rows = build_customer_changes(
                customer_ids,
                current_customers,
                existing_customers,
                existing_geographies,
            )
            product_rows = build_product_changes(
                product_ids,
                current_products,
                existing_products,
                supplier_keys,
            )

            geographies_written = clickhouse_insert(
                database, "dim_geography", geography_rows
            )
            customers_written = clickhouse_insert(
                database, "dim_customer", customer_rows
            )
            products_written = clickhouse_insert(
                database, "dim_products", product_rows
            )

            new_watermark = max(event["event_id"] for event in events)
            processed_count = complete_batch(
                cursor, watermark, new_watermark
            )
            connection.commit()

            print(
                "INCREMENTAL LOAD COMPLETED "
                f"events={len(events)} "
                f"processed={processed_count} "
                f"geographies={geographies_written} "
                f"customers={customers_written} "
                f"products={products_written} "
                f"watermark={new_watermark}",
                flush=True,
            )
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(
            f"INCREMENTAL LOAD FAILED: {error}",
            file=sys.stderr,
            flush=True,
        )
        raise
