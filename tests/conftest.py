import pytest
import duckdb
from ecom_pipeline import simulator
import pandas as pd

SCHEMA_SQL = """
CREATE TABLE distribution_centers (
    id BIGINT, name VARCHAR, latitude DOUBLE, longitude DOUBLE,
    distribution_center_geom VARCHAR);

CREATE TABLE products (
    id BIGINT, cost DOUBLE, category VARCHAR, name VARCHAR, brand VARCHAR,
    retail_price DOUBLE, department VARCHAR, sku VARCHAR, distribution_center_id BIGINT);

CREATE TABLE inventory_items (
    id BIGINT, product_id BIGINT, created_at TIMESTAMPTZ, sold_at TIMESTAMPTZ,
    cost DOUBLE, product_category VARCHAR, product_name VARCHAR, product_brand VARCHAR,
    product_retail_price DOUBLE, product_department VARCHAR, product_sku VARCHAR,
    product_distribution_center_id BIGINT);

CREATE TABLE users (
    id BIGINT, first_name VARCHAR, last_name VARCHAR, email VARCHAR, age BIGINT,
    gender VARCHAR, state VARCHAR, street_address VARCHAR, postal_code VARCHAR,
    city VARCHAR, country VARCHAR, latitude DOUBLE, longitude DOUBLE,
    traffic_source VARCHAR, created_at TIMESTAMPTZ, user_geom VARCHAR);

CREATE TABLE orders (
    order_id BIGINT, user_id BIGINT, status VARCHAR, gender VARCHAR,
    created_at TIMESTAMPTZ, returned_at TIMESTAMPTZ, shipped_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ, num_of_item BIGINT);

CREATE TABLE order_items (
    id BIGINT, order_id BIGINT, user_id BIGINT, product_id BIGINT,
    inventory_item_id BIGINT, status VARCHAR, created_at TIMESTAMPTZ,
    shipped_at TIMESTAMPTZ, delivered_at TIMESTAMPTZ, returned_at TIMESTAMPTZ,
    sale_price DOUBLE);

CREATE TABLE events (
    id BIGINT, user_id BIGINT, sequence_number BIGINT, session_id VARCHAR,
    created_at TIMESTAMPTZ, ip_address VARCHAR, city VARCHAR, state VARCHAR,
    postal_code VARCHAR, browser VARCHAR, traffic_source VARCHAR, uri VARCHAR,
    event_type VARCHAR);
"""

# A few hand-made rows. Each one exercises a rule from the replay design:
# - order 1 Processing: only created_at is set
# - order 2 Shipped: its item was created BEFORE the order
# - order 3 Complete, two items: one item created AFTER the order shipped
# - order 4 Cancelled: no later dates
# - event 2 is anonymous (user_id is NULL)
DATA_SQL = """
INSERT INTO distribution_centers VALUES
    (1, 'Memphis TN', 35.1174, -89.9711, 'POINT(-89.9711 35.1174)'),
    (2, 'Chicago IL', 41.8369, -87.6847, 'POINT(-87.6847 41.8369)');

INSERT INTO products VALUES
    (10, 20.0, 'Tops', 'Plain Tee', 'Acme', 40.0, 'Women', 'SKU10', 1),
    (11, 50.0, 'Jeans', 'Slim Jeans', 'Acme', 90.0, 'Men', 'SKU11', 2);

INSERT INTO inventory_items VALUES
    (100, 10, '2025-12-01 10:00:00+00', NULL, 20.0, 'Tops', 'Plain Tee', 'Acme', 40.0, 'Women', 'SKU10', 1),
    (101, 11, '2025-12-01 10:00:00+00', NULL, 50.0, 'Jeans', 'Slim Jeans', 'Acme', 90.0, 'Men', 'SKU11', 2),
    (102, 10, '2025-12-01 10:00:00+00', NULL, 20.0, 'Tops', 'Plain Tee', 'Acme', 40.0, 'Women', 'SKU10', 1),
    (103, 11, '2025-12-01 10:00:00+00', NULL, 50.0, 'Jeans', 'Slim Jeans', 'Acme', 90.0, 'Men', 'SKU11', 2),
    (104, 10, '2025-12-01 10:00:00+00', NULL, 20.0, 'Tops', 'Plain Tee', 'Acme', 40.0, 'Women', 'SKU10', 1);

INSERT INTO users VALUES
    (1, 'Ann', 'Lee', 'ann@example.com', 30, 'F', 'Texas', '1 Main St', '75001', 'Dallas',
     'United States', 32.7, -96.8, 'Search', '2026-01-01 08:00:00+00', 'POINT(-96.8 32.7)'),
    (2, 'Bo', 'Kim', 'bo@example.com', 41, 'M', 'Texas', '2 Oak Ave', '73301', 'Austin',
     'United States', 30.3, -97.7, 'Organic', '2026-01-02 09:00:00+00', 'POINT(-97.7 30.3)');

INSERT INTO orders VALUES
    (1, 1, 'Processing', 'F', '2026-01-03 10:00:00+00', NULL, NULL, NULL, 1),
    (2, 1, 'Shipped', 'F', '2026-01-03 12:00:00+00', NULL, '2026-01-04 09:00:00+00', NULL, 1),
    (3, 2, 'Complete', 'M', '2026-01-02 15:00:00+00', NULL, '2026-01-03 08:00:00+00', '2026-01-05 14:00:00+00', 2),
    (4, 2, 'Cancelled', 'M', '2026-01-03 09:00:00+00', NULL, NULL, NULL, 1);

INSERT INTO order_items VALUES
    (1, 1, 1, 10, 100, 'Processing', '2026-01-03 10:30:00+00', NULL, NULL, NULL, 40.0),
    (2, 2, 1, 11, 101, 'Shipped', '2026-01-03 11:30:00+00', '2026-01-04 09:00:00+00', NULL, NULL, 90.0),
    (3, 3, 2, 10, 102, 'Complete', '2026-01-03 20:00:00+00', '2026-01-03 08:00:00+00', '2026-01-05 14:00:00+00', NULL, 40.0),
    (4, 3, 2, 11, 103, 'Complete', '2026-01-02 15:30:00+00', '2026-01-03 08:00:00+00', '2026-01-05 14:00:00+00', NULL, 90.0),
    (5, 4, 2, 10, 104, 'Cancelled', '2026-01-03 09:00:00+00', NULL, NULL, NULL, 40.0);

INSERT INTO events VALUES
    (1, 1, 1, 'sess-a', '2026-01-01 08:05:00+00', '1.1.1.1', 'Dallas', 'Texas', '75001', 'Chrome', 'Search', '/', 'home'),
    (2, NULL, 1, 'sess-b', '2026-01-02 10:00:00+00', '2.2.2.2', 'Austin', 'Texas', '73301', 'Firefox', 'Organic', '/department/women', 'department'),
    (3, 2, 1, 'sess-c', '2026-01-02 14:50:00+00', '3.3.3.3', 'Austin', 'Texas', '73301', 'Chrome', 'Organic', '/cart', 'cart');
"""

@pytest.fixture
def duckdb_conn():
    """A fresh in-memory DuckDB with the seven thelook tables and a few hand-made rows."""
    conn = duckdb.connect(database=":memory:")
    try:
        conn.execute("SET TimeZone='UTC'")
        conn.execute(SCHEMA_SQL)
        conn.execute(DATA_SQL)
        yield conn
    finally:
        conn.close()

@pytest.fixture
def batch_result(duckdb_conn):
    """simulate_batch output for the fixture database"""
    result = simulator.simulate_batch(
        database_connection=duckdb_conn,
        window_start=pd.Timestamp(2026,1,1, tz='UTC'),
        window_end=pd.Timestamp(2026,1,2, tz='UTC'),
        seed=42
    )
    return result

