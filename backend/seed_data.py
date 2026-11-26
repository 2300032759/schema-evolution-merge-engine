import gc
import os
from backend.warehouse_manager import WarehouseManager


def reset_and_seed(wm: WarehouseManager | None = None) -> dict:
    wm = wm or WarehouseManager()
    gc.collect()

    # 1. Initialize Source OLTP (v1 schema) cleanly without OS file deletion locks
    sconn = wm.get_source_conn()
    try:
        sconn.execute("DROP TABLE IF EXISTS orders")
        sconn.execute("DROP TABLE IF EXISTS orders_new")
        sconn.execute("DROP TABLE IF EXISTS orders_breaking")
        sconn.execute("""
            CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY,
                customer_email TEXT NOT NULL,
                order_amount INTEGER NOT NULL,
                region_code TEXT,
                updated_at TEXT NOT NULL
            )
        """)
        sconn.executemany(
            "INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
            [
                (1001, "ananya.rao@klef.edu.in", 1200, "AP-SOUTH", "2026-11-20T10:00:00Z"),
                (1002, "rohan.verma@klef.edu.in", 2450, "TS-HYD", "2026-11-20T10:05:00Z"),
                (1003, "priya.nair@klef.edu.in", 890, "KA-BLR", "2026-11-20T10:12:00Z"),
            ],
        )
        sconn.commit()
    finally:
        sconn.close()

    # 2. Initialize Target Warehouse DuckDB (v1 schema matching baseline)
    tcon = wm.get_target_conn()
    try:
        tcon.execute("DROP VIEW IF EXISTS vw_regional_revenue")
        tcon.execute("DROP TABLE IF EXISTS orders")
        tcon.execute("""
            CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY,
                customer_email VARCHAR NOT NULL,
                order_amount INTEGER NOT NULL,
                region_code VARCHAR,
                updated_at VARCHAR NOT NULL
            )
        """)
        tcon.execute("""
            INSERT INTO orders VALUES
            (1001, 'ananya.rao@klef.edu.in', 1200, 'AP-SOUTH', '2026-11-20T10:00:00Z'),
            (1002, 'rohan.verma@klef.edu.in', 2450, 'TS-HYD', '2026-11-20T10:05:00Z')
        """)
        tcon.execute("""
            CREATE VIEW vw_regional_revenue AS
            SELECT region_code, COUNT(*) AS total_orders, SUM(order_amount) AS gross_revenue
            FROM orders
            GROUP BY region_code
        """)
    finally:
        tcon.close()

    return {"status": "initialized", "baseline_version": "v1.0.0", "source_rows": 3, "target_rows": 2}


def apply_drift_scenario(scenario: str, wm: WarehouseManager | None = None) -> dict:
    wm = wm or WarehouseManager()
    sconn = wm.get_source_conn()
    try:
        if scenario == "safe_evolution":
            cols = [r[1] for r in sconn.execute("PRAGMA table_info(orders)").fetchall()]
            if "loyalty_tier" not in cols:
                sconn.execute("ALTER TABLE orders ADD COLUMN loyalty_tier TEXT")
            sconn.execute("DROP TABLE IF EXISTS orders_new")
            sconn.execute("""
                CREATE TABLE orders_new (
                    order_id INTEGER PRIMARY KEY,
                    customer_email TEXT NOT NULL,
                    order_amount REAL NOT NULL,
                    region_code TEXT,
                    loyalty_tier TEXT,
                    updated_at TEXT NOT NULL
                )
            """)
            sconn.execute("""
                INSERT INTO orders_new (order_id, customer_email, order_amount, region_code, loyalty_tier, updated_at)
                SELECT order_id, customer_email, CAST(order_amount AS REAL), region_code, COALESCE(loyalty_tier, 'SILVER'), updated_at
                FROM orders
            """)
            sconn.execute("DROP TABLE orders")
            sconn.execute("ALTER TABLE orders_new RENAME TO orders")
            sconn.executemany(
                "INSERT OR REPLACE INTO orders VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (1003, "priya.nair@klef.edu.in", 945.50, "KA-BLR", "GOLD", "2026-11-22T09:15:00Z"),
                    (1004, "karthik.s@klef.edu.in", 3199.75, "AP-SOUTH", "PLATINUM", "2026-11-22T09:30:00Z"),
                ],
            )
            sconn.commit()
            return {"scenario": scenario, "description": "Added loyalty_tier, widened order_amount (INT->FLOAT), added 2 incremental rows."}

        elif scenario == "breaking_drift":
            sconn.execute("DROP TABLE IF EXISTS orders_breaking")
            sconn.execute("""
                CREATE TABLE orders_breaking (
                    order_id INTEGER PRIMARY KEY,
                    cust_email TEXT NOT NULL,
                    order_amount REAL NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            sconn.execute("""
                INSERT INTO orders_breaking (order_id, cust_email, order_amount, updated_at)
                SELECT order_id, customer_email, order_amount, updated_at FROM orders
            """)
            sconn.execute("DROP TABLE orders")
            sconn.execute("ALTER TABLE orders_breaking RENAME TO orders")
            sconn.execute(
                "INSERT OR REPLACE INTO orders VALUES (?, ?, ?, ?)",
                (1005, "sneha.m@klef.edu.in", 1750.00, "2026-11-22T10:00:00Z"),
            )
            sconn.commit()
            return {"scenario": scenario, "description": "Renamed customer_email->cust_email & dropped region_code (breaks vw_regional_revenue)."}

        else:
            raise ValueError(f"Unknown scenario: {scenario}")
    finally:
        sconn.close()