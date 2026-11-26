import os
import sqlite3
from typing import Any, Dict, List, Optional
import duckdb
from backend.models import ColumnDef, TableSchema

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
SQLITE_PATH = os.path.join(DATA_DIR, "source_oltp.db")
DUCKDB_PATH = os.path.join(DATA_DIR, "target_olap.duckdb")


def normalize_sql_type(raw_type: str) -> str:
    t = (raw_type or "TEXT").strip().upper()
    if any(k in t for k in ("BIGINT", "INT8", "INT64")):
        return "INT64"
    if any(k in t for k in ("INT", "SERIAL", "SHORT", "BYTE")):
        return "INT32"
    if any(k in t for k in ("DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC")):
        return "FLOAT64"
    if "BOOL" in t:
        return "BOOLEAN"
    if "TIMESTAMP" in t or "DATETIME" in t:
        return "TIMESTAMP"
    if "DATE" in t:
        return "DATE"
    return "STRING"


class WarehouseManager:
    def __init__(self, sqlite_path: str = SQLITE_PATH, duckdb_path: str = DUCKDB_PATH):
        os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
        self.sqlite_path = sqlite_path
        self.duckdb_path = duckdb_path

    def get_source_conn(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.sqlite_path)
        con.row_factory = sqlite3.Row
        return con

    def get_target_conn(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.duckdb_path)

    def reset_demo_warehouses(self) -> None:
        if os.path.exists(self.sqlite_path):
            os.remove(self.sqlite_path)
        if os.path.exists(self.duckdb_path):
            os.remove(self.duckdb_path)

        with self.get_source_conn() as scon:
            scon.execute("""
                CREATE TABLE orders (
                    order_id INTEGER PRIMARY KEY,
                    customer_email TEXT NOT NULL,
                    region_code TEXT NOT NULL,
                    order_amount INTEGER NOT NULL,
                    order_status TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            seed_rows = [
                (1001, "arjun.rao@klef.in", "SOUTH", 1200, "DELIVERED", "2026-01-10T09:00:00"),
                (1002, "priya.nair@klef.in", "WEST", 2450, "SHIPPED", "2026-01-10T09:15:00"),
                (1003, "karthik.v@klef.in", "NORTH", 890, "PROCESSING", "2026-01-10T09:30:00"),
                (1004, "meera.iyer@klef.in", "EAST", 3100, "DELIVERED", "2026-01-10T09:45:00"),
            ]
            scon.executemany(
                "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)",
                seed_rows,
            )
            scon.commit()

        dcon = self.get_target_conn()
        try:
            dcon.execute("""
                CREATE TABLE orders (
                    order_id INTEGER PRIMARY KEY,
                    customer_email VARCHAR NOT NULL,
                    region_code VARCHAR NOT NULL,
                    order_amount INTEGER NOT NULL,
                    order_status VARCHAR NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
            """)
            dcon.execute("""
                CREATE VIEW vw_regional_revenue AS
                SELECT region_code, COUNT(*) AS total_orders, SUM(order_amount) AS gross_revenue
                FROM orders
                GROUP BY region_code
            """)
        finally:
            dcon.close()

    def inspect_source_schema(self, table_name: str) -> TableSchema:
        with self.get_source_conn() as scon:
            rows = scon.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            if not rows:
                raise ValueError(f"Source table '{table_name}' does not exist.")
            count = scon.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            cols = [
                ColumnDef(
                    name=r["name"],
                    raw_type=r["type"] or "TEXT",
                    normalized_type=normalize_sql_type(r["type"]),
                    nullable=not bool(r["notnull"]),
                    primary_key=bool(r["pk"]),
                    default_value=r["dflt_value"],
                )
                for r in rows
            ]
            return TableSchema(database="SQLite_OLTP", table_name=table_name, columns=cols, row_count=count)

    def inspect_target_schema(self, table_name: str) -> TableSchema:
        dcon = self.get_target_conn()
        try:
            exists = dcon.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()[0]
            if not exists:
                raise ValueError(f"Target table '{table_name}' does not exist.")
            rows = dcon.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            count = dcon.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            cols = [
                ColumnDef(
                    name=r[1],
                    raw_type=r[2],
                    normalized_type=normalize_sql_type(r[2]),
                    nullable=not bool(r[3]),
                    primary_key=bool(r[5]),
                    default_value=str(r[4]) if r[4] is not None else None,
                )
                for r in rows
            ]
            return TableSchema(database="DuckDB_OLAP", table_name=table_name, columns=cols, row_count=count)
        finally:
            dcon.close()

    def inspect_target_views(self) -> List[Dict[str, str]]:
        """Dynamically introspects all user-defined SQL views in DuckDB catalog."""
        dcon = self.get_target_conn()
        try:
            rows = dcon.execute(
                "SELECT view_name, sql FROM duckdb_views() WHERE internal = false"
            ).fetchall()
            return [{"view_name": r[0], "sql": r[1] or ""} for r in rows]
        finally:
            dcon.close()

    def fetch_source_incremental(
        self, table_name: str, watermark_col: str, last_watermark: Optional[str]
    ) -> List[Dict[str, Any]]:
        with self.get_source_conn() as scon:
            if last_watermark:
                cur = scon.execute(
                    f"SELECT * FROM {table_name} WHERE {watermark_col} > ? ORDER BY {watermark_col} ASC",
                    (last_watermark,),
                )
            else:
                cur = scon.execute(f"SELECT * FROM {table_name} ORDER BY {watermark_col} ASC")
            return [dict(r) for r in cur.fetchall()]