import os
import sqlite3
import duckdb
from typing import Any, Dict, List
from backend.models import ColumnDef, TableSchema

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)

SOURCE_DB_PATH = os.path.join(DATA_DIR, "source_oltp.db")
TARGET_DB_PATH = os.path.join(DATA_DIR, "target_warehouse.duckdb")


def normalize_sql_type(raw_type: str) -> str:
    t = raw_type.strip().upper()
    if any(k in t for k in ["BIGINT", "INT64", "LONG"]):
        return "INT64"
    if any(k in t for k in ["INT", "INTEGER", "SMALLINT", "TINYINT"]):
        return "INT32"
    if any(k in t for k in ["DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC"]):
        return "FLOAT64"
    if any(k in t for k in ["BOOL"]):
        return "BOOLEAN"
    if any(k in t for k in ["TIMESTAMP", "DATETIME"]):
        return "TIMESTAMP"
    if "DATE" in t:
        return "DATE"
    return "STRING"


class WarehouseManager:
    def __init__(self, source_path: str = SOURCE_DB_PATH, target_path: str = TARGET_DB_PATH):
        self.source_path = source_path
        self.target_path = target_path

    def get_source_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.source_path)
        conn.row_factory = sqlite3.Row
        return conn

    def get_target_conn(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.target_path)

    def inspect_source_schema(self, table_name: str) -> TableSchema:
        conn = self.get_source_conn()
        try:
            cursor = conn.execute(f"PRAGMA table_info({table_name})")
            rows = cursor.fetchall()
            if not rows:
                raise ValueError(f"Source table '{table_name}' does not exist.")
            cols = [
                ColumnDef(
                    name=r["name"],
                    data_type=r["type"] or "TEXT",
                    normalized_type=normalize_sql_type(r["type"] or "TEXT"),
                    nullable=(r["notnull"] == 0 and r["pk"] == 0),
                    is_primary_key=(r["pk"] > 0),
                )
                for r in rows
            ]
            return TableSchema(warehouse="Source_OLTP_SQLite", table_name=table_name, columns=cols)
        finally:
            conn.close()

    def inspect_target_schema(self, table_name: str) -> TableSchema:
        con = self.get_target_conn()
        try:
            exists = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()[0]
            if not exists:
                return TableSchema(warehouse="Target_OLAP_DuckDB", table_name=table_name, columns=[])

            info = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            cols = [
                ColumnDef(
                    name=row[1],
                    data_type=row[2],
                    normalized_type=normalize_sql_type(row[2]),
                    nullable=(not row[3] and not row[5]),
                    is_primary_key=bool(row[5]),
                )
                for row in info
            ]
            return TableSchema(warehouse="Target_OLAP_DuckDB", table_name=table_name, columns=cols)
        finally:
            con.close()

    def fetch_source_incremental(self, table_name: str, watermark_col: str, last_watermark: str | None) -> List[Dict[str, Any]]:
        conn = self.get_source_conn()
        try:
            if last_watermark:
                cur = conn.execute(
                    f"SELECT * FROM {table_name} WHERE {watermark_col} > ? ORDER BY {watermark_col} ASC",
                    (last_watermark,),
                )
            else:
                cur = conn.execute(f"SELECT * FROM {table_name} ORDER BY {watermark_col} ASC")
            return [dict(row) for row in cur.fetchall()]
        finally:
            conn.close()

    def fetch_target_rows(self, table_name: str, limit: int = 50) -> List[Dict[str, Any]]:
        con = self.get_target_conn()
        try:
            exists = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()[0]
            if not exists:
                return []
            res = con.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
            cols = [desc[0] for desc in res.description]
            return [dict(zip(cols, row)) for row in res.fetchall()]
        finally:
            con.close()