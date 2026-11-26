import hashlib
import json
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List
from backend.models import (
    ChangeType,
    MergeAuditRecord,
    MergePolicy,
    MergeRequest,
    SchemaChange,
)
from backend.risk_analyzer import RiskAndImpactAnalyzer
from backend.schema_detector import SchemaDetector
from backend.schema_registry import SchemaRegistry
from backend.warehouse_manager import WarehouseManager

NORMALIZED_TO_DUCKDB = {
    "INT32": "INTEGER",
    "INT64": "BIGINT",
    "FLOAT64": "DOUBLE",
    "BOOLEAN": "BOOLEAN",
    "TIMESTAMP": "TIMESTAMP",
    "DATE": "DATE",
    "STRING": "VARCHAR",
}


class CrossWarehouseMergeEngine:
    def __init__(
        self,
        wm: WarehouseManager | None = None,
        registry: SchemaRegistry | None = None,
    ):
        self.wm = wm or WarehouseManager()
        self.registry = registry or SchemaRegistry()
        self.detector = SchemaDetector()
        self.analyzer = RiskAndImpactAnalyzer()

    def analyze_table(self, table_name: str, explicit_mappings: Dict[str, str] | None = None):
        src_schema = self.wm.inspect_source_schema(table_name)
        tgt_schema = self.wm.inspect_target_schema(table_name)
        target_views = self.wm.inspect_target_views()
        changes = self.detector.detect_changes(src_schema, tgt_schema, explicit_mappings)
        impact = self.analyzer.evaluate(table_name, changes, target_views)
        return src_schema, tgt_schema, impact

    def execute_incremental_merge(self, req: MergeRequest) -> MergeAuditRecord:
        t0 = time.perf_counter()
        batch_id = f"BATCH-{uuid.uuid4().hex[:8].upper()}"
        src_schema, tgt_schema, impact = self.analyze_table(req.table_name, req.column_mappings)

        # Ensure baseline v1 is registered if registry is empty
        if not self.registry.get_active_version(req.table_name) and tgt_schema.columns:
            self.registry.register_version(req.table_name, tgt_schema.columns, [])

        # Evaluate policy gate against detected changes
        blocked_changes: List[SchemaChange] = []
        approved_changes: List[SchemaChange] = []

        for chg in impact.changes:
            is_explicitly_approved = chg.change_id in req.approved_change_ids
            if req.policy == MergePolicy.FORCE_OVERRIDE or chg.auto_approved or is_explicitly_approved:
                approved_changes.append(chg)
            else:
                blocked_changes.append(chg)

        wm_before = self.registry.get_watermark(req.table_name)

        if blocked_changes and req.policy != MergePolicy.FORCE_OVERRIDE:
            duration_ms = round((time.perf_counter() - t0) * 1000, 2)
            active_ver = self.registry.get_active_version(req.table_name)
            audit = MergeAuditRecord(
                batch_id=batch_id,
                table_name=req.table_name,
                schema_version=active_ver.version_id if active_ver else "v1.0.0",
                policy_used=req.policy.value,
                status="QUARANTINED_SCHEMA_DRIFT",
                rows_read=0,
                rows_inserted=0,
                rows_updated=0,
                rows_quarantined=len(blocked_changes),
                data_quality_score=0.0,
                watermark_before=wm_before,
                watermark_after=wm_before,
                checksum="N/A",
                duration_ms=duration_ms,
                executed_at=datetime.now(timezone.utc).isoformat(),
                notes=f"Blocked {len(blocked_changes)} unapproved breaking change(s): "
                      + ", ".join(f"{c.change_id}:{c.change_type.value}" for c in blocked_changes),
            )
            self.registry.append_audit_log(audit.model_dump())
            return audit

        # Apply approved DDL migrations & set-based staging UPSERT to Target Warehouse (DuckDB)
        effective_mappings = dict(req.column_mappings)
        con = self.wm.get_target_conn()
        try:
            con.execute("BEGIN TRANSACTION")
            for chg in approved_changes:
                if chg.change_type == ChangeType.ADD_COLUMN and chg.source_column:
                    duck_type = NORMALIZED_TO_DUCKDB.get(chg.new_type or "STRING", "VARCHAR")
                    con.execute(f"ALTER TABLE {req.table_name} ADD COLUMN {chg.source_column} {duck_type}")

                elif chg.change_type == ChangeType.TYPE_WIDENING and chg.target_column:
                    duck_type = NORMALIZED_TO_DUCKDB.get(chg.new_type or "FLOAT64", "DOUBLE")
                    if req.table_name == "orders" and chg.target_column == "order_amount":
                        con.execute("DROP VIEW IF EXISTS vw_regional_revenue")
                    con.execute(f"ALTER TABLE {req.table_name} ALTER {chg.target_column} TYPE {duck_type}")
                    if req.table_name == "orders" and chg.target_column == "order_amount":
                        con.execute("""
                            CREATE VIEW vw_regional_revenue AS
                            SELECT region_code, COUNT(*) AS total_orders, SUM(order_amount) AS gross_revenue
                            FROM orders
                            GROUP BY region_code
                        """)

                elif chg.change_type == ChangeType.RENAME_CANDIDATE and chg.source_column and chg.target_column:
                    effective_mappings[chg.source_column] = chg.target_column

                elif chg.change_type == ChangeType.DROP_COLUMN and chg.target_column:
                    pass

            delta_rows = self.wm.fetch_source_incremental(req.table_name, "updated_at", wm_before)
            info = con.execute(f"PRAGMA table_info('{req.table_name}')").fetchall()
            tgt_col_names = [r[1] for r in info]
            non_null_cols = [r[1] for r in info if r[3]]
            pk_cols = [r[1] for r in info if r[5]] or [tgt_col_names[0]]
            pk_col = pk_cols[0]

            rows_inserted = 0
            rows_updated = 0
            rows_rejected_dq = 0
            wm_after = wm_before
            valid_mapped_rows = []

            for row in delta_rows:
                mapped_row = {}
                for src_k, val in row.items():
                    tgt_k = effective_mappings.get(src_k, src_k)
                    if tgt_k in tgt_col_names:
                        mapped_row[tgt_k] = val

                # Data quality validation: ensure primary key and NOT NULL fields are present
                if pk_col not in mapped_row or mapped_row[pk_col] is None:
                    rows_rejected_dq += 1
                    continue
                if any(mapped_row.get(c) is None for c in non_null_cols if c in mapped_row):
                    rows_rejected_dq += 1
                    continue

                valid_mapped_rows.append(mapped_row)
                row_wm = str(row.get("updated_at"))
                if not wm_after or row_wm > wm_after:
                    wm_after = row_wm

            if valid_mapped_rows:
                active_cols = list(valid_mapped_rows[0].keys())
                col_list_str = ", ".join(active_cols)
                placeholders = ", ".join("?" for _ in active_cols)

                # Vectorized set-based staging table upsert
                con.execute(f"CREATE TEMP TABLE _stage_delta AS SELECT {col_list_str} FROM {req.table_name} WHERE 1=0")
                con.executemany(
                    f"INSERT INTO _stage_delta ({col_list_str}) VALUES ({placeholders})",
                    [[r.get(c) for c in active_cols] for r in valid_mapped_rows],
                )

                rows_updated = con.execute(
                    f"SELECT COUNT(*) FROM _stage_delta s JOIN {req.table_name} t ON s.{pk_col} = t.{pk_col}"
                ).fetchone()[0]
                rows_inserted = len(valid_mapped_rows) - rows_updated

                non_pk_cols = [c for c in active_cols if c != pk_col]
                if non_pk_cols and rows_updated > 0:
                    set_expr = ", ".join(f"{c} = s.{c}" for c in non_pk_cols)
                    con.execute(
                        f"UPDATE {req.table_name} AS t SET {set_expr} FROM _stage_delta AS s WHERE t.{pk_col} = s.{pk_col}"
                    )

                if rows_inserted > 0:
                    con.execute(
                        f"""
                        INSERT INTO {req.table_name} ({col_list_str})
                        SELECT {col_list_str} FROM _stage_delta s
                        WHERE NOT EXISTS (SELECT 1 FROM {req.table_name} t WHERE t.{pk_col} = s.{pk_col})
                        """
                    )
                con.execute("DROP TABLE _stage_delta")

            con.execute("COMMIT")
        except Exception as exc:
            con.execute("ROLLBACK")
            raise RuntimeError(f"Transactional merge aborted & rolled back: {exc}") from exc
        finally:
            con.close()

        dq_score = (
            round((len(valid_mapped_rows) / len(delta_rows)) * 100.0, 2)
            if delta_rows
            else 100.0
        )

        final_tgt_schema = self.wm.inspect_target_schema(req.table_name)
        ver_rec = self.registry.register_version(
            req.table_name,
            final_tgt_schema.columns,
            [c.model_dump() for c in approved_changes],
        )
        if wm_after:
            self.registry.set_watermark(req.table_name, wm_after)

        checksum = hashlib.sha256(json.dumps(delta_rows, sort_keys=True, default=str).encode()).hexdigest()[:16]
        duration_ms = round((time.perf_counter() - t0) * 1000, 2)

        audit = MergeAuditRecord(
            batch_id=batch_id,
            table_name=req.table_name,
            schema_version=ver_rec.version_id,
            policy_used=req.policy.value,
            status="SUCCESS",
            rows_read=len(delta_rows),
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            rows_quarantined=rows_rejected_dq,
            data_quality_score=dq_score,
            watermark_before=wm_before,
            watermark_after=wm_after,
            checksum=checksum,
            duration_ms=duration_ms,
            executed_at=datetime.now(timezone.utc).isoformat(),
            notes=f"Applied {len(approved_changes)} schema change(s); merged {rows_inserted} inserts, {rows_updated} updates (DQ={dq_score}%).",
        )
        self.registry.append_audit_log(audit.model_dump())
        return audit

    def rollback_to_version(self, table_name: str, target_version_id: str) -> dict:
        versions = self.registry.list_versions(table_name)
        target_ver = next((v for v in versions if v.version_id == target_version_id), None)
        if not target_ver:
            raise ValueError(f"Version {target_version_id} not found for table {table_name}.")

        con = self.wm.get_target_conn()
        try:
            con.execute("BEGIN TRANSACTION")
            if table_name == "orders":
                con.execute("DROP VIEW IF EXISTS vw_regional_revenue")

            current_info = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            current_cols = {r[1] for r in current_info}
            current_types = {r[1]: r[2].upper() for r in current_info}
            pk_names = {r[1] for r in current_info if r[5]}
            target_cols = {c.name: c for c in target_ver.columns}

            for col_name in current_cols - set(target_cols.keys()):
                con.execute(f"ALTER TABLE {table_name} DROP COLUMN {col_name}")

            for col_name, col_def in target_cols.items():
                if col_name in current_cols and col_name not in pk_names:
                    duck_type = NORMALIZED_TO_DUCKDB.get(col_def.normalized_type, "VARCHAR")
                    if current_types.get(col_name) != duck_type:
                        con.execute(f"ALTER TABLE {table_name} ALTER {col_name} TYPE {duck_type}")

            if table_name == "orders" and {"region_code", "order_amount"}.issubset(set(target_cols.keys())):
                con.execute("""
                    CREATE VIEW vw_regional_revenue AS
                    SELECT region_code, COUNT(*) AS total_orders, SUM(order_amount) AS gross_revenue
                    FROM orders
                    GROUP BY region_code
                """)
            con.execute("COMMIT")
        except Exception as exc:
            con.execute("ROLLBACK")
            raise RuntimeError(f"Rollback failed: {exc}") from exc
        finally:
            con.close()

        new_ver = self.registry.register_version(
            table_name,
            target_ver.columns,
            [{"action": "ROLLBACK", "restored_from": target_version_id}],
        )
        return {"status": "rolled_back", "restored_version": target_version_id, "new_active_version": new_ver.version_id}