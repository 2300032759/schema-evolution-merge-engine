import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from backend.models import ColumnDef, SchemaVersionRecord

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
REGISTRY_FILE = os.path.join(DATA_DIR, "schema_registry.json")


class SchemaRegistry:
    def __init__(self, registry_path: str = REGISTRY_FILE):
        self.registry_path = registry_path
        os.makedirs(os.path.dirname(self.registry_path), exist_ok=True)
        if not os.path.exists(self.registry_path):
            self._save({"versions": [], "audit_logs": [], "watermarks": {}})

    def _load(self) -> Dict[str, Any]:
        with open(self.registry_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save(self, payload: Dict[str, Any]) -> None:
        with open(self.registry_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

    def reset(self) -> None:
        self._save({"versions": [], "audit_logs": [], "watermarks": {}})

    @staticmethod
    def compute_schema_hash(columns: List[ColumnDef]) -> str:
        canonical = json.dumps(
            [{"name": c.name, "type": c.normalized_type, "nullable": c.nullable, "pk": c.is_primary_key} for c in columns],
            sort_keys=True,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]

    def register_version(
        self,
        table_name: str,
        columns: List[ColumnDef],
        changes_applied: List[Dict[str, Any]],
    ) -> SchemaVersionRecord:
        db = self._load()
        table_versions = [v for v in db["versions"] if v["table_name"] == table_name]
        new_hash = self.compute_schema_hash(columns)

        # Return existing active version if schema hash is unchanged
        if table_versions and table_versions[-1]["schema_hash"] == new_hash:
            return SchemaVersionRecord(**table_versions[-1])

        for v in table_versions:
            v["status"] = "SUPERSEDED"

        next_num = len(table_versions) + 1
        rec = SchemaVersionRecord(
            version_id=f"v{next_num}.0.0",
            table_name=table_name,
            version_number=next_num,
            schema_hash=new_hash,
            columns=columns,
            changes_applied=changes_applied,
            created_at=datetime.now(timezone.utc).isoformat(),
            status="ACTIVE",
        )
        db["versions"].append(rec.model_dump())
        self._save(db)
        return rec

    def list_versions(self, table_name: Optional[str] = None) -> List[SchemaVersionRecord]:
        db = self._load()
        items = db["versions"]
        if table_name:
            items = [v for v in items if v["table_name"] == table_name]
        return [SchemaVersionRecord(**v) for v in items]

    def get_active_version(self, table_name: str) -> Optional[SchemaVersionRecord]:
        versions = self.list_versions(table_name)
        for v in reversed(versions):
            if v.status == "ACTIVE":
                return v
        return versions[-1] if versions else None

    def get_watermark(self, table_name: str) -> Optional[str]:
        return self._load().get("watermarks", {}).get(table_name)

    def set_watermark(self, table_name: str, watermark: str) -> None:
        db = self._load()
        db.setdefault("watermarks", {})[table_name] = watermark
        self._save(db)

    def append_audit_log(self, audit_entry: Dict[str, Any]) -> None:
        db = self._load()
        db.setdefault("audit_logs", []).insert(0, audit_entry)
        self._save(db)

    def get_audit_logs(self) -> List[Dict[str, Any]]:
        return self._load().get("audit_logs", [])