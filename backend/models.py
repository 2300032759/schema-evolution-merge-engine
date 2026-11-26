from __future__ import annotations
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class ChangeType(str, Enum):
    ADD_COLUMN = "ADD_COLUMN"
    DROP_COLUMN = "DROP_COLUMN"
    RENAME_CANDIDATE = "RENAME_CANDIDATE"
    TYPE_WIDENING = "TYPE_WIDENING"
    TYPE_NARROWING = "TYPE_NARROWING"
    NULLABILITY_TIGHTENED = "NULLABILITY_TIGHTENED"
    NULLABILITY_RELAXED = "NULLABILITY_RELAXED"


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class MergePolicy(str, Enum):
    AUTO_EVOLVE_SAFE = "AUTO_EVOLVE_SAFE"
    STRICT_BLOCK_BREAKING = "STRICT_BLOCK_BREAKING"
    FORCE_OVERRIDE = "FORCE_OVERRIDE"


class ColumnDef(BaseModel):
    name: str
    data_type: str
    normalized_type: str
    nullable: bool = True
    is_primary_key: bool = False


class TableSchema(BaseModel):
    warehouse: str
    table_name: str
    columns: List[ColumnDef]


class SchemaChange(BaseModel):
    change_id: str
    change_type: ChangeType
    source_column: Optional[str] = None
    target_column: Optional[str] = None
    old_type: Optional[str] = None
    new_type: Optional[str] = None
    old_nullable: Optional[bool] = None
    new_nullable: Optional[bool] = None
    similarity_score: float = 0.0
    risk_level: RiskLevel = RiskLevel.LOW
    risk_score: int = 0
    impacted_dependencies: List[str] = Field(default_factory=list)
    recommended_action: str = ""
    auto_approved: bool = True


class ImpactSummary(BaseModel):
    table_name: str
    overall_risk: RiskLevel
    max_risk_score: int
    breaking_changes_count: int
    safe_changes_count: int
    impacted_downstream_assets: List[str]
    can_auto_merge: bool
    changes: List[SchemaChange]


class SchemaVersionRecord(BaseModel):
    version_id: str
    table_name: str
    version_number: int
    schema_hash: str
    columns: List[ColumnDef]
    changes_applied: List[Dict[str, Any]]
    created_at: str
    status: str = "ACTIVE"


class MergeRequest(BaseModel):
    table_name: str = "orders"
    policy: MergePolicy = MergePolicy.STRICT_BLOCK_BREAKING
    column_mappings: Dict[str, str] = Field(default_factory=dict)
    approved_change_ids: List[str] = Field(default_factory=list)


class MergeAuditRecord(BaseModel):
    batch_id: str
    table_name: str
    schema_version: str
    policy_used: str
    status: str
    rows_read: int
    rows_inserted: int
    rows_updated: int
    rows_quarantined: int
    watermark_before: Optional[str]
    watermark_after: Optional[str]
    checksum: str
    duration_ms: float
    executed_at: str
    notes: str