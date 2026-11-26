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


class Severity(str, Enum):
    SAFE = "SAFE"
    WARNING = "WARNING"
    BREAKING = "BREAKING"


class MergePolicy(str, Enum):
    STRICT = "STRICT"
    STRICT_BLOCK_BREAKING = "STRICT"
    LENIENT = "LENIENT"
    FORCE_OVERRIDE = "FORCE_OVERRIDE"


class ColumnDef(BaseModel):
    name: str
    raw_type: str
    normalized_type: str
    nullable: bool = True
    primary_key: bool = False
    default_value: Optional[str] = None

    @property
    def is_primary_key(self) -> bool:
        return self.primary_key


class TableSchema(BaseModel):
    database: str
    table_name: str
    columns: List[ColumnDef]
    row_count: int = 0


class SchemaChange(BaseModel):
    change_id: str
    change_type: ChangeType
    source_column: Optional[str] = None
    target_column: Optional[str] = None
    old_type: Optional[str] = None
    new_type: Optional[str] = None
    old_nullable: Optional[bool] = None
    new_nullable: Optional[bool] = None
    similarity_score: Optional[float] = None
    severity: Severity = Severity.SAFE
    risk_score: int = Field(default=0, ge=0, le=100)
    affected_views: List[str] = []
    recommendation: str = ""
    auto_approved: bool = True


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class ImpactReport(BaseModel):
    table_name: str
    total_changes: int
    overall_severity: Severity
    max_risk_score: int
    changes: List[SchemaChange]
    recommendations: List[str] = []
    suggested_mappings: Dict[str, str] = {}
    can_auto_merge: bool

    @property
    def overall_risk(self) -> RiskLevel:
        if self.overall_severity == Severity.BREAKING:
            return RiskLevel.HIGH
        if self.overall_severity == Severity.WARNING and not self.can_auto_merge:
            return RiskLevel.MEDIUM
        return RiskLevel.LOW

    @property
    def impacted_downstream_assets(self) -> List[str]:
        assets = []
        for chg in self.changes:
            for v in chg.affected_views:
                if v not in assets:
                    assets.append(v)
        return assets

class MergeRequest(BaseModel):
    table_name: str = "orders"
    policy: MergePolicy = MergePolicy.STRICT
    approved_change_ids: List[str] = []
    column_mappings: Dict[str, str] = {}


class SchemaVersionRecord(BaseModel):
    version_id: str
    table_name: str
    schema_hash: str
    status: str = "ACTIVE"
    created_at: str
    columns: List[ColumnDef]
    applied_changes: List[Dict[str, Any]] = []


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
    data_quality_score: float = 100.0
    watermark_before: Optional[str] = None
    watermark_after: Optional[str] = None
    checksum: str
    duration_ms: float
    executed_at: str
    notes: str