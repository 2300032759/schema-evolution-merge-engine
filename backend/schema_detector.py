from difflib import SequenceMatcher
from typing import Dict, List
from backend.models import ChangeType, ColumnDef, SchemaChange, TableSchema

TYPE_WIDENING_PAIRS = {
    ("INT32", "INT64"),
    ("INT32", "FLOAT64"),
    ("INT64", "FLOAT64"),
    ("DATE", "TIMESTAMP"),
}


def _name_similarity(a: str, b: str) -> float:
    return round(SequenceMatcher(None, a.lower(), b.lower()).ratio(), 3)


class SchemaDetector:
    def __init__(self, rename_threshold: float = 0.65):
        self.rename_threshold = rename_threshold

    def detect_changes(
        self,
        source_schema: TableSchema,
        target_schema: TableSchema,
        explicit_mappings: Dict[str, str] | None = None,
    ) -> List[SchemaChange]:
        explicit_mappings = explicit_mappings or {}
        src_cols: Dict[str, ColumnDef] = {c.name: c for c in source_schema.columns}
        tgt_cols: Dict[str, ColumnDef] = {c.name: c for c in target_schema.columns}

        changes: List[SchemaChange] = []
        matched_src = set()
        matched_tgt = set()
        idx = 1

        # 1. Exact column name matches & explicit user mappings
        for s_name, s_col in src_cols.items():
            mapped_t_name = explicit_mappings.get(s_name, s_name)
            if mapped_t_name in tgt_cols:
                t_col = tgt_cols[mapped_t_name]
                matched_src.add(s_name)
                matched_tgt.add(mapped_t_name)

                if s_name != mapped_t_name:
                    changes.append(
                        SchemaChange(
                            change_id=f"CHG-{idx:03d}",
                            change_type=ChangeType.RENAME_CANDIDATE,
                            source_column=s_name,
                            target_column=mapped_t_name,
                            old_type=t_col.normalized_type,
                            new_type=s_col.normalized_type,
                            similarity_score=1.0,
                        )
                    )
                    idx += 1

                # Type evolution check
                if s_col.normalized_type != t_col.normalized_type:
                    if (t_col.normalized_type, s_col.normalized_type) in TYPE_WIDENING_PAIRS:
                        c_type = ChangeType.TYPE_WIDENING
                    else:
                        c_type = ChangeType.TYPE_NARROWING
                    changes.append(
                        SchemaChange(
                            change_id=f"CHG-{idx:03d}",
                            change_type=c_type,
                            source_column=s_name,
                            target_column=mapped_t_name,
                            old_type=t_col.normalized_type,
                            new_type=s_col.normalized_type,
                        )
                    )
                    idx += 1

                # Nullability check
                if t_col.nullable and not s_col.nullable:
                    changes.append(
                        SchemaChange(
                            change_id=f"CHG-{idx:03d}",
                            change_type=ChangeType.NULLABILITY_TIGHTENED,
                            source_column=s_name,
                            target_column=mapped_t_name,
                            old_nullable=t_col.nullable,
                            new_nullable=s_col.nullable,
                        )
                    )
                    idx += 1
                elif not t_col.nullable and s_col.nullable:
                    changes.append(
                        SchemaChange(
                            change_id=f"CHG-{idx:03d}",
                            change_type=ChangeType.NULLABILITY_RELAXED,
                            source_column=s_name,
                            target_column=mapped_t_name,
                            old_nullable=t_col.nullable,
                            new_nullable=s_col.nullable,
                        )
                    )
                    idx += 1

        unmatched_src = [c for name, c in src_cols.items() if name not in matched_src]
        unmatched_tgt = [c for name, c in tgt_cols.items() if name not in matched_tgt]

        # 2. Fuzzy rename detection among unmatched columns with compatible types
        paired_src = set()
        paired_tgt = set()
        for s_col in unmatched_src:
            best_t = None
            best_score = 0.0
            for t_col in unmatched_tgt:
                if t_col.name in paired_tgt:
                    continue
                if s_col.normalized_type == t_col.normalized_type:
                    score = _name_similarity(s_col.name, t_col.name)
                    if score >= self.rename_threshold and score > best_score:
                        best_score = score
                        best_t = t_col
            if best_t:
                paired_src.add(s_col.name)
                paired_tgt.add(best_t.name)
                changes.append(
                    SchemaChange(
                        change_id=f"CHG-{idx:03d}",
                        change_type=ChangeType.RENAME_CANDIDATE,
                        source_column=s_col.name,
                        target_column=best_t.name,
                        old_type=best_t.normalized_type,
                        new_type=s_col.normalized_type,
                        similarity_score=best_score,
                    )
                )
                idx += 1

        # 3. Remaining unmatched source columns -> ADD_COLUMN
        for s_col in unmatched_src:
            if s_col.name not in paired_src:
                changes.append(
                    SchemaChange(
                        change_id=f"CHG-{idx:03d}",
                        change_type=ChangeType.ADD_COLUMN,
                        source_column=s_col.name,
                        new_type=s_col.normalized_type,
                        new_nullable=s_col.nullable,
                    )
                )
                idx += 1

        # 4. Remaining unmatched target columns -> DROP_COLUMN
        for t_col in unmatched_tgt:
            if t_col.name not in paired_tgt:
                changes.append(
                    SchemaChange(
                        change_id=f"CHG-{idx:03d}",
                        change_type=ChangeType.DROP_COLUMN,
                        target_column=t_col.name,
                        old_type=t_col.normalized_type,
                        old_nullable=t_col.nullable,
                    )
                )
                idx += 1

        return changes