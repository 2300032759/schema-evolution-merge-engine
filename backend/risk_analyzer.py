import re
from typing import Dict, List
from backend.models import ChangeType, ImpactReport, SchemaChange, Severity


class RiskAndImpactAnalyzer:
    def _find_dependent_views(
        self, table_name: str, column_name: str | None, views: List[Dict[str, str]]
    ) -> List[str]:
        if not column_name:
            return []
        affected = []
        table_pat = re.compile(rf"\b{re.escape(table_name)}\b", re.IGNORECASE)
        col_pat = re.compile(rf"\b{re.escape(column_name)}\b", re.IGNORECASE)
        for v in views:
            sql_text = v.get("sql", "")
            if table_pat.search(sql_text) and col_pat.search(sql_text):
                affected.append(v["view_name"])
        return affected

    def evaluate(
        self,
        table_name: str,
        changes: List[SchemaChange],
        target_views: List[Dict[str, str]] | None = None,
    ) -> ImpactReport:
        views = target_views or []
        evaluated: List[SchemaChange] = []
        recommendations: List[str] = []
        suggested_mappings: Dict[str, str] = {}

        for chg in changes:
            target_col = chg.target_column or chg.source_column
            affected = self._find_dependent_views(table_name, target_col, views)

            if chg.change_type == ChangeType.ADD_COLUMN:
                chg.severity = Severity.SAFE
                chg.risk_score = 10
                chg.auto_approved = True
                chg.recommendation = (
                    f"Auto-evolve target table via ALTER TABLE {table_name} ADD COLUMN {chg.source_column}."
                )

            elif chg.change_type == ChangeType.TYPE_WIDENING:
                chg.severity = Severity.SAFE if not affected else Severity.WARNING
                chg.risk_score = 20 if not affected else 45
                chg.affected_views = affected
                chg.auto_approved = True
                chg.recommendation = (
                    f"Safe lossless promotion ({chg.old_type} -> {chg.new_type}); rebuild dependent views if needed."
                )

            elif chg.change_type == ChangeType.NULLABILITY_RELAXED:
                chg.severity = Severity.SAFE
                chg.risk_score = 15
                chg.affected_views = affected
                chg.auto_approved = True
                chg.recommendation = (
                    f"Nullability relaxed on '{chg.target_column}'; backward-compatible with target schema."
                )

            elif chg.change_type == ChangeType.RENAME_CANDIDATE:
                chg.severity = Severity.BREAKING
                chg.risk_score = 80 if not affected else 95
                chg.affected_views = affected
                chg.auto_approved = False
                if chg.source_column and chg.target_column:
                    suggested_mappings[chg.source_column] = chg.target_column
                chg.recommendation = (
                    f"Probable rename '{chg.target_column}' -> '{chg.source_column}' "
                    f"(similarity={chg.similarity_score}). Map '{chg.source_column}->{chg.target_column}' before merging."
                )
                recommendations.append(chg.recommendation)

            elif chg.change_type == ChangeType.DROP_COLUMN:
                chg.severity = Severity.BREAKING
                chg.risk_score = 95 if affected else 75
                chg.affected_views = affected
                chg.auto_approved = False
                view_note = f" Breaks view(s): {', '.join(affected)}." if affected else ""
                chg.recommendation = (
                    f"Source dropped '{chg.target_column}'.{view_note} Quarantine batch or approve soft-deprecation."
                )
                recommendations.append(chg.recommendation)

            elif chg.change_type == ChangeType.TYPE_NARROWING:
                chg.severity = Severity.BREAKING
                chg.risk_score = 85
                chg.affected_views = affected
                chg.auto_approved = False
                chg.recommendation = (
                    f"Lossy narrowing ({chg.old_type} -> {chg.new_type}) on '{chg.target_column}' risks truncation."
                )
                recommendations.append(chg.recommendation)

            elif chg.change_type == ChangeType.NULLABILITY_TIGHTENED:
                chg.severity = Severity.WARNING
                chg.risk_score = 50
                chg.affected_views = affected
                chg.auto_approved = False
                chg.recommendation = (
                    f"Nullability constraint tightened on '{chg.target_column}'; verify incoming batch has no NULLs."
                )
                recommendations.append(chg.recommendation)

            evaluated.append(chg)

        if not evaluated:
            recommendations.append("Schemas are synchronized. Safe to execute incremental watermark merge.")
        elif not recommendations:
            recommendations.append("All detected drift primitives are non-breaking and auto-approved for merge.")

        max_risk = max((c.risk_score for c in evaluated), default=0)
        if any(c.severity == Severity.BREAKING for c in evaluated):
            overall = Severity.BREAKING
        elif any(c.severity == Severity.WARNING for c in evaluated):
            overall = Severity.WARNING
        else:
            overall = Severity.SAFE

        can_auto = all(c.auto_approved for c in evaluated)
        return ImpactReport(
            table_name=table_name,
            total_changes=len(evaluated),
            overall_severity=overall,
            max_risk_score=max_risk,
            changes=evaluated,
            recommendations=recommendations,
            suggested_mappings=suggested_mappings,
            can_auto_merge=can_auto,
        )