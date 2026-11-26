from typing import Dict, List
from backend.models import ChangeType, ImpactSummary, RiskLevel, SchemaChange

# Simulated downstream lineage catalog mapping table.column -> dependent BI views/models
DOWNSTREAM_LINEAGE: Dict[str, List[str]] = {
    "orders.order_id": ["vw_regional_revenue", "ml_churn_features_v2", "fin_daily_reconciliation"],
    "orders.customer_email": ["crm_customer_360", "marketing_attribution_mart"],
    "orders.order_amount": ["vw_regional_revenue", "fin_daily_reconciliation", "exec_kpi_board"],
    "orders.region_code": ["vw_regional_revenue", "geo_supply_chain_cube"],
}


class RiskAndImpactAnalyzer:
    def evaluate(self, table_name: str, changes: List[SchemaChange]) -> ImpactSummary:
        all_impacted_assets = set()
        max_score = 0
        breaking_count = 0
        safe_count = 0

        for chg in changes:
            col_key = f"{table_name}.{chg.target_column or chg.source_column}"
            deps = DOWNSTREAM_LINEAGE.get(col_key, [])
            chg.impacted_dependencies = deps
            all_impacted_assets.update(deps)

            if chg.change_type == ChangeType.ADD_COLUMN:
                if chg.new_nullable:
                    chg.risk_level = RiskLevel.LOW
                    chg.risk_score = 15
                    chg.recommended_action = "Auto-apply ALTER TABLE ADD COLUMN (nullable)."
                    chg.auto_approved = True
                    safe_count += 1
                else:
                    chg.risk_level = RiskLevel.HIGH
                    chg.risk_score = 75
                    chg.recommended_action = "Supply default backfill value before adding NOT NULL column."
                    chg.auto_approved = False
                    breaking_count += 1

            elif chg.change_type == ChangeType.TYPE_WIDENING:
                chg.risk_level = RiskLevel.LOW
                chg.risk_score = 20
                chg.recommended_action = f"Safe lossless cast ({chg.old_type} -> {chg.new_type})."
                chg.auto_approved = True
                safe_count += 1

            elif chg.change_type == ChangeType.NULLABILITY_RELAXED:
                chg.risk_level = RiskLevel.LOW
                chg.risk_score = 10
                chg.recommended_action = "Safe constraint relaxation; auto-approved."
                chg.auto_approved = True
                safe_count += 1

            elif chg.change_type == ChangeType.RENAME_CANDIDATE:
                chg.risk_level = RiskLevel.MEDIUM if not deps else RiskLevel.HIGH
                chg.risk_score = 55 if not deps else 80
                chg.recommended_action = (
                    f"Map '{chg.source_column}' -> '{chg.target_column}' (similarity {chg.similarity_score}) "
                    "or approve alias migration."
                )
                chg.auto_approved = False
                breaking_count += 1

            elif chg.change_type == ChangeType.TYPE_NARROWING:
                chg.risk_level = RiskLevel.CRITICAL
                chg.risk_score = 92
                chg.recommended_action = "Quarantine: potential precision loss/truncation on cast."
                chg.auto_approved = False
                breaking_count += 1

            elif chg.change_type == ChangeType.NULLABILITY_TIGHTENED:
                chg.risk_level = RiskLevel.HIGH
                chg.risk_score = 78
                chg.recommended_action = "Validate zero NULL rows in target before tightening constraint."
                chg.auto_approved = False
                breaking_count += 1

            elif chg.change_type == ChangeType.DROP_COLUMN:
                chg.risk_level = RiskLevel.CRITICAL if deps else RiskLevel.HIGH
                chg.risk_score = 95 if deps else 75
                chg.recommended_action = (
                    f"BLOCK DROP: Column required by {len(deps)} downstream asset(s)."
                    if deps
                    else "Soft-deprecate column in target instead of hard drop."
                )
                chg.auto_approved = False
                breaking_count += 1

            if chg.risk_score > max_score:
                max_score = chg.risk_score

        if max_score >= 85:
            overall = RiskLevel.CRITICAL
        elif max_score >= 65:
            overall = RiskLevel.HIGH
        elif max_score >= 40:
            overall = RiskLevel.MEDIUM
        else:
            overall = RiskLevel.LOW

        return ImpactSummary(
            table_name=table_name,
            overall_risk=overall,
            max_risk_score=max_score,
            breaking_changes_count=breaking_count,
            safe_changes_count=safe_count,
            impacted_downstream_assets=sorted(all_impacted_assets),
            can_auto_merge=(breaking_count == 0),
            changes=changes,
        )