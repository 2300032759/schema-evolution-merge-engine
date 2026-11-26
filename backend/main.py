import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from backend.demo_simulator import apply_drift_scenario
from backend.merge_engine import CrossWarehouseMergeEngine
from backend.models import MergeRequest
from backend.schema_registry import SchemaRegistry
from backend.warehouse_manager import WarehouseManager

FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")

wm = WarehouseManager()
registry = SchemaRegistry()
engine = CrossWarehouseMergeEngine(wm=wm, registry=registry)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        wm.inspect_source_schema("orders")
        wm.inspect_target_schema("orders")
    except Exception:
        wm.reset_demo_warehouses()
        registry.reset()
        tgt = wm.inspect_target_schema("orders")
        registry.register_version("orders", tgt.columns, [])
    yield


app = FastAPI(
    title="Intelligent Schema-Evolution & Cross-Warehouse Merge Engine",
    version="1.1.0",
    lifespan=lifespan,
)

app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


@app.get("/")
def serve_console():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))


@app.get("/api/health")
def health_check():
    return {"status": "healthy", "engine": "CrossWarehouseMergeEngine", "version": "1.1.0"}


@app.get("/api/inspect/{table_name}")
def inspect_table(table_name: str):
    try:
        src, tgt, impact = engine.analyze_table(table_name)
        return {
            "source_schema": src.model_dump(),
            "target_schema": tgt.model_dump(),
            "impact_report": impact.model_dump(),
            "active_version": registry.get_active_version(table_name),
            "watermark": registry.get_watermark(table_name),
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/merge")
def run_merge(req: MergeRequest):
    try:
        audit = engine.execute_incremental_merge(req)
        return audit.model_dump()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/api/versions/{table_name}")
def get_versions(table_name: str):
    return [v.model_dump() for v in registry.list_versions(table_name)]


@app.get("/api/versions/{table_name}/compare")
def compare_versions(
    table_name: str,
    v1: str = Query(..., description="Baseline version ID"),
    v2: str = Query(..., description="Comparison version ID"),
):
    versions = {v.version_id: v for v in registry.list_versions(table_name)}
    if v1 not in versions or v2 not in versions:
        raise HTTPException(status_code=404, detail="One or both version IDs not found.")

    ver1, ver2 = versions[v1], versions[v2]
    cols1 = {c.name: c for c in ver1.columns}
    cols2 = {c.name: c for c in ver2.columns}

    added = [c for c in cols2 if c not in cols1]
    removed = [c for c in cols1 if c not in cols2]
    type_changed = [
        {"column": c, "from": cols1[c].normalized_type, "to": cols2[c].normalized_type}
        for c in cols1
        if c in cols2 and cols1[c].normalized_type != cols2[c].normalized_type
    ]
    return {
        "table_name": table_name,
        "v1": ver1.model_dump(),
        "v2": ver2.model_dump(),
        "diff": {
            "added_columns": added,
            "removed_columns": removed,
            "type_changes": type_changed,
            "identical": ver1.schema_hash == ver2.schema_hash,
        },
    }


@app.post("/api/rollback/{table_name}/{version_id}")
def rollback_version(table_name: str, version_id: str):
    try:
        return engine.rollback_to_version(table_name, version_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/api/audit")
def get_audit_logs():
    return registry.list_audit_logs()


@app.get("/api/report/{table_name}")
def generate_automated_report(table_name: str):
    try:
        src, tgt, impact = engine.analyze_table(table_name)
        versions = [v.model_dump() for v in registry.list_versions(table_name)]
        audits = [a for a in registry.list_audit_logs() if a.get("table_name") == table_name]
        successful = [a for a in audits if a.get("status") == "SUCCESS"]
        avg_latency = round(sum(a["duration_ms"] for a in successful) / len(successful), 2) if successful else 0.0
        avg_dq = round(sum(a.get("data_quality_score", 100.0) for a in successful) / len(successful), 2) if successful else 100.0

        report = {
            "report_title": f"Automated Schema Governance & Merge Compliance Report — {table_name}",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "engine_version": "1.1.0",
            "summary_kpis": {
                "active_schema_version": versions[-1]["version_id"] if versions else "v1.0.0",
                "current_drift_severity": impact.overall_severity.value,
                "max_risk_score": impact.max_risk_score,
                "total_batches_executed": len(audits),
                "quarantined_batches": sum(1 for a in audits if a.get("status") == "QUARANTINED_SCHEMA_DRIFT"),
                "average_merge_latency_ms": avg_latency,
                "average_data_quality_score": avg_dq,
            },
            "recommendations": impact.recommendations,
            "suggested_mappings": impact.suggested_mappings,
            "source_schema": src.model_dump(),
            "target_schema": tgt.model_dump(),
            "version_history": versions,
            "audit_trail": audits,
        }
        return JSONResponse(
            content=report,
            headers={"Content-Disposition": f'attachment; filename="{table_name}_governance_report.json"'},
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/demo/scenario/{scenario_name}")
def trigger_demo_scenario(scenario_name: str):
    try:
        if scenario_name == "reset":
            wm.reset_demo_warehouses()
            registry.reset()
            tgt = wm.inspect_target_schema("orders")
            v1 = registry.register_version("orders", tgt.columns, [])
            return {"status": "reset_complete", "active_version": v1.version_id}
        return apply_drift_scenario(scenario_name, wm)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))