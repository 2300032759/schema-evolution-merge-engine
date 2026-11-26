import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from backend.merge_engine import CrossWarehouseMergeEngine
from backend.models import MergeRequest
from backend.seed_data import apply_drift_scenario, reset_and_seed


@asynccontextmanager
async def lifespan(app: FastAPI):
    engine = CrossWarehouseMergeEngine()
    if not engine.registry.list_versions("orders"):
        reset_and_seed(engine.wm)
        tgt = engine.wm.inspect_target_schema("orders")
        engine.registry.register_version("orders", tgt.columns, [{"event": "INITIAL_SEED_v1"}])
    yield


app = FastAPI(
    title="Intelligent Schema-Evolution & Cross-Warehouse Merge Engine",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

FRONTEND_INDEX = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend", "index.html")


@app.get("/")
def serve_ui():
    return FileResponse(FRONTEND_INDEX)


@app.get("/api/health")
def health_check():
    return {"status": "healthy", "service": "schema-evolution-merge-engine", "version": "1.0.0"}


@app.post("/api/reset")
def api_reset():
    engine = CrossWarehouseMergeEngine()
    engine.registry.reset()
    res = reset_and_seed(engine.wm)
    tgt = engine.wm.inspect_target_schema("orders")
    ver = engine.registry.register_version("orders", tgt.columns, [{"event": "BASELINE_RESET"}])
    return {**res, "active_version": ver.version_id}


@app.post("/api/simulate/{scenario}")
def api_simulate(scenario: str):
    try:
        return apply_drift_scenario(scenario)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/api/analyze/{table_name}")
def api_analyze(table_name: str = "orders"):
    engine = CrossWarehouseMergeEngine()
    try:
        src, tgt, impact = engine.analyze_table(table_name)
        return {
            "source_schema": src.model_dump(),
            "target_schema": tgt.model_dump(),
            "impact_analysis": impact.model_dump(),
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/merge")
def api_merge(req: MergeRequest):
    engine = CrossWarehouseMergeEngine()
    try:
        audit = engine.execute_incremental_merge(req)
        return audit.model_dump()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/rollback/{table_name}/{version_id}")
def api_rollback(table_name: str, version_id: str):
    engine = CrossWarehouseMergeEngine()
    try:
        return engine.rollback_to_version(table_name, version_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/api/state/{table_name}")
def api_state(table_name: str = "orders"):
    engine = CrossWarehouseMergeEngine()
    return {
        "versions": [v.model_dump() for v in engine.registry.list_versions(table_name)],
        "audit_logs": engine.registry.get_audit_logs(),
        "target_rows": engine.wm.fetch_target_rows(table_name),
        "watermark": engine.registry.get_watermark(table_name),
    }