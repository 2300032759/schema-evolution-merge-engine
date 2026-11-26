import time
import pytest
from backend.merge_engine import CrossWarehouseMergeEngine
from backend.models import MergePolicy, MergeRequest
from backend.seed_data import apply_drift_scenario, reset_and_seed


@pytest.fixture
def engine():
    eng = CrossWarehouseMergeEngine()
    eng.registry.reset()
    reset_and_seed(eng.wm)
    tgt = eng.wm.inspect_target_schema("orders")
    eng.registry.register_version("orders", tgt.columns, [{"event": "TEST_INIT"}])
    return eng


def test_t01_baseline_initial_merge(engine):
    audit = engine.execute_incremental_merge(MergeRequest(table_name="orders"))
    assert audit.status == "SUCCESS"
    assert audit.rows_read == 3
    assert audit.rows_inserted == 1
    assert audit.rows_updated == 2


def test_t02_safe_schema_evolution_auto_merge(engine):
    engine.execute_incremental_merge(MergeRequest(table_name="orders"))
    apply_drift_scenario("safe_evolution", engine.wm)

    _, _, impact = engine.analyze_table("orders")
    assert impact.can_auto_merge is True
    assert impact.overall_risk.value == "LOW"

    audit = engine.execute_incremental_merge(MergeRequest(table_name="orders"))
    assert audit.status == "SUCCESS"
    assert audit.schema_version == "v2.0.0"
    assert audit.rows_read == 2
    assert audit.rows_inserted == 1
    assert audit.rows_updated == 1


def test_t03_breaking_drift_quarantined_by_strict_policy(engine):
    engine.execute_incremental_merge(MergeRequest(table_name="orders"))
    apply_drift_scenario("breaking_drift", engine.wm)

    _, _, impact = engine.analyze_table("orders")
    assert impact.can_auto_merge is False
    assert "vw_regional_revenue" in impact.impacted_downstream_assets

    audit = engine.execute_incremental_merge(
        MergeRequest(table_name="orders", policy=MergePolicy.STRICT_BLOCK_BREAKING)
    )
    assert audit.status == "QUARANTINED_SCHEMA_DRIFT"
    assert audit.rows_quarantined >= 2


def test_t04_force_override_with_rename_mapping_and_rollback(engine):
    engine.execute_incremental_merge(MergeRequest(table_name="orders"))
    apply_drift_scenario("safe_evolution", engine.wm)
    engine.execute_incremental_merge(MergeRequest(table_name="orders"))

    apply_drift_scenario("breaking_drift", engine.wm)
    audit = engine.execute_incremental_merge(
        MergeRequest(table_name="orders", policy=MergePolicy.FORCE_OVERRIDE)
    )
    assert audit.status == "SUCCESS"

    rb = engine.rollback_to_version("orders", "v1.0.0")
    assert rb["restored_version"] == "v1.0.0"
    tgt_after = engine.wm.inspect_target_schema("orders")
    col_names = [c.name for c in tgt_after.columns]
    assert "loyalty_tier" not in col_names


def test_t05_benchmark_1000_row_incremental_merge(engine):
    with engine.wm.get_source_conn() as sconn:
        batch = [
            (2000 + i, f"user{i}@klef.edu.in", 500 + i, "AP-SOUTH", f"2026-11-22T12:{i//60:02d}:{i%60:02d}Z")
            for i in range(1000)
        ]
        sconn.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", batch)

    t0 = time.perf_counter()
    audit = engine.execute_incremental_merge(MergeRequest(table_name="orders"))
    elapsed_ms = (time.perf_counter() - t0) * 1000
    assert audit.status == "SUCCESS"
    assert audit.rows_inserted == 1001
    print(f"\n[BENCHMARK] 1,001 incremental rows merged in {elapsed_ms:.2f} ms")