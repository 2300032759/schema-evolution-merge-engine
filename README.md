# Intelligent Schema-Evolution & Cross-Warehouse Merge Engine

**KL University (KLEF) — B.Tech Capstone Project (B23-SCHEMA EVOLUTION)**

## Overview
An automated cross-warehouse incremental merge engine that introspects source OLTP (SQLite) and target OLAP (DuckDB) schemas, classifies schema drift (`ADD_COLUMN`, `DROP_COLUMN`, `RENAME_CANDIDATE`, `TYPE_WIDENING`, `TYPE_NARROWING`, nullability changes), computes downstream dependency risk scores, enforces policy gates, maintains immutable SHA-256 schema versions with rollback support, and executes transactional watermark-based incremental `UPSERT` merges with full audit logging.

## Quick Start
```bash
python -m venv venv
.\venv\Scripts\activate
pip install --only-binary=:all: -r requirements.txt
python -m pytest -v -s
uvicorn backend.main:app --reload