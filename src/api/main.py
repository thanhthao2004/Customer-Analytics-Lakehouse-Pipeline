"""
Pipeline Control Center — FastAPI Backend
Runs on Ubuntu VM port 8000. Acts as the unified REST controller
for all Lakehouse pipeline services (Airflow, MinIO, Postgres, Spark, dbt).
"""

import os
import subprocess
import asyncio
from datetime import datetime
from typing import AsyncGenerator

import httpx
import psycopg2
from boto3 import client as boto3_client
from botocore.config import Config
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

app = FastAPI(
    title="Customer Analytics Lakehouse — Pipeline Control Center",
    description="Unified API to monitor and trigger all pipeline stages.",
    version="1.0.0",
)

# Allow the static frontend (any origin) to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Config ────────────────────────────────────────────────────────────────────
AIRFLOW_URL    = os.getenv("AIRFLOW_URL",    "http://airflow-webserver:8080")
AIRFLOW_USER   = os.getenv("AIRFLOW_USER",   "admin")
AIRFLOW_PASS   = os.getenv("AIRFLOW_PASS",   "adminpassword")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "adminpassword")
PG_DSN         = os.getenv(
    "DB_DSN",
    "host=postgres port=5432 dbname=customer_analytics user=analytics password=analytics123",
)
APP_ROOT = "/opt/app"   # mounted project root inside Docker

# ── Helpers ───────────────────────────────────────────────────────────────────

def _s3_client():
    return boto3_client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _run(cmd: list[str]) -> dict:
    """Run a subprocess and return stdout + returncode."""
    result = subprocess.run(
        cmd, capture_output=True, text=True, cwd=APP_ROOT
    )
    return {
        "stdout": result.stdout,
        "stderr": result.stderr,
        "returncode": result.returncode,
        "success": result.returncode == 0,
    }


async def _stream_cmd(cmd: list[str]) -> AsyncGenerator[str, None]:
    """Async generator that streams subprocess stdout line by line (for SSE)."""
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=APP_ROOT,
    )
    assert proc.stdout
    async for line in proc.stdout:
        yield f"data: {line.decode(errors='replace').rstrip()}\n\n"
    await proc.wait()
    yield f"data: [EXIT:{proc.returncode}]\n\n"


# ── Status ────────────────────────────────────────────────────────────────────

@app.get("/status", summary="Health check for all services")
async def get_status():
    services = {}

    # Airflow
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(
                f"{AIRFLOW_URL}/api/v1/health",
                auth=(AIRFLOW_USER, AIRFLOW_PASS),
            )
        services["airflow"] = "ok" if r.status_code == 200 else "degraded"
    except Exception:
        services["airflow"] = "offline"

    # MinIO
    try:
        _s3_client().list_buckets()
        services["minio"] = "ok"
    except Exception:
        services["minio"] = "offline"

    # PostgreSQL
    try:
        conn = psycopg2.connect(PG_DSN)
        conn.close()
        services["postgres"] = "ok"
    except Exception:
        services["postgres"] = "offline"

    # Self
    services["api"] = "ok"
    services["timestamp"] = datetime.utcnow().isoformat()
    return services


# ── Pipeline Triggers ─────────────────────────────────────────────────────────

@app.post("/pipeline/upload", summary="Upload raw Parquet files to MinIO")
def trigger_upload():
    return _run(["python3", "src/ingestion/upload_to_minio.py"])


@app.post("/pipeline/spark", summary="Submit PySpark processing job")
def trigger_spark():
    return _run(["spark-submit", "src/processing/spark_pipeline.py"])


@app.post("/pipeline/dbt", summary="Run dbt transformations")
def trigger_dbt():
    return _run([
        "bash", "-c",
        "cd dbt && dbt deps --profiles-dir . && dbt run --profiles-dir . && dbt test --profiles-dir ."
    ])


@app.post("/pipeline/run-all", summary="Run the full pipeline (upload → spark → dbt)")
def trigger_all():
    results = {}
    for label, cmd in [
        ("upload", ["python3", "src/ingestion/upload_to_minio.py"]),
        ("spark",  ["spark-submit", "src/processing/spark_pipeline.py"]),
        ("dbt",    ["bash", "-c", "cd dbt && dbt deps --profiles-dir . && dbt run --profiles-dir . && dbt test --profiles-dir ."]),
    ]:
        result = _run(cmd)
        results[label] = result
        if not result["success"]:
            results["stopped_at"] = label
            break
    return results


# ── Log Streaming (Server-Sent Events) ────────────────────────────────────────

@app.get("/logs/stream/spark", summary="Stream Spark job logs live")
async def stream_spark_logs():
    return StreamingResponse(
        _stream_cmd(["spark-submit", "src/processing/spark_pipeline.py"]),
        media_type="text/event-stream",
    )


@app.get("/logs/stream/dbt", summary="Stream dbt logs live")
async def stream_dbt_logs():
    return StreamingResponse(
        _stream_cmd(["bash", "-c", "cd dbt && dbt run --profiles-dir ."]),
        media_type="text/event-stream",
    )


# ── Airflow DAG Status ────────────────────────────────────────────────────────

@app.get("/airflow/dag-status", summary="Latest DAG run status")
async def airflow_dag_status():
    dag_id = "customer_lakehouse_pipeline"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns?limit=1&order_by=-execution_date",
                auth=(AIRFLOW_USER, AIRFLOW_PASS),
            )
        data = r.json()
        runs = data.get("dag_runs", [])
        if not runs:
            return {"status": "no_runs", "dag_id": dag_id}
        latest = runs[0]
        return {
            "dag_id": dag_id,
            "status": latest.get("state"),
            "run_id": latest.get("dag_run_id"),
            "start_date": latest.get("start_date"),
            "end_date": latest.get("end_date"),
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@app.post("/airflow/trigger", summary="Manually trigger the Airflow DAG")
async def trigger_dag():
    dag_id = "customer_lakehouse_pipeline"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(
                f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
                auth=(AIRFLOW_USER, AIRFLOW_PASS),
                json={"conf": {}},
            )
        return r.json()
    except Exception as e:
        return {"status": "error", "detail": str(e)}


# ── MinIO Bucket Info ─────────────────────────────────────────────────────────

@app.get("/minio/buckets", summary="List MinIO bucket contents summary")
def minio_buckets():
    try:
        s3 = _s3_client()
        buckets = s3.list_buckets().get("Buckets", [])
        result = []
        for bucket in buckets:
            name = bucket["Name"]
            try:
                paginator = s3.get_paginator("list_objects_v2")
                count = sum(
                    page.get("KeyCount", 0)
                    for page in paginator.paginate(Bucket=name)
                )
            except Exception:
                count = -1
            result.append({"bucket": name, "file_count": count})
        return {"buckets": result}
    except Exception as e:
        return {"error": str(e)}


# ── PostgreSQL Metrics ────────────────────────────────────────────────────────

@app.get("/postgres/metrics", summary="Row counts and sentiment summary from PostgreSQL")
def postgres_metrics():
    try:
        conn = psycopg2.connect(PG_DSN)
        cur = conn.cursor()

        metrics = {}

        cur.execute("SELECT COUNT(*) FROM staging.reviews")
        metrics["staging_total_rows"] = cur.fetchone()[0]

        cur.execute("""
            SELECT sentiment_label, COUNT(*) AS cnt
            FROM staging.reviews
            GROUP BY sentiment_label
        """)
        metrics["sentiment_breakdown"] = {row[0]: row[1] for row in cur.fetchall()}

        cur.execute("""
            SELECT platform, COUNT(*) AS cnt
            FROM staging.reviews
            GROUP BY platform
        """)
        metrics["platform_breakdown"] = {row[0]: row[1] for row in cur.fetchall()}

        cur.execute("SELECT MAX(created_at) FROM staging.reviews")
        last = cur.fetchone()[0]
        metrics["last_ingest"] = last.isoformat() if last else None

        conn.close()
        return metrics
    except Exception as e:
        return {"error": str(e)}
