"""
Pipeline Control Center — FastAPI Backend (Cloud Edition)
Runs on Railway / Fly.io. Replaces Airflow + MinIO with:
  - GitHub Actions  (scheduling + orchestration)
  - Cloudflare R2 / AWS S3 (storage)
  - Railway PostgreSQL (database)

Env vars (set as Railway service variables or GitHub Secrets):
  DB_DSN, POSTGRES_HOST/PORT/USER/PASSWORD/DB
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET,
  S3_ENDPOINT_URL, AWS_REGION
  GH_TOKEN, GH_REPO  (for pipeline status from GitHub API)
  STATUS_GIST_ID     (optional — Gist written by notify job)
"""

import os
import json
import subprocess
import asyncio
import urllib.request
from datetime import datetime, timezone
from typing import AsyncGenerator

import httpx
import psycopg2
from boto3 import client as boto3_client
from botocore.config import Config
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

app = FastAPI(
    title="Customer Analytics Lakehouse — Pipeline Control Center",
    description="Cloud-native REST API for monitoring and triggering the GHA-based pipeline.",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Config ─────────────────────────────────────────────────────────────────────
# Storage (Cloudflare R2 or AWS S3)
S3_BUCKET       = os.getenv("S3_BUCKET", "")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "")
AWS_ACCESS_KEY  = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY  = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION      = os.getenv("AWS_REGION", "auto")

# PostgreSQL (Railway)
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "analytics")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_DB   = os.getenv("POSTGRES_DB", "customer_analytics")
PG_DSN = (
    os.getenv("DB_DSN")
    or f"host={POSTGRES_HOST} port={POSTGRES_PORT} dbname={POSTGRES_DB} "
       f"user={POSTGRES_USER} password={POSTGRES_PASS}"
)

# GitHub (for triggering workflow_dispatch + reading run status)
GH_TOKEN = os.getenv("GH_TOKEN", "")
GH_REPO  = os.getenv("GH_REPO", "")          # e.g. "octocat/my-repo"

# Pipeline status Gist (written by the notify job)
STATUS_GIST_ID = os.getenv("STATUS_GIST_ID", "")

# Working directory inside the container
APP_ROOT = "/opt/app"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _s3_client():
    """Return a boto3 S3 client pointing at R2 or AWS."""
    return boto3_client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL or None,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
        config=Config(signature_version="s3v4"),
    )


def _pg_connect():
    return psycopg2.connect(PG_DSN)


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
    """Async generator that streams subprocess stdout line by line (SSE)."""
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


def _gh_api(path: str, method: str = "GET", body: dict | None = None) -> dict:
    """Call the GitHub REST API with the GH_TOKEN."""
    url = f"https://api.github.com{path}"
    data = json.dumps(body).encode() if body else None
    req  = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {GH_TOKEN}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except Exception as exc:
        return {"error": str(exc)}


# ── Status ────────────────────────────────────────────────────────────────────

@app.get("/status", summary="Health check for all cloud services")
async def get_status():
    services: dict = {}

    # PostgreSQL
    try:
        conn = _pg_connect()
        conn.close()
        services["postgres"] = "ok"
    except Exception:
        services["postgres"] = "offline"

    # S3 / R2
    try:
        _s3_client().list_buckets()
        services["s3"] = "ok"
    except Exception:
        services["s3"] = "offline"

    # GitHub Actions — last workflow run status
    if GH_TOKEN and GH_REPO:
        runs = _gh_api(
            f"/repos/{GH_REPO}/actions/workflows/lakehouse_pipeline.yml/runs?per_page=1"
        )
        run_list = runs.get("workflow_runs", [])
        if run_list:
            latest = run_list[0]
            services["last_pipeline_run"] = latest.get("conclusion") or latest.get("status")
            services["last_run_url"]      = latest.get("html_url")
        else:
            services["last_pipeline_run"] = "no_runs"
    else:
        services["last_pipeline_run"] = "gh_token_not_configured"

    # Pipeline status from Gist (written by notify job)
    if STATUS_GIST_ID and GH_TOKEN:
        gist = _gh_api(f"/gists/{STATUS_GIST_ID}")
        files = gist.get("files", {})
        if "pipeline_status.json" in files:
            raw = files["pipeline_status.json"].get("content", "{}")
            try:
                services["pipeline_status"] = json.loads(raw)
            except Exception:
                pass

    services["api"]       = "ok"
    services["timestamp"] = datetime.now(timezone.utc).isoformat()
    return services


# ── Pipeline Triggers (GHA workflow_dispatch) ─────────────────────────────────

@app.post("/pipeline/trigger", summary="Trigger the full GHA Lakehouse pipeline")
async def trigger_pipeline(run_full: bool = True):
    """Dispatches a workflow_dispatch event on the lakehouse_pipeline workflow."""
    if not GH_TOKEN or not GH_REPO:
        raise HTTPException(503, "GH_TOKEN and GH_REPO must be set to trigger pipelines.")
    result = _gh_api(
        f"/repos/{GH_REPO}/actions/workflows/lakehouse_pipeline.yml/dispatches",
        method="POST",
        body={
            "ref": "main",
            "inputs": {"run_full_pipeline": str(run_full).lower()},
        },
    )
    if "error" in result:
        raise HTTPException(500, detail=result["error"])
    return {"status": "dispatched", "repo": GH_REPO, "run_full_pipeline": run_full}


@app.post("/pipeline/spark", summary="Run PySpark pipeline (in-container, for local dev)")
def trigger_spark():
    return _run(["python3", "src/processing/spark_pipeline.py"])


@app.post("/pipeline/dbt", summary="Run dbt transformations (in-container)")
def trigger_dbt():
    return _run([
        "bash", "-c",
        "cd dbt && dbt deps --profiles-dir . && dbt run --profiles-dir . && dbt test --profiles-dir ."
    ])


@app.post("/pipeline/run-all", summary="Run full pipeline in-container (spark → dbt)")
def trigger_all():
    results = {}
    for label, cmd in [
        ("spark", ["python3", "src/processing/spark_pipeline.py"]),
        ("dbt",   ["bash", "-c",
                   "cd dbt && dbt deps --profiles-dir . && dbt run --profiles-dir . && dbt test --profiles-dir ."]),
    ]:
        result = _run(cmd)
        results[label] = result
        if not result["success"]:
            results["stopped_at"] = label
            break
    return results


# ── Log Streaming (SSE) ───────────────────────────────────────────────────────

@app.get("/logs/stream/spark", summary="Stream Spark job logs live")
async def stream_spark_logs():
    return StreamingResponse(
        _stream_cmd(["python3", "src/processing/spark_pipeline.py"]),
        media_type="text/event-stream",
    )


@app.get("/logs/stream/dbt", summary="Stream dbt logs live")
async def stream_dbt_logs():
    return StreamingResponse(
        _stream_cmd(["bash", "-c", "cd dbt && dbt run --profiles-dir ."]),
        media_type="text/event-stream",
    )


# ── GitHub Actions Run Status ─────────────────────────────────────────────────

@app.get("/github/runs", summary="Latest GHA pipeline run status")
async def github_run_status():
    if not GH_TOKEN or not GH_REPO:
        return {"error": "GH_TOKEN and GH_REPO not configured"}
    data = _gh_api(
        f"/repos/{GH_REPO}/actions/workflows/lakehouse_pipeline.yml/runs?per_page=5"
    )
    runs = data.get("workflow_runs", [])
    return {
        "runs": [
            {
                "run_id":     r.get("id"),
                "status":     r.get("status"),
                "conclusion": r.get("conclusion"),
                "started_at": r.get("run_started_at"),
                "url":        r.get("html_url"),
                "branch":     r.get("h