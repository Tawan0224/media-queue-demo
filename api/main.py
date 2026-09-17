"""
API service: receives uploads, creates a job record, publishes a message to RabbitMQ,
and replies immediately. It never converts video itself.
"""
import json
import os
import shutil
import uuid
from contextlib import asynccontextmanager

import pika
import psycopg2
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from psycopg2.extras import RealDictCursor

RABBITMQ_URL = os.environ["RABBITMQ_URL"]
DATABASE_URL = os.environ["DATABASE_URL"]
UPLOAD_DIR = "/data/uploads"
OUTPUT_DIR = "/data/outputs"
QUEUE = "video_jobs"

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ---------- Database helpers ----------
def run_sql(sql, params=(), fetch=None):
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            if fetch == "one":
                return cur.fetchone()
            if fetch == "all":
                return cur.fetchall()
    finally:
        conn.close()


def init_db():
    run_sql(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id          TEXT PRIMARY KEY,
            filename    TEXT NOT NULL,
            status      TEXT NOT NULL,          -- pending | processing | retrying | completed | failed
            attempts    INTEGER NOT NULL DEFAULT 0,
            worker      TEXT,
            error       TEXT,
            outputs     JSONB,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


# ---------- RabbitMQ helpers ----------
def declare_topology(channel):
    """Must match worker.py exactly (same names and arguments)."""
    channel.exchange_declare(exchange="dlx", exchange_type="direct", durable=True)
    channel.queue_declare(queue="video_jobs_failed", durable=True)
    channel.queue_bind(queue="video_jobs_failed", exchange="dlx", routing_key=QUEUE)
    channel.queue_declare(
        queue=QUEUE,
        durable=True,  # queue survives a RabbitMQ restart
        arguments={"x-dead-letter-exchange": "dlx"},  # rejected messages go to the failure queue
    )


def publish_job(message: dict):
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    try:
        channel = connection.channel()
        declare_topology(channel)
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # persistent message: written to disk
                content_type="application/json",
                headers={"x-retry-count": 0},
            ),
        )
    finally:
        connection.close()


# ---------- App ----------
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Media Upload Queue Demo", lifespan=lifespan)
app.mount("/media", StaticFiles(directory=OUTPUT_DIR), name="media")


@app.get("/")
def index():
    return FileResponse("static/index.html")


@app.post("/upload", status_code=202)
def upload(file: UploadFile = File(...)):
    job_id = str(uuid.uuid4())
    safe_name = os.path.basename(file.filename or "upload")
    path = os.path.join(UPLOAD_DIR, f"{job_id}_{safe_name}")

    # 1. Save the raw file only
    with open(path, "wb") as out:
        shutil.copyfileobj(file.file, out)

    # 2. Create the job record
    run_sql(
        "INSERT INTO jobs (id, filename, status) VALUES (%s, %s, 'pending')",
        (job_id, safe_name),
    )

    # 3. Put the job in the queue
    publish_job({"job_id": job_id, "file_path": path})

    # 4. Reply immediately
    return {"job_id": job_id, "status": "pending"}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = run_sql("SELECT * FROM jobs WHERE id = %s", (job_id,), fetch="one")
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.get("/jobs")
def list_jobs():
    return run_sql("SELECT * FROM jobs ORDER BY created_at DESC LIMIT 20", fetch="all")
