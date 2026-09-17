"""
Background worker: takes one job at a time from RabbitMQ, converts the video with ffmpeg,
updates the job status, and acknowledges the message ONLY after success.
"""
import json
import os
import socket
import subprocess
import time

import pika
import psycopg2
from psycopg2.extras import Json

RABBITMQ_URL = os.environ["RABBITMQ_URL"]
DATABASE_URL = os.environ["DATABASE_URL"]
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
SLOW_MODE_SECONDS = int(os.getenv("SLOW_MODE_SECONDS", "0"))
OUTPUT_DIR = "/data/outputs"
QUEUE = "video_jobs"
WORKER_ID = socket.gethostname()  # container ID, so you can see which worker did the job


def log(text):
    print(f"[{WORKER_ID}] {text}", flush=True)


# ---------- Database ----------
def update_job(job_id, **fields):
    columns = ", ".join(f"{name} = %s" for name in fields)
    values = [Json(v) if isinstance(v, (dict, list)) else v for v in fields.values()]
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                f"UPDATE jobs SET {columns}, updated_at = now() WHERE id = %s",
                (*values, job_id),
            )
    finally:
        conn.close()


# ---------- RabbitMQ topology (must match api/main.py) ----------
def declare_topology(channel):
    channel.exchange_declare(exchange="dlx", exchange_type="direct", durable=True)
    channel.queue_declare(queue="video_jobs_failed", durable=True)
    channel.queue_bind(queue="video_jobs_failed", exchange="dlx", routing_key=QUEUE)
    channel.queue_declare(queue=QUEUE, durable=True, arguments={"x-dead-letter-exchange": "dlx"})


# ---------- Video processing ----------
def run_ffmpeg(args):
    result = subprocess.run(
        ["ffmpeg", "-y", "-loglevel", "error", *args], capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip()[-500:] or "ffmpeg failed")


def process_video(job_id, input_path):
    if not os.path.exists(input_path):
        raise RuntimeError(f"Input file not found: {input_path}")

    out_dir = os.path.join(OUTPUT_DIR, job_id)
    os.makedirs(out_dir, exist_ok=True)

    # Thumbnail
    run_ffmpeg(["-i", input_path, "-vf", "thumbnail,scale=320:-2", "-frames:v", "1",
                os.path.join(out_dir, "thumbnail.jpg")])
    outputs = {"thumbnail": f"/media/{job_id}/thumbnail.jpg"}

    # Renditions (add 1080 here if you want; it is slower)
    for height in (360, 720):
        name = f"{height}p.mp4"
        run_ffmpeg([
            "-i", input_path,
            "-vf", f"scale=-2:{height}",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "28",
            "-c:a", "aac", "-b:a", "96k",
            "-movflags", "+faststart",
            os.path.join(out_dir, name),
        ])
        outputs[f"{height}p"] = f"/media/{job_id}/{name}"
        log(f"job {job_id}: {height}p done")

    return outputs


# ---------- Message handler ----------
def on_message(channel, method, properties, body):
    try:
        message = json.loads(body)
        job_id = message["job_id"]
        file_path = message["file_path"]
    except (ValueError, KeyError):
        log("bad message, sending to failure queue")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    retry_count = int((properties.headers or {}).get("x-retry-count", 0))
    log(f"received job {job_id} (retry {retry_count}, redelivered={method.redelivered})")

    try:
        update_job(job_id, status="processing", attempts=retry_count + 1, worker=WORKER_ID)

        if SLOW_MODE_SECONDS:
            log(f"slow mode: sleeping {SLOW_MODE_SECONDS}s (kill me now to test redelivery)")
            time.sleep(SLOW_MODE_SECONDS)

        outputs = process_video(job_id, file_path)
        update_job(job_id, status="completed", outputs=outputs, error=None)

        # Only now does RabbitMQ delete the message
        channel.basic_ack(delivery_tag=method.delivery_tag)
        log(f"job {job_id} completed")

    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        log(f"job {job_id} failed: {error}")

        if retry_count < MAX_RETRIES:
            update_job(job_id, status="retrying", error=error)
            time.sleep(2)  # small back-off
            channel.basic_publish(
                exchange="",
                routing_key=QUEUE,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json",
                    headers={"x-retry-count": retry_count + 1},
                ),
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
        else:
            update_job(job_id, status="failed", error=error)
            # requeue=False + dead-letter exchange => message moves to video_jobs_failed
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            log(f"job {job_id} moved to failure queue")


def main():
    params = pika.URLParameters(RABBITMQ_URL)
    params.heartbeat = 600                  # long ffmpeg jobs block the connection
    params.blocked_connection_timeout = 300

    while True:
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            declare_topology(channel)
            channel.basic_qos(prefetch_count=1)  # one job per worker at a time
            channel.basic_consume(queue=QUEUE, on_message_callback=on_message, auto_ack=False)
            log("waiting for jobs")
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            log("cannot reach RabbitMQ, retrying in 5s")
            time.sleep(5)


if __name__ == "__main__":
    main()
