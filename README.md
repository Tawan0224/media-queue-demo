# Media Upload Queue Demo (RabbitMQ)

Project 02 by <Your Name> (<Student ID>).

A media sharing site used to convert videos while the user waited (3 to 8 minutes). This demo separates
**receiving** a file from **processing** it: the API saves the file, creates a `pending` job, puts a message
on a RabbitMQ queue and replies instantly. Background workers convert the video and update the status.

## Run it

Requirements: Docker Desktop (includes Docker Compose).

```bash
git clone <your-repo-url>
cd media-queue-demo
docker compose up --build
```

| What | URL |
|---|---|
| Upload page and job table | http://localhost:8000 |
| API docs (Swagger) | http://localhost:8000/docs |
| RabbitMQ dashboard (guest / guest) | http://localhost:15672 |

## Architecture

```mermaid
flowchart LR
    U[User browser] -->|POST /upload| API[FastAPI web server]
    API -->|save raw file| S[(Shared storage)]
    API -->|insert job: pending| DB[(PostgreSQL)]
    API -->|publish job message| Q[[RabbitMQ: video_jobs]]
    API -->|202 + job_id in under 1s| U
    Q -->|one job at a time| W1[Worker 1]
    Q --> W2[Worker 2]
    Q --> W3[Worker N]
    W1 -->|ffmpeg: thumbnail, 360p, 720p| S
    W1 -->|processing / completed| DB
    Q -.->|rejected after max retries| DLQ[[video_jobs_failed]]
    U -->|GET /jobs/id every 2s| API
```

## Job lifecycle

```mermaid
sequenceDiagram
    participant User
    participant API
    participant RabbitMQ
    participant Worker
    participant DB
    User->>API: POST /upload (video)
    API->>DB: INSERT job (pending)
    API->>RabbitMQ: publish {job_id, file_path} (persistent)
    API-->>User: 202 {job_id}
    RabbitMQ->>Worker: deliver job (prefetch=1)
    Worker->>DB: status = processing
    Worker->>Worker: ffmpeg convert
    alt success
        Worker->>DB: status = completed
        Worker->>RabbitMQ: ack (message deleted)
    else worker crashes before ack
        RabbitMQ->>Worker: redeliver same job to another worker
    else error, retries left
        Worker->>RabbitMQ: republish with retry count + 1, ack original
    else error, no retries left
        Worker->>DB: status = failed
        Worker->>RabbitMQ: nack (requeue=false) → video_jobs_failed
    end
    User->>API: GET /jobs/{id}
    API-->>User: current status
```

## How the proposal's promises map to code

| Promise | Implementation |
|---|---|
| Instant reply | `api/main.py` `upload()` saves, inserts, publishes, returns. No conversion. |
| Jobs survive a restart | Queue `durable=True`, messages `delivery_mode=2`, RabbitMQ data on a volume |
| Crashed worker's job is retried | `auto_ack=False`; `basic_ack` only after success |
| One job per worker | `basic_qos(prefetch_count=1)` |
| Failure queue | `x-dead-letter-exchange: dlx` + `basic_nack(requeue=False)` after `MAX_RETRIES` |
| Scale without touching the website | `docker compose up -d --scale worker=3` |

## Demo scenarios

See [`docs/STEP_BY_STEP_GUIDE.md`](docs/STEP_BY_STEP_GUIDE.md), Phase 4.

## Reset everything

```bash
docker compose down -v
```
