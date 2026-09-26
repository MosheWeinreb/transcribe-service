# Design and limitations

## Flow

1. FastAPI validates limits and streams the upload to disk.
2. After saving the file and database job, it returns `202 Accepted` with a job ID.
3. Maintenance publishes pending IDs to RabbitMQ; a Celery worker claims each job.
4. FFmpeg decodes the media, Whisper transcribes it, and PostgreSQL stores the result.
5. The browser polls status and offers copy/download when complete.

The API serves the plain HTML/CSS/JavaScript UI. CPU-heavy transcription runs separately. SQLAlchemy manages database sessions; Pydantic validates configuration and response schemas. No frontend dependencies are required.

## Processes and recovery

Compose runs the API, worker, PostgreSQL, RabbitMQ, and maintenance. The `schema` container creates missing tables and exits successfully. FFmpeg runs inside the worker.

PostgreSQL owns job state. Maintenance handles pending publication, abandoned attempts, and expired files. This extra process lets accepted jobs survive a broker outage, at the cost of custom coordination code.

Temporary processing failures allow three total attempts by default, with 10- then 20-second backoff. Invalid media fails immediately. Worker crashes recover after a deadline. Conditional claims and attempt IDs prevent duplicate messages or stale workers from overwriting results; inference may repeat after a crash.

## API

| Endpoint | Purpose |
| --- | --- |
| `POST /api/jobs?filename=clip.wav` | Upload the raw binary file; returns a job ID and status URL |
| `GET /api/jobs/{id}` | Read `queued`, `processing`, `succeeded`, or `failed`, plus result/error |
| `GET /api/config` | Read public upload limits and UI timing |
| `GET /health/live` | Check that the API responds |
| `GET /health/ready` | Check the database schema and disk space |

Upload with `Content-Type: application/octet-stream`, not multipart. Upload percentage measures bytes sent; transcription shows stages, not an estimated percentage. Full schemas are at `/docs`.

## Deliberate limits

- Whisper small on CPU balances resources and accuracy; quality and sustained load have not been benchmarked.
- One job per browser tab; no batches, live transcript streaming, editing, or speaker labels.
- No user accounts, job ownership checks, or per-user rate limiting. Bind to localhost; add access controls before public use.
- Global limits bound uploads, queued jobs, and storage. Model files are cached; repeated uploads are not deduplicated.
- Shared local volumes suit one host. No automatic backups, high availability, or schema migrations.
- Files and results expire after 24 hours by default. Readiness does not verify model or worker availability.

## Checks

For development, install Python 3.12, uv, Node.js, and FFmpeg. Start PostgreSQL and RabbitMQ with Compose. Worker execution uses Linux containers.

```sh
docker compose up -d db rabbitmq
uv sync --locked
uv run ruff check .
uv run mypy
uv run python -m unittest discover -s tests -q
node tests/ui-state.mjs
```

Database tests create a separate temporary database; the configured database account needs permission to create databases. They do not clear application data. An optional real-model check is `uv run python scripts/smoke.py path/to/recording.wav` against a running service.
