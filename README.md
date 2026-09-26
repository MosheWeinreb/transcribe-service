# Transcribe Service

Upload audio/video or record in the browser to get a transcript using Whisper small. FastAPI accepts uploads while a separate worker handles transcription.

## Run

Start Docker Desktop with Linux containers (or Docker Engine with Compose), then run from the project folder:

**Windows / PowerShell**
```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run.ps1
```

**macOS / Linux**
```sh
sh ./scripts/run.sh
```

Open **http://localhost:8000**. Interactive API docs: **http://localhost:8000/docs**.

The script creates `.env` if missing, builds the image, and starts the services. The first transcription downloads and loads the model. No inference API key is required.

Edit [.env.example](.env.example)'s settings in your local `.env` to change limits. Defaults: 100 MiB, 10 minutes per recording, and one transcription worker. Multiple uploads can queue. Keep `uv.lock` committed so installations use the tested dependency versions.

```sh
docker compose logs -f api worker maintenance
docker compose stop
```

Stopping preserves stored data. Run the startup script again after code changes.

See [Design and limitations](docs/DESIGN.md) for the flow, API, deliberate shortcuts, and checks.
