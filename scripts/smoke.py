"""Opt-in, real-model check: python scripts/smoke.py path/to/recording.wav."""

import argparse
import json
import time
from pathlib import Path

import httpx


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", type=Path)
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument(
        "--credentials-file", type=Path, help="JSON file with username, password, and optional URL"
    )
    parser.add_argument("--timeout", type=int, default=1200)
    parser.add_argument("--expected", help="Optional text that must appear in the transcript")
    args = parser.parse_args()
    credentials = json.loads(args.credentials_file.read_text()) if args.credentials_file else None
    auth = (credentials["username"], credentials["password"]) if credentials else None
    url = credentials.get("url", args.url) if credentials else args.url
    with httpx.Client(base_url=url, auth=auth, timeout=120) as client:
        if credentials:
            for path in ("/", "/api/config", "/api/jobs/00000000-0000-0000-0000-000000000000"):
                assert client.get(path, auth=None).status_code == 401, path
            assert (
                client.get("/", auth=(credentials["username"], "wrong-password")).status_code == 401
            )
            assert (
                client.request(
                    "POST", "/api/jobs?filename=blocked.wav", content=b"x", auth=None
                ).status_code
                == 401
            )
            client.get("/").raise_for_status()
        config = client.get("/api/config").raise_for_status().json()
        # Measure upload-to-result latency separately from the access-control checks.
        started = time.monotonic()
        with args.file.open("rb") as recording:
            response = client.post(
                "/api/jobs",
                params={"filename": args.file.name},
                headers={"Content-Type": "application/octet-stream"},
                content=recording,
            )
        response.raise_for_status()
        job = response.json()
        print(json.dumps({"accepted": job["id"]}), flush=True)
        worst_health_seconds = 0.0
        while time.monotonic() - started < args.timeout:
            health_start = time.monotonic()
            client.get("/health/live").raise_for_status()
            worst_health_seconds = max(worst_health_seconds, time.monotonic() - health_start)
            result = client.get(job["status_url"]).raise_for_status().json()
            if result["status"] == "failed":
                raise AssertionError(result["error"])
            if result["status"] == "succeeded":
                if args.expected:
                    assert args.expected.casefold() in (result["transcript"] or "").casefold(), (
                        result
                    )
                print(
                    json.dumps(
                        {
                            "file": args.file.name,
                            "duration_seconds": result["duration_seconds"],
                            "elapsed_seconds": round(time.monotonic() - started, 2),
                            "max_health_latency_ms": round(worst_health_seconds * 1000),
                            "transcript": result["transcript"],
                        }
                    ),
                    flush=True,
                )
                return
            time.sleep(config["poll_interval_ms"] / 1000)
    raise TimeoutError("The job did not complete within the smoke-test timeout.")


if __name__ == "__main__":
    main()
