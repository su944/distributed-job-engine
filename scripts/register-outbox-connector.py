#!/usr/bin/env python3
"""Idempotently PUT the checked-in config, then require connector/task RUNNING."""
import json
import os
from pathlib import Path
import time
import urllib.request


def request(method, path, data=None):
    base = os.environ.get("CONNECT_URL", "http://127.0.0.1:18083").rstrip("/")
    req = urllib.request.Request(base + "/connectors/job-engine-outbox" + path,
                                 data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as response:
        body = response.read(65536)
        return json.loads(body) if body else None


def wait_running():
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        status = request("GET", "/status")
        tasks = status.get("tasks", [])
        if (status["connector"]["state"] == "RUNNING" and len(tasks) == 1
                and tasks[0]["state"] == "RUNNING"):
            return
        if any(t["state"] == "FAILED" for t in tasks):
            raise RuntimeError("connector task FAILED; inspect local Connect logs")
        time.sleep(1)
    raise RuntimeError("connector/task did not become RUNNING within 90s")


if __name__ == "__main__":
    config = Path(__file__).resolve().parents[1] / "deploy/debezium-outbox.json"
    request("PUT", "/config", config.read_bytes())
    wait_running()
    print("job-engine-outbox connector and task: RUNNING")
