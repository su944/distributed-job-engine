#!/usr/bin/env python3
"""Real API -> CDC -> Kafka -> dispatcher -> receipt -> worker CLI smoke.

Run on a disposable configured CDC stack with DATABASE_URL and KAFKA_BROKERS.
Appends one unique demo job. Uses loopback ports 18084-18086, preserves data.
"""
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import urllib.request
import uuid


def get(url):
    with urllib.request.urlopen(url, timeout=3) as response:
        return response.read()


def wait_for(check, processes):
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        if any(p.poll() is not None for p in processes):
            raise RuntimeError("smoke process exited early")
        try:
            if check():
                return
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError("smoke condition timed out")


def main():
    if not os.environ.get("DATABASE_URL") or not os.environ.get("KAFKA_BROKERS"):
        raise RuntimeError("DATABASE_URL and KAFKA_BROKERS required; use disposable CDC stack")
    os.chdir(Path(__file__).resolve().parents[1])
    nonce = str(uuid.uuid4())
    base_env = dict(os.environ, JOB_NAMESPACE="smoke-" + nonce)
    processes = []
    with tempfile.TemporaryDirectory(prefix="job-worker-smoke-") as directory:
        subprocess.run(["go", "run", "./cmd/migrate"], env=base_env, check=True)
        binaries = {}
        for command in ("api", "dispatcher", "worker"):
            binaries[command] = str(Path(directory) / command)
            subprocess.run(["go", "build", "-o", binaries[command], "./cmd/" + command],
                           env=base_env, check=True)
        settings = {
            "api": {"HTTP_ADDR": "127.0.0.1:18084", "JOB_TYPES": "demo.echo"},
            "dispatcher": {"DISPATCH_HTTP_ADDR": "127.0.0.1:18086",
                           "DISPATCH_GROUP": "smoke-" + nonce},
            "worker": {"WORKER_HTTP_ADDR": "127.0.0.1:18085", "WORKER_ID": "smoke-" + nonce,
                       "WORKER_CAPACITY": "2", "WORKER_POLL_INTERVAL": "50ms"},
        }
        with (Path(directory) / "process.log").open("w") as log:
            try:
                for command, overrides in settings.items():
                    env = dict(base_env, **overrides)
                    processes.append(subprocess.Popen([binaries[command]], env=env,
                                                      stdout=log, stderr=log))
                for port in (18084, 18085, 18086):
                    wait_for(lambda port=port: get(f"http://127.0.0.1:{port}/health/ready"), processes)
                req = urllib.request.Request("http://127.0.0.1:18084/v1/jobs",
                    data=b'{"type":"demo.echo","payload":{"message":"worker-smoke"}}',
                    headers={"Content-Type": "application/json", "Idempotency-Key": "smoke-" + nonce})
                with urllib.request.urlopen(req, timeout=5) as response:
                    assert response.status == 202
                    job_id = json.load(response)["id"]
                wait_for(lambda: json.loads(get("http://127.0.0.1:18084/v1/jobs/" + job_id))["status"]
                         == "SUCCEEDED", processes)
                result = json.loads(get("http://127.0.0.1:18084/v1/jobs/" + job_id))
                assert result["attempts"] == 1
                wait_for(lambda: "worker_recorded_total 1" in
                         get("http://127.0.0.1:18085/metrics").decode(), processes)
                for process in processes:
                    process.send_signal(signal.SIGTERM)
                for process in processes:
                    assert process.wait(timeout=25) == 0
                print(f"worker CLI smoke: PASS job={job_id} status=SUCCEEDED attempts=1; SIGTERM exits=0")
            finally:
                for process in processes:
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=25)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()


if __name__ == "__main__":
    main()
