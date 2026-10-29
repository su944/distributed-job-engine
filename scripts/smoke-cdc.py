#!/usr/bin/env python3
"""Part 2 real-stack smoke; requires API_URL and configured disposable CDC stack.

Uses Kafka's bundled observer CLI, never the Go dispatcher. Appends test rows;
pauses/restarts the connector. Preserves volumes, slots and offsets.
"""
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request
import uuid

import importlib.util

spec = importlib.util.spec_from_file_location("connector", Path(__file__).with_name(
    "register-outbox-connector.py"))
connector = importlib.util.module_from_spec(spec)
spec.loader.exec_module(connector)
target_jobs = set()
compose = ["docker", "compose", "-p", os.environ.get("COMPOSE_PROJECT_NAME", "job-engine-m1"),
           "--profile", "cdc"]


def sql(statement):
    return subprocess.check_output(compose + ["exec", "-T", "postgres", "psql", "-X", "-qAt",
        "-v", "ON_ERROR_STOP=1", "-U", "jobengine", "-d", "jobengine"],
        input=statement, text=True).strip()


def submit():
    req = urllib.request.Request(os.environ["API_URL"].rstrip("/") + "/v1/jobs",
        data=b'{"type":"invoice.send","payload":{"secret":"must-not-reach-kafka"}}',
        headers={"Content-Type": "application/json", "Idempotency-Key": "cdc-" + str(uuid.uuid4())})
    with urllib.request.urlopen(req, timeout=10) as response:
        assert response.status == 202, response.status
        job = json.load(response)["id"]
    uuid.UUID(job)  # Only validated UUIDs are interpolated into test SQL.
    target_jobs.add(job)
    event = sql(f"SELECT id FROM public.outbox_events WHERE aggregateid='{job}';")
    uuid.UUID(event)
    return job, event


def observe():
    result = subprocess.run(compose + ["exec", "-T", "kafka",
        "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "kafka:9092",
        "--topic", "jobs.ready.v1", "--from-beginning", "--timeout-ms", "10000",
        "--property", "print.key=true", "--property", "print.headers=true"],
        capture_output=True, text=True, timeout=60)
    # Kafka's console consumer exits 0 after its idle timeout on this pinned image.
    if result.returncode != 0:
        raise RuntimeError("Kafka observation CLI failed")
    records = []
    for line in result.stdout.splitlines():
        headers, key, value = line.split("\t", 2)
        # Retained volumes may contain prior tests' intentional poison records.
        if key not in target_jobs:
            continue
        event = json.loads(value)
        assert set(event) == {"schema_version", "event_id", "job_id", "kind", "occurred_at"}
        assert event["kind"] == "JobReady" and event["schema_version"] == 1
        assert key == event["job_id"]
        assert "id:" + event["event_id"] in headers
        records.append(event)
    return records


def require_event(records, job, event, exact=False):
    matches = [r for r in records if r["event_id"] == event]
    assert (len(matches) == 1 if exact else len(matches) >= 1), len(matches)
    assert all(r["job_id"] == job for r in matches)
    print(f"event={event} job={job} records={len(matches)}")


def main():
    connector.wait_running()
    job, event = submit()
    time.sleep(2)
    require_event(observe(), job, event, exact=True)
    rolled = str(uuid.uuid4())
    barrier = str(uuid.uuid4())
    def insert(event_id):
        return ("INSERT INTO public.outbox_events(id,aggregatetype,aggregateid,type,payload) "
                f"SELECT '{event_id}',aggregatetype,aggregateid,type,"
                f"jsonb_set(payload,'{{event_id}}',to_jsonb('{event_id}'::text)) "
                f"FROM public.outbox_events WHERE id='{event}';")
    sql("BEGIN;" + insert(rolled) + "ROLLBACK;" + insert(barrier))
    time.sleep(2)
    records = observe()
    require_event(records, job, barrier, exact=True)
    assert not any(r["event_id"] == rolled for r in records)
    print("rollback absent after same-key committed WAL barrier: PASS")
    connector.request("PUT", "/pause")
    try:
        deadline = time.monotonic() + 30
        while True:
            status = connector.request("GET", "/status")
            if status["connector"]["state"] == "PAUSED" and all(
                    t["state"] == "PAUSED" for t in status["tasks"]):
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("pause timed out")
            time.sleep(0.5)
        backlog_job, backlog_event = submit()
    finally:
        connector.request("PUT", "/resume")
    connector.request("POST", "/restart?includeTasks=true&onlyFailed=false")
    connector.wait_running()
    time.sleep(2)
    records = observe()
    require_event(records, job, event)
    require_event(records, backlog_job, backlog_event)
    print("connector restart retained earlier event and delivered paused backlog: PASS")
    print("Part 2 CDC smoke: PASS (at-least-once; restart duplicates permitted)")


if __name__ == "__main__":
    main()
