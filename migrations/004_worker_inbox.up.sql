-- Receipt lookup and bounded READY selection for the worker inbox drain.
CREATE INDEX dispatch_events_job_id ON dispatch_events(job_id);
CREATE INDEX jobs_ready_inbox ON jobs(namespace,available_at,id) WHERE status='READY';
