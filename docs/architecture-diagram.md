# Architecture diagrams

Proposed Phase 0 design. Standard Mermaid blocks are intended for GitHub Markdown.
All state changes are PostgreSQL transactions; dotted edges denote observations.

## Full system architecture

```mermaid
flowchart LR
    Client[REST client] --> API[Go API]
    API --> PG[(PostgreSQL jobs and attempts)]
    API --> Outbox[(Transactional outbox)]
    Outbox --> CDC[Debezium on Kafka Connect]
    CDC --> Kafka[Kafka jobs.ready.v1]
    Kafka --> Dispatcher[Go notification dispatcher]
    Dispatcher --> Receipts[(PostgreSQL receipts and quarantine)]
    Kafka -. Future worker group .-> Worker[Future bounded Go workers]
    Worker --> PG
    Worker --> Provider[External service with stable effect key]
    Scheduler[Recovery scheduler] --> PG
    Scheduler --> Outbox
    API -.-> Telemetry[Logs metrics traces]
    Worker -.-> Telemetry
    Scheduler -.-> Telemetry
    CDC -.-> Telemetry
```

Jobs and outbox are tables in the same PostgreSQL database. There is no atomic
transaction across PostgreSQL, Kafka, and the external provider.

## Job submission and transactional outbox

```mermaid
sequenceDiagram
    participant C as Client
    participant A as API
    participant D as PostgreSQL
    C->>A: POST job with Idempotency-Key
    A->>A: Validate and hash canonical request
    A->>D: BEGIN and insert unique submission key
    alt New key
        A->>D: Insert READY job and JobReady outbox row
        A->>D: COMMIT
        D-->>A: Durable commit acknowledged
        A-->>C: 202 and job Location
    else Existing key with same hash
        A->>D: Read existing job and end transaction
        A-->>C: 200 and existing job
    else Existing key with different hash
        A->>D: ROLLBACK
        A-->>C: 409 idempotency conflict
    end
    Note over C,D: Ambiguous commit requires retry with the same key
```

## CDC and Kafka dispatch

```mermaid
sequenceDiagram
    participant D as PostgreSQL WAL
    participant C as Debezium
    participant K as Kafka
    participant W as Go dispatcher
    participant J as Jobs database
    D->>C: Committed outbox insert
    C->>K: JobReady keyed by job ID
    Note over C,K: Connector replay may duplicate records
    K->>W: Poll within bounded capacity
    W->>J: Receipt or quarantine transaction
    alt Durable outcome committed
        J-->>W: Received, duplicate, or quarantined
        W->>K: Commit handled record offset plus one
    else DB unavailable or commit uncertain
        W->>W: Stop instance and replay on restart
        Note over W,K: Do not advance past unhandled offset
    end
    Note over W,J: No claim, state transition, or execution in M3
```

Malformed records follow durable quarantine before offset acknowledgement, as
specified in the architecture document; they never authorize execution.

## Worker job claiming and execution

```mermaid
sequenceDiagram
    participant W as Worker
    participant D as PostgreSQL
    participant H as Handler
    participant P as External provider
    W->>W: Reserve execution slot
    W->>D: Lock READY job and check due time and budget
    D->>D: Increment attempt and token, set lease, insert attempt
    D-->>W: Claim committed with token
    W->>H: Execute outside transaction with deadline
    par Handler execution
        H->>P: Stable job and effect idempotency key
        P-->>H: Result or unknown outcome
    and Lease maintenance
        W->>D: Renew only matching unexpired token
        D-->>W: Renewal result
    end
    H-->>W: Classified outcome
    W->>D: Fenced job and attempt completion transaction
    alt Still current and unexpired
        D-->>W: Outcome committed
    else Stale or expired
        D-->>W: Reject result
    end
    W->>W: Release slot
```

No execution occurs if claim fails. An ambiguous claim must be resolved by
reading the attempt identity before starting the handler.

## Job lifecycle state machine

```mermaid
stateDiagram-v2
    [*] --> READY: Submission commits
    READY --> RUNNING: Due claim with budget
    RUNNING --> SUCCEEDED: Fenced success
    RUNNING --> FAILED: Permanent or exhausted known failure
    RUNNING --> RETRY_WAIT: Safe transient failure or safe expiry
    RUNNING --> RECONCILE: Unknown unsafe outcome or unsafe expiry
    RETRY_WAIT --> READY: Due promotion plus outbox
    RECONCILE --> SUCCEEDED: Evidence proves success
    RECONCILE --> FAILED: Evidence proves failure
    RECONCILE --> RETRY_WAIT: Audited safe retry with budget
    SUCCEEDED --> [*]
    FAILED --> [*]
```

Expired replay-safe attempts with exhausted budgets transition to FAILED.
RUNNING transitions require current unexpired ownership except scheduler expiry
recovery. Terminal states do not accept claims or subsequent result updates.

## Lease expiry, recovery, and retries

```mermaid
flowchart TD
    Scan[Bounded scheduler scan with SKIP LOCKED] --> Expired{RUNNING lease expired?}
    Expired -->|Yes| Fence[Close attempt and increment token]
    Fence --> Safe{Registered replay safe?}
    Safe -->|No| Reconcile[RECONCILE and alert]
    Safe -->|Yes| Budget{Attempts remain?}
    Budget -->|No| Failed[FAILED]
    Budget -->|Yes| Wait[RETRY_WAIT with capped jitter]
    Expired -->|No| Due{RETRY_WAIT due?}
    Due -->|Yes| Promote[READY plus outbox in one transaction]
    Wait --> Later[Later scheduler pass]
    Later --> Due
    Due -->|No| Repair{READY dispatch overdue?}
    Repair -->|Yes| Emit[New outbox event and dispatch timestamp]
    Repair -->|No| End[End bounded pass]
    Promote --> CDC[CDC then Kafka then conditional claim]
    Emit --> CDC
```

## Graceful shutdown

```mermaid
sequenceDiagram
    participant O as Operating system
    participant S as Go service
    participant H as Active handlers
    participant D as PostgreSQL
    O->>S: SIGTERM
    S->>S: Fail readiness, stop intake and scheduler scans
    S->>S: Bound in-flight request and claim settlement
    loop Until drain deadline
        S->>D: Renew active leases within attempt deadlines
        H-->>S: Completed outcomes
        S->>D: Fenced completion commits
    end
    S->>H: Cancel remaining execution contexts
    S->>S: Stop renewals and close clients within deadline
    Note over H,D: Do not release ownership while side effects may continue
    S-->>O: Exit
    Note over D: Scheduler later recovers expired attempts
```
