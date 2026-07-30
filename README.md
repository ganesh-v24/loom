# Workflow Engine

A small, self-hosted workflow engine: define a business process as YAML,
submit instances via REST, watch them execute step by step — with retries, a
manual approval gate, and Saga-style compensation on failure — across four
independently-deployable services. Think "the part of Temporal or Camunda
that fits in a handful of Spring Boot apps."

For the full architecture rationale, design trade-offs, and interview Q&A,
see [`docs/INTERVIEW_GUIDE.md`](docs/INTERVIEW_GUIDE.md). This file only
covers how to run it.

## Services

| Service | Port | Role |
|---|---|---|
| `api-service` | 8080 | REST API + dashboard: submit definitions, start/approve instances |
| `workflow-service` | 8081 | The state machine "brain": decides what happens next, drives Saga compensation |
| `worker-service` | 8082 | Executes step business logic and compensation actions |
| `notification-service` | 8083 | Handles `EMAIL_NOTIFY` steps independently of the general worker pool |

Each service has its own Postgres database (`api_db`, `workflow_db`,
`worker_db`, `notification_db` — one Postgres server, four logical
databases) and they communicate exclusively over Kafka — no service calls
another service's Java code or queries another service's tables; they only
share `workflow-shared` (the YAML parsing/validation library and the Kafka
event records). See the interview guide's Phase 4 section for how
cross-service reads work without shared tables.

## Running it

Infra only — the four services themselves are plain `mvnw spring-boot:run`,
not containerized (keeps local iteration fast):

```bash
docker compose up -d          # Postgres on 5432, Kafka on 9092
```

The four databases are created by `docker/init-databases.sql`, which only
runs on a **fresh** Postgres volume (Postgres skips init scripts if
`/var/lib/postgresql/data` already has data in it). If you're upgrading from
an earlier phase's `docker compose up`, reset the volume first:
`docker compose down -v`.

Build once from the reactor root, then start each service in its own
terminal:

```bash
./mvnw install -DskipTests
```

```bash
./mvnw -pl api-service spring-boot:run
./mvnw -pl workflow-service spring-boot:run
./mvnw -pl worker-service spring-boot:run
./mvnw -pl notification-service spring-boot:run
```

Open the dashboard at http://localhost:8080/.

### Submit the example workflow

```powershell
$body = @{ name = "loan-approval"; body = Get-Content examples/loan-approval.yaml -Raw } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8080/api/workflow-definitions -Method Post -ContentType "application/json" -Body $body
```

Give it a moment before starting an instance against it — `workflow-service`
and `worker-service` each replicate the definition asynchronously from a
Kafka event, so starting an instance immediately can (rarely) race that
replication. See the interview guide's Phase 4 section for why this is an
accepted trade-off rather than a bug.

### Happy path: start an instance

```powershell
Invoke-RestMethod -Uri http://localhost:8080/api/instances -Method Post -ContentType "application/json" -Body '{"definitionName":"loan-approval"}'
```

The response includes the instance `id`. Watch it on the dashboard — it runs
`submitLoan` → `validateDocuments` → `creditCheck` (retries once, by design,
to demonstrate the retry policy) → `fraudCheck`, then pauses at **Manager
Approval**. Click **Approve** on the dashboard, or:

```powershell
Invoke-RestMethod -Uri http://localhost:8080/api/instances/<id>/approve -Method Post
```

It continues through `disbursement` → `finalizeLoan` → `emailCustomer` and
completes.

### Saga path: trigger compensation

Start an instance with `simulateFailure: true` in the payload:

```powershell
Invoke-RestMethod -Uri http://localhost:8080/api/instances -Method Post -ContentType "application/json" -Body '{"definitionName":"loan-approval","payload":{"simulateFailure":true}}'
```

After approval, `finalizeLoan` fails immediately (no retries). Since
`disbursement` already succeeded and declares a `compensate` handler,
the instance moves to `COMPENSATING`, runs `reverseDisbursement`, and lands on
`COMPENSATED` — visible on the dashboard and in the instance's audit log.

## Tests

Per-module unit tests only (`./mvnw test` from the root runs all of them) —
a full 4-service, 4-database, Kafka-backed integration test is intentionally
out of scope, see the interview guide for why:

- `workflow-shared`: `DefinitionValidatorTest` — rejects cycles, dangling
  `next` references, and `AUTOMATIC` steps missing a handler.
- `worker-service`: `WorkflowStepWorkerTest` — retry-via-republish, DLQ after
  exhausted attempts, success path, and idempotency guards.

## What's deliberately out of scope

Cron-triggered workflows, parallel/conditional branching, workflow versioning
in the dashboard, a visual execution graph, Prometheus/Grafana metrics,
containerizing the services themselves, and the outbox pattern in any
service besides `workflow-service`. See the interview guide for the full
by-phase breakdown of what was built and why each boundary was drawn where it was.
