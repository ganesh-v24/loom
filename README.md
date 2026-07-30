# Workflow Engine

A small, self-hosted workflow engine: define a business process as YAML,
submit instances via REST, watch them execute step by step — with retries, a
manual approval gate, and Saga-style compensation on failure — across four
independently-deployable services. Think "the part of Temporal or Camunda
that fits in a handful of Spring Boot apps."

For the full architecture rationale, design trade-offs, and interview Q&A,
see [`docs/INTERVIEW_GUIDE.md`](docs/INTERVIEW_GUIDE.md). This file only
covers how to set up and run it.

**There is no UI for creating workflows or starting instances.** The
dashboard (`localhost:8080/`) is read + approve-only — it lists instances,
shows step/audit history, and has an Approve button for paused ones.
Submitting a definition and starting an instance is REST-API-only; see
[Using it](#using-it-no-ui-for-this-part) below for the exact commands.

## Services

| Service | Port | Role |
|---|---|---|
| `api-service` | 8080 | REST API + dashboard: submit definitions, start/approve instances |
| `workflow-service` | 8081 | The state machine "brain": decides what happens next, drives Saga compensation |
| `worker-service` | 8082 | Executes step business logic and compensation actions |
| `notification-service` | 8083 | Handles `EMAIL_NOTIFY` steps independently of the general worker pool |

Only `api-service` serves HTTP — the other three are Kafka consumers with no
web server at all, so `curl localhost:8081` (etc.) is expected to fail to
connect; that's not a bug.

Each service has its own Postgres database (`api_db`, `workflow_db`,
`worker_db`, `notification_db` — one Postgres server, four logical
databases) and they communicate exclusively over Kafka — no service calls
another service's Java code or queries another service's tables; they only
share `workflow-shared` (the YAML parsing/validation library and the Kafka
event records). See the interview guide's Phase 4 section for how
cross-service reads work without shared tables.

---

## Setting up on a new machine, from zero

### Prerequisites

1. **JDK 21** — this project pins Java 21 in every module's `pom.xml`.
   ```powershell
   winget install EclipseAdoptium.Temurin.21.JDK
   ```
   After install, open a **new** terminal and confirm:
   ```powershell
   java -version
   ```
2. **Docker Desktop** — runs Postgres and Kafka. Nothing else needs Docker;
   the four services run as plain Java processes, not containers.
   ```powershell
   winget install Docker.DockerDesktop
   ```
   Launch Docker Desktop once from the Start menu and let it finish starting
   (whale icon in the system tray goes solid) before continuing.
3. **Git**, to clone the repo. (Maven is *not* required — the project ships
   its own wrapper, `mvnw`/`mvnw.cmd`, which downloads the right Maven
   version automatically the first time you run it.)

### Clone and build

```powershell
git clone https://github.com/ganesh-v24/loom.git
cd loom
.\mvnw.cmd install -DskipTests
```

This builds all five Maven modules (`workflow-shared` plus the four
services) and installs them into your local `~/.m2` repository. First run
will take a few minutes (downloading dependencies); later runs are fast.

### Start the infrastructure

```powershell
docker compose up -d
```

Starts Postgres (port 5432) and Kafka (port 9092). The four databases
(`api_db`, `workflow_db`, `worker_db`, `notification_db`) are created
automatically by `docker/init-databases.sql` — but **only on a fresh
Postgres volume**: Postgres skips init scripts if its data directory already
has data in it. If you ever need to start over completely (e.g. after
pulling schema-changing updates), reset both volumes first:

```powershell
docker compose down -v
docker compose up -d
```

Verify the databases exist:
```powershell
docker exec workflow-engine-postgres psql -U workflow -d postgres -c "\l"
```
You should see `api_db`, `workflow_db`, `worker_db`, `notification_db` in
the list.

### Start the four services

Each one runs in the foreground, so use **four separate terminals** (four
tabs in Windows Terminal works well). In each, from the repo root:

```powershell
.\mvnw.cmd -pl api-service spring-boot:run
```
```powershell
.\mvnw.cmd -pl workflow-service spring-boot:run
```
```powershell
.\mvnw.cmd -pl worker-service spring-boot:run
```
```powershell
.\mvnw.cmd -pl notification-service spring-boot:run
```

Each prints `Started ...Application in N seconds` when ready. Once
`api-service` is up, confirm with:
```powershell
curl http://localhost:8080/
```

To stop a service: `Ctrl+C` in its terminal. To restart after a code change:
rebuild once from the root (`.\mvnw.cmd install -DskipTests`), then in the
affected service's terminal, `Ctrl+C` and re-run the same `spring-boot:run`
command (up-arrow + Enter works).

To stop the infrastructure: `docker compose down` (add `-v` only if you
want to wipe all data and start fresh next time).

---

## Using it (no UI for this part)

Open the dashboard at **http://localhost:8080/** to *watch* things happen,
but every write action below is a REST call.

### 1. Submit a workflow definition

```powershell
$body = @{ name = "loan-approval"; body = Get-Content examples/loan-approval.yaml -Raw } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8080/api/workflow-definitions -Method Post -ContentType "application/json" -Body $body
```

Give it a moment before starting an instance against it — `workflow-service`
and `worker-service` each replicate the definition asynchronously from a
Kafka event, so starting an instance immediately can (rarely) race that
replication (see the interview guide's Phase 4 section for why this is an
accepted trade-off, not a bug).

To submit your **own** workflow instead of the example: write a YAML file
following the same shape as `examples/loan-approval.yaml` (steps, `type` of
`AUTOMATIC`/`MANUAL_APPROVAL`/`EMAIL_NOTIFY`, `next`, optional `retry` and
`compensate`), then submit it the same way with a different `name`/path.

### 2. Start an instance (happy path)

```powershell
Invoke-RestMethod -Uri http://localhost:8080/api/instances -Method Post -ContentType "application/json" -Body '{"definitionName":"loan-approval"}'
```

The response includes the instance `id`. Watch it on the dashboard — it runs
`submitLoan` → `validateDocuments` → `creditCheck` (retries once, by design,
to demonstrate the retry policy) → `fraudCheck`, then pauses at **Manager
Approval**.

### 3. Approve a paused instance

Click **Approve** on the dashboard (the one UI action that does exist), or:
```powershell
Invoke-RestMethod -Uri http://localhost:8080/api/instances/<id>/approve -Method Post
```

It continues through `disbursement` → `finalizeLoan` → `emailCustomer` and
completes.

### 4. Check status/history at any time

```powershell
Invoke-RestMethod -Uri http://localhost:8080/api/instances/<id>
Invoke-RestMethod -Uri http://localhost:8080/api/instances                 # list all
```
(Same data the dashboard shows, as JSON.)

### 5. Saga path: trigger compensation

Start an instance with `simulateFailure: true` in the payload:
```powershell
Invoke-RestMethod -Uri http://localhost:8080/api/instances -Method Post -ContentType "application/json" -Body '{"definitionName":"loan-approval","payload":{"simulateFailure":true}}'
```
Approve it the same way as step 3. `finalizeLoan` then fails immediately (no
retries). Since `disbursement` already succeeded and declares a `compensate`
handler, the instance moves to `COMPENSATING`, runs `reverseDisbursement`,
and lands on `COMPENSATED` — visible on the dashboard and in the instance's
audit log.

---

## Troubleshooting

- **A service fails to start with a Postgres `TimeZone` error** — already
  fixed for you (`-Duser.timezone=UTC` is baked into each service's
  `pom.xml`), but if you ever see it again after moving to a different JDK,
  it means the JVM's default timezone name isn't one this Postgres image's
  tzdata recognizes.
- **`ObjectMapper` bean not found** at startup — shouldn't happen on a clean
  checkout; if it does, check that `api-service`, `workflow-service`, and
  `worker-service` each still have their `JacksonConfig` class.
- **An instance seems permanently stuck** (status never changes) after you
  pulled new code that changes an event's fields — old messages already
  sitting in Kafka topics can be incompatible with the new code. Full reset
  fixes it: `docker compose down -v && docker compose up -d`, then resubmit
  your definition and start a fresh instance (old ones are lost, which is
  fine for local dev).
- **Want to inspect what's actually on a Kafka topic** (useful if something
  looks stuck): `docker exec workflow-engine-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <topic-name> --from-beginning --property print.headers=true`
- **Want to check a database table directly**: `docker exec workflow-engine-postgres psql -U workflow -d <api_db|workflow_db|worker_db|notification_db> -c "SELECT * FROM <table>;"`

## Tests

Per-module unit tests only (`.\mvnw.cmd test` from the root runs all of
them) — a full 4-service, 4-database, Kafka-backed integration test is
intentionally out of scope, see the interview guide for why:

- `workflow-shared`: `DefinitionValidatorTest` — rejects cycles, dangling
  `next` references, and `AUTOMATIC` steps missing a handler.
- `worker-service`: `WorkflowStepWorkerTest` — retry-via-republish, DLQ after
  exhausted attempts, success path, and idempotency guards.

## What's deliberately out of scope

A UI for creating definitions or starting instances, cron-triggered
workflows, parallel/conditional branching, workflow versioning in the
dashboard, a visual execution graph, Prometheus/Grafana metrics,
containerizing the services themselves, and the outbox pattern in any
service besides `workflow-service`. See the interview guide for the full
by-phase breakdown of what was built and why each boundary was drawn where it was.
