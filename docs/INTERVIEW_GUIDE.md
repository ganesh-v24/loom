# Workflow Engine — Interview Guide

## How to use this doc

Read the narrative ("What & why") to rebuild the mental model before an
interview. Use the Q&A to rehearse specific answers — each one is grounded in
an actual file/method in this repo, not a generic textbook answer, so you can
open the file and point at the line if asked to go deeper. This file is
updated at the end of every phase; README.md stays separate and only covers
how to run the app.

---

## Phase 1 — In-process sequential engine

### What & why

The engine's job is to execute a workflow *definition* (YAML: an ordered list
of steps with a `start` pointer and per-step `next`) against a *workflow
instance* (one running execution of that definition), persisting every state
transition so the process survives restarts and produces an audit trail.

Three packages carry the weight:

- **`definition/`** — `WorkflowDefinitionSpec`/`StepSpec` are plain Java
  records parsed from YAML via Jackson (`DefinitionParser`). `DefinitionValidator`
  rejects a broken definition (duplicate step ids, a `next` or `start`
  pointing at a step that doesn't exist, an `AUTOMATIC` step with no handler,
  or a cycle) *before* any instance can be created from it — validation is a
  one-time cost at submit time, not something re-checked on every execution.
- **`persistence/`** — four JPA entities: `WorkflowDefinitionEntity` (the
  YAML, versioned by auto-incrementing per name), `WorkflowInstanceEntity`
  (status + `currentStepId`, the state machine's position), `StepExecutionEntity`
  (one row per attempt — this is what the retry count and audit trail are
  built from), `AuditLogEntryEntity` (append-only, human-readable event log).
- **`engine/handlers/`** — one `@Component("beanName")` per business step
  (`validateDocuments`, `creditCheck`, etc.), each implementing `BusinessStepHandler.execute(StepContext) -> StepResult`.
  This is the extension point: adding a new kind of step to a real workflow
  means adding one small class here, nothing else.

Phase 1's dispatch mechanism (since replaced in Phase 2, see below) was a
`WorkflowExecutor` looping on a `ThreadPoolTaskExecutor`: multiple instances
ran concurrently, one instance's steps ran strictly in order, retries were a
`for` loop with backoff between attempts, and a `MANUAL_APPROVAL` step simply
set the instance to `WAITING_APPROVAL` and stopped the loop until
`POST /api/instances/{id}/approve` resumed it.

### Likely questions & how to answer them

**"Why validate the definition before running it, instead of failing at the
broken step when you get there?"**
Two reasons. First, a workflow might not reach a broken step for hours (a
manual approval sitting in someone's queue) — you want to know the whole
definition is well-formed at submit time, not discover a dangling reference
mid-flight in production. Second, validation is O(steps), done once, versus
paying a mental cost every time you reason about "can this instance get
stuck." See `DefinitionValidator.validate()` — it does three passes:
existence checks, then a walk from `start` following `next` with a visited
set to catch cycles.

**"Why records for `StepSpec`/`WorkflowDefinitionSpec` instead of classes?"**
They're pure data parsed from YAML with no identity and no mutation after
parsing — records give you `equals`/`hashCode`/`toString` for free and make
immutability the default, which matters here because `WorkflowStepWorker` and
`WorkflowCoordinator` both read the *same* parsed spec from a shared cache
(`WorkflowSpecLoader`) — if it were mutable, one path accidentally mutating it
would corrupt the other's view.

**"What happens if two different steps have the same id?"**
Rejected at validation: `DefinitionValidator` builds a `Map<String, StepSpec>`
with `putIfAbsent` and throws `DefinitionValidationException` on the first
duplicate, before the definition is ever persisted.

**"Why is retry per-step configuration instead of global?"**
Different steps have wildly different failure profiles — a local validation
step probably shouldn't retry at all (`RetrySpec.DEFAULT` = 1 attempt), while
a call to an external credit bureau is exactly the kind of thing that's worth
retrying with backoff. Hardcoding one global policy would either retry things
that will never succeed on retry, or fail to retry things that transiently
fail. See `examples/loan-approval.yaml`'s `creditCheck` step for the
`retry: { maxAttempts: 3, backoffMs: 500 }` shape.

**"Why did you separate `BusinessStepHandler` implementations from the
engine that calls them?"** This paid off directly in Phase 2: the Kafka
migration only touched *how a step gets triggered* — every handler in
`engine/handlers/` is untouched, because they only ever see a `StepContext`
and return a `StepResult`, with zero knowledge of whether they were invoked
from a thread pool or a Kafka listener.

---

## Phase 2 — Kafka-driven step dispatch

### What & why

Phase 2 replaces *only the dispatch transport* — how "run this step next"
gets from one point in the code to another — with Kafka events. The business
logic (handlers, validation, entities) is unchanged; this is the point worth
making in an interview: decoupling transport from business logic is exactly
why this migration was low-risk, and you can prove it by diffing
`engine/handlers/`, `definition/`, `persistence/` between the two phases and
showing they didn't change.

```
InstanceService.startInstance() / approveInstance()
        │
        ▼
WorkflowCoordinator.beginOrResumeAt(instance, spec)
        │  MANUAL_APPROVAL? → pause (WAITING_APPROVAL), stop here.
        │  otherwise → WorkflowEventProducer.requestStepExecution(...)
        ▼
   Kafka topic: workflow.step.execute   (3 partitions, key = instanceId)
        │
        ▼
WorkflowStepWorker.onStepExecutionRequested   (@KafkaListener, concurrency=3)
        │  idempotency guards (see below), then executes the step handler
        │  success            → publishStepCompleted(success=true)
        │  fail, retries left → requestStepExecution(attempt+1)   [republish]
        │  fail, exhausted    → publishToDlq(...) + publishStepCompleted(success=false)
        ▼
   Kafka topic: workflow.step.completed (3 partitions, key = instanceId)
        │
        ▼
WorkflowCoordinator.onStepExecutionCompleted   (@KafkaListener)
        │  idempotency guards, then persists StepExecutionEntity/audit,
        │  decides: next step? pause for approval? COMPLETED? FAILED?
        ▼
   loops back to beginOrResumeAt(...) for the next step, or stops
```

Every event carries `instanceId` as the Kafka **partition key** — this is
the concrete mechanism that gives per-instance ordering *and* cross-instance
concurrency: all events for one instance land on the same partition (so they
process in order), while different instances spread across partitions and
get handled by different consumer threads in the `concurrency = "3"` listener
pool.

### Key design decisions

**1. Retry via republish, not a loop.** `WorkflowStepWorker` doesn't retry
in a `for` loop anymore — a failed attempt publishes a *new*
`StepExecutionRequested` with `attempt + 1` back onto the same topic. This
makes every retry a real, observable Kafka message (you can watch it in a
consumer console) and means a crashed worker doesn't lose the retry — the
message is just redelivered to another consumer in the group.

**2. Two different dead-letter mechanisms — know which is which:**
- **Business DLQ** (`workflow.step.dlq`, `WorkflowEventProducer.publishToDlq`):
  our own code publishes here when a step exhausts its *configured* retry
  policy. This is an expected business outcome (the credit bureau really is
  down) — it's data, not a bug.
- **Infra DLQ** (`KafkaTopicConfig.kafkaErrorHandler`, a `DefaultErrorHandler`
  + `DeadLetterPublishingRecoverer` → publishes to `<topic>.DLT`): catches
  *unexpected* exceptions escaping a `@KafkaListener` method entirely — a bug,
  a bad deserialization — that have nothing to do with the workflow's own
  retry policy. Spring Boot auto-detects a single `CommonErrorHandler`/
  `DefaultErrorHandler` bean in the context and wires it into the
  auto-configured listener container factory automatically (verified against
  Spring Boot's `KafkaAnnotationDrivenConfiguration`, which injects it via
  `ObjectProvider<CommonErrorHandler>`) — no extra container wiring needed.

**3. Idempotent consumers, not exactly-once.** Kafka's default delivery is
at-least-once, so a message can be redelivered (consumer crash after
processing, before offset commit). Both listeners check persisted state
before acting instead of reaching for Kafka transactions:
- `WorkflowStepWorker`: bails if the instance isn't `RUNNING`, isn't still on
  this `stepId`, or if a `StepExecutionEntity` for this exact
  (`stepId`, `attempt`) already shows `SUCCEEDED` (see `alreadySucceeded()`).
- `WorkflowCoordinator`: bails if the instance isn't `RUNNING`, or isn't still
  on this `stepId` (meaning a *different* completed-event already advanced
  it — this event is a stale duplicate).

**4. One shared "what happens at this step" decision.** Three different call
sites — starting an instance, approving a paused one, and advancing after a
step completes — all need to answer "is the step I just arrived at manual or
automatic?" That logic lives in exactly one place,
`WorkflowCoordinator.beginOrResumeAt(instance, spec)`, called by
`InstanceService.startInstance`, `InstanceService.approveInstance`, and
`WorkflowCoordinator.onStepExecutionCompleted` itself — so the pause/resume
check can't drift out of sync between the three paths.

### Likely questions & how to answer them

**"Why Kafka instead of just calling the next step's handler directly?"**
For a single instance on a single node, you wouldn't need to. The point is
what it buys you once you have many instances and want workers to scale
independently of the API layer: the producer (whatever decides "run this
step") and the consumer (whatever actually runs it) are now different
processes that can be deployed, scaled, and restarted independently — which
is exactly what Phase 3's service split needs, and Phase 2 proves the event
contract works before paying the cost of separate deployables.

**"How do you get per-instance ordering with concurrent processing?"**
Partition key = `instanceId` (see every `kafkaTemplate.send(topic, instanceId.toString(), event)`
call in `WorkflowEventProducer`). Kafka guarantees ordering *within* a
partition, so all of one instance's events are ordered, while the 3 partitions
on each topic let unrelated instances process in parallel.

**"What happens if the same event is delivered twice?"**
Walk through a concrete case: `WorkflowStepWorker` processes a
`StepExecutionRequested`, successfully runs the handler, publishes
`StepExecutionCompleted`, but crashes before committing its consumer offset.
The message redelivers. `alreadySucceeded()` finds the `StepExecutionEntity`
already marked `SUCCEEDED` for that exact attempt and returns early — no
double execution, no duplicate `StepExecutionCompleted`.

**"Difference between your business DLQ and Spring Kafka's DLT?"**
Answered above in decision #2 — business DLQ is an expected outcome from our
own retry policy; the DLT is Spring Kafka's safety net for exceptions we
didn't anticipate at all.

**"Why not use Kafka transactions / exactly-once semantics here?"**
Exactly-once would add real operational complexity (transactional producers,
read-committed consumers) to solve a problem the idempotency checks already
solve more cheaply. It would be worth it if consumers had side effects that
couldn't be made idempotent (e.g., actually charging a credit card inside the
listener) — worth naming as the condition under which you'd reach for it.

**"How would this scale to 10x traffic?"** Increase partition count on both
topics (more parallelism ceiling), increase `concurrency` on
`WorkflowStepWorker`'s `@KafkaListener` up to the partition count (more
consumer threads per instance), and — this is the Phase 3 answer — split
`WorkflowStepWorker` into its own deployable so worker capacity scales
independently of the REST API.

**"What didn't change when you migrated to Kafka?"** `engine/handlers/*`,
`definition/*`, `persistence/*`, all the `api/dto/*` records, and the
dashboard templates — worth naming explicitly, since it's the evidence that
the transport/business-logic separation from Phase 1 actually held up under
a real architecture change, not just in theory.

---

## Phase 3 — Service split + Saga compensation

### What & why

Phase 2 proved the event contracts work; Phase 3 proves they work *across
process boundaries*, splitting the monolith into four independently
deployable services — `api-service`, `workflow-service`, `worker-service`,
`notification-service` — plus `workflow-shared`, a small library (the YAML
DSL parser/validator and the Kafka event records) that every service depends
on but that never contains persistence or business logic. This is the "why"
that matters in an interview: a shared *library* and a shared *database
schema* are very different couplings. Sharing the parsing code and event
contracts is cheap and necessary — every service needs to agree on what a
`StepExecutionCompleted` looks like. Sharing JPA entity *classes* would have
been a mistake even though it's tempting (less duplication): it would quietly
re-couple services that are supposed to be independently deployable, because
changing one service's persistence code would force a recompile of all of
them. So each service gets its own small `persistence` package mapped to the
same physical tables — deliberate duplication, not an oversight.

**Why one shared Postgres database instead of one per service:** database-
per-service is the "more correct" microservices answer, but it requires
solving data consistency across services (sagas, outbox pattern, eventual
consistency in reads) for a problem this project doesn't have yet — every
service's view of a `WorkflowInstanceEntity` needs to be current, not
eventually consistent, for the state machine to make sense. Shared database
is a well-known, named trade-off (not a "mistake") for exactly this
situation: independent deployability without the cost of distributed data.

### The event flow that replaced a direct method call

The trickiest part of the split: `InstanceService.startInstance`/`approveInstance`
used to call `WorkflowCoordinator.beginOrResumeAt(...)` as a plain Java method
— impossible once `api-service` and `workflow-service` are different
processes. The fix is a new topic, `workflow.instance.lifecycle`, carrying
`InstanceLifecycleRequested(instanceId, definitionId, stepId, reason)`
(`reason` is `STARTED` or `APPROVED`). `api-service` publishes it wherever it
used to make that direct call; `workflow-service`'s
`WorkflowCoordinatorService.onInstanceLifecycleRequested` consumes it and runs
the exact same `beginOrResumeAt` logic. The one exception: if an approval's
next step is `null` (the workflow is over), `api-service` still marks the
instance `COMPLETED` itself — no cross-service round trip needed for a
decision that doesn't require the spec-parsing logic living in
workflow-service.

`EMAIL_NOTIFY` steps got the same treatment in reverse: `worker-service`
used to call `EmailService` in-process; now it recognizes the step type and
publishes `NotificationRequested` to `workflow.notification.requested`
instead of running anything itself. `notification-service` consumes it, owns
writing that step's `StepExecutionEntity` row, and publishes
`StepExecutionCompleted` back onto the topic `workflow-service` already
listens to — from `workflow-service`'s point of view, it's just another
producer on a topic it doesn't care who's publishing to.

### Saga compensation

`StepSpec` gained an optional `compensate` field (a bean name, same shape as
`handler` — reuses `BusinessStepHandler`, no new interface). In
`examples/loan-approval.yaml`, `disbursement` declares
`compensate: reverseDisbursement`, and a new step `finalizeLoan` sits right
after it — `AUTOMATIC`, no retries, fails immediately if the instance's
payload has `simulateFailure: true`. One definition demonstrates both paths.

When `WorkflowCoordinatorService.onStepExecutionCompleted` receives a
permanent failure (retries exhausted), instead of jumping straight to
`FAILED` it asks: "of the steps that already *succeeded* on this instance,
which ones declared a `compensate` handler?" (`compensableStepIdsAscending`
— filters this instance's `StepExecutionEntity` history to `SUCCEEDED` rows
whose step has a `compensate`). None → `FAILED`, unchanged from Phase 2 (a
failure before `disbursement` has nothing to undo). Some → `COMPENSATING`,
and it walks them **newest-first**: publish `CompensationRequested` for the
most recent one, wait for `CompensationCompleted`, move to the next-older
one, repeat until none remain, then `COMPENSATED`.

**The interesting implementation detail:** `currentStepId` gets reused as
"the step this instance is currently occupied with" rather than adding a
second tracking column — while `RUNNING` it means the next step to execute;
while `COMPENSATING` it means the compensation target currently being undone.
This is a deliberate field reuse, documented in
`WorkflowCoordinatorService`'s class comment, not an accident — worth being
ready to defend as "one field, context-dependent meaning, versus a second
column that would only ever be populated during one status" if asked whether
it's a code smell.

**Best-effort compensation:** if a compensation handler itself fails
(`CompensationCompleted(success=false)`), `WorkflowCoordinatorService` logs it
(`COMPENSATION_FAILED` audit entry) and *still moves on* to the next older
step, rather than getting the instance stuck. The alternative — halting the
whole rollback because one undo failed — would leave the instance in limbo
indefinitely with no automatic recovery path; best-effort at least guarantees
forward progress toward a terminal state, at the cost of a manual follow-up
being needed for whichever specific compensation failed. That trade-off is
visible in the audit log, not hidden.

### Likely questions & how to answer them

**"Why didn't api-service just call workflow-service's REST API directly
instead of another Kafka topic?"** Consistency: every cross-service
interaction in this system is an event, which means every one of them gets
the same idempotency/ordering/retry story for free. Introducing a
synchronous HTTP call for just this one case would mean designing a *second*
failure-handling story (timeouts, retries, circuit breaking) for that one
path — one consistent mechanism beats two.

**"Why does the shared library contain the parser but not the entities?"**
Answered above in "What & why" — a shared parsing library (stateless,
read-only, needs to produce byte-identical results everywhere) is a much
weaker coupling than shared persistence classes (which would force every
service to recompile whenever any service's data model changes).

**"Walk me through what happens if the compensation handler crashes."**
`WorkflowStepWorker.onCompensationRequested` catches the exception, treats it
as `StepResult.failure(...)`, and still publishes `CompensationCompleted`
with `success=false` — the Saga keeps moving (see best-effort compensation
above) rather than the event silently disappearing and the instance getting
stuck in `COMPENSATING` forever.

**"Is `reverseDisbursement` idempotent? What if `CompensationRequested` gets
redelivered?"** Not in this demo (it just logs) — but it's called out
explicitly in `WorkflowStepWorker`'s Javadoc as something a *real*
compensation handler must be, precisely because Kafka's at-least-once
delivery means it can run more than once for the same request. Good answer:
"the demo doesn't need it, but I know where it would have to go and why."

**"What would you change to make this a 'real' microservices architecture?"**
Database-per-service (with either an outbox pattern for cross-service
consistency, or accepting eventual consistency in read models), containerize
each service with its own Dockerfile, and add distributed tracing (a trace id
threaded through every event) so a single instance's journey across four
services is debuggable as one timeline instead of four separate log streams.
All named, none built — a good list to have ready when asked "what's next."

---

## Phase 4 — Database-per-service (final phase)

### What & why

Phase 3 gave each service its own entity/repo *classes*, but all four still
pointed at one shared set of physical tables — explicitly flagged at the
time as a pragmatic stepping stone, not the textbook-correct answer. Phase 4
finishes the job: `api_db`, `workflow_db`, `worker_db`, `notification_db` —
four separate databases (one Postgres server, `docker/init-databases.sql`),
no service ever runs SQL against another service's table again.

The hard part was never the database split itself — it's that api-service
could no longer directly query the instance/step/audit rows workflow-service,
worker-service, and notification-service write, and workflow-service/
worker-service could no longer directly query the definitions api-service
writes. Every one of those reads got resolved the same way:
**event-carried state transfer** — the reading service becomes a Kafka
consumer too, building its own local, read-optimized copy from events. This
is a real CQRS read side, not a diagram-only concept: `api-service`'s
`InstanceSummaryView`/`StepExecutionView`/`AuditLogView` and `workflow-
service`/`worker-service`'s local `WorkflowDefinitionEntity` replicas are all
the same pattern applied four times, deliberately, rather than four
different ad-hoc solutions.

### The two decisions worth being ready to defend

**1. Why don't shared library and shared database mean the same kind of
coupling?** `workflow-shared` (the DSL parser/validator and the event
records) is depended on by every service, same as before — but that's a
world apart from a shared *database schema*. A shared library is stateless,
versioned, and produces byte-identical results everywhere; a shared table
means two services' runtime behavior is entangled through mutable state
neither fully controls. Splitting the database while keeping
`workflow-shared` isn't a half-measure — it's drawing the coupling boundary
in the right place.

**2. Why one Postgres *server* with four databases, not four servers?**
Logical database separation gives the identical ownership guarantee (no
service can `JOIN` across another service's tables, no shared `ddl-auto`
risk) as full physical isolation, for a fraction of the local infrastructure
cost. The interview-honest answer: this is a portfolio-scoped simplification
— in a real multi-team deployment you'd likely want physical isolation too
(blast-radius containment, independent scaling, independent backup/restore),
but that's an operational concern orthogonal to the *code-level* discipline
this phase is actually about.

### Instance ids are now assigned by api-service, not generated by a database

Before Phase 4, `WorkflowInstanceEntity.id` was `@GeneratedValue
@UuidGenerator` — whichever database inserted first (api-service,
synchronously) generated it. Once workflow-service creates that row instead,
and api-service must return the id in its `202 Accepted` HTTP response
*before* that row exists anywhere, the id has to be assigned by application
code before any database sees it. `InstanceService.startInstance` now calls
`UUID.randomUUID()` itself; `WorkflowInstanceEntity.id` (and its api-service
read-model counterpart, `InstanceSummaryView.id`) became a plain assigned
`@Id`, no generator, everywhere it appears. This is a standard pattern
(client/application-assigned keys) directly forced by the ownership split —
a good concrete answer if asked "what had to change at the data-modeling
level, not just the infrastructure level."

### Optimistic local writes, reconciled by the next authoritative event

`InstanceService.startInstance` and `approveInstance` both write an
optimistic guess into `InstanceSummaryView` *before* publishing the event
that tells workflow-service what actually happened — otherwise `GET
/api/instances/{id}` would 404 (or show stale data after an approval) during
the round-trip to workflow-service and back. The next `InstanceStateChanged`
event is treated as authoritative and simply overwrites the guess. This is
deliberately narrated as "optimistic echo, reconciled later," not hidden —
the two could theoretically diverge for a brief window (e.g., if
workflow-service's Saga logic immediately routes to compensation rather than
the plain next step api-service guessed), and that's an accepted, bounded
inconsistency window, not a bug to be embarrassed about.

### The outbox pattern — implemented once, in workflow-service

Every Kafka publish has a "what if the database commit succeeds but the
broker publish fails" problem — a dual-write across two different systems
that can't be made atomic without something like XA transactions, which
nobody wants to operate. `workflow-service`'s `WorkflowEventProducer` doesn't
call `KafkaTemplate` directly for *any* of its four event types anymore —
`enqueue()` inserts an `OutboxEventEntity` row instead, and
`WorkflowCoordinatorService` is class-level `@Transactional`, so that outbox
insert commits in the exact same Postgres transaction as the instance-table
write it accompanies. A separate `OutboxRelay` (`@Scheduled(fixedDelay =
500)`) polls unpublished rows and sends them independently, retrying
indefinitely on failure. The guarantee this buys: *if* the instance state
change is durably committed, the corresponding event *will* eventually reach
Kafka — never "committed but silently never published."

Deliberately not duplicated in worker-service/notification-service — those
still call `KafkaTemplate` directly after their writes. Naming that boundary
explicitly (rather than pretending it's uniform) is itself part of a good
answer: "I know exactly where the dual-write risk still exists in this
system, and chose not to fix it everywhere given the scope."

One more consequence worth naming: workflow-service dropped its own local
audit table entirely in this phase. In Phase 3 it made sense (something,
somewhere, could still query the shared table); once tables stopped being
shared, that local copy had no reader at all — `AuditEventOccurred` plus
api-service's `AuditLogView` fully replaced it. A nice example of database-
per-service *removing* code, not just adding ceremony.

### Likely questions & how to answer them

**"What happens if you start an instance right after submitting its
definition?"** It can race `WorkflowDefinitionPublished` reaching
workflow-service — `WorkflowSpecLoader.load()` would throw "missing workflow
definition" if the replica hasn't landed yet. Named and accepted, not
solved: a synchronous fallback (workflow-service calling api-service's REST
API on a cache miss) would reintroduce exactly the coupling this phase set
out to remove, for a race window that's milliseconds wide in practice.

**"Why does `StepExecutionRecorded` carry `status` as a String instead of
reusing your `StepExecutionStatus` enum?"** The event contract needs to
outlive any one service's internal representation. If worker-service's
persistence layer ever renamed or restructured that enum, a shared enum
reference would silently break every consumer's deserialization; a plain
string decouples "what the wire format looks like" from "how one service
happens to model it internally today."

**"Walk me through what makes the outbox pattern actually atomic here."**
`OutboxEventEntity` insert and the `WorkflowInstanceEntity` update happen via
two repository calls inside one `@Transactional` method — same
`EntityManager`, same underlying JDBC connection, same Postgres transaction.
Either both rows exist after commit, or (on rollback) neither does. The
*relay* publishing to Kafka afterward is intentionally NOT part of that
transaction — it's allowed to fail and retry, because Kafka isn't
transactional with Postgres; the outbox row itself is the durable record
that "this needs to be published," independent of whether the first attempt
succeeds.

**"Isn't 'optimistic write, corrected later' just eventual consistency with
extra steps?"** Yes, named honestly rather than dressed up — the
alternative (block the HTTP response until workflow-service confirms) would
mean api-service's availability depends on workflow-service being up and
fast, exactly the coupling an event-driven design is supposed to avoid. The
trade-off is explicit: slightly stale reads for a moment, versus a
synchronous dependency between two services that are supposed to scale and
fail independently.

**"What's still not 'real' database-per-service here?"** All four databases
live on one Postgres server/container — a deliberate portfolio-scoped
simplification (same section above). A production system would likely want
separate Postgres instances (or managed databases) per service for genuine
blast-radius isolation, independent scaling, and independent backup/restore
policies.
