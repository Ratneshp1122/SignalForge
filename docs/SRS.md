# SignalForge Engineering Guide

Software requirements specification, architecture, implementation roadmap, and performance engineering

Version 1.0 | 7 November 2026 | Prepared for Ratnesh P

## 1 Purpose and scope

SignalForge reconstructs the delivery history of messages from unreliable provider callbacks. It accepts duplicate, delayed, malformed, and out-of-order events, produces a deterministic lifecycle for each message attempt, and publishes delivery analytics by tenant, channel, market, and event-time window. The engineering problem is preserving correct outcomes through retries and recovery while keeping processing bounded.

This document specifies the broader target design. The repository README and CLAIMS.md describe the implemented subset, and VALIDATION.md links executed tests. The 35,000 events/second, 100 million events, and 475 GB figures are separate evaluation targets. Achieving one does not establish the others. Proposed ingestion/query services and distributed deployment extensions below are roadmap items, not assertions of current implementation.

### Product boundaries

In scope: synthetic traffic generation, authenticated callback ingestion, durable raw retention, validation and quarantine, event deduplication, attempt-level lifecycle reconstruction, late-data correction, windowed delivery analytics, REST query APIs, checkpoint recovery, reconciliation, and reproducible benchmarks.

Out of scope for the initial release: actually sending SMS/email, storing message bodies or phone numbers, provider billing, multi-region consensus, unlimited historical live state, automatic schema inference, and claims of fault tolerance from a single broker. The demo uses synthetic identifiers and provider simulators.

### Users and workflows

- An integration developer submits provider callbacks and diagnoses rejected payloads.
- An analyst queries counts and delivery rates for a channel and market over a stated window.
- An operator observes ingestion failures, lag, quarantines, late arrivals, and disk pressure, then runs a controlled replay.
- An evaluator generates a seeded workload, injects failures, and compares observed output with an independent oracle.

A minimum demonstration submits three attempts, injects duplicate callbacks and reordered timestamps, restarts processing, and proves that the final unique events and message outcomes equal the oracle. Build this demonstration before the large load test.

## 2 Vocabulary and correctness contract

An event is a provider observation. A message is a business request. An attempt is one provider delivery attempt; a retry is a new attempt. A replay is reprocessing retained observations, not a new business delivery. Event time is when the provider says the observation occurred; ingestion time is when SignalForge accepted it. Processing time is local execution time.

The event identity is (tenant_id, provider, event_id). The lifecycle key is (tenant_id, message_id, attempt_id). Never deduplicate by message_id: SENT and DELIVERED for one message are different observations. If the same event identity arrives with a different canonical payload hash, record an IDENTITY_CONFLICT rather than silently replacing the original.

The transport is at least once. PostgreSQL projections become effectively once within the documented retention scope through unique event keys and atomic transactions. This does not create a distributed transaction spanning Kafka, PostgreSQL, and Parquet. Spark foreachBatch defaults to at-least-once execution; retry-safe writes must be supplied by the application. [S1]

### Invariants

- SF-I01: Each accepted raw Kafka record is eventually classified as a valid event, duplicate, identity conflict, or quarantine record. Track raw records by topic, partition, and offset.
- SF-I02: A valid event identity produces at most one canonical fact during the configured deduplication retention period.
- SF-I03: Reordering an identical set of observations produces the same attempt projection and timeline.
- SF-I04: Window event counts never increase solely because a micro-batch is retried.
- SF-I05: A confirmed delivery is never reversed by an earlier SENT observation.
- SF-I06: Conflicting terminal outcomes are visible as CONFLICT; they do not become a hidden success or failure.
- SF-I07: Tenant A cannot read, replay, or quarantine-manage tenant B's data.
- SF-I08: No acknowledged ingress event is intentionally discarded without a durable classification or a documented expiry boundary.

Invariants apply to the retained dataset and declared durability profile. A disk lost on a one-broker laptop can lose acknowledged events. Retention expiry can remove replayability. Make both limitations explicit in the run manifest.

## 3 Software requirements specification

Priority P0 means required for a correct initial release. P1 adds operational completeness. P2 is a scale or usability extension. Each requirement has a test ID defined in section 10.

| ID | Priority | Requirement and acceptance criterion | Test |
|---|---|---|---|
| SF-F01 | P0 | Accept a valid authorized event only after its raw Kafka append is acknowledged; return 202 with a receipt. Return 503 on unavailable durability. | SF-T01 |
| SF-F02 | P0 | Validate required fields, enums, size, timestamps, and tenant ownership. Capture downstream schema failures with source coordinates and reason. | SF-T02 |
| SF-F03 | P0 | Collapse identical event identities; surface changed-payload identity conflicts without updating canonical facts. | SF-T03 |
| SF-F04 | P0 | Reconstruct attempt outcome deterministically under every tested permutation of the same event set. | SF-T04 |
| SF-F05 | P0 | Persist canonical events, receipts, and lifecycle updates atomically for each committed database chunk. | SF-T05 |
| SF-F06 | P0 | Preserve post-restart correctness using checkpoints and database deduplication. | SF-T06 |
| SF-F07 | P0 | Return stable, tenant-scoped pagination for attempts and event histories. | SF-T07 |
| SF-F08 | P1 | Produce event counts and message-cohort delivery rates with explicit denominators and correction versions. | SF-T08 |
| SF-F09 | P1 | Retain and report very late events; reconciliation updates affected historical projections. | SF-T09 |
| SF-F10 | P1 | Run range-limited replay into a separate namespace with an auditable run ID. | SF-T10 |
| SF-F11 | P1 | Publish archive manifests with row counts, checksums, and committed object paths. | SF-T11 |
| SF-F12 | P2 | Support compatible schema evolution without changing the meaning of existing event versions. | SF-T12 |

### Nonfunctional requirements

SF-N01 correctness: zero oracle mismatches in the deterministic acceptance corpus and zero unexplained missing records after a controlled drain. This is a finite-test criterion, not a universal loss guarantee.

SF-N02 bounded memory: cap input records per trigger, state retention, payload size, sink concurrency, and replay ranges. A workload with increasing unique keys must reach a documented steady-state memory range when old state expires.

SF-N03 freshness: initial local objective is p95 accepted-to-projection visibility below 10 seconds at a workload established by a calibration run. Record p99 and worst case as well. Overload may increase lag; it must not silently corrupt results.

SF-N04 availability and recovery: report process restart time and backlog catch-up separately. The laptop profile has no host-failure availability objective. A resilient profile uses at least three brokers with replication factor 3 and minimum in-sync replicas 2, plus separately protected database and checkpoint storage.

SF-N05 observability: expose stage throughput, input lag, processing duration, late events, quarantine reasons, sink retries, storage use, and archive backlog. Use no unbounded message IDs as metric labels.

SF-N06 maintainability: version contracts, migrations, replay logic, and benchmark configuration. Require deterministic unit tests and database integration tests in CI before merges.

## 4 Architecture and deployment

[[ARCHITECTURE]]

The raw event log is the replay input. The canonical database is the query source for the initial implementation. Parquet is a separately reconciled archive. The first design favors transparent correctness over maximum throughput; large-scale storage changes come after measurement.

### Component responsibilities

- generator: creates seeded event sequences, independently expected outcomes, arrival schedules, and fault distributions. Never reuse the production reducer to generate expected answers.
- ingest-api: authenticates producers, resolves tenant context, enforces limits, performs envelope checks, and waits for durable enqueue acknowledgement.
- contracts: versioned event schemas, enums, canonical hash rules, and compatibility fixtures.
- stream-job: consumes raw Kafka records, validates, groups by lifecycle key, performs idempotent canonical writes, and exposes processing progress.
- projection-store: canonical facts, attempt projections, source receipts, quarantines, replay runs, and window revisions.
- analytics-job: builds event-time aggregates from canonical facts. It can initially run as bounded SQL recomputation rather than another continuous Spark query.
- archive-job: exports bounded committed ranges to immutable Parquet files and publishes manifests after verification.
- query-api: reads attempt histories, analytics, and operational status; it never mutates canonical events.

### Runtime baseline and compatibility

Use Java 17 for the Spark 3.5.6 reference path, with the matching spark-sql-kafka-0-10_2.12 artifact at 3.5.6. Match the Scala binary suffix and Spark version exactly. Spark's versioned integration guide documents these coordinates. [S2] Use a separate Java toolchain if the API uses Java 21. PostgreSQL 17 is the reference database. Kafka 4.1 is a candidate broker line; run connector compatibility and restart tests before adopting that exact combination.

These are reference compatibility choices, not a claim that they are the newest or appropriate for indefinite production use. Choose supported patched distributions before deployment, pin full versions and image digests in a dependency manifest, and record them with benchmark results. Do not force-upgrade Spark's transitive Kafka clients blindly.

### Three deployment profiles

Profile A, no Docker/WSL: a pure Java domain library, seeded JSONL generator, in-memory adapter, and fixtures. This validates lifecycle logic and schemas; it does not validate Kafka, Spark, database transactions, or durability. Optionally use a separately installed PostgreSQL for database tests.

Profile B, 8GB laptop: run one broker, PostgreSQL, and one Spark local process with local[2]. Start with Kafka heap 512 MB, PostgreSQL container budget roughly 512 MB, Spark heap 1-1.5 GB, and a small generator. Heap is not total resident memory; allow native memory, filesystem cache, Docker/WSL overhead, and Windows headroom. Start with 500-2,000 events/second only as exploratory offered rates, reduce them if swapping begins, and omit Grafana until needed. Do not run PayFlow at the same time.

Profile C, performance lab: separate generator host, durable SSD storage, multiple brokers, explicit Spark executor resources, monitored PostgreSQL, and retained checkpoints. Begin at 16-32GB total test-machine RAM as a planning range, not a capacity guarantee. Record the actual host topology. An external HDD can host sources, but latency-sensitive database and broker workloads should be evaluated on the actual target storage.

## 5 Event contracts and interfaces

### Canonical event envelope

```json
{
  "schema_version": 1,
  "event_id": "evt_000041",
  "tenant_id": "tenant_demo",
  "provider": "simulator",
  "message_id": "msg_0017",
  "attempt_id": "attempt_01",
  "event_type": "DELIVERED",
  "event_time": "2026-11-07T10:00:05Z",
  "channel": "SMS",
  "market": "IN",
  "provider_sequence": 3,
  "attributes": {"reason_code": null}
}
```

The API sets received_at, authenticated principal, trace_id, and source metadata; it does not trust client-supplied tenant authority. The body tenant must match the credential scope. Allowed event types are ACCEPTED, SENT, DELIVERED, FAILED, and EXPIRED. Schema v1 permits a bounded allowlist of non-sensitive attributes. Unknown versions and invalid enums receive explicit validation errors or downstream quarantine reasons.

Set a proposed 16 KiB event limit and reject oversized requests with 413. Require UTC timestamps, bounded string lengths, allowed channel/market values, and no timestamp more than five minutes in the future. Treat older valid timestamps as late, not intrinsically invalid. Define canonical hashing over normalized business fields and exclude transport timestamps and trace IDs. Publish normalization rules and golden hashes.

| Interface | Contract | Outcomes |
|---|---|---|
| POST /v1/events | Bearer-scoped producer; one v1 envelope | 202 after Kafka ack; 400/401/403/413/429/503 otherwise |
| GET /v1/messages/{messageId}/attempts | Tenant from auth; cursor and limit up to 100 | Attempt outcomes and evidence completeness |
| GET /v1/attempts/{attemptId}/events | Message ID filter; stable event-time plus event-ID cursor | Canonical timeline; conflicts separately |
| GET /v1/analytics | from, to, channel, market, metric, window | Counts, denominator, revision, freshness, finality |
| GET /v1/quarantine | Operator scope; reason and date filters | Redacted records and source coordinates |
| POST /v1/replays | Operator scope; source range, destination namespace, reason | 202 plus replay ID; rejects overlapping unsafe runs |
| GET /v1/replays/{id} | Operator scope | Progress, counts, validation outcome |
| GET /actuator/health | Restricted operational network | Liveness separate from dependency readiness |

Errors use a stable object with code, message, trace_id, retryable, and field_errors. Do not expose stack traces. For ingress retries, identical events may enter the raw log more than once; the canonical store removes duplicate effects. A 202 guarantees only the configured raw-log acknowledgement, not immediate dashboard visibility.

### Kafka topics and keys

Use sf.raw.v1 keyed by tenant_id plus message_id plus attempt_id. Start with three partitions for local integration and evaluate six or twelve in the scale profile. Partition counts are experimental knobs, not magic throughput multipliers. Partition order is arrival order, not provider event-time order.

Optional topics sf.quarantine.v1 and sf.lifecycle.v1 are projections of durable database records via a transactional outbox. Do not independently emit a quarantine message and update PostgreSQL while assuming both succeed together. For the MVP, operators query quarantines directly from PostgreSQL.

Set producer idempotence, acks=all, bounded delivery timeouts, and compatible retry/in-flight settings. Kafka's producer idempotence handles a subset of transport retries; application event identity still matters. [S3] Local replication factor 1 remains a single copy even with acks=all.

## 6 Data model and lifecycle algorithms

### Logical schema

| Relation | Primary or unique key | Important fields |
|---|---|---|
| canonical_event | tenant_id, provider, event_id | message_id, attempt_id, type, event_time, received_at, payload_hash, channel, market |
| source_receipt | topic, partition_id, offset_id | classification, canonical identity, run_id, processed_at |
| message_attempt | tenant_id, message_id, attempt_id | observed flags, outcome, earliest timestamps, conflict flag, revision |
| event_conflict | source coordinates | original hash, incoming hash, reason |
| quarantine | source coordinates | schema version, reason, bounded redacted payload |
| window_snapshot | namespace, tenant, channel, market, window_start, metric | numerator, denominator, revision, finality |
| archive_manifest | namespace, export_id | covered range, files, checksums, row counts, committed_at |
| replay_run | replay_id | source bounds, namespace, code version, status, counts |

Use timestamptz, explicit NOT NULL constraints, foreign keys where operationally appropriate, and indexes matching tenant/time access. Add an event lookup index on (tenant_id, message_id, attempt_id, event_time, event_id). A globally unique dedup key conflicts with naive date partitioning: PostgreSQL partitioned unique constraints require inclusion of the partition key. Retain a separate identity registry if canonical events are date-partitioned later. Document expiry of that registry.

### Deterministic reducer

Maintain an evidence set instead of assigning status from the last arrival. ACCEPTED and SENT are progress observations. DELIVERED, FAILED, and EXPIRED are terminal observations. With no terminal evidence, outcome is SENT, ACCEPTED, or UNKNOWN according to observed progress. One terminal type selects that outcome. Two different terminal types select CONFLICT. Missing ACCEPTED or SENT evidence does not block a terminal outcome, but sets incomplete_history=true.

Preserve earliest timestamp for each event type and the terminal timestamp used for analytics. Negative delivery latency, impossible chronology, and contradictory provider metadata become explicit quality flags. Do not silently clamp latency to zero. A retry must use a distinct attempt_id so a failed first attempt does not conflict with a delivered second attempt. Message-level success means at least one delivered attempt; a message failure needs a defined business completion rule and cannot be inferred solely from one failed attempt.

### Transactional canonical write

For each bounded chunk, sort lifecycle keys, open one transaction, and lock each attempt row in that order. Create missing attempt rows with INSERT ON CONFLICT DO NOTHING before locking. Insert canonical events with the event unique key, compare hashes for conflicts, update the evidence projection only for newly inserted facts, and insert source receipts in the same transaction. Commit before allowing the Spark task to succeed. On deadlock or serialization error, retry the whole chunk with bounded jitter.

```text
for chunk in input_partition:
  begin
  ensure attempt rows; lock keys in sorted order
  for raw_record in chunk:
    validate and classify
    insert event if identity is new
    if existing identity: compare canonical hash
    update projection only from newly inserted facts
    write receipt or quarantine for source coordinates
  commit
```

Do not collect an entire micro-batch into driver memory. Repartition by lifecycle key and use bounded per-partition JDBC writers. Multiple chunks can commit before another chunk fails; Spark retry will revisit the committed records, whose unique keys suppress repeated effects. A single driver-side batch marker is not sufficient if executor writes committed independently. Avoid claiming whole-batch atomicity.

### Windowing and late events

Keep two metrics distinct. Event throughput counts unique observations in a one-minute event-time window. Delivery rate groups attempts by their ACCEPTED-time cohort: delivered attempts divided by accepted attempts, with unknown and conflict counts exposed. DELIVERED events in minute X divided by ACCEPTED events in minute X is not a valid cohort delivery rate.

First implement window snapshots by recomputing affected windows from canonical facts and replacing complete values with a monotonically increasing revision. Never increment counters blindly on replay. Resolve overlapping recomputations using a per-window lock or guarded revision update. A snapshot's watermark/finality and data cutoff must be returned to API callers.

For a later Spark event-time aggregate path, start with a ten-minute watermark only after measuring the generator's lateness distribution. Watermarks bound state and affect which late observations influence a streaming result; they are not a promise that every older record is dropped identically. Keep raw/canonical data independently and reconcile older windows. Avoid chaining unsupported stateful operations in one query; use durable intermediate datasets when required. [S1]

### PostgreSQL and Parquet consistency

The archive is asynchronous. Export a bounded source range, write immutable files to a temporary run directory, validate row counts and checksums, and commit a manifest that points only to complete files. On local/HDFS storage an atomic rename can publish a completed directory; object storage needs a manifest/transactional table protocol rather than assuming rename is atomic. A manifest-aware reader ignores uncommitted files.

If export crashes after writing files but before committing the manifest, retry or clean orphaned files using the export ID. If the manifest exists, verify checksums and skip duplicate publication. Do not call arbitrary foreachBatch Parquet append idempotent. Never delete the only canonical copy before archive reconciliation and restore tests pass.

## 7 Build sequence and repository structure

```text
signalforge/
  contracts/          schemas and golden envelopes
  domain/             lifecycle reducer and identity rules
  generator/          seeded workload plus independent oracle
  ingest-api/         authentication and Kafka producer
  stream-job/         Spark query and transactional sink
  query-api/          read-only projections and analytics
  migrations/        database schema and indexes
  archive/            export and manifest verification
  tests/              integration, replay, fault fixtures
  deploy/             local and resilient profiles
  benchmarks/         workload definitions and raw results
  docs/               decisions, diagrams, runbooks
```

Milestone SF-M1: implement the reducer as a pure function, event validation, canonical hashing, and 100 seeded small cases. Test all permutations for small event sets. Exit when duplicate and reordered fixtures match the independent oracle.

SF-M2: implement database migrations and a plain Java canonical writer before Spark. Demonstrate chunk rollback, duplicate retries, conflicting hashes, and tenant isolation with real PostgreSQL. Exit when SF-T02 through SF-T05 pass.

SF-M3: add ingress and Kafka. Write a deterministic producer, enforce the 202 boundary, and test broker outage. Exit when accepted raw records can be reconciled with source receipts.

SF-M4: wrap the proven writer in Spark foreachBatch. Add bounded per-partition writes, stable checkpoints, and failure injection between database commit and task completion. Exit when restart output exactly matches a no-fault run.

SF-M5: implement cohort/window queries, quarantine inspection, late corrections, and Parquet manifests. Exit when the API distinguishes provisional and reconciled results and archive checksums match.

SF-M6: create benchmark scripts, structured result files, and a demonstration dashboard. Raise load only after correctness tests remain green. Every optimization must be compared with the same seed and workload.

Proposed commands after these modules are implemented:

```bash
./mvnw -pl domain,contracts,generator test
./mvnw verify -Pintegration
docker compose --profile signalforge up -d
spark-submit --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  --class io.signalforge.StreamMain stream-job.jar
./scripts/run-benchmark.sh --profile laptop --seed 42
./scripts/reconcile.sh --run-id RUN_ID
```

These paths and scripts are implementation contracts to create, not commands supplied as working application code in this documentation package. In Windows PowerShell use mvnw.cmd; the multiline shell examples assume WSL/Linux once available.

## 8 Efficiency and scale strategy

Measure first: producer acknowledgement latency, Kafka lag by partition, trigger duration, records per trigger, shuffle bytes, state-store size, JDBC transaction duration, database lock waits, disk throughput, and JVM GC. A high generator send rate can coexist with a slow sink and a growing backlog.

| Symptom | Experiment | Correctness or operational guard |
|---|---|---|
| Kafka producer waits dominate | Batch and compress, then vary linger and batch size | Preserve acknowledgement and timeout semantics |
| One hot partition | Inspect tenant/attempt distribution and partition mapping | Never randomly split one attempt without a merge strategy |
| JDBC round trips dominate | Batch inserts; stage and merge using COPY at larger scale | Preserve identity and receipt transactions |
| Database contention rises | Reduce writer concurrency and lock chunk size | Keep deterministic key lock order |
| State memory grows | Bound retention; inspect watermark progress; test RocksDB state store | Retain older canonical facts for reconciliation |
| Small Parquet files multiply | Increase export size and compact committed partitions | Publish replacement manifests atomically |
| Dashboard queries slow | Add matching composite indexes and precomputed snapshots | Bound time ranges and expose freshness |
| Disk approaches capacity | Pause ingestion admission and shorten retained test runs | Never silently delete required replay input |

The first optimization should usually be eliminating per-event network/database calls. Start with chunks of roughly 100-1,000 records and benchmark; larger transactions can increase lock duration and memory. Configure JDBC writer concurrency below the database connection budget. Use EXPLAIN ANALYZE on representative data, with awareness that it actually executes a query.

For a scale-oriented fork, keep canonical immutable facts in a transactional lake table and maintain only compact serving projections in PostgreSQL. This reduces database event volume but introduces a new consistency and operating model. Evaluate Iceberg/Delta or a dedicated analytical store as a new architecture decision; do not simply replace filenames and retain old correctness claims.

## 9 Benchmark specification

Separate offered rate, broker-acknowledged raw rate, canonical unique-event rate, projection-visible rate, and archive-committed rate. Report the duplicate percentage so a workload full of duplicates cannot appear faster by doing less useful work. Count malformed events and rejected requests separately.

Run a small correctness trial, a calibrated baseline, a step-load test, a 30-minute steady-state test, a fault-recovery trial, and a longer storage/retention trial. Use at least three repetitions for performance comparisons. Record warmup and cooldown separately. A steady-state pass requires stable lag and bounded memory, not merely a short high peak.

Proposed workload: 70% SMS, 20% EMAIL, 10% PUSH; ten tenants; a configurable skewed-tenant variant; 5% duplicates; 2% out-of-order arrivals; 0.5% invalid envelopes; explicit rare delays beyond the live watermark. Publish these as parameters and vary them individually. Generate expectations from the canonical event set before randomizing arrival order.

35,000 events/second over 100 million raw events requires about 2,857 seconds, or 47.6 minutes, before pauses or backlog drain. 475 GB divided by 100 million events is about 4,750 bytes per event in decimal units. At that payload size, 35,000 events/second is about 166 MB/second of uncompressed payload before replication, protocol overhead, indexes, checkpoints, and archives. The full storage footprint can greatly exceed 475 GB. Do not attempt that dataset on an unmeasured laptop disk budget.

Use a 100,000-event smoke run, then one million events, then a bounded ten-million-event run if resources permit. Capacity estimates are planning calculations; they are not measured throughput. A compressed archive's size cannot be reported as the raw input size without explicitly naming both.

The run manifest must include run ID, commit, dependency lock, CPU, RAM, disk model, operating system, broker count/partitions/replication, Spark executor configuration, database settings, payload byte distribution, seed, counts, errors, time series, p50/p95/p99 freshness, recovery timings, reconciliation report, and checksums. Preserve raw logs rather than just screenshots.

## 10 Verification and failure matrix

| Test | Injection or input | Required assertion |
|---|---|---|
| SF-T01 | Stop broker before ingest acknowledgement | No false 202; retry preserves canonical identity |
| SF-T02 | Missing IDs, huge payload, bad enum, unknown schema | Explicit rejection/quarantine reason and bounded storage |
| SF-T03 | Same ID twice; same ID with changed payload | One canonical fact; conflict reported for changed hash |
| SF-T04 | All small-set permutations including contradictory terminals | Same reducer result and explicit conflict state |
| SF-T05 | Kill writer between event and projection statements | Full transaction rollback or complete consistent commit |
| SF-T06 | Kill process after commit but before checkpoint progress | Retried records create no duplicate effect |
| SF-T07 | Cross-tenant ID and pagination-boundary requests | No data leakage; stable complete pagination |
| SF-T08 | Retry a batch affecting a finalized cohort window | Same counts; versioned correction only for new evidence |
| SF-T09 | DELIVERED arrives beyond watermark | Canonical retention and explicit corrected historical result |
| SF-T10 | Replay a closed range into a fresh namespace | Counts and hashes match; current namespace untouched |
| SF-T11 | Crash before/after archive manifest publication | No incomplete export visible and no duplicate committed range |
| SF-T12 | Add optional field; change required field incompatibly | Compatible fixture passes; incompatible version rejected |

Add property tests for reducer associativity, commutativity, and duplicate idempotence over valid observation sets. Track conflict cases separately rather than excluding them from accuracy. Test input partitions with no progress, clock skew, poison events, database outages, partition reassignment, and checkpoint restoration. Broker leader-failure tests require a multi-broker profile.

Release acceptance requires all P0 tests, a zero-mismatch reconciliation run, documented P1 gaps, working setup from a fresh clone, and a retained result bundle. A dashboard alone is not evidence of pipeline correctness.

## 11 Security and operations

Scope credentials by tenant and operation; use producer-only credentials for ingress and read-only credentials for query APIs. Production-like profiles require encrypted transport and broker/database authentication. Bind local developer services to loopback. Keep credentials outside source control. Redact callback contents in logs and cap quarantine payload retention.

Runbook for rising lag: determine whether lag grows at ingress, Spark processing, or database commit; check disk and lock waits; reduce admitted rate or maxOffsetsPerTrigger; fix the bottleneck; verify drain and reconciliation before resuming scale tests.

Runbook for poison events: preserve source coordinates and a redacted sample, classify the schema error, deploy a compatible parser fix, then replay only the affected range into a review namespace. Do not edit the original event in place.

Runbook for lost checkpoint: stop the job, preserve the damaged checkpoint, select a known source range, start a new query namespace, and reconcile the canonical store. Do not delete a checkpoint casually and restart at latest offsets. Changes to state schemas or partitioning may require a migration and fresh replay.

Retention defaults for local demonstrations are proposed: raw Kafka 24 hours or a stricter measured size cap, canonical facts seven days, quarantines seven days, and benchmark evidence retained outside those cleanup jobs. Align deduplication retention with the maximum supported replay interval. Test backup restore independently of process restart.

## 12 Architecture decisions and improvement backlog

ADR-SF01: use an immutable raw log plus deterministic projections. Consequence: more storage and explicit reconciliation, with repeatable debugging.

ADR-SF02: use PostgreSQL idempotent writes for the first canonical sink. Consequence: simple correctness proofs and likely write-throughput limits; optimize only after measuring.

ADR-SF03: keep archive publication separate from canonical commit. Consequence: eventual archive freshness, no false cross-store atomicity claim.

ADR-SF04: model attempts separately and represent contradictory terminals as conflicts. Consequence: extra domain states, but no misleading success counts.

ADR-SF05: start with SQL window recomputation before complex chained Spark state. Consequence: lower initial complexity and a reference oracle for later streaming optimization.

Prioritized improvements: first deterministic tests and transactional writes; then late correction and archive recovery; then monitoring and bottleneck profiling; then staged bulk ingestion; then a lake/analytics-store fork, CDC, and resilient deployment. Add multi-region processing only after defining conflict resolution, failover ownership, and storage guarantees.

For an interview, be able to explain why a watermark does not remove the need for reconciliation, why Kafka idempotence does not deduplicate arbitrary business callbacks, and why retrying a partially committed Spark batch remains safe in your sink design.

## References

[S1] Apache Spark 3.5.6 Structured Streaming guide: foreachBatch, watermarking, state, and recovery semantics. https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html

[S2] Apache Spark 3.5.6 Kafka integration: dependency coordinates, source configuration, offsets, and sink caveats. https://spark.apache.org/docs/3.5.6/structured-streaming-kafka-integration.html

[S3] Apache Kafka 4.1 producer configuration: acknowledgements, idempotence, retries, and batching. https://kafka.apache.org/41/configuration/producer-configs/

[S4] Apache Spark 3.5.6 runtime overview. https://spark.apache.org/docs/3.5.6/index.html

[S5] PostgreSQL 17 explicit locking. https://www.postgresql.org/docs/17/explicit-locking.html

Sources consulted 7 November 2026. Requirement thresholds, architectures, budgets, algorithms, and staged plans are proposed engineering decisions in this guide; source documentation supports platform behavior, not achieved project performance.
