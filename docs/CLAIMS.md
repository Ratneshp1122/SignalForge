# Claim-to-evidence map

| Requested capability or claim | Implementation / evidence | Status |
|---|---|---|
| Kafka + Spark Structured Streaming | StreamingJob.java and Compose smoke test | Implemented |
| Delayed callback deduplication | Durable event primary key + canonical hash | Implemented; no bounded watermark expiry |
| Reordered lifecycle reconstruction | Evidence bit union, terminal conflict state | Implemented; unit and DB tests |
| Schema quarantine | Quarantine table keyed by Kafka source coordinates | Implemented |
| Windowed channel/market analytics | Minute-cohort delivery_analytics SQL view | Implemented; late evidence updates live results |
| Checkpoint restart | Persistent Spark checkpoint volume; SIGKILL/replay smoke test | Implemented |
| Idempotent PostgreSQL/Parquet sinks | Transactional event identities; durable archive batch and atomic directory publication | Implemented; local single-host filesystem scope |
| Consumer lag | Inspect reads Kafka end offsets and PostgreSQL committed progress | Implemented; source-progress metric, not consumer-group metric |
| COPY staging and set-based commit | CopyWriter, staging.sql, merge-stage.sql; concurrent/replay/conflict DB tests | Implemented; sink-only and full-pipeline three-trial measurements in benchmarks/runs |
| 35,000+ events/sec | Configurable paced Kafka generator | Target only; no sustained processing benchmark claimed |
| 100M events / 475GB | Configurable count and padding; actual acknowledged count and serialized-byte counters | Target only; no completed large run claimed |

Exact logical bytes, stored bytes, and unique events are different measurements. CI checks correctness on a small workload. The SRS describes additional planned services; README documents the actual implementation boundary.
