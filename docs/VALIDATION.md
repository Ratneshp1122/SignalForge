# Executed validation

On 7 November 2026, [CI run 42373567714](https://github.com/Ratneshp1122/SignalForge/actions/runs/42373567714) passed on commit `a701f84` using Java 17 and Docker on an Ubuntu GitHub-hosted runner:

- 5 lifecycle/schema/generator unit tests passed.
- 3 real PostgreSQL integration tests passed (Testcontainers).
- The complete Kafka/Spark/PostgreSQL/Parquet Compose smoke test passed.
- 400 callbacks were produced, Spark was killed with SIGKILL, restarted with its original checkpoint, and the 400 callbacks were replayed.
- The database retained exactly 300 unique events and 100 DELIVERED lifecycles.
- All four Kafka partition progress gaps drained to zero.
- An independent Spark read of published Parquet directories asserted exactly 300 rows and 300 unique identities for the run.

This is correctness evidence for a small workload. It does not establish 35,000 sustained events/sec, 100M processed events, 475GB of traffic, high availability, or production deployment readiness. Inspect subsequent CI runs for later commit validation; formatting/documentation changes do not alter the measured run's commit identity.
