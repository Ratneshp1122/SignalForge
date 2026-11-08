# Three full-pipeline trials

Includes Kafka, Spark, PostgreSQL, archive publication and lag verification. Not a 100M-event or 475GB run.

| Trial | Raw callbacks | Logical bytes | Raw events/s | Canonical events/s | Correct |
|---|---:|---:|---:|---:|---|
| 1 | 100000 | 25557120 | 3879.51 | 2909.63 | True |
| 2 | 100000 | 25557120 | 4296.78 | 3222.58 | True |
| 3 | 100000 | 25557120 | 4598.85 | 3449.14 | True |
