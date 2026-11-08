# Isolated JDBC sink comparison

Scope: one writer with JSON parsing and committed database writes. Excludes Kafka, Spark, Parquet, and end-to-end lag. Three trials per variant; each variant runs on its own GitHub runner. Hardware is recorded, not assumed identical.

| Mode | Trial | Raw events/s | Canonical events/s | Correct |
|---|---:|---:|---:|---|
| copy | 1 | 9398.54 | 7048.90 | True |
| copy | 2 | 10303.24 | 7727.43 | True |
| copy | 3 | 11576.51 | 8682.38 | True |
| row | 1 | 1869.79 | 1402.34 | True |
| row | 2 | 1903.78 | 1427.84 | True |
| row | 3 | 1985.22 | 1488.91 | True |

Workflow: https://github.com/Ratneshp1122/SignalForge/actions/runs/42456891969

Missing trials are failures/incomplete runs, not zero-duration successes; inspect the build-and-run logs.
