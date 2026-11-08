import os,json,pathlib
root=pathlib.Path('benchmarks/runs')/os.environ['RUN_ID']
lines=['# Isolated JDBC sink comparison','','Scope: one writer with JSON parsing and committed database writes. Excludes Kafka, Spark, Parquet, and end-to-end lag. Three trials per variant; each variant runs on its own GitHub runner. Hardware is recorded, not assumed identical.','','| Mode | Trial | Raw events/s | Canonical events/s | Correct |','|---|---:|---:|---:|---|']
for p in sorted(root.rglob('run-*.json')):
 d=json.loads(p.read_text());lines.append(f"| {d['mode']} | {d['trial']} | {d['rawEventsPerSecond']:.2f} | {d['canonicalEventsPerSecond']:.2f} | {d['correct']} |")
lines+=['',f"Workflow: https://github.com/{os.environ.get('GITHUB_REPOSITORY','Ratneshp1122/SignalForge')}/actions/runs/{os.environ['RUN_ID']}", '', 'Missing trials are failures/incomplete runs, not zero-duration successes; inspect the build-and-run logs.']
(root/'README.md').write_text('\n'.join(lines)+'\n')
