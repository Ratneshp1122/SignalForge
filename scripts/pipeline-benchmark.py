import json,os,pathlib,subprocess,time,uuid,threading
root=pathlib.Path('evidence');root.mkdir(exist_ok=True)
def cmd(*args):return subprocess.check_output(args,text=True).strip()
def compose(*args):return cmd('docker','compose',*args)
def sql(query):return compose('exec','-T','postgres','psql','-U','signalforge','-Atc',query)
for name,args in [('cpu',['lscpu']),('memory',['free','-b']),('kernel',['uname','-a'])]:(root/(name+'.txt')).write_text(cmd(*args)+'\n')
commit=cmd('git','rev-parse','HEAD');(root/'commit.txt').write_text(commit+'\n')
events=int(os.environ.get('EVENTS','100000'));rate=int(os.environ.get('RATE','10000'))
assert events%4==0 and 4<=events<=1000000 and 1<=rate<=35000
for trial in range(1,4):
 tenant='pipeline-'+str(uuid.uuid4());path=root/('run-'+str(trial));path.mkdir(exist_ok=True)
 started=time.monotonic();output=compose('run','--rm','-e','EVENTS='+str(events),'-e','RATE='+str(rate),'-e','RUN_ID='+tenant,'generator');(path/'generator.txt').write_text(output)
 generator=next(json.loads(line) for line in reversed(output.splitlines()) if line.startswith('{') and 'acknowledged' in line)
 end=time.monotonic()+240;drained=False
 while time.monotonic()<end:
  counts=sql(f"SELECT count(*),count(*) FILTER(WHERE b.state='PUBLISHED') FROM event e LEFT JOIN archive_batch b ON b.id=e.archive_id WHERE tenant='{tenant}'")
  if counts==f'{events//4*3}|{events//4*3}':
   lag=compose('run','--rm','inspect');(path/'lag.txt').write_text(lag)
   partitions=[line for line in lag.splitlines() if line.startswith('partition=')]
   if len(partitions)==4 and all(line.endswith('lag=0') for line in partitions):drained=True;break
  time.sleep(1)
 seconds=time.monotonic()-started
 correct=drained and sql(f"SELECT count(*) FROM lifecycle WHERE tenant='{tenant}' AND status='DELIVERED'")==str(events//4)
 if correct:(path/'parquet-audit.txt').write_text(compose('run','--rm','-e','RUN_ID='+tenant,'-e','EXPECTED_UNIQUE='+str(events//4*3),'audit'))
 result={'trial':trial,'commit':commit,'rawEvents':events,'canonicalEvents':events//4*3,'logicalPayloadBytes':generator['payloadBytes'],'secondsToAllSinksAndZeroLag':seconds,'processedRawEventsPerSecond':events/seconds,'canonicalEventsPerSecond':events/4*3/seconds,'correct':correct,'requestedGeneratorRate':rate,'scope':'generator process launch through PostgreSQL + Parquet publication + zero lag; actual Parquet audit follows timed interval','generator':generator}
 (path/'result.json').write_text(json.dumps(result,indent=2));(path/'containers.txt').write_text(cmd('docker','stats','--no-stream'))
 if not correct:raise AssertionError('pipeline failed to drain accurately')
lines=['# Three full-pipeline trials','','Includes Kafka, Spark, PostgreSQL, archive publication and lag verification. Not a 100M-event or 475GB run.','','| Trial | Raw callbacks | Logical bytes | Raw events/s | Canonical events/s | Correct |','|---|---:|---:|---:|---:|---|']
for p in sorted(root.glob('run-*/result.json')):
 d=json.loads(p.read_text());lines.append(f"| {d['trial']} | {d['rawEvents']} | {d['logicalPayloadBytes']} | {d['processedRawEventsPerSecond']:.2f} | {d['canonicalEventsPerSecond']:.2f} | {d['correct']} |")
(root/'README.md').write_text('\n'.join(lines)+'\n')
