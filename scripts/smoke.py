"""Requires an already running Compose stack. Checks replay, checkpoint restart, and both sinks."""
import subprocess,time,json
def compose(*args):return subprocess.check_output(['docker','compose',*args],text=True).strip()
def sql(query):return compose('exec','-T','postgres','psql','-U','signalforge','-Atc',query)
def wait(predicate,seconds=180):
    end=time.monotonic()+seconds
    while time.monotonic()<end:
        try:
            if predicate():return
        except subprocess.CalledProcessError:pass
        time.sleep(1)
    raise AssertionError('stream did not reach expected durable state')
run='smoke-'+str(time.time_ns())
def generate():return compose('run','--rm','-e','EVENTS=400','-e','RATE=1000','-e','RUN_ID='+run,'generator')
generate()
wait(lambda:sql(f"SELECT count(*) FROM event WHERE tenant='{run}'")=='300')
wait(lambda:sql(f"SELECT count(*) FROM lifecycle WHERE tenant='{run}' AND status='DELIVERED'")=='100')
wait(lambda:sql(f"SELECT count(*) FROM event e JOIN archive_batch b ON b.id=e.archive_id WHERE e.tenant='{run}' AND b.state='PUBLISHED'")=='300')
compose('kill','-s','SIGKILL','stream');compose('up','-d','stream')
generate()
time.sleep(10)
assert sql(f"SELECT count(*) FROM event WHERE tenant='{run}'")=='300'
assert sql(f"SELECT count(*) FROM lifecycle WHERE tenant='{run}' AND status='DELIVERED'")=='100'
assert 'lag=0' in compose('run','--rm','inspect')
print(json.dumps({'smoke':'passed','runId':run,'rawEvents':800,'uniqueEvents':300,'deliveredLifecycles':100,'checks':['deduplication','out-of-order lifecycle','archive publication','SIGKILL checkpoint restart']}))
