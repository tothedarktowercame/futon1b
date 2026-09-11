"""Fixed, bounded, sequential standalone XTDB test matrix. Never opens a store path."""
import hashlib,json,os,pathlib,signal,subprocess,time
ROOT=pathlib.Path(__file__).resolve().parents[2]
OUT=pathlib.Path(__file__).resolve().parent
MANIFEST=OUT/'freeze.json'

def sha(p): return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
def sources():
    paths=subprocess.check_output(['git','ls-files','*.clj','deps.edn'],cwd=ROOT,text=True).splitlines()
    return {p:sha(ROOT/p) for p in paths}
def configurations():
    return [('baseline',4,4,3),('workers',8,4,3),('workers-memory',8,8,6)]

if not MANIFEST.exists():
    cp=subprocess.check_output(['clojure','-Spath'],cwd=ROOT,text=True).strip()
    jars={p:sha(p) for p in cp.split(os.pathsep) if p.endswith('.jar')}
    frozen={'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
            'source_hashes':sources(),'jar_hashes':jars,'runner_sha256':sha(__file__),
            'workload':{'seed_entries':5000,'entry_payload_bytes':1024,'memory_edges':128,
                        'foreground':{'point':60,'projection':4},
                        'additional_mixed':{'scan':16,'background_write':16},'wave_size':16},
            'cases':configurations(),'repeats':2,'case_timeout_seconds':240,
            'order':['foreground-first','mixed-first'],
            'limitations':['No HTTP listener, production store, FTS sidecar or APM agent is used.',
                           'Real bounded request executor and XTDB query/projection code are exercised.',
                           'Internal query concurrency remains fixed; HTTP expensive-read admission wrapper is not exercised.',
                           'Small synthetic corpus cannot establish production memory sizing.'],
            'java':subprocess.check_output(['java','-version'],stderr=subprocess.STDOUT,text=True)}
    MANIFEST.write_text(json.dumps(frozen,indent=2)+'\n')
    print('FROZEN; invoke again to execute',flush=True)
    raise SystemExit(0)
frozen=json.loads(MANIFEST.read_text())
if frozen['source_hashes']!=sources() or frozen['runner_sha256']!=sha(__file__):
    raise SystemExit('source drift: no execution')
for p,h in frozen['jar_hashes'].items():
    if sha(p)!=h:raise SystemExit('dependency drift: '+p)
for repeat,order in enumerate(frozen['order'],1):
    cases=list(configurations())
    if repeat==2:cases.reverse()
    for label,workers,heap,direct in cases:
        if frozen['source_hashes']!=sources():raise SystemExit('source drift during matrix')
        case=f'{repeat}-{label}'; result=OUT/(case+'.json'); log=OUT/(case+'.log')
        if result.exists() or log.exists():raise SystemExit('retained case exists; refusing overwrite: '+case)
        opts=['--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED',
              '-Dio.netty.tryReflectionSetAccessible=true','-Djava.net.preferIPv4Stack=true',
              f'-Xmx{heap}g',f'-XX:MaxDirectMemorySize={direct}g','-XX:MaxMetaspaceSize=2g']
        edn='{:aliases {:tuning {:jvm-opts ['+' '.join(json.dumps(x) for x in opts)+']}}}'
        cmd=['/usr/bin/time','-f','{"max_rss_kb":%M,"user_s":%U,"system_s":%S,"wall_s":%e}',
             '-o',str(OUT/(case+'.resources.json')),'clojure','-Sdeps',edn,'-M:tuning',
             str(OUT/'probe.clj'),str(workers),str(result),order]
        started=time.time(); print('START',case,order,flush=True)
        with log.open('x') as f:
            p=subprocess.Popen(cmd,cwd=ROOT,stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
            try:code=p.wait(timeout=frozen['case_timeout_seconds'])
            except subprocess.TimeoutExpired:
                os.killpg(p.pid,signal.SIGTERM)
                try:p.wait(timeout=5)
                except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait()
                code='timeout'
        (OUT/(case+'.receipt.json')).write_text(json.dumps({'command':cmd,'started_unix':started,
            'elapsed_s':time.time()-started,'exit':code,'log_sha256':sha(log),
            'source_drift_after':frozen['source_hashes']!=sources()},indent=2)+'\n')
        print('END',case,code,round(time.time()-started,2),flush=True)
        if code!=0:raise SystemExit('case failed; retained; no automatic retry')
