"""Summarize all retained cases; failures remain in the denominator."""
import collections,json,math,pathlib
root=pathlib.Path(__file__).resolve().parent

def pct(xs,q):
    xs=sorted(xs)
    return round(xs[max(0,math.ceil(len(xs)*q)-1)],3) if xs else None
rows=[]
for p in sorted(root.glob('[12]-*.json')):
    if '.' in p.stem or p.stem=='freeze':continue
    d=json.loads(p.read_text())
    if 'scenarios' not in d:continue
    resource=json.loads(p.with_name(p.stem+'.resources.json').read_text())
    for s in d['scenarios']:
        points=[r for r in s['results'] if r['kind']=='point']
        projections=[r for r in s['results'] if r['kind']=='projection']
        errors=collections.Counter(str(r.get('error_code',r.get('error-code'))) for r in s['results'] if not r['ok'])
        rows.append({'case':p.stem,'mixed':s['mixed'],'workers':s['workers'],
                     'requests':len(s['results']),'failures':sum(errors.values()),'errors':errors,
                     'point_p95_ms':pct([r['total-ms'] for r in points],.95),
                     'point_queue_p95_ms':pct([r['queue-ms'] for r in points],.95),
                     'projection_p95_ms':pct([r['total-ms'] for r in projections],.95),
                     'elapsed_ms':round(s['elapsed-ms'],3),
                     'heap_max_mib':s['heap-max-bytes']/1048576,
                     'process_max_rss_mib':round(resource['max_rss_kb']/1024,1)})
print(json.dumps({'quantile':'nearest rank; two repetitions; exploratory, not capacity certification',
                  'rows':rows},indent=2))
