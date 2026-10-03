import sys, re, collections
d = collections.defaultdict(list)
for line in sys.stdin:
    kv = dict(p.split("=", 1) for p in line.split()[1:] if "=" in p)
    run = "beside" if "beside" in kv.get("ep", "") else ("alone" if "alone" in kv.get("ep", "") else "intake")
    if "-slow-" in kv.get("ep", ""):
        run += "-slow"
    for k, v in kv.items():
        if k in ("ep",):
            continue
        d[(run, k)].append(float(v))
def q(xs, p):
    xs = sorted(xs); return xs[min(len(xs) - 1, int(len(xs) * p))]
for (run, k), xs in sorted(d.items()):
    print(f"{run:12} {k:18} n={len(xs):5} p50={q(xs,.5):10.0f} p90={q(xs,.9):10.0f} p99={q(xs,.99):10.0f} max={max(xs):10.0f}")
