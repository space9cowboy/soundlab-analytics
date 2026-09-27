import subprocess, json, collections, os, signal, sys
B = "soundlab-raw-558852"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
INTERDITS = {"user_name", "submission_client", "submission_client_version", "media_player", "tags", "rating"}
ls = subprocess.run(["aws", "s3", "ls", f"s3://{B}/listenbrainz/ecoutes/", "--recursive"],
                    capture_output=True, text=True, check=True).stdout.splitlines()
cles = {}
for l in ls:
    p = l.split()
    if len(p) < 4 or not p[3].endswith(".json.zst"):
        continue
    d = p[3].split("date=")[1][:10]
    m = d[:7]
    if m not in cles or d < cles[m][0]:
        cles[m] = (d, p[3])
stats = collections.defaultdict(lambda: [0, set()])
alertes = collections.Counter()
def nom(v):
    return "null" if v is None else type(v).__name__
def marche(o, p, vus):
    if isinstance(o, dict):
        for k, v in o.items():
            if k in INTERDITS:
                alertes[k] += 1
            marche(v, p + "." + k if p else k, vus)
    elif isinstance(o, list):
        vus.add((p, "list"))
        for v in o:
            marche(v, p + "[]", vus)
    else:
        vus.add((p, nom(o)))
total = 0
for m in sorted(cles):
    d, k = cles[m]
    pr = subprocess.Popen(f"aws s3 cp 's3://{B}/{k}' - 2>/dev/null | zstd -dc 2>/dev/null",
                          shell=True, stdout=subprocess.PIPE, start_new_session=True)
    n = 0
    for ligne in pr.stdout:
        vus = set()
        marche(json.loads(ligne), "", vus)
        for c in vus:
            stats[c][0] += 1
            stats[c][1].add(m)
        n += 1
        if n >= N:
            break
    if n >= N and pr.poll() is None:
        try:
            os.killpg(pr.pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass
    pr.stdout.close()
    pr.wait()
    total += n
    print(f"MOIS {m} {d} {n}", file=sys.stderr)
print("MOIS_ECHANTILLONNES", len(cles))
print("LIGNES", total)
chemins = sorted({p for p, t in stats})
print("CHEMINS", len(chemins))
for (p, t), (n, ms) in sorted(stats.items()):
    print(f"{p}\t{t}\t{n}\t{len(ms)}\t{min(ms)}\t{max(ms)}")
conflits = [p for p in chemins if len({t for q, t in stats if q == p and t != "null"}) > 1]
print("CONFLITS_TYPE", len(conflits))
for p in conflits:
    print("CONFLIT", p, sorted({t for q, t in stats if q == p}))
print("CLES_INTERDITES", sum(alertes.values()), dict(alertes))
