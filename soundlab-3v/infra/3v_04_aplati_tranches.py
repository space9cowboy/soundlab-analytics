import argparse, calendar, json, os, re, subprocess, sys, time

APP = "00g8l9brbs9e3f1d"
LOGS = "s3://soundlab-logs-558852/emr-serverless/applications/" + APP + "/jobs/{}/SPARK_DRIVER/stdout.gz"
TOTAL = 695656837

def mois_manifestes(fichiers):
    mois = {}
    for f in fichiers:
        for e in json.load(open(f))["mois"]:
            assert e["mois"] not in mois, "mois en double " + e["mois"]
            mois[e["mois"]] = e["ecoutes"]
    return sorted(mois.items())

def tranches(mois, cible):
    res, cur = [], []
    for m, n in mois:
        if cur and sum(x for _, x in cur) + n > cible:
            res.append(cur)
            cur = []
        cur.append((m, n))
    if cur:
        res.append(cur)
    out = []
    for i, t in enumerate(res, 1):
        a, b = t[0][0], t[-1][0]
        y, mo = map(int, b.split("-"))
        out.append((i, a + "-01", "%s-%02d" % (b, calendar.monthrange(y, mo)[1]), sum(x for _, x in t), len(t)))
    return out

def lancer(i, debut, fin, attendu, brut):
    log = "data/aplati_tranche_%02d.log" % i
    with open(log, "w") as f:
        subprocess.run(["bash", "infra/submit_job.sh", "jobs/3v_24_aplatissement.py", brut + "/ecoutes", debut, fin,
                        brut + "/aplati", brut + "/rebut", str(attendu)], stdout=f, stderr=subprocess.STDOUT)
    m = re.search(r"jobRunId = ([a-z0-9]+)", open(log).read())
    if not m:
        return None, "", log
    jid = m.group(1)
    sortie = subprocess.run(["bash", "-o", "pipefail", "-c", "aws s3 cp '%s' - | gunzip" % LOGS.format(jid)],
                            capture_output=True, text=True).stdout
    return jid, sortie, log

def cout(jid):
    for _ in range(20):
        r = json.loads(subprocess.run(["aws", "emr-serverless", "get-job-run", "--application-id", APP,
                                       "--job-run-id", jid, "--output", "json"],
                                      capture_output=True, text=True, check=True).stdout)["jobRun"]
        v = (r.get("billedResourceUtilization") or {}).get("vCPUHour")
        if v is not None:
            return r["state"], r.get("totalExecutionDurationSeconds"), v
        time.sleep(15)
    return r["state"], r.get("totalExecutionDurationSeconds"), None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cible-lignes", type=int, default=100000000)
    ap.add_argument("--journal", default="data/aplati_tranches.journal")
    ap.add_argument("--tranches", type=int, default=0)
    ap.add_argument("--plan-seul", action="store_true")
    a = ap.parse_args()
    assert not os.environ.get("SL_SPARK_PARAMS"), "SL_SPARK_PARAMS doit etre vide"
    brut = "s3://%s/listenbrainz" % os.environ["SL_B_RAW"]
    mois = mois_manifestes(["data/manifeste_listenbrainz_2663_essai.json", "data/manifeste_listenbrainz_2663_50gio.json"])
    total = sum(n for _, n in mois)
    print("MANIFESTES mois", len(mois), "ecoutes", total, "ATTENDU", TOTAL, flush=True)
    if total != TOTAL:
        print("MANIFESTES_INCOHERENTS")
        sys.exit(1)
    plan = tranches(mois, a.cible_lignes)
    for i, d, f, n, nm in plan:
        print("TRANCHE %02d %s %s mois %d lignes %d" % (i, d, f, nm, n), flush=True)
    print("TRANCHES", len(plan), "SOMME", sum(p[3] for p in plan), flush=True)
    if a.plan_seul:
        return
    faits = set()
    if os.path.exists(a.journal):
        faits = {int(l.split()[1]) for l in open(a.journal) if l.startswith("FAIT ")}
    a_faire = [p for p in plan if p[0] not in faits]
    if a.tranches:
        a_faire = a_faire[:a.tranches]
    print("DEJA_FAITES", len(faits), "A_LANCER", len(a_faire), flush=True)
    for i, d, f, n, nm in a_faire:
        t0 = time.time()
        jid, sortie, log = lancer(i, d, f, n, brut)
        garde = [l for l in sortie.splitlines() if re.match(r"(LUES|CATEGORIES|MOTIFS|CLES_INCONNUES|  CLE |ECHECS|PLAN|SORTIE|COUVERTURE|FICHIERS|COLONNES|PHASES|APLATI_)", l)]
        for l in garde:
            print("  [%02d] %s" % (i, l), flush=True)
        if not jid or "APLATI_OK" not in sortie:
            print("TRANCHE_ECHEC %02d jobRunId %s journal %s" % (i, jid, log))
            sys.exit(1)
        etat, duree, vcpu = cout(jid)
        with open(a.journal, "a") as j:
            j.write("FAIT %02d %s %s %d %s %s %s\n" % (i, d, f, n, jid, duree, vcpu))
        print("TRANCHE_OK %02d %s duree_s %s vcpu %s mur_s %d" % (i, jid, duree, vcpu, time.time() - t0), flush=True)
    faits = {int(l.split()[1]) for l in open(a.journal) if l.startswith("FAIT ")} if os.path.exists(a.journal) else set()
    print("BILAN tranches_faites", len(faits), "/", len(plan))

if __name__ == "__main__":
    main()
