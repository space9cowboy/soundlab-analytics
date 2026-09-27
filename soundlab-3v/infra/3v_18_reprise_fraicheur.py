# 3v_18 (tache 5.1) : reprise des metriques de fraicheur des dumps charges par la machine avant la v3 du pilotage.
# Pour chaque sortie de l'etat Inscrire dans l'historique des executions (dumps 2676 a 2681), rappelle l'action
# inscrire du Lambda v3 avec l'heure reelle d'inscription : la metrique est publiee telle qu'elle etait a ce moment,
# horodatee a ce moment. L'entree du registre est reecrite a l'identique (empreinte controlee avant et apres).
# Garde : refuse si l'espace SoundLab/3V contient deja des metriques (pas de double publication).
import datetime, hashlib, json, os, subprocess, sys, tempfile, time

MACHINE = "arn:aws:states:eu-north-1:589276558852:stateMachine:soundlab-3v-chargement-incremental"
REGISTRE = "s3://soundlab-curated-558852/trois_v/_etat/chargements_incr.json"
FONCTION = "soundlab-3v-pilotage"
ESPACE = "SoundLab/3V"


def aws(*args, texte=False):
    r = subprocess.run(["aws", *args], capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit("ERREUR_AWS " + " ".join(args[:2]) + " : " + r.stderr.strip()[-300:])
    return r.stdout if texte else json.loads(r.stdout)


def registre():
    b = subprocess.run(["aws", "s3", "cp", REGISTRE, "-"], capture_output=True, check=True).stdout
    return json.loads(b), hashlib.sha256(b).hexdigest()


n_metriques = aws("cloudwatch", "list-metrics", "--namespace", ESPACE, "--query", "length(Metrics)", "--output", "json")
if n_metriques != 0:
    sys.exit(f"REPRISE_DEJA_FAITE : {n_metriques} metriques dans {ESPACE}, rien n'est publie")

reg, sha_avant = registre()
print("REGISTRE_AVANT", sha_avant[:12], "entrees", len(reg["dumps"]))

points = {}
for ex in aws("stepfunctions", "list-executions", "--state-machine-arn", MACHINE, "--output", "json")["executions"]:
    hist = aws("stepfunctions", "get-execution-history", "--execution-arn", ex["executionArn"], "--output", "json")
    for e in hist["events"]:
        d = e.get("stateExitedEventDetails")
        if e["type"] == "TaskStateExited" and d and d["name"] == "Inscrire":
            sortie = json.loads(d["output"])
            n = sortie["n"]
            if n in points:
                sys.exit(f"INSCRIPTION_DOUBLE {n} : rien n'est publie")
            t = datetime.datetime.fromisoformat(e["timestamp"]) if isinstance(e["timestamp"], str) else e["timestamp"]
            points[n] = t.timestamp()
print("INSCRIPTIONS_TROUVEES", sorted(points))
if sorted(points) != [str(x) for x in range(2676, 2682)]:
    sys.exit("POINTS_INATTENDUS : rien n'est publie")

resultats = {}
for n in sorted(points):
    ent = reg["dumps"][n]
    ev = {"action": "inscrire", "n": n, "job": ent["job"], "verdict": ent["verdict"], "start": ent["start"],
          "end": ent["end"], "ecoutes": ent["ecoutes_manifeste"], "_maintenant": points[n]}
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(ev, f)
    out = tempfile.mktemp(suffix=".json")
    err = aws("lambda", "invoke", "--function-name", FONCTION, "--cli-binary-format", "raw-in-base64-out",
              "--payload", "file://" + f.name, "--query", "FunctionError", "--output", "text", out, texte=True).strip()
    r = json.load(open(out))
    os.unlink(f.name); os.unlink(out)
    if err != "None":
        sys.exit(f"ECHEC_LAMBDA {n} : {r} (points deja publies : {sorted(resultats)})")
    if r["inscrit"] != ent:
        sys.exit(f"ENTREE_DIFFERENTE {n} : {r['inscrit']} / {ent}")
    resultats[n] = r["metriques"]
    m = r["metriques"]
    print(f"PUBLIE {n} inscrit {datetime.datetime.fromtimestamp(points[n], datetime.timezone.utc):%Y-%m-%d %H:%M:%S} UTC"
          f" | fraicheur {m['FraicheurDisponibiliteSecondes'] / 3600:.2f} h | recentes {m['PartEcoutesRecentesPourcent']} %"
          f" | ecoutes {int(m['EcoutesChargees'])}")

_, sha_apres = registre()
print("REGISTRE_APRES", sha_apres[:12], "IDENTIQUE" if sha_apres == sha_avant else "DIFFERENT")
if sha_apres != sha_avant:
    sys.exit("REGISTRE_MODIFIE")

debut = datetime.datetime.fromtimestamp(min(points.values()) - 3600, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
fin = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
for essai in range(12):
    vus = aws("cloudwatch", "get-metric-statistics", "--namespace", ESPACE, "--metric-name", "FraicheurDisponibiliteSecondes",
              "--dimensions", "Name=Source,Value=ListenBrainz", "--start-time", debut, "--end-time", fin,
              "--period", "60", "--statistics", "SampleCount", "--output", "json")["Datapoints"]
    total = int(sum(p["SampleCount"] for p in vus))
    if total >= len(points):
        break
    time.sleep(15)
print("POINTS_LUS_DANS_CLOUDWATCH", total, "attendus", len(points))
print("REPRISE_OK" if total == len(points) else "REPRISE_INCOMPLETE")
