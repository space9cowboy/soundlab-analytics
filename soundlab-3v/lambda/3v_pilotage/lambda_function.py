# Lambda de pilotage ListenBrainz (tache 3.3, E2) v3 (tache 5.1) : inscrire publie la fraicheur dans CloudWatch
# (espace SoundLab/3V) AVANT d'ecrire le registre : une entree au registre implique des metriques publiees.
# v2 : inscrire accepte la sortie brute de startJobRun.sync (cle emr).
# v1 : trois actions appelees par la machine a etats.
#  planifier  : dumps incrementaux publies de numero strictement superieur au dernier du registre, dans
#               l'ordre ; liste vide si le registre est vide (point de depart impose) ou si une autre
#               execution de la machine a etats est en cours.
#  continuite : relit le manifeste ecrit par le Lambda d'ingestion et applique la meme regle que 3v_09 v2
#               (bornes START/END) ; TROU ou CHEVAUCHEMENT = exception ContinuiteRompue (arret nomme).
#  inscrire   : relit la sortie du pilote Spark de 3v_24 (APLATI_OK, MODE, SORTIE LIGNES) et ajoute
#               l'entree au registre, au meme format que 3v_09 v2.
# Aucune donnee personnelle ni secret n'est lu ici : manifestes et registre ne portent que des comptages.
import datetime, gzip, json, re, time, urllib.request

BASE = "https://data.metabrainz.org/pub/musicbrainz/listenbrainz/incremental/"
B_CUR = "soundlab-curated-558852"
B_LOG = "soundlab-logs-558852"
K_REGISTRE = "trois_v/_etat/chargements_incr.json"
K_MANIF = "trois_v/_manifestes/listenbrainz/manifeste_listenbrainz_{}_incremental.json"
K_STDOUT = "emr-serverless/applications/{}/jobs/{}/SPARK_DRIVER/stdout.gz"
APP_EMR = "00g8l9brbs9e3f1d"
ESPACE_METRIQUES = "SoundLab/3V"
DIMENSIONS = [{"Name": "Source", "Value": "ListenBrainz"}]
MOTIF_DUMP = re.compile(r"listenbrainz-dump-(\d+)-(\d{8})-(\d{6})-incremental")


class ContinuiteRompue(Exception):
    pass


class LogsAbsents(Exception):
    pass


class SortieNonConforme(Exception):
    pass


class RegistreVide(Exception):
    pass


def _code(e):
    return getattr(e, "response", {}).get("Error", {}).get("Code")


def lire_json(s3, bucket, cle):
    return json.loads(s3.get_object(Bucket=bucket, Key=cle)["Body"].read())


def lire_registre(s3):
    try:
        return lire_json(s3, B_CUR, K_REGISTRE)
    except Exception as e:
        if _code(e) in ("NoSuchKey", "404"):
            return {"dumps": {}}
        raise


def continuite(dump, man, registre):
    """Copie conforme de 3v_09 v2. Verdicts : PREMIER, SUITE, RECHARGEMENT, TROU, CHEVAUCHEMENT."""
    charges = registre["dumps"]
    if str(dump) in charges:
        return "RECHARGEMENT", "dump deja charge, ecrasement de ses partitions"
    deb, fin = man["bornes"]["START_TIMESTAMP"], man["bornes"]["END_TIMESTAMP"]
    suiv = [int(d) for d in charges if int(d) > int(dump)]
    if suiv:
        n = str(min(suiv))
        deb_n = charges[n]["start"]
        if fin != deb_n:
            return ("TROU" if fin < deb_n else "CHEVAUCHEMENT"), f"fin {fin} / debut du dump suivant {n} {deb_n}"
    prec = [int(d) for d in charges if int(d) < int(dump)]
    if not prec:
        return "PREMIER", "aucun dump anterieur charge"
    p = str(max(prec))
    fin_p = charges[p]["end"]
    if deb == fin_p:
        return "SUITE", f"debut {deb} = fin du dump {p}"
    return ("TROU" if deb > fin_p else "CHEVAUCHEMENT"), f"debut {deb} / fin du dump {p} {fin_p}"


def planifier(event, s3, sfn, ouvrir):
    # Garde : une seule execution a la fois (le registre n'a pas de verrou).
    machine, moi = event.get("machine"), event.get("execution")
    if machine and moi:
        en_cours = [x["executionArn"] for x in
                    sfn.list_executions(stateMachineArn=machine, statusFilter="RUNNING", maxResults=10)["executions"]]
        autres = [x for x in en_cours if x != moi]
        if autres:
            return {"dumps": [], "motif": "EXECUTION_CONCURRENTE", "autres": autres}
    registre = lire_registre(s3)
    if not registre["dumps"]:
        raise RegistreVide("registre vide : le point de depart doit etre charge a la main (3v_09)")
    dernier = max(int(d) for d in registre["dumps"])
    page = ouvrir(BASE).read().decode("utf-8", "replace")
    publies = {}
    for m in MOTIF_DUMP.finditer(page):
        publies[int(m.group(1))] = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    if not publies:
        raise SortieNonConforme("aucun dump reconnu dans l'index publie")
    a_charger = [{"n": str(n), "dump": publies[n]} for n in sorted(publies) if n > dernier]
    return {"dumps": a_charger, "motif": "RIEN_A_CHARGER" if not a_charger else "A_CHARGER",
            "dernier_charge": str(dernier), "dernier_publie": str(max(publies))}


def controle_continuite(event, s3):
    n = str(event["n"])
    man = lire_json(s3, B_CUR, K_MANIF.format(n))
    if man.get("statut") != "OK" or man.get("mode") != "incremental" or str(man.get("dump_id")) != n:
        raise SortieNonConforme(f"manifeste non conforme pour {n}")
    verdict, detail = continuite(n, man, lire_registre(s3))
    if verdict in ("TROU", "CHEVAUCHEMENT"):
        raise ContinuiteRompue(f"{n} {verdict} - {detail}")
    b = man["bornes"]
    return {"n": n, "verdict": verdict, "detail": detail, "start": b["START_TIMESTAMP"], "end": b["END_TIMESTAMP"],
            "ecoutes": man["ecoutes_total"], "attendu": str(man["ecoutes_total"]),
            "reception": b["START_TIMESTAMP"][:10]}


def lire_sortie_pilote(texte, n):
    lignes = texte.splitlines()
    if "APLATI_OK" not in lignes:
        raise SortieNonConforme(f"APLATI_OK absent de la sortie du dump {n}")
    if not any(l.startswith(f"MODE incremental dump={n} ") or l == f"MODE incremental dump={n}" for l in lignes):
        raise SortieNonConforme(f"la sortie ne concerne pas le dump {n}")
    m = [re.match(r"SORTIE LIGNES (\d+)\b", l) for l in lignes]
    m = [x for x in m if x]
    if len(m) != 1:
        raise SortieNonConforme(f"{len(m)} ligne(s) SORTIE LIGNES pour le dump {n}")
    return int(m[0].group(1))


def trouver_job(obj):
    """Identifiant du job dans la sortie de emr-serverless:startJobRun.sync, quelle que soit la casse."""
    vus = set()
    def parcourir(o):
        if isinstance(o, dict):
            for k, v in o.items():
                if k.lower() == "jobrunid" and isinstance(v, str):
                    vus.add(v)
                else:
                    parcourir(v)
        elif isinstance(o, list):
            for v in o:
                parcourir(v)
    parcourir(obj)
    if len(vus) != 1:
        raise SortieNonConforme(f"{len(vus)} identifiant(s) de job dans la sortie EMR")
    return vus.pop()


def lire_jours(texte, n):
    m = [re.match(r"JOURS partitions_jour (\d+) ecoutes_recentes (\d+) ecoutes_anciennes (\d+)\b", l)
         for l in texte.splitlines()]
    m = [x for x in m if x]
    if len(m) != 1:
        raise SortieNonConforme(f"{len(m)} ligne(s) JOURS pour le dump {n}")
    return int(m[0].group(2)), int(m[0].group(3))


def metriques_fraicheur(end, lignes, recentes, anciennes, n, maintenant):
    fin = datetime.datetime.fromisoformat(end).timestamp()
    if recentes + anciennes != lignes:
        raise SortieNonConforme(f"JOURS {recentes}+{anciennes} different de SORTIE LIGNES {lignes} pour le dump {n}")
    fraicheur = maintenant - fin
    if fraicheur <= 0:
        raise SortieNonConforme(f"fraicheur negative ou nulle pour le dump {n}")
    part = 100.0 * recentes / lignes if lignes else 0.0
    return [("FraicheurDisponibiliteSecondes", round(fraicheur, 1), "Seconds"),
            ("PartEcoutesRecentesPourcent", round(part, 4), "Percent"),
            ("EcoutesChargees", float(lignes), "Count")]


def inscrire(event, s3, cw):
    n = str(event["n"])
    job = event["job"] if "job" in event else trouver_job(event["emr"])
    try:
        brut = s3.get_object(Bucket=B_LOG, Key=K_STDOUT.format(APP_EMR, job))["Body"].read()
    except Exception as e:
        if _code(e) in ("NoSuchKey", "404"):
            raise LogsAbsents(f"stdout du job {job} pas encore publie")
        raise
    texte = gzip.decompress(brut).decode("utf-8", "replace")
    lignes = lire_sortie_pilote(texte, n)
    recentes, anciennes = lire_jours(texte, n)
    maintenant = event.get("_maintenant", time.time())
    mesures = metriques_fraicheur(event["end"], lignes, recentes, anciennes, n, maintenant)
    horodatage = datetime.datetime.fromtimestamp(maintenant, datetime.timezone.utc)
    cw.put_metric_data(Namespace=ESPACE_METRIQUES, MetricData=[
        {"MetricName": nom, "Dimensions": DIMENSIONS, "Timestamp": horodatage, "Value": val, "Unit": unite}
        for nom, val, unite in mesures])
    registre = lire_registre(s3)
    entree = {"start": event["start"], "end": event["end"], "ecoutes_manifeste": int(event["ecoutes"]),
              "lignes_aplati": lignes, "job": job, "verdict": event["verdict"]}
    registre["dumps"][n] = entree
    s3.put_object(Bucket=B_CUR, Key=K_REGISTRE, Body=json.dumps(registre, indent=1).encode(),
                  ContentType="application/json")
    return {"n": n, "inscrit": entree, "metriques": {nom: val for nom, val, _ in mesures}}


def handler(event, context=None):
    action = event["action"]
    s3 = event.get("_s3")
    sfn = event.get("_sfn")
    cw = event.get("_cw")
    if s3 is None or (sfn is None and action == "planifier") or (cw is None and action == "inscrire"):
        import boto3
        s3 = s3 or boto3.client("s3")
        sfn = sfn or boto3.client("stepfunctions")
        cw = cw or boto3.client("cloudwatch")
    ouvrir = event.get("_ouvrir") or (lambda u: urllib.request.urlopen(u, timeout=30))
    if action == "planifier":
        return planifier(event, s3, sfn, ouvrir)
    if action == "continuite":
        return controle_continuite(event, s3)
    if action == "inscrire":
        return inscrire(event, s3, cw)
    raise ValueError("action inconnue " + action)
