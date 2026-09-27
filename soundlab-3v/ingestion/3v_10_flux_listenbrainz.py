# 3v_10 v2 (B8) : ingestion ListenBrainz en LISTE BLANCHE.
# Seules les cles "gardes" du contrat de schema sont ecrites, a chaque niveau (racine, track_metadata,
# additional_info, mbid_mapping, elements de mbid_mapping.artists). Toute autre cle est retiree avant
# ecriture et comptee dans le manifeste par famille (interdites_ou_purgees, ecartees, inconnues) :
# noms et effectifs seulement, jamais de valeur. user_id pseudonymise (HMAC-SHA256 sale, 32 hex).
# Le manifeste enregistre aussi les bornes START/END du dump (tranches contigues, politique L5).
# v3 (S1) : --mode incremental ecrit le dump TEL QUEL en un seul objet sous
# <dest>/dump=<id>/part-<id>.json.zst (zone de transit) ; les ecoutes y couvrent des milliers de
# jours (mesure : 7 534 jours pour le dump 2674), la repartition par date est faite par Spark (3.1/3.2).
# Le mode complet (defaut) est inchange : un objet par jour d'ecoute, controle de mois.
import sys, os, json, tarfile, hmac, hashlib, subprocess, datetime, argparse, collections, time, shlex, signal

p = argparse.ArgumentParser()
p.add_argument("--dest", required=True)
p.add_argument("--dump-id", required=True)
p.add_argument("--mois-max", default=None)
p.add_argument("--seuil-octets-json", type=int, default=0)
p.add_argument("--secret-id", default="soundlab/pseudonymisation-salt-listenbrainz")
p.add_argument("--sel-factice", action="store_true")
p.add_argument("--manifeste", required=True)
p.add_argument("--reprendre-apres", default=None)
p.add_argument("--contrat", default="config/3v_contrat_listenbrainz_v1.json")
p.add_argument("--mode", choices=["complet", "incremental"], default="complet")
a = p.parse_args()
INCR = a.mode == "incremental"
assert not (INCR and (a.mois_max or a.seuil_octets_json or a.reprendre_apres)), "options du mode complet interdites en incremental"

if a.sel_factice:
    assert not a.dest.startswith("s3://"), "sel factice interdit vers S3"
    sel = b"0" * 64
else:
    sel = subprocess.run(["aws", "secretsmanager", "get-secret-value", "--secret-id", a.secret_id,
                          "--query", "SecretString", "--output", "text"],
                         capture_output=True, text=True, check=True).stdout.strip().encode()
    assert len(sel) == 64, "sel de longueur inattendue"

CONTRAT = json.load(open(a.contrat, encoding="utf-8"))
NIV = CONTRAT["niveaux"]
INTERDITES = set(CONTRAT["interdites_toujours"])
PREFIXE = {"racine": "racine.", "tm": "tm.", "ai": "ai.", "mm": "mm.", "artiste": "mm.artists[]."}
UTC = datetime.timezone.utc
retirees = {"interdites_ou_purgees": collections.Counter(), "ecartees": collections.Counter(),
            "inconnues": collections.Counter()}


def filtrer(obj, niveau):
    gardes = NIV[niveau]["gardes"]
    out = {}
    for k, v in obj.items():
        d = gardes.get(k)
        if d is None:
            if k in INTERDITES or k in NIV[niveau]["purges"]:
                fam = "interdites_ou_purgees"
            elif k in NIV[niveau]["ecartes"]:
                fam = "ecartees"
            else:
                fam = "inconnues"
            retirees[fam][PREFIXE[niveau] + k] += 1
            continue
        if d["type"] == "objet" and isinstance(v, dict):
            v = filtrer(v, d["niveau"])
        elif d["type"] == "liste_objet" and isinstance(v, list):
            v = [filtrer(x, d["niveau"]) if isinstance(x, dict) else x for x in v]
        out[k] = v
    return out


def ouvrir(jour):
    part = f"dump={a.dump_id}" if INCR else f"date={jour}"
    if a.dest.startswith("s3://"):
        cible = f"{a.dest}/{part}/part-{a.dump_id}.json.zst"
        cmd = f"zstd -q -3 -c | aws s3 cp - {shlex.quote(cible)} --only-show-errors"
    else:
        dossier = f"{a.dest}/{part}"
        os.makedirs(dossier, exist_ok=True)
        cmd = f"zstd -q -3 -c > {shlex.quote(dossier + '/part-' + a.dump_id + '.json.zst')}"
    return subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, start_new_session=True)


def abandonner(ecrivains):
    for w in ecrivains.values():
        try:
            os.killpg(w.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


man = {"dump_id": a.dump_id, "dest": a.dest, "debut": time.strftime("%Y-%m-%dT%H:%M:%S"),
       "version_ingestion": "3v_10 v3 liste blanche", "mode": a.mode, "contrat_version": CONTRAT["version"],
       "bornes": {}, "mois": [], "ecoutes_total": 0, "octets_json": 0, "arret_apres": None,
       "statut": "EN_COURS", "mois_ignores": 0, "attente_max_s": 0.0}
if INCR:
    man["fichiers"] = []
jours_incr = collections.Counter()
json_cumul = 0
t0 = time.time()
ecrivains = {}
try:
    with tarfile.open(fileobj=sys.stdin.buffer, mode="r|") as t:
        for m in t:
            nom = m.name.rsplit("/", 1)[-1]
            if nom in ("START_TIMESTAMP", "END_TIMESTAMP", "SCHEMA_SEQUENCE"):
                man["bornes"][nom] = t.extractfile(m).read().decode("utf-8").strip()
                continue
            if not m.name.endswith(".listens"):
                continue
            morceaux = m.name.split("/")
            annee, mois = int(morceaux[-2]), int(morceaux[-1].split(".")[0])
            cle_mois = f"{annee:04d}-{mois:02d}"
            if a.reprendre_apres and cle_mois <= a.reprendre_apres:
                f = t.extractfile(m)
                while f.read(1 << 20):
                    pass
                json_cumul += m.size
                man["mois_ignores"] += 1
                continue
            if not INCR:
                ecrivains = {}
            par_jour = collections.Counter()
            for ligne in t.extractfile(m):
                json_cumul += len(ligne)
                d = json.loads(ligne)
                dt = datetime.datetime.fromtimestamp(d["timestamp"], UTC)
                if not INCR and (dt.year, dt.month) != (annee, mois):
                    raise RuntimeError(f"ecoute hors mois dans {m.name}")
                d = filtrer(d, "racine")
                d["user_id"] = hmac.new(sel, str(d["user_id"]).encode(), hashlib.sha256).hexdigest()[:32]
                jour = dt.date().isoformat()
                cle_w = "dump" if INCR else jour
                w = ecrivains.get(cle_w)
                if w is None:
                    w = ecrivains[cle_w] = ouvrir(jour)
                w.stdin.write((json.dumps(d, separators=(",", ":")) + "\n").encode())
                par_jour[jour] += 1
            n = sum(par_jour.values())
            man["ecoutes_total"] += n
            if INCR:
                jours_incr.update(par_jour)
                man["fichiers"].append({"fichier_mois": cle_mois, "ecoutes": n})
                print(f"{cle_mois} (fichier) {n} ecoutes {len(par_jour)} jours json_cumul={json_cumul} t={time.time()-t0:.0f}s", flush=True)
                continue
            t_att = time.time()
            for jour, w in ecrivains.items():
                w.stdin.close()
                if w.wait() != 0:
                    raise RuntimeError(f"ecriture en echec pour {jour}")
            attente = time.time() - t_att
            man["attente_max_s"] = round(max(man["attente_max_s"], attente), 1)
            ecrivains = {}
            man["mois"].append({"mois": cle_mois, "ecoutes": n, "jours": len(par_jour)})
            print(f"{cle_mois} {n} ecoutes {len(par_jour)} jours json_cumul={json_cumul} attente={attente:.1f}s t={time.time()-t0:.0f}s", flush=True)
            if (a.mois_max and cle_mois >= a.mois_max) or (a.seuil_octets_json and json_cumul >= a.seuil_octets_json):
                man["arret_apres"] = cle_mois
                break
    if INCR:
        t_att = time.time()
        for cle_w, w in ecrivains.items():
            w.stdin.close()
            if w.wait() != 0:
                raise RuntimeError("ecriture en echec pour le dump")
        man["attente_max_s"] = round(time.time() - t_att, 1)
        ecrivains = {}
        par_mois = collections.defaultdict(lambda: [0, 0])
        for j, c in jours_incr.items():
            par_mois[j[:7]][0] += c
            par_mois[j[:7]][1] += 1
        man["mois"] = [{"mois": k, "ecoutes": v[0], "jours": v[1]} for k, v in sorted(par_mois.items())]
        man["jours_distincts"] = len(jours_incr)
        man["premier_jour"] = min(jours_incr) if jours_incr else None
        man["dernier_jour"] = max(jours_incr) if jours_incr else None
    man["statut"] = "OK"
except BaseException as e:
    abandonner(ecrivains)
    man["statut"] = f"ECHEC {type(e).__name__}: {e}"
finally:
    man["octets_json"] = json_cumul
    man["duree_s"] = round(time.time() - t0, 1)
    man["cles_retirees"] = {f: dict(sorted(c.items())) for f, c in retirees.items()}
    json.dump(man, open(a.manifeste, "w"), indent=2)
    print("STATUT", man["statut"], "MOIS", len(man["mois"]), "ECOUTES", man["ecoutes_total"], "ARRET_APRES", man["arret_apres"],
          "IGNORES", man["mois_ignores"], "ATTENTE_MAX_S", man["attente_max_s"], flush=True)
    print("CLES_RETIREES", {f: sum(c.values()) for f, c in retirees.items()},
          "INCONNUES_DISTINCTES", len(retirees["inconnues"]), flush=True)
    sys.exit(0 if man["statut"] == "OK" else 1)
