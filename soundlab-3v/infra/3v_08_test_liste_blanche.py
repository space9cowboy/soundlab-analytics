# 3v_08 v2 (B8 + S1) : test local de l'ingestion 3v_10 v2 en liste blanche (B8). Sel factice, aucune ecriture S3.
# Construit un dump synthetique (une ecoute par famille de cles), l'ingere, puis verifie :
# sortie limitee aux cles gardees a chaque niveau, texte libre absent des octets ecrits,
# user_id pseudonymise, comptage exact des cles retirees, bornes du dump enregistrees.
import io, json, os, subprocess, sys, tarfile, tempfile, re

CONTRAT = "config/3v_contrat_listenbrainz_v1.json"
NIV = json.load(open(CONTRAT, encoding="utf-8"))["niveaux"]
TS = 1893456000 + 3600
L = [
    {"user_id": "u1", "user_name": "nom-fictif", "timestamp": TS, "recording_msid": "m1",
     "track_metadata": {"artist_name": "A", "track_name": "T", "tags": ["x"], "track_mbid": "tm-inconnue",
                        "media-player": "p",
                        "additional_info": {"duration_ms": 1, "origin_url": "u", "ip_addr": "192.0.2.1",
                                            "submission_client": "c", "comment": "TEXTE_LIBRE_SECRET", "tags": ["y"]},
                        "mbid_mapping": {"recording_mbid": "r", "zzz": 1,
                                         "artists": [{"artist_mbid": "a", "artist_credit_name": "N", "cle_x": "y"}]}}},
    {"user_id": "u2", "timestamp": TS + 60, "recording_msid": "m2",
     "track_metadata": {"artist_name": "B", "track_name": "U"}},
    {"user_id": "u3", "user_name": "autre-nom", "timestamp": TS + 120, "recording_msid": "m3",
     "track_metadata": {"artist_name": "C", "track_name": "V", "additional_info": {"rating": 5}}},
]
ATTENDU = {"interdites_ou_purgees": {"ai.ip_addr": 1, "ai.rating": 1, "ai.submission_client": 1, "ai.tags": 1,
                                     "racine.user_name": 2, "tm.tags": 1},
           "ecartees": {"ai.origin_url": 1, "tm.media-player": 1},
           "inconnues": {"ai.comment": 1, "mm.artists[].cle_x": 1, "mm.zzz": 1, "tm.track_mbid": 1}}
BORNES = {"START_TIMESTAMP": "2030-01-01 00:00:00.000001+00:00", "END_TIMESTAMP": "2030-01-02 00:00:00.000002+00:00"}


def hors_liste(o, niveau, pre=""):
    g = NIV[niveau]["gardes"]
    out = []
    for k, v in o.items():
        if k not in g:
            out.append(pre + k)
        elif g[k]["type"] == "objet" and isinstance(v, dict):
            out += hors_liste(v, g[k]["niveau"], pre + k + ".")
        elif g[k]["type"] == "liste_objet" and isinstance(v, list):
            for x in v:
                if isinstance(x, dict):
                    out += hors_liste(x, g[k]["niveau"], pre + k + "[].")
    return out


assert hors_liste(L[0], "racine"), "controle du controle : le verificateur doit voir les cles hors liste"
buf = io.BytesIO()
with tarfile.open(fileobj=buf, mode="w") as t:
    for nom, val in list(BORNES.items()) + [("listens/2030/1.listens", "".join(json.dumps(x) + "\n" for x in L))]:
        b = val.encode()
        ti = tarfile.TarInfo("dump-test/" + nom)
        ti.size = len(b)
        t.addfile(ti, io.BytesIO(b))
with tempfile.TemporaryDirectory() as d:
    man = os.path.join(d, "man.json")
    r = subprocess.run([sys.executable, "ingestion/3v_10_flux_listenbrainz.py", "--dest", d + "/out", "--dump-id", "test",
                        "--sel-factice", "--manifeste", man, "--contrat", CONTRAT],
                       input=buf.getvalue(), capture_output=True)
    print(r.stdout.decode().strip())
    brut = subprocess.run("cat " + d + "/out/date=*/part-test.json.zst | zstd -dc", shell=True,
                          capture_output=True, check=True).stdout
    lignes = [json.loads(x) for x in brut.decode().splitlines()]
    m = json.load(open(man))
    ecarts = [c for x in lignes for c in hors_liste(x, "racine")]
    uid_ok = all(re.fullmatch("[0-9a-f]{32}", x["user_id"]) for x in lignes)
    fuite = [s for s in (b"TEXTE_LIBRE_SECRET", b"nom-fictif", b"192.0.2.1", b"autre-nom") if s in brut]
    ok = {
        "statut": r.returncode == 0 and m["statut"] == "OK",
        "lignes": len(lignes) == len(L),
        "cles_hors_liste_en_sortie": not ecarts,
        "valeurs_sensibles_absentes": not fuite,
        "user_id_pseudonymise": uid_ok,
        "comptage_exact": m["cles_retirees"] == ATTENDU,
        "bornes": all(m["bornes"].get(k) == v for k, v in BORNES.items()),
    }
    for k, v in ok.items():
        print("CONTROLE", k, "OK" if v else "ECHEC")
    if not ok["comptage_exact"]:
        print("VU", m["cles_retirees"])
    if ecarts or fuite:
        print("ECARTS", ecarts, "FUITES", fuite)
    print("LISTE_BLANCHE_OK" if all(ok.values()) else "LISTE_BLANCHE_ECHEC")
    ok_complet = all(ok.values())

# --- Mode incremental (S1) : deux fichiers .listens, dates d'ecoute sur plusieurs annees,
# un seul objet ecrit sous dump=<id>, aucune ecoute perdue, statistiques par mois d'ecoute.
import datetime
def ts(j):
    return int(datetime.datetime.fromisoformat(j + "T12:00:00+00:00").timestamp())
I9 = [dict(L[1], timestamp=ts("2005-02-13")), dict(L[0], timestamp=ts("2026-09-22")), dict(L[2], timestamp=ts("2026-09-22"))]
I10 = [dict(L[1], timestamp=ts("2026-10-01")), dict(L[1], timestamp=ts("2024-10-05"))]
buf = io.BytesIO()
with tarfile.open(fileobj=buf, mode="w") as t:
    for nom, val in list(BORNES.items()) + [("listens/2026/9.listens", "".join(json.dumps(x) + "\n" for x in I9)),
                                            ("listens/2026/10.listens", "".join(json.dumps(x) + "\n" for x in I10))]:
        b = val.encode()
        ti = tarfile.TarInfo("dump-incr/" + nom)
        ti.size = len(b)
        t.addfile(ti, io.BytesIO(b))
with tempfile.TemporaryDirectory() as d:
    man = os.path.join(d, "man.json")
    r = subprocess.run([sys.executable, "ingestion/3v_10_flux_listenbrainz.py", "--dest", d + "/out", "--dump-id", "incr",
                        "--sel-factice", "--manifeste", man, "--contrat", CONTRAT, "--mode", "incremental"],
                       input=buf.getvalue(), capture_output=True)
    print(r.stdout.decode().strip())
    fichiers = sorted(os.path.relpath(os.path.join(x, f), d + "/out") for x, _, fs in os.walk(d + "/out") for f in fs)
    brut = subprocess.run(["zstd", "-dc", d + "/out/dump=incr/part-incr.json.zst"], capture_output=True).stdout
    lignes = [json.loads(x) for x in brut.decode().splitlines()]
    m = json.load(open(man))
    ok2 = {
        "statut_incremental": r.returncode == 0 and m["statut"] == "OK" and m["mode"] == "incremental",
        "un_seul_objet_dump": fichiers == ["dump=incr/part-incr.json.zst"],
        "aucune_ecoute_perdue": len(lignes) == 5 and m["ecoutes_total"] == 5,
        "mois_d_ecoute": m["mois"] == [{"mois": "2005-02", "ecoutes": 1, "jours": 1}, {"mois": "2024-10", "ecoutes": 1, "jours": 1},
                                       {"mois": "2026-09", "ecoutes": 2, "jours": 1}, {"mois": "2026-10", "ecoutes": 1, "jours": 1}],
        "bornes_jours": (m["premier_jour"], m["dernier_jour"], m["jours_distincts"]) == ("2005-02-13", "2026-10-01", 4),
        "liste_blanche_incremental": not [c for x in lignes for c in hors_liste(x, "racine")]
                                     and b"TEXTE_LIBRE_SECRET" not in brut,
    }
    for k, v in ok2.items():
        print("CONTROLE", k, "OK" if v else "ECHEC")
    if not ok2["un_seul_objet_dump"]:
        print("FICHIERS", fichiers)
    print("INCREMENTAL_OK" if all(ok2.values()) else "INCREMENTAL_ECHEC")
sys.exit(0 if ok_complet and all(ok2.values()) else 1)
