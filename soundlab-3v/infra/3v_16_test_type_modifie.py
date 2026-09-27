# 3v_16 (tache 3.3, decision du 26/09) : tests locaux de 3v_24 v6 (rebut sous seuil des lignes TYPE_MODIFIE).
# Lance le job en Spark local sur des lots fabriques de 20 000 lignes (seuil 1e-4 -> 2 lignes tolerees) :
#   C0 sain ; C1 1 ligne ai.music_service booleen (cas reel de 2680) ; C2 2 lignes (limite) ; C3 3 lignes (hors seuil) ;
#   C4 2 champs differents ; C5 1 ligne ai.duration_ms texte (champ entier, lecture Spark non conforme) ;
#   C6 = C1 passe a 3v_24 v5 (doit echouer : preuve que les tests distinguent v5 et v6).
# Usage : python3 infra/3v_16_test_type_modifie.py jobs/3v_24_aplatissement.py <v5 sauvegarde> <contrat json>
# Le job est copie en variante locale (lecture incrementale en .json au lieu de .json.zst, seule difference).
import glob, hashlib, json, os, re, shutil, subprocess, sys, tempfile

V6, V5, CONTRAT = sys.argv[1:4]
N = 20000
T0 = 1790251200            # 2026-09-24 12:00:00 UTC
RECEPTION = "2026-09-25"
racine = tempfile.mkdtemp(prefix="3v16_")


def variante(src, nom):
    code = open(src).read()
    cible = "part-{DUMP}.json.zst"
    assert code.count(cible) == 1, "lecture incrementale du brut attendue une seule fois"
    dst = os.path.join(racine, nom)
    open(dst, "w").write(code.replace(cible, "part-{DUMP}.json"))
    return dst


def ligne(i, alterations):
    ai = {"duration_ms": 1000 + i, "music_service": "spotify.com"}
    tm = {"artist_name": "A%d" % (i % 50), "track_name": "T%d" % i, "release_name": "R%d" % (i % 70), "additional_info": ai}
    d = {"user_id": hashlib.md5(str(i % 3000).encode()).hexdigest(), "timestamp": T0 + i,
         "recording_msid": "m%d" % i, "track_metadata": tm}
    for chemin, valeur in alterations.get(i, []):
        o = d
        *tete, fin = chemin.split(".")
        for k in tete:
            o = o[k]
        o[fin] = valeur
    return json.dumps(d)


MS = "track_metadata.additional_info.music_service"
CAS = {
    "C0": {},
    "C1": {7: [(MS, True)]},
    "C2": {7: [(MS, True)], 900: [(MS, False)]},
    "C3": {7: [(MS, True)], 900: [(MS, False)], 5000: [(MS, True)]},
    "C4": {7: [(MS, True)], 900: [("track_metadata.release_name", 42)]},
    "C5": {7: [("track_metadata.additional_info.duration_ms", "abc")]},
}


def lancer(job, cas, n_dump):
    brut = os.path.join(racine, cas, "brut")
    os.makedirs(os.path.join(brut, "dump=%d" % n_dump))
    with open(os.path.join(brut, "dump=%d" % n_dump, "part-%d.json" % n_dump), "w") as f:
        for i in range(N):
            f.write(ligne(i, CAS[cas.split("_")[0]]) + "\n")
    ap, rb = os.path.join(racine, cas, "aplati"), os.path.join(racine, cas, "rebut")
    r = subprocess.run([sys.executable, job, brut, "-", "-", ap, rb, str(N), CONTRAT,
                        "--incremental", str(n_dump), "--reception", RECEPTION],
                       capture_output=True, text=True, env=dict(os.environ, PYSPARK_PYTHON=sys.executable))
    out = r.stdout
    fichiers = lambda d: len(glob.glob(d + "/**/*.parquet", recursive=True))
    m = re.search(r"SORTIE LIGNES (\d+)", out)
    motifs = {}
    if fichiers(rb):
        from pyspark.sql import SparkSession
        sp = SparkSession.builder.master("local[1]").getOrCreate()
        motifs = {x["motif"]: x["count"] for x in sp.read.parquet(rb).groupBy("motif").count().collect()}
    return {"rc": r.returncode, "ok": "APLATI_OK" in out, "contrat_echec": "CONTRAT_ECHEC" in out,
            "lignes": int(m.group(1)) if m else None, "f_aplati": fichiers(ap), "f_rebut": fichiers(rb),
            "motifs": motifs, "tm": re.findall(r"^TYPE_MODIFIE .*$", out, re.M), "fin": out.strip().splitlines()[-3:],
            "err": r.stderr[-1500:] if r.returncode and not out.strip() else ""}


j6, j5 = variante(V6, "job_v6.py"), variante(V5, "job_v5.py")
res = []


def verif(nom, cond, detail):
    res.append(bool(cond))
    print(("OK    " if cond else "ECHEC ") + nom + ("" if cond else "  " + json.dumps(detail, default=str)[:1500]), flush=True)


r = lancer(j6, "C0", 901)
verif("C0 sain : APLATI_OK, 20000 lignes, aucun rebut", r["ok"] and r["lignes"] == N and r["f_rebut"] == 0 and not r["tm"], r)
r = lancer(j6, "C1", 902)
verif("C1 1 ligne booleen : TOLERE, 19999 lignes, rebut type_modifie:ai.music_service = 1",
      r["ok"] and r["lignes"] == N - 1 and r["motifs"] == {"type_modifie:ai.music_service": 1}
      and any("TOLERE" in x for x in r["tm"]), r)
r = lancer(j6, "C2", 903)
verif("C2 2 lignes (limite 1e-4) : TOLERE, 19998 lignes, rebut 2",
      r["ok"] and r["lignes"] == N - 2 and r["motifs"] == {"type_modifie:ai.music_service": 2}, r)
r = lancer(j6, "C3", 904)
verif("C3 3 lignes : REFUSE, CONTRAT_ECHEC, aucune ecriture",
      r["rc"] == 1 and r["contrat_echec"] and r["f_aplati"] == 0 and r["f_rebut"] == 0 and any("REFUSE" in x for x in r["tm"]), r)
r = lancer(j6, "C4", 905)
verif("C4 2 champs differents : REFUSE, CONTRAT_ECHEC, aucune ecriture",
      r["rc"] == 1 and r["contrat_echec"] and r["f_aplati"] == 0 and r["f_rebut"] == 0 and any("REFUSE" in x for x in r["tm"]), r)
r = lancer(j6, "C5", 906)
verif("C5 champ entier en texte : TOLERE, rebut type_modifie:ai.duration_ms = 1",
      r["ok"] and r["lignes"] == N - 1 and r["motifs"] == {"type_modifie:ai.duration_ms": 1}, r)
r = lancer(j5, "C1_v5", 907)
verif("C6 meme lot que C1 avec v5 : CONTRAT_ECHEC (v5 et v6 se distinguent)",
      r["rc"] == 1 and r["contrat_echec"] and r["f_aplati"] == 0, r)

shutil.rmtree(racine, ignore_errors=True)
print(("V6_TESTS_OK" if all(res) else "V6_TESTS_ECHEC"), "%d/%d" % (sum(res), len(res)))
sys.exit(0 if all(res) else 1)
