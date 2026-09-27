# 3v_28 (tache 3.3, echec du dump 2683 lors d'un chargement nocturne) : tests locaux de 3v_24 v7 (dump vide).
#   V0 lot vide, attendu 0 : v7 APLATI_OK, DUMP_VIDE, SORTIE LIGNES 0, aucune ecriture ; v6 sur le meme lot : echec (temoin).
#   V1 3 lignes toutes incognito : v7 doit echouer (APLATI_VIDE_NON_ATTENDU), aucune ecriture.
#   V2 lot vide, attendu 1 : v7 doit echouer.
#   V3 lot normal de 2 000 lignes : v7 et v6 donnent la meme sortie (lignes, fichiers, empreinte des lignes).
# Usage : python3 infra/3v_28_test_dump_vide.py <v7> <v6> <contrat json>
import glob, hashlib, json, os, re, shutil, subprocess, sys, tempfile

V7, V6, CONTRAT = sys.argv[1:4]
T0 = 1790251200
RECEPTION = "2026-09-25"
racine = tempfile.mkdtemp(prefix="3v28_")


def variante(src, nom):
    code = open(src).read()
    cible = "part-{DUMP}.json.zst"
    assert code.count(cible) == 1
    dst = os.path.join(racine, nom)
    open(dst, "w").write(code.replace(cible, "part-{DUMP}.json"))
    return dst


def ligne(i, incognito=False):
    ai = {"duration_ms": 1000 + i, "music_service": "spotify.com"}
    if incognito:
        ai["incognito_mode"] = True
    tm = {"artist_name": "A%d" % (i % 50), "track_name": "T%d" % i, "release_name": "R%d" % (i % 70), "additional_info": ai}
    return json.dumps({"user_id": hashlib.md5(str(i % 300).encode()).hexdigest(), "timestamp": T0 + i,
                       "recording_msid": "m%d" % i, "track_metadata": tm})


def lancer(job, cas, n_dump, lignes, attendu):
    brut = os.path.join(racine, cas, "brut")
    os.makedirs(os.path.join(brut, "dump=%d" % n_dump))
    with open(os.path.join(brut, "dump=%d" % n_dump, "part-%d.json" % n_dump), "w") as f:
        for l in lignes:
            f.write(l + "\n")
    ap, rb = os.path.join(racine, cas, "aplati"), os.path.join(racine, cas, "rebut")
    r = subprocess.run([sys.executable, job, brut, "-", "-", ap, rb, str(attendu), CONTRAT,
                        "--incremental", str(n_dump), "--reception", RECEPTION],
                       capture_output=True, text=True, env=dict(os.environ, PYSPARK_PYTHON=sys.executable))
    out = r.stdout
    fic = sorted(glob.glob(ap + "/**/*.parquet", recursive=True))
    m = re.search(r"^SORTIE LIGNES (\d+)\b", out, re.M)
    emp = None
    if fic:
        from pyspark.sql import SparkSession, functions as F
        sp = SparkSession.builder.master("local[1]").getOrCreate()
        d = sp.read.parquet(ap)
        emp = (d.count(), d.select(F.sum(F.xxhash64(*[c for c in sorted(d.columns)])).alias("h")).first()["h"])
    return {"rc": r.returncode, "ok": re.search(r"^APLATI_OK$", out, re.M) is not None, "vide": "DUMP_VIDE" in out,
            "non_attendu": "APLATI_VIDE_NON_ATTENDU" in out, "lignes": int(m.group(1)) if m else None,
            "n_sortie": len(re.findall(r"^SORTIE LIGNES ", out, re.M)),
            "jours": re.findall(r"^JOURS partitions_jour .*$", out, re.M), "mode": re.findall(r"^MODE .*$", out, re.M),
            "f": len(fic), "f_rebut": len(glob.glob(rb + "/**/*.parquet", recursive=True)), "emp": emp,
            "fin": out.strip().splitlines()[-4:], "err": r.stderr[-1200:] if r.returncode else ""}


j7, j6 = variante(V7, "job_v7.py"), variante(V6, "job_v6.py")
res = []


def verif(nom, cond, detail):
    res.append(bool(cond))
    print(("OK    " if cond else "ECHEC ") + nom + ("" if cond else "  " + json.dumps(detail, default=str)[:1500]), flush=True)


r = lancer(j7, "V0", 2683, [], 0)
verif("V0 v7 lot vide : rc 0, APLATI_OK, DUMP_VIDE, SORTIE LIGNES 0 (une seule ligne), JOURS 0, MODE, aucune ecriture",
      r["rc"] == 0 and r["ok"] and r["vide"] and r["lignes"] == 0 and r["n_sortie"] == 1
      and r["jours"] == ["JOURS partitions_jour 0 ecoutes_recentes 0 ecoutes_anciennes 0 reception 2026-09-25"]
      and r["mode"] == ["MODE incremental dump=2683 DOUBLONS_LOT 0"] and r["f"] == 0 and r["f_rebut"] == 0, r)
r = lancer(j6, "V0_v6", 2683, [], 0)
verif("V0 temoin v6 meme lot : echec (le cas etait bien non gere)", r["rc"] != 0 and not r["ok"], r)
r = lancer(j7, "V1", 2690, [ligne(i, True) for i in range(3)], 3)
verif("V1 v7 3 lignes incognito : echec APLATI_VIDE_NON_ATTENDU, aucune ecriture",
      r["rc"] == 1 and r["non_attendu"] and not r["ok"] and r["f"] == 0, r)
r = lancer(j7, "V2", 2691, [], 1)
verif("V2 v7 lot vide mais attendu 1 : echec", r["rc"] == 1 and not r["ok"] and r["f"] == 0, r)
a = lancer(j7, "V3_v7", 2692, [ligne(i) for i in range(2000)], 2000)
b = lancer(j6, "V3_v6", 2692, [ligne(i) for i in range(2000)], 2000)
verif("V3 lot normal : v7 = v6 (APLATI_OK, 2000 lignes, memes fichiers, meme empreinte)",
      a["ok"] and b["ok"] and a["lignes"] == b["lignes"] == 2000 and a["f"] == b["f"] > 0 and a["emp"] == b["emp"]
      and not a["vide"], {"v7": a, "v6": b})

shutil.rmtree(racine, ignore_errors=True)
print(("V7_TESTS_OK" if all(res) else "V7_TESTS_ECHEC"), "%d/%d" % (sum(res), len(res)))
sys.exit(0 if all(res) else 1)
