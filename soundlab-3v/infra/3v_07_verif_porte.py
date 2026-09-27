# 3v_07 v2 (B8) : verifie les deux rapports de la porte 3v_30 (critere de fin de la tache 2.5).
# Sain : SUCCES, tous les controles reussis. Degrade : ECHEC, chaque controle bloquant en echec,
# et Q5 signale exactement les lots attendus (mesures du 26/09/2026).
import json, subprocess, sys
BASE = "s3://soundlab-curated-558852/trois_v/_rapports/qualite/"
ATT_MOIS = ["2005-02", "2005-03", "2005-04", "2005-05", "2005-06", "2005-07"]
ATT_INCR = ["20260927"]
ATT_INCONNUES = {"ai.comment": 3, "tm.track_mbid": 2}

def dernier(jeu):
    ls = subprocess.run(["aws", "s3", "ls", BASE], capture_output=True, text=True, check=True).stdout.split()
    noms = sorted(x for x in ls if x.startswith("rapport_%s_" % jeu))
    assert noms, "aucun rapport " + jeu
    txt = subprocess.run(["aws", "s3", "cp", BASE + noms[-1], "-"], capture_output=True, text=True, check=True).stdout
    return noms[-1], json.loads(txt)

ok = True
n, s = dernier("sain")
res = {r["controle"]: r for r in s["resultats"]}
ko = [c for c, r in res.items() if r["statut"] != "REUSSI"]
print("SAIN", n, "statut", s["statut"], "controles", len(res), "non_reussis", ko)
ok &= s["statut"] == "SUCCES" and len(res) == 12 and not ko
n, d = dernier("degrade")
res = {r["controle"]: r for r in d["resultats"]}
bloq = [c for c, r in res.items() if r["severite"] == "BLOQUANT"]
passent = [c for c in bloq if res[c]["statut"] != "ECHOUE"]
m = res["Q5_volume_mensuel"]["obtenu"]["hors_bornes"]
i = res["Q5_volume_incremental"]["obtenu"]["hors_bornes"]
print("DEGRADE", n, "statut", d["statut"], "bloquants", len(bloq), "bloquants_non_echoues", passent)
print("Q5_MOIS", m, "ATTENDU", ATT_MOIS)
print("Q5_INCR", i, "ATTENDU", ATT_INCR)
q1 = res["Q1_cles_inconnues_ingestion"]
print("Q1_INGESTION", q1["statut"], q1["obtenu"]["inconnues"], "ATTENDU", ATT_INCONNUES)
ok &= (d["statut"] == "ECHEC" and len(bloq) == 9 and not passent and m == ATT_MOIS and i == ATT_INCR
      and q1["statut"] == "ECHOUE" and q1["obtenu"]["inconnues"] == ATT_INCONNUES)
print("VERIF_PORTE_OK" if ok else "VERIF_PORTE_KO")
sys.exit(0 if ok else 1)
