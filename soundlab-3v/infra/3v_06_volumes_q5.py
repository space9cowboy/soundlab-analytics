# 3v_06 v2 (B8, L6) : construit les series de volumes de Q5 (porte 3v_30) a partir des manifestes
# d'ingestion (mois) et des tailles des incrementaux ListenBrainz (fichier "numero octets").
# Usage : python3 infra/3v_06_volumes_q5.py data/incrementaux_tailles.txt
import glob, json, sys
mois = {}
for f in sorted(glob.glob("data/*.json")):
    try:
        d = json.load(open(f))
    except Exception:
        continue
    if not (isinstance(d, dict) and isinstance(d.get("mois"), list) and "dump_id" in d):
        continue
    for e in d["mois"]:
        assert e["mois"] not in mois, "mois en double " + e["mois"]
        mois[e["mois"]] = e["ecoutes"]
total = sum(mois.values())
assert total == 695656837, "total des manifestes %d, attendu 695656837" % total
incr = []
for l in open(sys.argv[1]):
    n, t = l.split()
    assert t.isdigit(), "taille absente pour " + n
    incr.append((n, int(t)))
incr.sort(key=lambda x: int(x[0].split("-")[0]))
m = [{"lot": k, "n": v} for k, v in sorted(mois.items())]
i = [{"lot": k, "n": v} for k, v in incr]
# Degrade : series reelles + un jour SYNTHETIQUE ou seule une tranche de minuit existe
# (dump plein manquant) ; Q5 doit le signaler, et seulement lui.
i_deg = i + [{"lot": "9999-20260927-000000-SYNTHETIQUE", "n": 3000}]
json.dump({"mois": m, "incrementaux": i_deg}, open("data/3v_volumes_degrade.json", "w"), indent=1)
sain = {"mois": [x for x in m if x["lot"] >= "2005-08"], "incrementaux": i}
json.dump(sain, open("data/3v_volumes_sain.json", "w"), indent=1)
print("VOLUMES degrade mois", len(m), "incr", len(i), "| sain mois", len(sain["mois"]), "incr", len(sain["incrementaux"]),
      "| total", total)
