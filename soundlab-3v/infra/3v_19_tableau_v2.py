# 3v_19 (tache 5.1) : tableau soundlab-3v v2. Periode de 60 s sur les trois widgets de metriques (un dump quotidien
# et sa tranche de minuit, inscrits a deux minutes d'intervalle, ne sont plus fusionnes dans la meme heure) ;
# axe de fraicheur libelle en heures, sans l'unite automatique (Seconds) heritee de la metrique.
# Ne modifie que le tableau soundlab-3v ; le tableau du Bloc 6 (soundlab-bigdata) n'est pas touche.
import json, subprocess, sys, tempfile

def aws(*a):
    r = subprocess.run(["aws", *a], capture_output=True, text=True)
    if r.returncode:
        sys.exit("ERREUR_AWS " + r.stderr.strip()[-300:])
    return r.stdout

corps = json.loads(aws("cloudwatch", "get-dashboard", "--dashboard-name", "soundlab-3v", "--query", "DashboardBody", "--output", "text"))
titres = [w["properties"].get("title") for w in corps["widgets"] if w["type"] == "metric"]
attendus = ["Fraicheur de disponibilite (heures)", "Part des ecoutes recentes (%)", "Ecoutes chargees par jour"]
if titres != attendus:
    sys.exit("TABLEAU_INATTENDU %s : rien n'est modifie" % titres)
for w in corps["widgets"]:
    if w["type"] != "metric":
        continue
    p = w["properties"]
    if p["title"].startswith("Ecoutes"):
        p["period"] = 86400
        continue
    p["period"] = 60
    p["setPeriodToTimeRange"] = False
    if p["title"].startswith("Fraicheur"):
        p["yAxis"] = {"left": {"min": 0, "label": "heures", "showUnits": False}}
    else:
        p["yAxis"] = {"left": {"min": 0, "max": 100, "label": "%", "showUnits": False}}
corps["periodOverride"] = "inherit"
with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
    json.dump(corps, f)
msg = aws("cloudwatch", "put-dashboard", "--dashboard-name", "soundlab-3v", "--dashboard-body", "file://" + f.name,
          "--query", "length(DashboardValidationMessages)", "--output", "text").strip()
print("MESSAGES_DE_VALIDATION", msg)
relu = json.loads(aws("cloudwatch", "get-dashboard", "--dashboard-name", "soundlab-3v", "--query", "DashboardBody", "--output", "text"))
for w in relu["widgets"]:
    if w["type"] == "metric":
        p = w["properties"]
        print("WIDGET", p["title"], "| periode", p["period"], "| axe", p.get("yAxis", {}).get("left", {}))
print("PERIODE_DU_TABLEAU", relu.get("periodOverride"))
print("BLOC6", aws("cloudwatch", "list-dashboards", "--dashboard-name-prefix", "soundlab-bigdata", "--query",
                   "DashboardEntries[0].LastModified", "--output", "text").strip())
