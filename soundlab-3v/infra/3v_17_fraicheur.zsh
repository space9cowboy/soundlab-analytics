#!/usr/bin/env zsh
# 3v_17 (tache 5.1) : mesure de la fraicheur.
# 1. Role de pilotage v2 (PutMetricData sur SoundLab/3V seulement), simule chemin par chemin (infra/3v_12).
# 2. Lambda de pilotage v3 : inscrire publie FraicheurDisponibiliteSecondes, PartEcoutesRecentesPourcent et
#    EcoutesChargees avant d'ecrire le registre.
# 3. Tableau de bord soundlab-3v (celui du Bloc 6, soundlab-bigdata, n'est pas modifie).
# Aucune execution de la machine a etats n'est lancee : la premiere publication viendra du chargement de 02:00 UTC.
set -euo pipefail
REGION=eu-north-1; COMPTE=589276558852
TABLEAU=soundlab-3v
T=$(mktemp -d)

echo "--- 1. Role de pilotage v2"
zsh infra/3v_12_role_pilotage.zsh

echo "--- 2. Lambda de pilotage v3"
aws lambda update-function-code --function-name soundlab-3v-pilotage \
  --zip-file fileb://lambda/3v_pilotage/paquet.zip --query 'CodeSha256' --output text
aws lambda wait function-updated-v2 --function-name soundlab-3v-pilotage && echo "PILOTAGE_V3_EN_LIGNE"

echo "--- 3. Tableau de bord ${TABLEAU}"
aws cloudwatch list-dashboards --dashboard-name-prefix ${TABLEAU} --query 'length(DashboardEntries)' --output text \
  | grep -qx 0 || { echo "TABLEAU_EXISTANT ${TABLEAU} : rien n'est ecrase"; exit 1; }
cat > ${T}/tableau.json <<EOF
{"start":"-P7D","widgets":[
 {"type":"text","x":0,"y":0,"width":24,"height":3,"properties":{"markdown":"## SoundLab 3V : fraicheur du chargement quotidien ListenBrainz\\nPublie par le Lambda soundlab-3v-pilotage a chaque dump inscrit au registre (machine soundlab-3v-chargement-incremental, 02:00 UTC). **Fraicheur** : heure d'inscription moins fin de la fenetre de reception du dump. **Part recente** : ecoutes dont le retard au jour de reception est <= 30 jours."}},
 {"type":"metric","x":0,"y":3,"width":12,"height":6,"properties":{"title":"Fraicheur de disponibilite (heures)","region":"${REGION}","view":"timeSeries","stat":"Maximum","period":3600,
  "metrics":[[{"expression":"m1/3600","label":"Fraicheur (h)","id":"e1"}],["SoundLab/3V","FraicheurDisponibiliteSecondes","Source","ListenBrainz",{"id":"m1","visible":false}]],
  "yAxis":{"left":{"min":0}}}},
 {"type":"metric","x":12,"y":3,"width":12,"height":6,"properties":{"title":"Part des ecoutes recentes (%)","region":"${REGION}","view":"timeSeries","stat":"Maximum","period":3600,
  "metrics":[["SoundLab/3V","PartEcoutesRecentesPourcent","Source","ListenBrainz"]],"yAxis":{"left":{"min":0,"max":100}}}},
 {"type":"metric","x":0,"y":9,"width":12,"height":6,"properties":{"title":"Ecoutes chargees par jour","region":"${REGION}","view":"bar","stat":"Sum","period":86400,
  "metrics":[["SoundLab/3V","EcoutesChargees","Source","ListenBrainz"]]}},
 {"type":"alarm","x":12,"y":9,"width":12,"height":6,"properties":{"title":"Echec d'un job EMR (alarme du Bloc 6)",
  "alarms":["arn:aws:cloudwatch:${REGION}:${COMPTE}:alarm:soundlab-emr-jobs-en-echec"]}}
]}
EOF
python3 -m json.tool ${T}/tableau.json > /dev/null && echo "JSON_TABLEAU_OK"
aws cloudwatch put-dashboard --dashboard-name ${TABLEAU} --dashboard-body file://${T}/tableau.json \
  --query 'length(DashboardValidationMessages)' --output text | sed 's/^/MESSAGES_DE_VALIDATION /'
aws cloudwatch get-dashboard --dashboard-name ${TABLEAU} --query 'DashboardBody' --output text \
  | python3 -c 'import json,sys; b=json.load(sys.stdin); print("WIDGETS", len(b["widgets"]), [w["type"] for w in b["widgets"]])'
echo "bloc 6 inchange : $(aws cloudwatch list-dashboards --dashboard-name-prefix soundlab-bigdata --query 'DashboardEntries[0].LastModified' --output text)"
rm -rf ${T}
echo "FRAICHEUR_DEPLOYEE"
