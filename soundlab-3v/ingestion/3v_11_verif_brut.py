import sys, json, re, subprocess, datetime, collections
racine, dump_id = sys.argv[1], sys.argv[2]
SUPPR = {"submission_client", "media_player", "submission_client_version", "tags", "rating"}
UTC = datetime.timezone.utc
seau = racine[5:].split("/", 1)[0]
sortie = subprocess.run(["aws", "s3", "ls", racine + "/", "--recursive"], capture_output=True, text=True, check=True).stdout
cles = [l.split()[-1] for l in sortie.splitlines() if l.strip()]
cles = [c for c in cles if f"part-{dump_id}.json.zst" in c]
c = collections.Counter()
jetons = set()
for cle in cles:
    jour = re.search(r"date=(\d{4}-\d{2}-\d{2})/", cle).group(1)
    brut = subprocess.run(f"aws s3 cp s3://{seau}/{cle} - | zstd -dc", shell=True, capture_output=True, check=True).stdout
    for l in brut.splitlines():
        d = json.loads(l)
        c["lignes"] += 1
        c["user_name"] += "user_name" in d
        c["jeton_invalide"] += not re.fullmatch(r"[0-9a-f]{32}", str(d["user_id"]))
        c["hors_partition"] += datetime.datetime.fromtimestamp(d["timestamp"], UTC).date().isoformat() != jour
        ai = d.get("track_metadata", {}).get("additional_info") or {}
        c["champ_supprime_present"] += any(k in ai for k in SUPPR)
        jetons.add(d["user_id"])
print("FICHIERS", len(cles))
for k in ["lignes", "user_name", "jeton_invalide", "hors_partition", "champ_supprime_present"]:
    print(k.upper(), c[k])
print("JETONS_DISTINCTS", len(jetons))
