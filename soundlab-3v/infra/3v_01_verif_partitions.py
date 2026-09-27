import json, re, sys, datetime, collections
REGLES = [
    ("ecoutes", re.compile(r"^listenbrainz/ecoutes/date=(\d{4}-\d{2}-\d{2})/[^/]+$"), lambda g: g),
    ("aplati", re.compile(r"^listenbrainz/aplati/mois=(\d{4}-\d{2})/[^/]+$"), lambda g: g + "-01"),
    ("rebut", re.compile(r"^listenbrainz/rebut/mois=(\d{4}-\d{2})/[^/]+$"), lambda g: g + "-01"),
]
MARQUEURS = {"listenbrainz/aplati/_SUCCESS", "listenbrainz/rebut/_SUCCESS"}
def classe(c):
    if c in MARQUEURS:
        return "marqueur"
    for nom, motif, jour in REGLES:
        m = motif.match(c)
        if m:
            try:
                datetime.date.fromisoformat(jour(m.group(1)))
                return nom
            except ValueError:
                return None
    return None
cles = json.load(sys.stdin) or []
compte = collections.Counter()
hors = []
for c in cles:
    k = classe(c)
    if k is None:
        hors.append(c)
    else:
        compte[k] += 1
print("OBJETS", len(cles))
for nom in ("ecoutes", "aplati", "rebut", "marqueur"):
    print(nom.upper(), compte[nom])
print("HORS_CONVENTION", len(hors))
for c in hors:
    print("  REFUSE", c)
sys.exit(1 if hors else 0)
