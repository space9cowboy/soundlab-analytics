# 3v_11 v3 (tache 5.1) : T12 ajoute (metriques de fraicheur ; sortie de test completee d'une ligne JOURS). v2 (tache 3.3, E2) : T11 ajoute (sortie de startJobRun.sync). Tests locaux du Lambda de pilotage, sans AWS ni reseau.
# Faux S3 / Step Functions / HTTP en memoire. Donnees : registre reel (2674, 2675), les 15 derniers noms
# publies et les 3 lignes reelles de la sortie du pilote du job 00g92i6hfdmqjg1f.
# T1 equivalence exhaustive de continuite() avec 3v_09 v2 (importe tel quel depuis infra/).
# Chaque test peut echouer ; sortie finale PILOTAGE_TESTS_OK n/n ou PILOTAGE_TESTS_ECHEC.
import gzip, importlib.util, io, itertools, json, sys


def charger(nom, chemin):
    spec = importlib.util.spec_from_file_location(nom, chemin)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


L = charger("pilotage", "lambda/3v_pilotage/lambda_function.py")
R09 = charger("r09", "infra/3v_09_charger_incremental.py")


class Absent(Exception):
    def __init__(self):
        self.response = {"Error": {"Code": "NoSuchKey"}}


class FauxS3:
    def __init__(self, objets):
        self.o = dict(objets)
        self.ecrits = []
    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.o:
            raise Absent()
        return {"Body": io.BytesIO(self.o[(Bucket, Key)])}
    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.o[(Bucket, Key)] = Body
        self.ecrits.append((Bucket, Key))


class FauxCW:
    def __init__(self, echec=False):
        self.envois, self.echec = [], echec
    def put_metric_data(self, Namespace, MetricData):
        if self.echec:
            raise RuntimeError("PutMetricData refuse")
        self.envois.append((Namespace, MetricData))


class FauxSFN:
    def __init__(self, en_cours):
        self.e = en_cours
    def list_executions(self, stateMachineArn, statusFilter, maxResults):
        assert statusFilter == "RUNNING"
        return {"executions": [{"executionArn": a} for a in self.e]}


REGISTRE = {"dumps": {
    "2674": {"start": "2026-09-22 00:00:02.910209+00:00", "end": "2026-09-23 00:00:03.324607+00:00",
             "ecoutes_manifeste": 5008271, "lignes_aplati": 5008271, "job": "00g92i0qvka5ig1f", "verdict": "PREMIER"},
    "2675": {"start": "2026-09-23 00:00:03.324607+00:00", "end": "2026-09-23 00:00:03.469785+00:00",
             "ecoutes_manifeste": 1, "lignes_aplati": 1, "job": "00g92i6hfdmqjg1f", "verdict": "SUITE"}}}
NOMS = """2666-20260917-000002 2667-20260918-000003 2668-20260919-000002 2669-20260920-000003
2670-20260921-000003 2672-20260922-000003 2673-20260922-000002 2674-20260923-000003 2675-20260923-000003
2676-20260924-000002 2677-20260924-000002 2678-20260925-000003 2679-20260925-000003 2680-20260926-000003
2681-20260926-000003""".split()
# L'index Apache cite chaque nom deux fois (href et texte) : on reproduit cette forme.
PAGE = "\n".join(f'<a href="listenbrainz-dump-{x}-incremental/">listenbrainz-dump-{x}-incremental/</a>'
                 for x in NOMS).encode()
SORTIE = ("CONTRAT_OK\nJOURS partitions_jour 1 ecoutes_recentes 1 ecoutes_anciennes 0 reception 2026-09-23\n"
          "SORTIE LIGNES 1 MOIS 1 REBUT_RELU 0 UID_INVALIDES 0 DATES_INCOHERENTES 0\n"
          "MODE incremental dump=2675 DOUBLONS_LOT 0\nAPLATI_OK\n")
K_REG = (L.B_CUR, L.K_REGISTRE)
res = []


def verif(nom, cond, detail=""):
    res.append(bool(cond))
    print(("OK    " if cond else "ECHEC ") + nom + ("" if cond else "  " + str(detail)), flush=True)


def leve(exc, f):
    try:
        f()
    except exc as e:
        return str(e) or True
    except Exception as e:
        return False
    return False


def page(u):
    assert u == L.BASE, u
    return io.BytesIO(PAGE)


def s3_base():
    return FauxS3({K_REG: json.dumps(REGISTRE).encode()})


def man(n, deb, fin, ecoutes=7, statut="OK", mode="incremental"):
    return json.dumps({"dump_id": str(n), "statut": statut, "mode": mode, "ecoutes_total": ecoutes,
                       "bornes": {"START_TIMESTAMP": deb, "END_TIMESTAMP": fin}}).encode()


# T1 : equivalence de continuite() avec 3v_09 v2 sur toutes les combinaisons de bornes
T = ["t0", "t1", "t2", "t3", "t4", "t5"]
ecarts, cas = [], 0
for charges in ([], ["10"], ["12"], ["10", "12"], ["11"], ["10", "11", "12"]):
    for (i, j) in itertools.combinations_with_replacement(range(len(T)), 2):
        reg = {"dumps": {}}
        for d in charges:
            k = int(d) - 10
            reg["dumps"][d] = {"start": T[2 * k] if 2 * k < 6 else "t5", "end": T[min(2 * k + 1, 5)]}
        m = {"bornes": {"START_TIMESTAMP": T[i], "END_TIMESTAMP": T[j]}}
        cas += 1
        if L.continuite("11", m, reg) != R09.continuite("11", m, reg):
            ecarts.append((charges, i, j))
verif(f"T1 continuite identique a 3v_09 v2 ({cas} cas)", not ecarts, ecarts[:3])

# T2 : planifier sur le registre reel -> 2676..2681 dans l'ordre, noms complets
s3 = s3_base()
r = L.handler({"action": "planifier", "_s3": s3, "_sfn": FauxSFN([]), "_ouvrir": page,
               "machine": "arn:m", "execution": "arn:moi"})
attendu = [{"n": str(n), "dump": x} for n, x in zip(range(2676, 2682), NOMS[-6:])]
verif("T2 planifier -> 2676..2681", r["dumps"] == attendu and r["dernier_charge"] == "2675"
      and r["dernier_publie"] == "2681" and r["motif"] == "A_CHARGER", r)
verif("T2b planifier n'ecrit rien", s3.ecrits == [], s3.ecrits)

# T3 : trou de numerotation (2671 absent) sans effet ; registre a jour -> liste vide
reg = json.loads(json.dumps(REGISTRE))
reg["dumps"]["2681"] = dict(reg["dumps"]["2675"])
s3 = FauxS3({K_REG: json.dumps(reg).encode()})
r = L.handler({"action": "planifier", "_s3": s3, "_sfn": FauxSFN([]), "_ouvrir": page})
verif("T3 registre a jour -> RIEN_A_CHARGER", r["dumps"] == [] and r["motif"] == "RIEN_A_CHARGER", r)

# T4 : autre execution en cours -> liste vide, index non lu
lu = []
r = L.handler({"action": "planifier", "_s3": s3_base(), "_sfn": FauxSFN(["arn:autre", "arn:moi"]),
               "_ouvrir": lambda u: lu.append(u) or page(u), "machine": "arn:m", "execution": "arn:moi"})
verif("T4 execution concurrente -> vide", r["dumps"] == [] and r["motif"] == "EXECUTION_CONCURRENTE" and not lu, r)
r = L.handler({"action": "planifier", "_s3": s3_base(), "_sfn": FauxSFN(["arn:moi"]), "_ouvrir": page,
               "machine": "arn:m", "execution": "arn:moi"})
verif("T4b seule l'execution courante -> planifie", len(r["dumps"]) == 6, r)

# T5 : registre absent -> RegistreVide ; index sans dump -> SortieNonConforme
verif("T5 registre absent -> RegistreVide",
      leve(L.RegistreVide, lambda: L.handler({"action": "planifier", "_s3": FauxS3({}), "_sfn": FauxSFN([]),
                                                "_ouvrir": page})))
verif("T5b index vide -> SortieNonConforme",
      leve(L.SortieNonConforme, lambda: L.handler({"action": "planifier", "_s3": s3_base(), "_sfn": FauxSFN([]),
                                                     "_ouvrir": lambda u: io.BytesIO(b"<html></html>")})))

# T6 : continuite SUITE pour 2676 ; attendu en texte ; reception = START[:10]
fin75 = REGISTRE["dumps"]["2675"]["end"]
s3 = s3_base()
s3.o[(L.B_CUR, L.K_MANIF.format("2676"))] = man(2676, fin75, "2026-09-24 00:00:02.1+00:00", ecoutes=4321)
r = L.handler({"action": "continuite", "n": "2676", "_s3": s3})
verif("T6 continuite 2676 SUITE", r["verdict"] == "SUITE" and r["attendu"] == "4321" and r["ecoutes"] == 4321
      and r["reception"] == "2026-09-23" and r["start"] == fin75, r)

# T7 : trou, chevauchement, manifeste non conforme, manifeste d'un autre dump
for nom, deb, exc in (("TROU", "2026-09-23 00:00:04+00:00", L.ContinuiteRompue),
                      ("CHEVAUCHEMENT", "2026-09-23 00:00:03+00:00", L.ContinuiteRompue)):
    s3 = s3_base()
    s3.o[(L.B_CUR, L.K_MANIF.format("2676"))] = man(2676, deb, "2026-09-24 00:00:02+00:00")
    msg = leve(exc, lambda: L.handler({"action": "continuite", "n": "2676", "_s3": s3}))
    verif(f"T7 {nom} -> ContinuiteRompue", msg and nom in str(msg), msg)
for nom, contenu in (("statut ECHEC", man(2676, fin75, "x", statut="ECHEC")),
                     ("mode complet", man(2676, fin75, "x", mode="complet")),
                     ("dump_id 2677", man(2677, fin75, "x"))):
    s3 = s3_base()
    s3.o[(L.B_CUR, L.K_MANIF.format("2676"))] = contenu
    verif(f"T7b manifeste {nom} -> SortieNonConforme",
          leve(L.SortieNonConforme, lambda: L.handler({"action": "continuite", "n": "2676", "_s3": s3})))

# T8 : inscrire depuis la sortie reelle ; format d'entree identique au registre existant
K_LOG = (L.B_LOG, L.K_STDOUT.format(L.APP_EMR, "jobX"))
ev = {"action": "inscrire", "n": "2676", "job": "jobX", "verdict": "SUITE", "start": fin75,
      "end": "2026-09-24 00:00:02.1+00:00", "ecoutes": 1, "_cw": FauxCW(), "_maintenant": 1790208002.1 + 7200}
s3 = s3_base()
s3.o[K_LOG] = gzip.compress(SORTIE.replace("dump=2675", "dump=2676").encode())
r = L.handler(dict(ev, _s3=s3))
nouv = json.loads(s3.o[K_REG])
verif("T8 inscrire ecrit 2676 et garde 2674/2675",
      s3.ecrits == [K_REG] and sorted(nouv["dumps"]) == ["2674", "2675", "2676"]
      and nouv["dumps"]["2674"] == REGISTRE["dumps"]["2674"] and nouv["dumps"]["2676"]["lignes_aplati"] == 1, nouv)
verif("T8b memes champs que les entrees de 3v_09",
      set(nouv["dumps"]["2676"]) == set(REGISTRE["dumps"]["2675"]), sorted(nouv["dumps"]["2676"]))

# T9 : log absent -> LogsAbsents ; sortie d'un autre dump, sans APLATI_OK, sans SORTIE -> refus, rien d'ecrit
s3 = s3_base()
verif("T9 stdout absent -> LogsAbsents", leve(L.LogsAbsents, lambda: L.handler(dict(ev, _s3=s3))))
for nom, texte in (("autre dump", SORTIE),
                   ("sans APLATI_OK", SORTIE.replace("dump=2675", "dump=2676").replace("APLATI_OK\n", "APLATI_ECHEC\n")),
                   ("sans SORTIE LIGNES", "MODE incremental dump=2676 DOUBLONS_LOT 0\nAPLATI_OK\n"),
                   ("dump=26760", SORTIE.replace("dump=2675", "dump=26760"))):
    s3 = s3_base()
    s3.o[K_LOG] = gzip.compress(texte.encode())
    ok = leve(L.SortieNonConforme, lambda: L.handler(dict(ev, _s3=s3)))
    verif(f"T9b sortie {nom} -> refus sans ecriture", ok and s3.ecrits == [], s3.ecrits)

# T10 : sortie reelle du job 00g92i6hfdmqjg1f relue comme 3v_09 -> 1 ligne
verif("T10 lecture de la sortie reelle 2675 = 1", L.lire_sortie_pilote(SORTIE, "2675") == 1)

# T11 (v2) : identifiant du job lu dans la sortie brute de startJobRun.sync, quelle que soit la forme
ev_sfn = {k: v for k, v in ev.items() if k != "job"}
for nom, emr in (("PascalCase imbrique", {"JobRun": {"JobRunId": "jobX", "State": "SUCCESS"}}),
                 ("camelCase plat", {"applicationId": "a", "jobRunId": "jobX", "arn": "x"})):
    s3 = s3_base()
    s3.o[K_LOG] = gzip.compress(SORTIE.replace("dump=2675", "dump=2676").encode())
    r = L.handler(dict(ev_sfn, emr=emr, _s3=s3))
    verif(f"T11 job lu en {nom}", r["inscrit"]["job"] == "jobX" and s3.ecrits == [K_REG], r)
for nom, emr in (("absent", {"State": "SUCCESS"}), ("deux valeurs", {"JobRunId": "a", "JobRun": {"JobRunId": "b"}})):
    s3 = s3_base()
    s3.o[K_LOG] = gzip.compress(SORTIE.replace("dump=2675", "dump=2676").encode())
    ok = leve(L.SortieNonConforme, lambda: L.handler(dict(ev_sfn, emr=emr, _s3=s3)))
    verif(f"T11b identifiant {nom} -> refus sans ecriture", ok and s3.ecrits == [], s3.ecrits)

# T12 (v3, tache 5.1) : metriques de fraicheur publiees avant l'ecriture du registre
SORTIE_2680 = ("CONTRAT_OK\nJOURS partitions_jour 32 ecoutes_recentes 1402493 ecoutes_anciennes 4952490 reception 2026-09-25\n"
               "JOURS_INCOHERENTS 0\nSORTIE LIGNES 6354983 MOIS 260 REBUT_RELU 1 UID_INVALIDES 0 DATES_INCOHERENTES 0\n"
               "MODE incremental dump=2680 DOUBLONS_LOT 0\nAPLATI_OK\n")
ev80 = {"action": "inscrire", "n": "2680", "job": "jobY", "verdict": "SUITE", "start": "2026-09-25 00:00:03.3+00:00",
        "end": "2026-09-26 00:00:03.469785+00:00", "ecoutes": 6354984}
K80 = (L.B_LOG, L.K_STDOUT.format(L.APP_EMR, "jobY"))
t_fin = 1790380803.469785
s3 = s3_base(); s3.o[K80] = gzip.compress(SORTIE_2680.encode()); cw = FauxCW()
r = L.handler(dict(ev80, _s3=s3, _cw=cw, _maintenant=t_fin + 3 * 3600 + 5))
env = {m["MetricName"]: (m["Value"], m["Unit"], m["Dimensions"]) for _, d in cw.envois for m in d}
verif("T12 fraicheur 10805 s, part recentes 22,0692 %, 6354983 ecoutes, espace SoundLab/3V",
      [e[0] for e in cw.envois] == ["SoundLab/3V"] and env["FraicheurDisponibiliteSecondes"][:2] == (10805.0, "Seconds")
      and env["PartEcoutesRecentesPourcent"][:2] == (22.0692, "Percent") and env["EcoutesChargees"][:2] == (6354983.0, "Count")
      and all(v[2] == [{"Name": "Source", "Value": "ListenBrainz"}] for v in env.values()) and s3.ecrits == [K_REG], env)
verif("T12b horodatage du fichier END en format registre relu (microsecondes, +00:00)",
      abs(t_fin - __import__("datetime").datetime.fromisoformat(ev80["end"]).timestamp()) < 1e-6, t_fin)
for nom, texte in (("sans JOURS", SORTIE_2680.replace("JOURS partitions_jour 32 ecoutes_recentes 1402493 ecoutes_anciennes 4952490 reception 2026-09-25\n", "")),
                   ("JOURS incoherent", SORTIE_2680.replace("ecoutes_anciennes 4952490", "ecoutes_anciennes 4952491"))):
    s3 = s3_base(); s3.o[K80] = gzip.compress(texte.encode()); cw = FauxCW()
    ok = leve(L.SortieNonConforme, lambda: L.handler(dict(ev80, _s3=s3, _cw=cw, _maintenant=t_fin + 60)))
    verif(f"T12c sortie {nom} -> refus, ni metrique ni registre", ok and not cw.envois and s3.ecrits == [], (cw.envois, s3.ecrits))
s3 = s3_base(); s3.o[K80] = gzip.compress(SORTIE_2680.encode())
ok = leve(RuntimeError, lambda: L.handler(dict(ev80, _s3=s3, _cw=FauxCW(echec=True), _maintenant=t_fin + 60)))
verif("T12d echec CloudWatch -> registre non ecrit", ok and s3.ecrits == [], s3.ecrits)
s3 = s3_base(); s3.o[K80] = gzip.compress(SORTIE_2680.encode()); cw = FauxCW()
ok = leve(L.SortieNonConforme, lambda: L.handler(dict(ev80, _s3=s3, _cw=cw, _maintenant=t_fin - 1)))
verif("T12e horloge anterieure a END -> refus", ok and not cw.envois and s3.ecrits == [], cw.envois)

n_ok = sum(res)
print(("PILOTAGE_TESTS_OK" if all(res) else "PILOTAGE_TESTS_ECHEC"), f"{n_ok}/{len(res)}")
sys.exit(0 if all(res) else 1)
