import sys, time, math
t_app = time.time()

# AN2 + AN4 : auditeurs distincts par titre, concentration, annees d'ecoute (AN2) ;
# representativite ListenBrainz face a Kaggle par annee de sortie, genre et artiste (AN4).
# Lit la table aplatie (jetons pseudonymises) ; n'ecrit que des comptages par titre ou par annee, sans jeton.

def quantile(v, q):
    s = sorted(v)
    if not s:
        return None
    p = (len(s) - 1) * q
    b = int(math.floor(p)); h = min(b + 1, len(s) - 1)
    return s[b] + (s[h] - s[b]) * (p - b)

def rangs(v):
    o = sorted(range(len(v)), key=lambda i: v[i]); r = [0.0] * len(v); i = 0
    while i < len(o):
        j = i
        while j + 1 < len(o) and v[o[j + 1]] == v[o[i]]:
            j += 1
        for k in range(i, j + 1):
            r[o[k]] = (i + j) / 2.0 + 1.0
        i = j + 1
    return r

def pearson(x, y):
    n = len(x); mx, my = sum(x) / n, sum(y) / n
    sxy = sum((a - mx) * (b - my) for a, b in zip(x, y))
    sxx = sum((a - mx) ** 2 for a in x); syy = sum((b - my) ** 2 for b in y)
    return sxy / math.sqrt(sxx * syy) if sxx and syy else 0.0

def spearman(x, y):
    return pearson(rangs(x), rangs(y))

def kappa(a, b):
    n11 = sum(1 for p, q in zip(a, b) if p and q); n10 = sum(1 for p, q in zip(a, b) if p and not q)
    n01 = sum(1 for p, q in zip(a, b) if not p and q); n = len(a); n00 = n - n11 - n10 - n01
    po = (n11 + n00) / n; pe = ((n11 + n10) * (n11 + n01) + (n01 + n00) * (n10 + n00)) / float(n * n)
    return (po - pe) / (1 - pe) if pe < 1 else 0.0, (n11, n10, n01, n00)

def profil(v):
    s = sorted(v); tot = float(sum(s)) or 1.0; top = s[-max(1, len(s) // 100):]
    return {"n": len(s), "p10": quantile(s, .1), "p50": quantile(s, .5), "p75": quantile(s, .75), "p90": quantile(s, .9),
            "p99": round(quantile(s, .99), 1), "max": s[-1], "part_top1pct": round(100 * sum(top) / tot, 2)}

def parts(d):
    t = float(sum(d.values())) or 1.0
    return {k: v / t for k, v in d.items()}

def tvd(p, q):
    return 0.5 * sum(abs(p.get(k, 0.0) - q.get(k, 0.0)) for k in set(p) | set(q))

def decennie(y):
    return "inconnue" if y is None or y <= 0 else "%d0s" % (int(y) // 10)

if __name__ == "__main__":
    from pyspark.sql import SparkSession, functions as F
    (aplati, cur, res, sortie, total_att, couv_att, communs_att, p75_att, kappa_att) = sys.argv[1:10]
    total_att, couv_att, communs_att, p75_att, kappa_att = int(total_att), int(couv_att), int(communs_att), float(p75_att), float(kappa_att)
    spark = SparkSession.builder.appName("3v_an2_an4").getOrCreate()
    spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
    col = F.col
    TRANCHES = [("2002-01", "2008-12")] + [("%d-01" % a, "%d-12" % a) for a in range(2009, 2017)]
    VIDE = F.lit("~")

    def cle_ascii(t):
        return F.when(t.rlike("^[\\x00-\\x7F]*$"), F.nullif(F.regexp_replace(F.lower(t), "[^a-z0-9_]", ""), F.lit("")))

    def sp_extrait(x):
        return (F.when(x.rlike("^[0-9A-Za-z]{22}$"), x)
                 .when(x.rlike("track[/:][0-9A-Za-z]{22}"), F.regexp_extract(x, "track[/:]([0-9A-Za-z]{22})", 1)))

    # 1. Correspondance d'AN1 (meme regle : cle, spotify, msid -> titre Kaggle)
    corr = (spark.read.parquet(res).where(col("track_id").isNotNull())
            .select("recording_msid", F.coalesce("cle", VIDE).alias("k_cle"), F.coalesce("sp", VIDE).alias("k_sp"), "track_id")
            .distinct())
    ambig = corr.groupBy("recording_msid", "k_cle", "k_sp").agg(F.countDistinct("track_id").alias("n")).where(col("n") > 1).count()
    print("CORRESPONDANCE cles_ambigues", ambig, flush=True)

    # 2. Ecoutes par tranche : (titre, jeton, annee) et totaux par jeton
    t0 = time.time()
    src = spark.read.parquet(aplati)
    tmp_t, tmp_u = sortie + "/_tmp_an2_titre_jeton", sortie + "/_tmp_an2_jeton"
    for i, (a, b) in enumerate(TRANCHES):
        e = (src.where(col("mois").between(a, b))
             .select("user_id", F.substring("mois", 1, 4).cast("int").alias("annee"), "recording_msid",
                     F.coalesce(cle_ascii(F.concat_ws("", col("artist_name"), col("track_name"))), VIDE).alias("k_cle"),
                     F.coalesce(sp_extrait(col("spotify_id")), sp_extrait(col("spotify_track_uri")), VIDE).alias("k_sp")))
        mode = "overwrite" if i == 0 else "append"
        e.groupBy("user_id").agg(F.count(F.lit(1)).alias("n")).write.mode(mode).parquet(tmp_u)
        (e.join(corr, ["recording_msid", "k_cle", "k_sp"])
          .groupBy("track_id", "user_id", "annee").agg(F.count(F.lit(1)).alias("n"))
          .write.mode(mode).parquet(tmp_t))
    tu = spark.read.parquet(tmp_u).groupBy("user_id").agg(F.sum("n").alias("n"))
    tt = spark.read.parquet(tmp_t)
    total = tu.agg(F.sum("n")).first()[0]; couv = tt.agg(F.sum("n")).first()[0]
    print("ENTREES ecoutes", total, "ATTENDU", total_att, "couvertes", couv, "ATTENDU", couv_att,
          "duree_s", round(time.time() - t0, 1), flush=True)

    # 3. AN2 : concentration par auditeur
    ut = [r["n"] for r in tu.collect()]
    uc = [r["n"] for r in tt.groupBy("user_id").agg(F.sum("n").alias("n")).collect()]
    print("AUDITEURS toutes_ecoutes", profil(ut), flush=True)
    print("AUDITEURS ecoutes_couvertes", profil(uc), flush=True)
    for x in tt.groupBy("annee").agg(F.sum("n").alias("e"), F.countDistinct("user_id").alias("a")).orderBy("annee").collect():
        print("ANNEE_ECOUTE", x["annee"], "ecoutes", x["e"], "auditeurs", x["a"], flush=True)

    # 4. AN2 : par titre, ecoutes et auditeurs distincts
    pt = (tt.groupBy("track_id", "user_id").agg(F.sum("n").alias("n"))
            .groupBy("track_id").agg(F.sum("n").alias("lb_ecoutes"), F.count(F.lit(1)).alias("lb_auditeurs")))
    lab = (spark.read.parquet(cur + "/songs_features_labeled")
           .select("track_id", "artist", "year", "genre", "total_plays", "unique_listeners", "is_hit").dropDuplicates(["track_id"]))
    L = lab.join(pt, "track_id", "full").collect()
    A = [x for x in L if x["is_hit"] is not None and x["lb_ecoutes"] is not None]
    B = [x for x in L if x["lb_ecoutes"] is not None]
    print("POPULATIONS communs", len(A), "ATTENDU", communs_att, "trouves", len(B), flush=True)
    print("PROFIL_AUDITEURS_communs", profil([x["lb_auditeurs"] for x in A]), flush=True)
    print("PROFIL_ECOUTES_PAR_AUDITEUR_communs", profil([x["lb_ecoutes"] / x["lb_auditeurs"] for x in A]), flush=True)

    kk = [x["is_hit"] == 1 for x in A]
    p75v = quantile([x["lb_ecoutes"] for x in A], .75)
    kv, mv = kappa(kk, [x["lb_ecoutes"] >= p75v for x in A])
    print("CIBLE_VOLUME seuil", p75v, "ATTENDU", p75_att, "kappa", round(kv, 4), "ATTENDU", kappa_att, flush=True)
    p75a = quantile([x["lb_auditeurs"] for x in A], .75)
    la = [x["lb_auditeurs"] >= p75a for x in A]
    ka, ma = kappa(kk, la)
    print("CIBLE_AUDITEURS seuil", p75a, "positifs", sum(la), "part_%", round(100.0 * sum(la) / len(A), 2),
          "matrice", ma, "kappa", round(ka, 4), flush=True)
    s = lambda f, g: round(spearman([f(x) for x in A], [g(x) for x in A]), 4)
    print("SPEARMAN auditeurs_lb_vs_unique_listeners_kaggle", s(lambda x: x["lb_auditeurs"], lambda x: x["unique_listeners"]),
          "auditeurs_lb_vs_total_plays_kaggle", s(lambda x: x["lb_auditeurs"], lambda x: x["total_plays"]),
          "ecoutes_lb_vs_unique_listeners_kaggle", s(lambda x: x["lb_ecoutes"], lambda x: x["unique_listeners"]),
          "auditeurs_lb_vs_ecoutes_lb", s(lambda x: x["lb_auditeurs"], lambda x: x["lb_ecoutes"]), flush=True)

    # 5. AN4 : representativite sur les titres communs (parts d'ecoutes)
    def compare(nom, cle, min_titres=1, top=0):
        dk, dl, nt = {}, {}, {}
        for x in A:
            c = cle(x)
            dk[c] = dk.get(c, 0) + x["total_plays"]; dl[c] = dl.get(c, 0) + x["lb_ecoutes"]; nt[c] = nt.get(c, 0) + 1
        pk, pl = parts(dk), parts(dl)
        print("AN4", nom, "modalites", len(pk), "TVD", round(tvd(pk, pl), 4), flush=True)
        if top == 0:
            for c in sorted(pk, key=lambda c: -pk[c]):
                print("AN4_PART", nom, c, "titres", nt[c], "kaggle_%", round(100 * pk[c], 2), "lb_%", round(100 * pl[c], 2),
                      "rapport", round(pl[c] / pk[c], 3) if pk[c] else None, flush=True)
        else:
            el = [c for c in pk if nt[c] >= min_titres and pk[c] > 0 and pl[c] > 0]
            print("AN4", nom, "spearman_parts", round(spearman([pk[c] for c in el], [pl[c] for c in el]), 4),
                  "retenus", len(el), "min_titres", min_titres, flush=True)
            r = sorted(el, key=lambda c: pl[c] / pk[c])
            for sens, lst in (("SUR", r[::-1][:top]), ("SOUS", r[:top])):
                for c in lst:
                    print("AN4_ARTISTE", sens, c, "titres", nt[c], "kaggle_%", round(100 * pk[c], 3), "lb_%", round(100 * pl[c], 3),
                          "rapport", round(pl[c] / pk[c], 2), flush=True)
    compare("decennie_sortie", lambda x: decennie(x["year"]))
    compare("genre", lambda x: x["genre"] if x["genre"] and str(x["genre"]).strip().lower() not in ("nan", "none", "null") else "(absent)")
    compare("artiste", lambda x: x["artist"], min_titres=5, top=10)

    # 6. Ecriture : comptages par titre et par annee, sans jeton
    out = spark.createDataFrame([(x["track_id"], int(x["lb_ecoutes"]), int(x["lb_auditeurs"])) for x in B],
                                "track_id string, lb_ecoutes long, lb_auditeurs long")
    out.coalesce(1).write.mode("overwrite").parquet(sortie + "/an2_auditeurs_par_titre")
    relu = spark.read.parquet(sortie + "/an2_auditeurs_par_titre")
    sans_jeton = "user_id" not in relu.columns
    n_relu = relu.count()
    print("ECRIT an2_auditeurs_par_titre", n_relu, "sans_jeton", sans_jeton, flush=True)

    ok = (total == total_att and couv == couv_att and len(A) == communs_att and ambig == 0 and sans_jeton
          and n_relu == len(B) and p75v == p75_att and round(kv, 4) == kappa_att)
    print("AN24_OK" if ok else "AN24_ECHEC", "duree_s", round(time.time() - t_app, 1), flush=True)
    if not ok:
        sys.exit(1)
