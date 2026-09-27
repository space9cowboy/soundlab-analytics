import sys, time, math
t_app = time.time()

# AN3 : cible construite depuis ListenBrainz et concordance avec la cible Kaggle (+ profil des ecoutes par titre, AN2).
# Lecture : table an1_combos_resolus (AN1) et songs_features_labeled. Ecriture : une table par titre sous le prefixe
# d'analyse (identifiants de titres et comptages, aucune donnee d'auditeur).

def quantile(v, q):
    s = sorted(v)
    if not s:
        return None
    p = (len(s) - 1) * q
    b = int(math.floor(p))
    h = min(b + 1, len(s) - 1)
    return s[b] + (s[h] - s[b]) * (p - b)

def rangs(v):
    ordre = sorted(range(len(v)), key=lambda i: v[i])
    r = [0.0] * len(v)
    i = 0
    while i < len(ordre):
        j = i
        while j + 1 < len(ordre) and v[ordre[j + 1]] == v[ordre[i]]:
            j += 1
        moy = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            r[ordre[k]] = moy
        i = j + 1
    return r

def pearson(x, y):
    n = len(x)
    mx, my = sum(x) / n, sum(y) / n
    sxy = sum((a - mx) * (b - my) for a, b in zip(x, y))
    sxx = sum((a - mx) ** 2 for a in x)
    syy = sum((b - my) ** 2 for b in y)
    return sxy / math.sqrt(sxx * syy) if sxx and syy else 0.0

def spearman(x, y):
    return pearson(rangs(x), rangs(y))

def concordance(a, b):
    n11 = sum(1 for p, q in zip(a, b) if p and q)
    n10 = sum(1 for p, q in zip(a, b) if p and not q)
    n01 = sum(1 for p, q in zip(a, b) if not p and q)
    n00 = len(a) - n11 - n10 - n01
    n = len(a)
    po = (n11 + n00) / n
    pe = ((n11 + n10) * (n11 + n01) + (n01 + n00) * (n10 + n00)) / float(n * n)
    kappa = (po - pe) / (1 - pe) if pe < 1 else 0.0
    return {"n11": n11, "n10": n10, "n01": n01, "n00": n00, "accord": po, "kappa": kappa}

def profil(v):
    s = sorted(v)
    tot = float(sum(s)) or 1.0
    top = s[-max(1, len(s) // 100):]
    return {"n": len(s), "p10": quantile(s, .1), "p25": quantile(s, .25), "p50": quantile(s, .5), "p75": quantile(s, .75),
            "p90": quantile(s, .9), "p99": quantile(s, .99), "max": s[-1], "part_top1pct": round(100 * sum(top) / tot, 2)}

if __name__ == "__main__":
    from pyspark.sql import SparkSession, functions as F
    res, cur, sortie, total_att, couv_att, commun_att = sys.argv[1:7]
    total_att, couv_att, commun_att = int(total_att), int(couv_att), int(commun_att)
    spark = SparkSession.builder.appName("3v_an3_cible_listenbrainz").getOrCreate()
    spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
    col = F.col
    r = spark.read.parquet(res).select("track_id", "ecoutes")
    t = r.agg(F.sum("ecoutes").alias("tot"), F.sum(F.when(col("track_id").isNotNull(), col("ecoutes")).otherwise(0)).alias("couv")).first()
    lb = r.where(col("track_id").isNotNull()).groupBy("track_id").agg(F.sum("ecoutes").alias("lb_plays"))
    lab = spark.read.parquet(cur + "/songs_features_labeled").select("track_id", "total_plays", "is_hit").dropDuplicates(["track_id"])
    lignes = lab.join(lb, "track_id", "full").collect()
    print("ENTREES ecoutes", t["tot"], "ATTENDU", total_att, "couvertes", t["couv"], "ATTENDU", couv_att, "titres", len(lignes), flush=True)

    A = [x for x in lignes if x["is_hit"] is not None and x["lb_plays"] is not None]
    B = [x for x in lignes if x["lb_plays"] is not None]
    E = [x for x in lignes if x["is_hit"] is not None]
    print("POPULATIONS etiquetes_kaggle", len(E), "communs", len(A), "ATTENDU", commun_att, "trouves_lb", len(B),
          "nouveaux", len(B) - len(A), flush=True)

    # Regle Kaggle reproduite par la methode appliquee a ListenBrainz (quantile lineaire, >=)
    tk = quantile([x["total_plays"] for x in E], .75)
    hits_k = sum(1 for x in E if x["is_hit"] == 1)
    rep = sum(1 for x in E if x["total_plays"] >= tk)
    min_hit = min(x["total_plays"] for x in E if x["is_hit"] == 1)
    max_non = max(x["total_plays"] for x in E if x["is_hit"] == 0)
    methode_ok = rep == hits_k
    print("REGLE_KAGGLE seuil_p75", tk, "positifs_reproduits", rep, "positifs_kaggle", hits_k,
          "min_succes", min_hit, "max_non_succes", max_non, "REPRODUITE" if methode_ok else "ECART", flush=True)

    print("PROFIL_KAGGLE_communs", profil([x["total_plays"] for x in A]), flush=True)
    print("PROFIL_LB_communs", profil([x["lb_plays"] for x in A]), flush=True)
    print("PROFIL_LB_trouves", profil([x["lb_plays"] for x in B]), flush=True)

    tA = quantile([x["lb_plays"] for x in A], .75)
    tB = quantile([x["lb_plays"] for x in B], .75)
    kA = [x["is_hit"] == 1 for x in A]
    lA = [x["lb_plays"] >= tA for x in A]
    lBA = [x["lb_plays"] >= tB for x in A]
    cA = concordance(kA, lA)
    cB = concordance(kA, lBA)
    rho = spearman([x["total_plays"] for x in A], [x["lb_plays"] for x in A])
    print("CIBLE_LB_A seuil_p75", tA, "positifs", sum(lA), "part_%", round(100.0 * sum(lA) / len(A), 2), flush=True)
    print("CONCORDANCE_A", {k: (round(v, 4) if isinstance(v, float) else v) for k, v in cA.items()}, flush=True)
    print("SPEARMAN_A total_plays_vs_lb_plays", round(rho, 4), flush=True)
    nouv = [x for x in B if x["is_hit"] is None]
    print("CIBLE_LB_B seuil_p75", tB, "positifs", sum(1 for x in B if x["lb_plays"] >= tB),
          "part_%", round(100.0 * sum(1 for x in B if x["lb_plays"] >= tB) / len(B), 2),
          "nouveaux_positifs", sum(1 for x in nouv if x["lb_plays"] >= tB), "sur", len(nouv), flush=True)
    print("CONCORDANCE_B_sur_communs", {k: (round(v, 4) if isinstance(v, float) else v) for k, v in cB.items()}, flush=True)

    out = spark.createDataFrame(
        [(x["track_id"], x["total_plays"], x["is_hit"], int(x["lb_plays"]),
          int(x["lb_plays"] >= tA) if x["is_hit"] is not None else None, int(x["lb_plays"] >= tB)) for x in B],
        "track_id string, total_plays long, is_hit int, lb_plays long, is_hit_lb_communs int, is_hit_lb_trouves int")
    out.coalesce(1).write.mode("overwrite").parquet(sortie + "/an3_cible_par_titre")
    relu = spark.read.parquet(sortie + "/an3_cible_par_titre").count()
    print("ECRIT an3_cible_par_titre", relu, flush=True)

    ok = (t["tot"] == total_att and t["couv"] == couv_att and len(A) == commun_att and methode_ok and relu == len(B)
          and cA["n11"] + cA["n10"] + cA["n01"] + cA["n00"] == len(A))
    print("AN3_OK" if ok else "AN3_ECHEC", "duree_s", round(time.time() - t_app, 1), flush=True)
    if not ok:
        sys.exit(1)
