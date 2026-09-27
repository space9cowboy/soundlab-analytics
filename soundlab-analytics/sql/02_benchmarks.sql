-- =============================================================================
--  SoundLab Analytics — Bloc 6 Big Data
--  Benchmarks Redshift : mesure de l'effet des choix de modélisation
--
--  Chaque scénario est exécuté sur deux tables portant EXACTEMENT les mêmes
--  données :
--    · les tables témoins  (_naif)  — DISTSTYLE EVEN, aucun tri, aucun encodage
--    · les tables retenues          — DISTKEY / DISTSTYLE ALL, tri, encodages
--
--  Trois précautions rendent les mesures exploitables :
--
--  1. CACHE DE RÉSULTATS DÉSACTIVÉ. Redshift renvoie en quelques
--     millisecondes le résultat d'une requête déjà vue. Sans cette
--     désactivation, on mesurerait le cache et non la modélisation.
--     Le réglage n'est valable que dans la session — d'où la session
--     persistante maintenue par infra/rs.sh.
--
--  2. UNE PASSE DE CHAUFFE AVANT CHAQUE MESURE. Redshift compile les segments
--     de requête à la première exécution et met le code compilé en cache. La
--     première passe mesure donc la compilation autant que l'exécution. Seule
--     la seconde passe est retenue.
--
--  3. ORDRE ALTERNÉ. Le témoin passe avant la version optimisée dans chaque
--     scénario, pour qu'un éventuel réchauffement du cache disque profite au
--     témoin plutôt qu'à la version qu'on cherche à valoriser.
--
--  Réserve honnête : Redshift Serverless ajuste sa capacité en cours
--  d'exécution. Les écarts inférieurs à 20 % ne sont pas significatifs ;
--  ceux d'un facteur 2 ou plus le sont.
-- =============================================================================

-- @ Desactivation du cache de resultats
SET enable_result_cache_for_session TO off;

-- =============================================================================
--  SCÉNARIO A — Agrégation par piste
--
--  C'est LA requête métier du projet : elle produit total_plays et
--  unique_listeners, les deux variables dont dépend le modèle.
--
--  Attendu : la version optimisée, distribuée sur track_id, agrège localement
--  sur chaque tranche. Le témoin, distribué en tourniquet, doit redistribuer
--  les 9,7 M de lignes sur le réseau avant de pouvoir regrouper.
-- =============================================================================

-- @ A0 · chauffe · temoin
SELECT track_id, SUM(playcount) AS total_plays, COUNT(DISTINCT user_id_hash) AS auditeurs
FROM soundlab.fact_listening_naif GROUP BY track_id ORDER BY total_plays DESC LIMIT 10;

-- @ A0 · chauffe · optimise
SELECT track_id, SUM(playcount) AS total_plays, COUNT(DISTINCT user_id_hash) AS auditeurs
FROM soundlab.fact_listening GROUP BY track_id ORDER BY total_plays DESC LIMIT 10;

-- @ A1 · MESURE · temoin (DISTSTYLE EVEN)
SELECT track_id, SUM(playcount) AS total_plays, COUNT(DISTINCT user_id_hash) AS auditeurs
FROM soundlab.fact_listening_naif GROUP BY track_id ORDER BY total_plays DESC LIMIT 10;

-- @ A2 · MESURE · optimise (DISTKEY track_id)
SELECT track_id, SUM(playcount) AS total_plays, COUNT(DISTINCT user_id_hash) AS auditeurs
FROM soundlab.fact_listening GROUP BY track_id ORDER BY total_plays DESC LIMIT 10;

-- =============================================================================
--  SCÉNARIO B — Jointure faits × dimension
--
--  Attendu : dim_track étant répliquée par DISTSTYLE ALL, chaque tranche
--  dispose localement de la dimension entière et la jointure ne fait circuler
--  aucune donnée. Les deux tables témoins, distribuées en tourniquet, doivent
--  diffuser ou redistribuer.
-- =============================================================================

-- @ B0 · chauffe · temoin
SELECT d.genre, COUNT(*) AS lignes, SUM(f.playcount) AS ecoutes
FROM soundlab.fact_listening_naif f JOIN soundlab.dim_track_naif d ON d.track_id = f.track_id
WHERE d.genre IS NOT NULL GROUP BY d.genre ORDER BY ecoutes DESC LIMIT 10;

-- @ B0 · chauffe · optimise
SELECT d.genre, COUNT(*) AS lignes, SUM(f.playcount) AS ecoutes
FROM soundlab.fact_listening f JOIN soundlab.dim_track d ON d.track_id = f.track_id
WHERE d.genre IS NOT NULL GROUP BY d.genre ORDER BY ecoutes DESC LIMIT 10;

-- @ B1 · MESURE · temoin (EVEN x EVEN)
SELECT d.genre, COUNT(*) AS lignes, SUM(f.playcount) AS ecoutes
FROM soundlab.fact_listening_naif f JOIN soundlab.dim_track_naif d ON d.track_id = f.track_id
WHERE d.genre IS NOT NULL GROUP BY d.genre ORDER BY ecoutes DESC LIMIT 10;

-- @ B2 · MESURE · optimise (DISTKEY x DISTSTYLE ALL)
SELECT d.genre, COUNT(*) AS lignes, SUM(f.playcount) AS ecoutes
FROM soundlab.fact_listening f JOIN soundlab.dim_track d ON d.track_id = f.track_id
WHERE d.genre IS NOT NULL GROUP BY d.genre ORDER BY ecoutes DESC LIMIT 10;

-- =============================================================================
--  SCÉNARIO C — Filtre sélectif sur la clé de tri
--
--  Redshift mémorise, pour chaque bloc d'un mébioctet, la valeur minimale et
--  maximale de chaque colonne : ce sont les « zone maps ». Sur une table
--  triée, un filtre de plage permet d'éliminer la quasi-totalité des blocs
--  sans les lire. Sur une table non triée, les valeurs étant dispersées
--  partout, aucun bloc ne peut être écarté.
-- =============================================================================

-- @ C0 · chauffe · temoin
SELECT COUNT(*) AS lignes, SUM(playcount) AS ecoutes
FROM soundlab.fact_listening_naif WHERE track_id >= 'TRA' AND track_id < 'TRC';

-- @ C0 · chauffe · optimise
SELECT COUNT(*) AS lignes, SUM(playcount) AS ecoutes
FROM soundlab.fact_listening WHERE track_id >= 'TRA' AND track_id < 'TRC';

-- @ C1 · MESURE · temoin (aucun tri)
SELECT COUNT(*) AS lignes, SUM(playcount) AS ecoutes
FROM soundlab.fact_listening_naif WHERE track_id >= 'TRA' AND track_id < 'TRC';

-- @ C2 · MESURE · optimise (SORTKEY track_id)
SELECT COUNT(*) AS lignes, SUM(playcount) AS ecoutes
FROM soundlab.fact_listening WHERE track_id >= 'TRA' AND track_id < 'TRC';

-- =============================================================================
--  SCÉNARIO D — Empreinte disque
--
--  Mêmes lignes, mêmes valeurs. La seule différence est l'encodage :
--  ZSTD sur les chaînes, AZ64 sur les entiers et horodatages, contre RAW
--  partout sur les témoins.
-- =============================================================================

-- @ D · Empreinte disque et parametres physiques
SELECT
    "table"        AS table_name,
    diststyle      AS distribution,
    sortkey1       AS cle_de_tri,
    encoded        AS encodage,
    tbl_rows       AS lignes,
    size           AS blocs_1mo
FROM svv_table_info
WHERE schema = 'soundlab'
ORDER BY "table";

-- =============================================================================
--  SCÉNARIO E — Plans d'exécution
--
--  C'est la preuve la plus directe, parce que le planificateur nomme lui-même
--  l'opération réseau :
--    DS_DIST_NONE   aucune redistribution — les données sont déjà au bon endroit
--    DS_DIST_ALL_NONE  la dimension est déjà répliquée partout
--    DS_BCAST_INNER diffusion de la table interne vers toutes les tranches
--    DS_DIST_BOTH   redistribution des deux côtés — le cas le plus coûteux
-- =============================================================================

-- @ E1 · Plan · jointure temoin
EXPLAIN
SELECT d.genre, SUM(f.playcount)
FROM soundlab.fact_listening_naif f JOIN soundlab.dim_track_naif d ON d.track_id = f.track_id
GROUP BY d.genre;

-- @ E2 · Plan · jointure optimisee
EXPLAIN
SELECT d.genre, SUM(f.playcount)
FROM soundlab.fact_listening f JOIN soundlab.dim_track d ON d.track_id = f.track_id
GROUP BY d.genre;
