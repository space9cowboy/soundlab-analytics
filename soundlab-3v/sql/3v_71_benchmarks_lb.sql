-- =============================================================================
--  SoundLab Analytics — chantier trois V, tâche 4.1
--  Banc d'essai de l'entrepôt ListenBrainz : même protocole que
--  sql/02_benchmarks.sql du Bloc 6, avec deux corrections issues de ses limites.
--
--    ../soundlab-analytics/infra/rs.sh sql/3v_71_benchmarks_lb.sql
--
--  Protocole repris du Bloc 6 :
--    1. cache de résultats désactivé dans la session persistante de rs.sh ;
--    2. une passe de chauffe par table avant les mesures ;
--    3. le témoin passe avant l'optimisé ;
--    4. seuil de significativité : 20 %.
--  Corrections (benchmarks Bloc 6, § 3.1 et § 7) :
--    · requête jetable initiale pour absorber le démarrage à froid (R5) ;
--    · trois mesures par table en ordre alterné T, O, T, O, T, O ; on retient la
--      médiane, au lieu d'une exécution unique.
--  Témoin : DISTSTYLE EVEN, aucun tri, ENCODE RAW, mêmes 520 186 856 lignes.
-- =============================================================================

-- @ Desactivation du cache de resultats
SET enable_result_cache_for_session TO off;

-- @ Requete jetable (demarrage a froid, non mesuree)
SELECT COUNT(*) FROM soundlab_lb.dim_date;

-- =============================================================================
--  SCÉNARIO A — Agregation par titre sur toute la periode
--  Attendu : l'optimise, distribue sur recording_msid, agrege localement ; le temoin redistribue 520 M de lignes.
-- =============================================================================

-- @ A0 · chauffe · temoin
SELECT recording_msid, SUM(ecoutes) AS ecoutes, SUM(auditeurs) AS auditeurs_jour
FROM soundlab_lb.fait_ecoutes_jour_naif GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ A0 · chauffe · optimise
SELECT recording_msid, SUM(ecoutes) AS ecoutes, SUM(auditeurs) AS auditeurs_jour
FROM soundlab_lb.fait_ecoutes_jour GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ A1T · MESURE · temoin (DISTSTYLE EVEN)
SELECT recording_msid, SUM(ecoutes) AS ecoutes, SUM(auditeurs) AS auditeurs_jour
FROM soundlab_lb.fait_ecoutes_jour_naif GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ A1O · MESURE · optimise (DISTKEY recording_msid)
SELECT recording_msid, SUM(ecoutes) AS ecoutes, SUM(auditeurs) AS auditeurs_jour
FROM soundlab_lb.fait_ecoutes_jour GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ A2T · MESURE · temoin (DISTSTYLE EVEN)
SELECT recording_msid, SUM(ecoutes) AS ecoutes, SUM(auditeurs) AS auditeurs_jour
FROM soundlab_lb.fait_ecoutes_jour_naif GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ A2O · MESURE · optimise (DISTKEY recording_msid)
SELECT recording_msid, SUM(ecoutes) AS ecoutes, SUM(auditeurs) AS auditeurs_jour
FROM soundlab_lb.fait_ecoutes_jour GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ A3T · MESURE · temoin (DISTSTYLE EVEN)
SELECT recording_msid, SUM(ecoutes) AS ecoutes, SUM(auditeurs) AS auditeurs_jour
FROM soundlab_lb.fait_ecoutes_jour_naif GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ A3O · MESURE · optimise (DISTKEY recording_msid)
SELECT recording_msid, SUM(ecoutes) AS ecoutes, SUM(auditeurs) AS auditeurs_jour
FROM soundlab_lb.fait_ecoutes_jour GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- =============================================================================
--  SCÉNARIO B — Jointure faits x dim_date (ecoutes par annee et jour de semaine)
--  Attendu : dim_date repliquee (ALL), aucune circulation de donnees ; le temoin diffuse ou redistribue.
-- =============================================================================

-- @ B0 · chauffe · temoin
SELECT d.annee, d.jour_semaine, SUM(f.ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour_naif f JOIN soundlab_lb.dim_date_naif d ON d.date_jour = f.date_jour
GROUP BY d.annee, d.jour_semaine ORDER BY d.annee, d.jour_semaine LIMIT 10;

-- @ B0 · chauffe · optimise
SELECT d.annee, d.jour_semaine, SUM(f.ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour f JOIN soundlab_lb.dim_date d ON d.date_jour = f.date_jour
GROUP BY d.annee, d.jour_semaine ORDER BY d.annee, d.jour_semaine LIMIT 10;

-- @ B1T · MESURE · temoin (EVEN x EVEN)
SELECT d.annee, d.jour_semaine, SUM(f.ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour_naif f JOIN soundlab_lb.dim_date_naif d ON d.date_jour = f.date_jour
GROUP BY d.annee, d.jour_semaine ORDER BY d.annee, d.jour_semaine LIMIT 10;

-- @ B1O · MESURE · optimise (DISTKEY x ALL)
SELECT d.annee, d.jour_semaine, SUM(f.ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour f JOIN soundlab_lb.dim_date d ON d.date_jour = f.date_jour
GROUP BY d.annee, d.jour_semaine ORDER BY d.annee, d.jour_semaine LIMIT 10;

-- @ B2T · MESURE · temoin (EVEN x EVEN)
SELECT d.annee, d.jour_semaine, SUM(f.ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour_naif f JOIN soundlab_lb.dim_date_naif d ON d.date_jour = f.date_jour
GROUP BY d.annee, d.jour_semaine ORDER BY d.annee, d.jour_semaine LIMIT 10;

-- @ B2O · MESURE · optimise (DISTKEY x ALL)
SELECT d.annee, d.jour_semaine, SUM(f.ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour f JOIN soundlab_lb.dim_date d ON d.date_jour = f.date_jour
GROUP BY d.annee, d.jour_semaine ORDER BY d.annee, d.jour_semaine LIMIT 10;

-- @ B3T · MESURE · temoin (EVEN x EVEN)
SELECT d.annee, d.jour_semaine, SUM(f.ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour_naif f JOIN soundlab_lb.dim_date_naif d ON d.date_jour = f.date_jour
GROUP BY d.annee, d.jour_semaine ORDER BY d.annee, d.jour_semaine LIMIT 10;

-- @ B3O · MESURE · optimise (DISTKEY x ALL)
SELECT d.annee, d.jour_semaine, SUM(f.ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour f JOIN soundlab_lb.dim_date d ON d.date_jour = f.date_jour
GROUP BY d.annee, d.jour_semaine ORDER BY d.annee, d.jour_semaine LIMIT 10;

-- =============================================================================
--  SCÉNARIO C — Filtre d'un mois sur la cle de tri (juin 2016)
--  Attendu : les zone maps ecartent les blocs hors du mois sur la table triee ; le temoin lit tout.
-- =============================================================================

-- @ C0 · chauffe · temoin
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour_naif WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- @ C0 · chauffe · optimise
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- @ C1T · MESURE · temoin (aucun tri)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour_naif WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- @ C1O · MESURE · optimise (SORTKEY date_jour)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- @ C2T · MESURE · temoin (aucun tri)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour_naif WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- @ C2O · MESURE · optimise (SORTKEY date_jour)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- @ C3T · MESURE · temoin (aucun tri)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour_naif WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- @ C3O · MESURE · optimise (SORTKEY date_jour)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- =============================================================================
--  SCÉNARIO F — Fenetre recente : titres les plus ecoutes depuis le 2026-08-28
--  Cas d'usage de la velocite : tri (fenetre) et distribution (regroupement) jouent ensemble.
-- =============================================================================

-- @ F0 · chauffe · temoin
SELECT recording_msid, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour_naif WHERE date_jour >= '2026-08-28'
GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ F0 · chauffe · optimise
SELECT recording_msid, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2026-08-28'
GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ F1T · MESURE · temoin (EVEN, aucun tri)
SELECT recording_msid, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour_naif WHERE date_jour >= '2026-08-28'
GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ F1O · MESURE · optimise (DISTKEY + SORTKEY)
SELECT recording_msid, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2026-08-28'
GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ F2T · MESURE · temoin (EVEN, aucun tri)
SELECT recording_msid, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour_naif WHERE date_jour >= '2026-08-28'
GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ F2O · MESURE · optimise (DISTKEY + SORTKEY)
SELECT recording_msid, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2026-08-28'
GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ F3T · MESURE · temoin (EVEN, aucun tri)
SELECT recording_msid, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour_naif WHERE date_jour >= '2026-08-28'
GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ F3O · MESURE · optimise (DISTKEY + SORTKEY)
SELECT recording_msid, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2026-08-28'
GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- =============================================================================
--  SCÉNARIO D — Empreinte disque, asymétrie, part non triée
-- =============================================================================

-- @ D · Parametres physiques
SELECT "table" AS table_name, diststyle AS distribution, sortkey1 AS cle_de_tri, encoded AS encodage,
       tbl_rows AS lignes, size AS blocs_1mo, skew_rows AS asymetrie, unsorted AS pct_non_trie
FROM svv_table_info
WHERE schema = 'soundlab_lb'
ORDER BY "table";

-- =============================================================================
--  SCÉNARIO E — Plans d'exécution : le planificateur nomme l'opération réseau
--    DS_DIST_NONE      aucune redistribution
--    DS_DIST_ALL_NONE  la dimension est déjà répliquée partout
--    DS_BCAST_INNER    diffusion de la table interne vers toutes les tranches
--    DS_DIST_BOTH      redistribution des deux côtés
-- =============================================================================

-- @ E1 · Plan · jointure temoin
EXPLAIN
SELECT d.annee, SUM(f.ecoutes) FROM soundlab_lb.fait_ecoutes_jour_naif f
JOIN soundlab_lb.dim_date_naif d ON d.date_jour = f.date_jour GROUP BY d.annee;

-- @ E2 · Plan · jointure optimisee
EXPLAIN
SELECT d.annee, SUM(f.ecoutes) FROM soundlab_lb.fait_ecoutes_jour f
JOIN soundlab_lb.dim_date d ON d.date_jour = f.date_jour GROUP BY d.annee;

-- @ E3 · Plan · agregation par titre temoin
EXPLAIN
SELECT recording_msid, SUM(ecoutes) FROM soundlab_lb.fait_ecoutes_jour_naif GROUP BY recording_msid;

-- @ E4 · Plan · agregation par titre optimisee
EXPLAIN
SELECT recording_msid, SUM(ecoutes) FROM soundlab_lb.fait_ecoutes_jour GROUP BY recording_msid;
