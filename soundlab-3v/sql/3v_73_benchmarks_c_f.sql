-- =============================================================================
--  SoundLab Analytics — chantier trois V, tâche 4.1e
--  Scénarios C et F rejoués contre le témoin mélangé, protocole de 3v_71 :
--  cache coupé, requête jetable, chauffe, trois mesures en ordre alterné.
--    ../soundlab-analytics/infra/rs.sh sql/3v_73_benchmarks_c_f.sql
-- =============================================================================

-- @ Desactivation du cache de resultats
SET enable_result_cache_for_session TO off;

-- @ Requete jetable (demarrage a froid, non mesuree)
SELECT COUNT(*) FROM soundlab_lb.dim_date;

-- =============================================================================
--  SCÉNARIO C' — Filtre d'un mois sur la cle de tri (juin 2016)
-- =============================================================================

-- @ C0 · chauffe · temoin melange
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour_naif_melange WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- @ C0 · chauffe · optimise
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- @ C1T · MESURE · temoin (melange, aucun tri)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour_naif_melange WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- @ C1O · MESURE · optimise (SORTKEY date_jour)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- @ C2T · MESURE · temoin (melange, aucun tri)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour_naif_melange WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- @ C2O · MESURE · optimise (SORTKEY date_jour)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- @ C3T · MESURE · temoin (melange, aucun tri)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour_naif_melange WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- @ C3O · MESURE · optimise (SORTKEY date_jour)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- =============================================================================
--  SCÉNARIO F' — Fenetre recente : titres les plus ecoutes depuis le 2026-08-28
-- =============================================================================

-- @ F0 · chauffe · temoin melange
SELECT recording_msid, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour_naif_melange WHERE date_jour >= '2026-08-28'
GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ F0 · chauffe · optimise
SELECT recording_msid, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2026-08-28'
GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ F1T · MESURE · temoin (melange, EVEN)
SELECT recording_msid, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour_naif_melange WHERE date_jour >= '2026-08-28'
GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ F1O · MESURE · optimise (DISTKEY + SORTKEY)
SELECT recording_msid, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2026-08-28'
GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ F2T · MESURE · temoin (melange, EVEN)
SELECT recording_msid, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour_naif_melange WHERE date_jour >= '2026-08-28'
GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ F2O · MESURE · optimise (DISTKEY + SORTKEY)
SELECT recording_msid, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2026-08-28'
GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ F3T · MESURE · temoin (melange, EVEN)
SELECT recording_msid, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour_naif_melange WHERE date_jour >= '2026-08-28'
GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ F3O · MESURE · optimise (DISTKEY + SORTKEY)
SELECT recording_msid, SUM(ecoutes) AS ecoutes
FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2026-08-28'
GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;
