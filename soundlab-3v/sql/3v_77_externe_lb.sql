-- =============================================================================
--  SoundLab Analytics — chantier trois V, tâche 4.3
--  Tables externes sur le Parquet partitionné, comparées au chargement interne
--
--    ../soundlab-analytics/infra/rs.sh sql/3v_77_externe_lb.sql
--
--  Prérequis : infra/3v_76_catalogue_lb.sh (table soundlab_curated.lb_faits_jour,
--  431 partitions origine=/mois=). Le schéma externe soundlab_lb_ext pointe sur la
--  base Glue soundlab_curated ; le rôle SoundLabRedshiftS3Role n'y a que des droits
--  de lecture, inchangés.
--
--  Même protocole que 3v_71 : cache coupé, requête jetable, chauffe, trois mesures
--  en ordre alterné (externe puis interne). Scénario G : même requête que C sans
--  le prédicat sur la partition mois, pour mesurer ce que coûte un élagage oublié.
--  La colonne date du Parquet s'appelle "date" : mot réservé, toujours entre
--  guillemets côté externe.
-- =============================================================================

-- @ Creation du schema externe soundlab_lb_ext
CREATE EXTERNAL SCHEMA IF NOT EXISTS soundlab_lb_ext
FROM DATA CATALOG DATABASE 'soundlab_curated'
IAM_ROLE '{{ROLE_ARN}}';

-- @ Controle · memes donnees que l'interne (attendu : 502960355 / 695640731 et 17226501 / 24184191)
SELECT origine, COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes, MIN("date") AS dmin, MAX("date") AS dmax
FROM soundlab_lb_ext.lb_faits_jour GROUP BY origine ORDER BY 1;

-- @ Desactivation du cache de resultats
SET enable_result_cache_for_session TO off;

-- @ Requete jetable (demarrage a froid, non mesuree)
SELECT COUNT(*) FROM soundlab_lb.dim_date;

-- =============================================================================
--  SCÉNARIO A — Agregation par titre sur toute la periode
-- =============================================================================

-- @ A0 · chauffe · externe
SELECT recording_msid, SUM(ecoutes) AS ecoutes FROM soundlab_lb_ext.lb_faits_jour GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ A0 · chauffe · interne
SELECT recording_msid, SUM(ecoutes) AS ecoutes FROM soundlab_lb.fait_ecoutes_jour GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ A1E · MESURE · externe (Spectrum)
SELECT recording_msid, SUM(ecoutes) AS ecoutes FROM soundlab_lb_ext.lb_faits_jour GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ A1I · MESURE · interne (DISTKEY + SORTKEY)
SELECT recording_msid, SUM(ecoutes) AS ecoutes FROM soundlab_lb.fait_ecoutes_jour GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ A2E · MESURE · externe (Spectrum)
SELECT recording_msid, SUM(ecoutes) AS ecoutes FROM soundlab_lb_ext.lb_faits_jour GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ A2I · MESURE · interne (DISTKEY + SORTKEY)
SELECT recording_msid, SUM(ecoutes) AS ecoutes FROM soundlab_lb.fait_ecoutes_jour GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ A3E · MESURE · externe (Spectrum)
SELECT recording_msid, SUM(ecoutes) AS ecoutes FROM soundlab_lb_ext.lb_faits_jour GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ A3I · MESURE · interne (DISTKEY + SORTKEY)
SELECT recording_msid, SUM(ecoutes) AS ecoutes FROM soundlab_lb.fait_ecoutes_jour GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- =============================================================================
--  SCÉNARIO C — Juin 2016, avec predicat de partition cote externe
-- =============================================================================

-- @ C0 · chauffe · externe
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes FROM soundlab_lb_ext.lb_faits_jour WHERE mois = '2016-06' AND "date" >= '2016-06-01' AND "date" < '2016-07-01';

-- @ C0 · chauffe · interne
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- @ C1E · MESURE · externe (Spectrum)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes FROM soundlab_lb_ext.lb_faits_jour WHERE mois = '2016-06' AND "date" >= '2016-06-01' AND "date" < '2016-07-01';

-- @ C1I · MESURE · interne (DISTKEY + SORTKEY)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- @ C2E · MESURE · externe (Spectrum)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes FROM soundlab_lb_ext.lb_faits_jour WHERE mois = '2016-06' AND "date" >= '2016-06-01' AND "date" < '2016-07-01';

-- @ C2I · MESURE · interne (DISTKEY + SORTKEY)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- @ C3E · MESURE · externe (Spectrum)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes FROM soundlab_lb_ext.lb_faits_jour WHERE mois = '2016-06' AND "date" >= '2016-06-01' AND "date" < '2016-07-01';

-- @ C3I · MESURE · interne (DISTKEY + SORTKEY)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- =============================================================================
--  SCÉNARIO G — Juin 2016, SANS predicat de partition cote externe (elagage impossible)
-- =============================================================================

-- @ G0 · chauffe · externe
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes FROM soundlab_lb_ext.lb_faits_jour WHERE "date" >= '2016-06-01' AND "date" < '2016-07-01';

-- @ G0 · chauffe · interne
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- @ G1E · MESURE · externe (Spectrum)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes FROM soundlab_lb_ext.lb_faits_jour WHERE "date" >= '2016-06-01' AND "date" < '2016-07-01';

-- @ G1I · MESURE · interne (DISTKEY + SORTKEY)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- @ G2E · MESURE · externe (Spectrum)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes FROM soundlab_lb_ext.lb_faits_jour WHERE "date" >= '2016-06-01' AND "date" < '2016-07-01';

-- @ G2I · MESURE · interne (DISTKEY + SORTKEY)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- @ G3E · MESURE · externe (Spectrum)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes FROM soundlab_lb_ext.lb_faits_jour WHERE "date" >= '2016-06-01' AND "date" < '2016-07-01';

-- @ G3I · MESURE · interne (DISTKEY + SORTKEY)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2016-06-01' AND date_jour < '2016-07-01';

-- =============================================================================
--  SCÉNARIO F — Fenetre recente depuis le 2026-08-28
-- =============================================================================

-- @ F0 · chauffe · externe
SELECT recording_msid, SUM(ecoutes) AS ecoutes FROM soundlab_lb_ext.lb_faits_jour WHERE mois >= '2026-08' AND "date" >= '2026-08-28' GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ F0 · chauffe · interne
SELECT recording_msid, SUM(ecoutes) AS ecoutes FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2026-08-28' GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ F1E · MESURE · externe (Spectrum)
SELECT recording_msid, SUM(ecoutes) AS ecoutes FROM soundlab_lb_ext.lb_faits_jour WHERE mois >= '2026-08' AND "date" >= '2026-08-28' GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ F1I · MESURE · interne (DISTKEY + SORTKEY)
SELECT recording_msid, SUM(ecoutes) AS ecoutes FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2026-08-28' GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ F2E · MESURE · externe (Spectrum)
SELECT recording_msid, SUM(ecoutes) AS ecoutes FROM soundlab_lb_ext.lb_faits_jour WHERE mois >= '2026-08' AND "date" >= '2026-08-28' GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ F2I · MESURE · interne (DISTKEY + SORTKEY)
SELECT recording_msid, SUM(ecoutes) AS ecoutes FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2026-08-28' GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ F3E · MESURE · externe (Spectrum)
SELECT recording_msid, SUM(ecoutes) AS ecoutes FROM soundlab_lb_ext.lb_faits_jour WHERE mois >= '2026-08' AND "date" >= '2026-08-28' GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- @ F3I · MESURE · interne (DISTKEY + SORTKEY)
SELECT recording_msid, SUM(ecoutes) AS ecoutes FROM soundlab_lb.fait_ecoutes_jour WHERE date_jour >= '2026-08-28' GROUP BY recording_msid ORDER BY ecoutes DESC LIMIT 10;

-- =============================================================================
--  Élagage et volume lus côté externe, par requête (30 dernières minutes)
-- =============================================================================

-- @ Detail des lectures externes
SELECT query_id, SUM(total_partitions) AS partitions_totales, SUM(qualified_partitions) AS partitions_lues,
       SUM(scanned_files) AS fichiers_lus, SUM(returned_rows) AS lignes, ROUND(SUM(returned_bytes) / 1048576.0, 1) AS mio
FROM sys_external_query_detail
WHERE start_time >= DATEADD(minute, -30, GETDATE())
GROUP BY query_id ORDER BY query_id;
