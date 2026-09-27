-- =============================================================================
--  SoundLab Analytics — chantier trois V, tâche 4.2
--  Test de la fusion incrémentale : critère « une fusion rejouée deux fois
--  laisse la table identique ».
--
--    ../soundlab-analytics/infra/rs.sh sql/3v_75_test_fusion.sql
--
--  Un test qui rejoue une fusion sur une table déjà à jour réussirait même avec
--  une fusion qui ne fait rien. On perturbe donc d'abord la partie incr de la
--  table (un jour supprimé, un compte faussé), puis :
--    1. la fusion doit la ramener exactement à son empreinte initiale ;
--    2. la fusion rejouée ne doit trouver aucun écart et laisser l'empreinte
--       inchangée.
--  Une fusion inerte échoue en 1 ; une insertion sans MERGE échoue en 2
--  (doublons) ; une mise à jour sans insertion échoue en 1 (jour non restauré).
--  L'empreinte : SUM(FNV_HASH(ligne)) sur la partie incr, avec lignes, écoutes
--  et auditeurs. La partie complet doit rester intacte tout du long.
--
--  stg_incr contient déjà la sortie courante de 3v_61 (chargée par 3v_70).
--  Première instruction : suppression du témoin mélangé (accord de Loïc, 27/09).
-- =============================================================================

-- @ Suppression du temoin melange (accord du 27/09)
DROP TABLE IF EXISTS soundlab_lb.fait_ecoutes_jour_naif_melange;

-- @ Empreinte 0 · etat initial (incr)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes, SUM(auditeurs) AS auditeurs,
       SUM(FNV_HASH(date_jour::VARCHAR || '|' || recording_msid || '|' || ecoutes::VARCHAR || '|' || auditeurs::VARCHAR)::DECIMAL(38,0)) AS empreinte
FROM soundlab_lb.fait_ecoutes_jour WHERE origine = 'incr';

-- @ Perturbation · groupes du 2026-09-26 a supprimer
SELECT COUNT(*) AS groupes_supprimes FROM soundlab_lb.fait_ecoutes_jour WHERE origine = 'incr' AND date_jour = '2026-09-26';

-- @ Perturbation 1 · suppression du 2026-09-26
DELETE FROM soundlab_lb.fait_ecoutes_jour WHERE origine = 'incr' AND date_jour = '2026-09-26';

-- @ Perturbation 2 · un compte fausse le 2026-09-25
UPDATE soundlab_lb.fait_ecoutes_jour SET ecoutes = ecoutes + 1
WHERE origine = 'incr' AND date_jour = '2026-09-25'
  AND recording_msid = (SELECT MIN(recording_msid) FROM soundlab_lb.stg_incr WHERE date_jour = '2026-09-25');

-- @ Empreinte perturbee (doit differer de l'empreinte 0)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes, SUM(auditeurs) AS auditeurs,
       SUM(FNV_HASH(date_jour::VARCHAR || '|' || recording_msid || '|' || ecoutes::VARCHAR || '|' || auditeurs::VARCHAR)::DECIMAL(38,0)) AS empreinte
FROM soundlab_lb.fait_ecoutes_jour WHERE origine = 'incr';

-- @ Fusion 1 · calcul des ecarts
DROP TABLE IF EXISTS soundlab_lb.stg_incr_ecarts;

-- @ Fusion 1 · table des ecarts
CREATE TABLE soundlab_lb.stg_incr_ecarts DISTKEY (recording_msid) AS
SELECT s.date_jour, s.recording_msid, s.ecoutes, s.auditeurs,
       CASE WHEN f.recording_msid IS NULL THEN 'nouveau' ELSE 'modifie' END AS nature
FROM soundlab_lb.stg_incr s
LEFT JOIN soundlab_lb.fait_ecoutes_jour f
       ON f.origine = 'incr' AND f.date_jour = s.date_jour AND f.recording_msid = s.recording_msid
WHERE f.recording_msid IS NULL OR f.ecoutes <> s.ecoutes OR f.auditeurs <> s.auditeurs;

-- @ Fusion 1 · ecarts (attendu : nouveaux = groupes supprimes, modifie = 1)
SELECT nature, COUNT(*) AS groupes FROM soundlab_lb.stg_incr_ecarts GROUP BY nature ORDER BY 1;

-- @ Fusion 1 · MERGE
MERGE INTO soundlab_lb.fait_ecoutes_jour
USING soundlab_lb.stg_incr_ecarts e
ON fait_ecoutes_jour.origine = 'incr'
   AND fait_ecoutes_jour.date_jour = e.date_jour
   AND fait_ecoutes_jour.recording_msid = e.recording_msid
WHEN MATCHED THEN UPDATE SET ecoutes = e.ecoutes, auditeurs = e.auditeurs
WHEN NOT MATCHED THEN INSERT VALUES (e.date_jour, e.recording_msid, e.ecoutes, e.auditeurs, 'incr');

-- @ Empreinte 1 · apres fusion (attendu : identique a l'empreinte 0)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes, SUM(auditeurs) AS auditeurs,
       SUM(FNV_HASH(date_jour::VARCHAR || '|' || recording_msid || '|' || ecoutes::VARCHAR || '|' || auditeurs::VARCHAR)::DECIMAL(38,0)) AS empreinte
FROM soundlab_lb.fait_ecoutes_jour WHERE origine = 'incr';

-- @ Fusion 2 · rejeu, calcul des ecarts
DROP TABLE IF EXISTS soundlab_lb.stg_incr_ecarts;

-- @ Fusion 2 · table des ecarts
CREATE TABLE soundlab_lb.stg_incr_ecarts DISTKEY (recording_msid) AS
SELECT s.date_jour, s.recording_msid, s.ecoutes, s.auditeurs,
       CASE WHEN f.recording_msid IS NULL THEN 'nouveau' ELSE 'modifie' END AS nature
FROM soundlab_lb.stg_incr s
LEFT JOIN soundlab_lb.fait_ecoutes_jour f
       ON f.origine = 'incr' AND f.date_jour = s.date_jour AND f.recording_msid = s.recording_msid
WHERE f.recording_msid IS NULL OR f.ecoutes <> s.ecoutes OR f.auditeurs <> s.auditeurs;

-- @ Fusion 2 · ecarts (attendu : aucune ligne)
SELECT nature, COUNT(*) AS groupes FROM soundlab_lb.stg_incr_ecarts GROUP BY nature ORDER BY 1;

-- @ Fusion 2 · MERGE rejoue
MERGE INTO soundlab_lb.fait_ecoutes_jour
USING soundlab_lb.stg_incr_ecarts e
ON fait_ecoutes_jour.origine = 'incr'
   AND fait_ecoutes_jour.date_jour = e.date_jour
   AND fait_ecoutes_jour.recording_msid = e.recording_msid
WHEN MATCHED THEN UPDATE SET ecoutes = e.ecoutes, auditeurs = e.auditeurs
WHEN NOT MATCHED THEN INSERT VALUES (e.date_jour, e.recording_msid, e.ecoutes, e.auditeurs, 'incr');

-- @ Empreinte 2 · apres rejeu (attendu : identique a l'empreinte 1)
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes, SUM(auditeurs) AS auditeurs,
       SUM(FNV_HASH(date_jour::VARCHAR || '|' || recording_msid || '|' || ecoutes::VARCHAR || '|' || auditeurs::VARCHAR)::DECIMAL(38,0)) AS empreinte
FROM soundlab_lb.fait_ecoutes_jour WHERE origine = 'incr';

-- @ Controle · partie complet intacte
SELECT COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes FROM soundlab_lb.fait_ecoutes_jour WHERE origine = 'complet';

-- @ Controle · part non triee apres fusion
SELECT "table" AS table_name, tbl_rows AS lignes, size AS blocs_1mo, unsorted AS pct_non_trie
FROM svv_table_info WHERE schema = 'soundlab_lb' ORDER BY "table";
