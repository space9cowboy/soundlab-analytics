-- =============================================================================
--  SoundLab Analytics — chantier trois V, tâche 4.1e
--  Témoin réellement non trié pour les scénarios C et F
--
--    ../soundlab-analytics/infra/rs.sh sql/3v_72_temoin_melange.sql
--
--  Soupçon (27/09) : fait_ecoutes_jour_naif a été rempli par INSERT … SELECT *
--  depuis la table triée par date ; ses blocs sont probablement rangés par date
--  par accident, ce qui donnerait au témoin des zone maps efficaces et viderait
--  le scénario C de son sens. sys_query_detail n'a pas permis de trancher
--  (blocks_read = 0 et input_rows = 520 M pour toutes les requêtes, optimisé
--  compris : l'instrument ne distingue pas les cas).
--
--  Correction : un second témoin, mêmes données, mêmes réglages (EVEN, RAW,
--  aucun tri), rempli dans un ordre pseudo-aléatoire déterministe (MD5).
--  L'ancien témoin n'est ni modifié ni supprimé.
--
--  Contrôle d'ordre qui peut échouer : les 200 000 premières lignes lues sans
--  ORDER BY suivent l'ordre de stockage. Une table rangée par date y montre une
--  plage de dates étroite ; une table mélangée, presque toute la période.
-- =============================================================================

-- @ Suppression fait_ecoutes_jour_naif_melange (rejeu)
DROP TABLE IF EXISTS soundlab_lb.fait_ecoutes_jour_naif_melange CASCADE;

-- @ Creation de fait_ecoutes_jour_naif_melange (temoin melange)
CREATE TABLE soundlab_lb.fait_ecoutes_jour_naif_melange (
    date_jour       DATE         ENCODE RAW,
    recording_msid  VARCHAR(64)  ENCODE RAW,
    ecoutes         BIGINT       ENCODE RAW,
    auditeurs       BIGINT       ENCODE RAW,
    origine         VARCHAR(8)   ENCODE RAW
)
DISTSTYLE EVEN;

-- @ Tri automatique desactive sur le temoin melange
ALTER TABLE soundlab_lb.fait_ecoutes_jour_naif_melange ALTER SORTKEY NONE;

-- @ Insertion dans un ordre pseudo-aleatoire
INSERT INTO soundlab_lb.fait_ecoutes_jour_naif_melange
SELECT * FROM soundlab_lb.fait_ecoutes_jour
ORDER BY MD5(recording_msid || date_jour::VARCHAR || origine);

-- @ ANALYZE fait_ecoutes_jour_naif_melange
ANALYZE soundlab_lb.fait_ecoutes_jour_naif_melange;

-- @ Controle 1 · memes donnees que l'optimise
SELECT 'melange' AS table_chargee, COUNT(*) AS lignes, SUM(ecoutes) AS ecoutes FROM soundlab_lb.fait_ecoutes_jour_naif_melange
UNION ALL SELECT 'optimise', COUNT(*), SUM(ecoutes) FROM soundlab_lb.fait_ecoutes_jour
ORDER BY 1;

-- @ Controle 2 · ordre de stockage (200 000 premieres lignes lues)
SELECT 'naif' AS table_lue, MIN(date_jour) AS dmin, MAX(date_jour) AS dmax, COUNT(DISTINCT annee_mois) AS mois_distincts
FROM (SELECT f.date_jour, TO_CHAR(f.date_jour, 'YYYY-MM') AS annee_mois FROM soundlab_lb.fait_ecoutes_jour_naif f LIMIT 200000) a
UNION ALL
SELECT 'melange', MIN(date_jour), MAX(date_jour), COUNT(DISTINCT annee_mois)
FROM (SELECT m.date_jour, TO_CHAR(m.date_jour, 'YYYY-MM') AS annee_mois FROM soundlab_lb.fait_ecoutes_jour_naif_melange m LIMIT 200000) b
UNION ALL
SELECT 'optimise', MIN(date_jour), MAX(date_jour), COUNT(DISTINCT annee_mois)
FROM (SELECT o.date_jour, TO_CHAR(o.date_jour, 'YYYY-MM') AS annee_mois FROM soundlab_lb.fait_ecoutes_jour o LIMIT 200000) c
ORDER BY 1;

-- @ Controle 3 · parametres physiques
SELECT "table" AS table_name, diststyle AS distribution, sortkey1 AS cle_de_tri, encoded AS encodage,
       tbl_rows AS lignes, size AS blocs_1mo
FROM svv_table_info
WHERE schema = 'soundlab_lb' AND "table" LIKE 'fait%'
ORDER BY "table";
