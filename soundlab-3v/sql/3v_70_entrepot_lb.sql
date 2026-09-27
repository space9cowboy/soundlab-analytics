-- =============================================================================
--  SoundLab Analytics — chantier trois V, tâche 4.1
--  Entrepôt ListenBrainz : dimension temps, faits jour × titre, table témoin
--
--  Exécution depuis la racine de soundlab-3v, avec le pilote v1 (lecture seule) :
--    ../soundlab-analytics/infra/rs.sh sql/3v_70_entrepot_lb.sql
--
--  Schéma distinct soundlab_lb : le schéma soundlab du Bloc 6 n'est pas touché.
--  Seule lecture hors de soundlab_lb : soundlab.dim_track, utilisée comme
--  générateur de numéros pour la dimension temps (50 683 lignes, 8 858 utiles).
--
--  Source : sortie du job 3v_61 (décision du 27/09 : deux origines, un grain).
--    origine=complet  502 960 355 groupes, 695 640 731 écoutes, 2002-10 à 2016-12
--    origine=incr      17 226 501 groupes,  24 184 191 écoutes, 2005-02 à 2026-09
--  Aucune colonne de jeton : l'entrepôt ne reçoit que des comptes.
--
--  Mêmes conventions que sql/01_schema_etoile.sql du Bloc 6 :
--    · COPY FORMAT AS PARQUET associe les colonnes PAR POSITION ; l'ordre des
--      tables de transit reproduit celui des fichiers : date, recording_msid,
--      ecoutes, auditeurs (mois est une partition, absente des fichiers).
--    · témoin : DISTSTYLE EVEN, aucun tri, ENCODE RAW, mêmes données.
--
--  Les faits sont insérés en UNE seule instruction dans une table vide : Redshift
--  trie alors les lignes à l'insertion. Deux insertions successives laisseraient
--  la seconde dans la région non triée et fausseraient le scénario C.
--
--  Le script est rejouable : il supprime d'abord les tables de soundlab_lb qu'il
--  crée lui-même, et rien d'autre.
-- =============================================================================

-- @ Creation du schema soundlab_lb
CREATE SCHEMA IF NOT EXISTS soundlab_lb;

-- @ Suppression fait_ecoutes_jour (rejeu)
DROP TABLE IF EXISTS soundlab_lb.fait_ecoutes_jour CASCADE;

-- @ Suppression fait_ecoutes_jour_naif (rejeu)
DROP TABLE IF EXISTS soundlab_lb.fait_ecoutes_jour_naif CASCADE;

-- @ Suppression dim_date (rejeu)
DROP TABLE IF EXISTS soundlab_lb.dim_date CASCADE;

-- @ Suppression dim_date_naif (rejeu)
DROP TABLE IF EXISTS soundlab_lb.dim_date_naif CASCADE;

-- @ Suppression stg_complet (rejeu)
DROP TABLE IF EXISTS soundlab_lb.stg_complet CASCADE;

-- @ Suppression stg_incr (rejeu)
DROP TABLE IF EXISTS soundlab_lb.stg_incr CASCADE;

-- =============================================================================
--  DIMENSION TEMPS — dim_date
--
--  Un jour par ligne, du 2002-10-01 (première écoute du dump complet) au
--  2026-12-31 : 8 858 lignes, calendrier continu, y compris les jours sans
--  écoute. DISTSTYLE ALL : répliquée sur chaque tranche, la jointure avec les
--  faits ne fait circuler aucune donnée (même raisonnement que dim_track).
--  jour_semaine suit la norme ISO : 1 = lundi, 7 = dimanche.
-- =============================================================================

-- @ Creation de dim_date (optimisee)
CREATE TABLE soundlab_lb.dim_date (
    date_jour     DATE      NOT NULL ENCODE AZ64,
    annee         SMALLINT  NOT NULL ENCODE AZ64,
    trimestre     SMALLINT  NOT NULL ENCODE AZ64,
    mois          SMALLINT  NOT NULL ENCODE AZ64,
    annee_mois    CHAR(7)   NOT NULL ENCODE ZSTD,
    semaine       SMALLINT  NOT NULL ENCODE AZ64,
    jour_semaine  SMALLINT  NOT NULL ENCODE AZ64,
    est_weekend   BOOLEAN   NOT NULL ENCODE RAW
)
DISTSTYLE ALL
COMPOUND SORTKEY (date_jour);

-- @ Remplissage de dim_date (8 858 jours attendus)
INSERT INTO soundlab_lb.dim_date
SELECT d,
       EXTRACT(year FROM d)::SMALLINT,
       EXTRACT(quarter FROM d)::SMALLINT,
       EXTRACT(month FROM d)::SMALLINT,
       TO_CHAR(d, 'YYYY-MM'),
       EXTRACT(week FROM d)::SMALLINT,
       (CASE WHEN DATE_PART(dow, d) = 0 THEN 7 ELSE DATE_PART(dow, d) END)::SMALLINT,
       DATE_PART(dow, d) IN (0, 6)
FROM (SELECT DATEADD(day, (ROW_NUMBER() OVER (ORDER BY track_id) - 1)::INTEGER, '2002-10-01'::DATE)::DATE AS d
      FROM soundlab.dim_track) s
WHERE d <= '2026-12-31'::DATE;

-- @ Creation de dim_date_naif (temoin)
CREATE TABLE soundlab_lb.dim_date_naif (
    date_jour     DATE      ENCODE RAW,
    annee         SMALLINT  ENCODE RAW,
    trimestre     SMALLINT  ENCODE RAW,
    mois          SMALLINT  ENCODE RAW,
    annee_mois    CHAR(7)   ENCODE RAW,
    semaine       SMALLINT  ENCODE RAW,
    jour_semaine  SMALLINT  ENCODE RAW,
    est_weekend   BOOLEAN   ENCODE RAW
)
DISTSTYLE EVEN;

-- @ Remplissage de dim_date_naif
INSERT INTO soundlab_lb.dim_date_naif SELECT * FROM soundlab_lb.dim_date;

-- =============================================================================
--  TRANSIT — une table par origine, ordre des colonnes = ordre du Parquet
-- =============================================================================

-- @ Creation de stg_complet
CREATE TABLE soundlab_lb.stg_complet (
    date_jour       DATE,
    recording_msid  VARCHAR(64),
    ecoutes         BIGINT,
    auditeurs       BIGINT
);

-- @ Creation de stg_incr
CREATE TABLE soundlab_lb.stg_incr (
    date_jour       DATE,
    recording_msid  VARCHAR(64),
    ecoutes         BIGINT,
    auditeurs       BIGINT
);

-- @ COPY stg_complet (502 960 355 lignes attendues)
COPY soundlab_lb.stg_complet
FROM 's3://{{BUCKET_CUR}}/trois_v/entrepot/faits_jour/origine=complet/'
IAM_ROLE '{{ROLE_ARN}}'
FORMAT AS PARQUET;

-- @ COPY stg_incr (17 226 501 lignes attendues)
COPY soundlab_lb.stg_incr
FROM 's3://{{BUCKET_CUR}}/trois_v/entrepot/faits_jour/origine=incr/'
IAM_ROLE '{{ROLE_ARN}}'
FORMAT AS PARQUET;

-- =============================================================================
--  TABLE DE FAITS — fait_ecoutes_jour
--
--  DISTKEY (recording_msid) : la requête métier regroupe par titre ; toutes les
--  lignes d'un titre résident sur la même tranche et s'agrègent localement.
--  COMPOUND SORTKEY (date_jour) : le nouvel axe d'accès est la fenêtre de temps
--  (un mois, les 30 derniers jours) ; les zone maps écartent les blocs hors
--  fenêtre sans les lire. C'est le choix que 4.1 doit mesurer contre témoin.
--  origine distingue complet et incr : la clé d'un groupe est
--  (date_jour, recording_msid, origine), ce que la fusion de 4.2 exploitera.
-- =============================================================================

-- @ Creation de fait_ecoutes_jour (optimisee)
CREATE TABLE soundlab_lb.fait_ecoutes_jour (
    date_jour       DATE         NOT NULL ENCODE AZ64,
    recording_msid  VARCHAR(64)  NOT NULL ENCODE ZSTD,
    ecoutes         BIGINT       NOT NULL ENCODE AZ64,
    auditeurs       BIGINT       NOT NULL ENCODE AZ64,
    origine         VARCHAR(8)   NOT NULL ENCODE ZSTD
)
DISTKEY (recording_msid)
COMPOUND SORTKEY (date_jour);

-- @ Creation de fait_ecoutes_jour_naif (temoin)
CREATE TABLE soundlab_lb.fait_ecoutes_jour_naif (
    date_jour       DATE         ENCODE RAW,
    recording_msid  VARCHAR(64)  ENCODE RAW,
    ecoutes         BIGINT       ENCODE RAW,
    auditeurs       BIGINT       ENCODE RAW,
    origine         VARCHAR(8)   ENCODE RAW
)
DISTSTYLE EVEN;

-- @ Insertion unique des deux origines dans fait_ecoutes_jour
INSERT INTO soundlab_lb.fait_ecoutes_jour
SELECT date_jour, recording_msid, ecoutes, auditeurs, 'complet' FROM soundlab_lb.stg_complet
UNION ALL
SELECT date_jour, recording_msid, ecoutes, auditeurs, 'incr' FROM soundlab_lb.stg_incr;

-- @ Insertion dans fait_ecoutes_jour_naif (memes donnees)
INSERT INTO soundlab_lb.fait_ecoutes_jour_naif SELECT * FROM soundlab_lb.fait_ecoutes_jour;

-- @ ANALYZE dim_date
ANALYZE soundlab_lb.dim_date;

-- @ ANALYZE dim_date_naif
ANALYZE soundlab_lb.dim_date_naif;

-- @ ANALYZE fait_ecoutes_jour
ANALYZE soundlab_lb.fait_ecoutes_jour;

-- @ ANALYZE fait_ecoutes_jour_naif
ANALYZE soundlab_lb.fait_ecoutes_jour_naif;

-- =============================================================================
--  CONTRÔLES — chaque valeur attendue vient d'une mesure indépendante (3v_61)
--    lignes : 520 186 856 = 502 960 355 + 17 226 501
--    écoutes : 719 824 922 = 695 640 731 + 24 184 191
--    dim_date : 8 858 jours ; dates de faits absentes de dim_date : 0
-- =============================================================================

-- @ Controle 1 · volumetries et sommes par table et origine
SELECT 'fait' AS table_chargee, origine, COUNT(*) AS lignes, SUM(ecoutes) AS ecoutes, MIN(date_jour) AS dmin, MAX(date_jour) AS dmax
FROM soundlab_lb.fait_ecoutes_jour GROUP BY origine
UNION ALL
SELECT 'naif', origine, COUNT(*), SUM(ecoutes), MIN(date_jour), MAX(date_jour)
FROM soundlab_lb.fait_ecoutes_jour_naif GROUP BY origine
UNION ALL
SELECT 'stg_complet', '-', COUNT(*), SUM(ecoutes), MIN(date_jour), MAX(date_jour) FROM soundlab_lb.stg_complet
UNION ALL
SELECT 'stg_incr', '-', COUNT(*), SUM(ecoutes), MIN(date_jour), MAX(date_jour) FROM soundlab_lb.stg_incr
ORDER BY 1, 2;

-- @ Controle 2 · dim_date et dates orphelines
SELECT (SELECT COUNT(*) FROM soundlab_lb.dim_date) AS jours,
       (SELECT MIN(date_jour) FROM soundlab_lb.dim_date) AS premier,
       (SELECT MAX(date_jour) FROM soundlab_lb.dim_date) AS dernier,
       (SELECT COUNT(*) FROM soundlab_lb.dim_date WHERE jour_semaine NOT BETWEEN 1 AND 7) AS jours_semaine_hors_bornes,
       (SELECT COUNT(DISTINCT f.date_jour) FROM soundlab_lb.fait_ecoutes_jour f
          LEFT JOIN soundlab_lb.dim_date d ON d.date_jour = f.date_jour WHERE d.date_jour IS NULL) AS dates_orphelines;

-- @ Controle 3 · parametres physiques, asymetrie et part non triee
SELECT "table" AS table_name, diststyle AS distribution, sortkey1 AS cle_de_tri, encoded AS encodage,
       tbl_rows AS lignes, size AS blocs_1mo, skew_rows AS asymetrie, unsorted AS pct_non_trie
FROM svv_table_info
WHERE schema = 'soundlab_lb'
ORDER BY "table";
