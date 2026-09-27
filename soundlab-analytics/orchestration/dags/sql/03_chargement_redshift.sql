-- =============================================================================
--  SoundLab Analytics - chargement de l'entrepot depuis la couche curated
--
--  Execute par la tache 11_chargement_redshift du DAG, via l'API de donnees,
--  sous l'identite IAMR:SoundLabAirflowRole.
--
--  ORDRE DES COLONNES : COPY FORMAT AS PARQUET associe les colonnes PAR
--  POSITION. Le schema de sql/01_schema_etoile.sql reproduit exactement
--  l'ordre du Parquet produit par Spark. Ne pas modifier l'un sans l'autre :
--  un decalage ne leve aucune erreur, il charge des valeurs dans les
--  mauvaises colonnes.
--
--  ABSENCE D'ANALYZE : cette instruction exige d'etre proprietaire de la
--  table. L'orchestrateur ne l'est pas, volontairement. Redshift declenche
--  une analyse automatique apres une modification substantielle.
--
--  TRUNCATE ET ATOMICITE : TRUNCATE valide implicitement. Si le COPY qui
--  suit echoue, la table reste vide jusqu'au prochain chargement reussi.
--  C'est accepte ici parce que le controle de volumetrie final rend la
--  situation immediatement visible. Un contexte de production exigerait un
--  chargement en table tampon puis une permutation par ALTER TABLE RENAME
--  dans une transaction.
--
--  Les tables temoins _naif ne sont PAS rechargees : elles n'ont servi qu'a
--  l'etude de benchmark, et l'orchestrateur n'y a aucun droit.
-- =============================================================================

-- @ Vider la dimension
TRUNCATE soundlab.dim_track;

-- @ Charger dim_track depuis le Parquet curated
COPY soundlab.dim_track
FROM 's3://{{BUCKET_CUR}}/music_info/'
IAM_ROLE '{{ROLE_ARN}}'
FORMAT AS PARQUET;

-- @ Vider la table de faits
TRUNCATE soundlab.fact_listening;

-- @ Charger fact_listening depuis le Parquet curated
COPY soundlab.fact_listening
FROM 's3://{{BUCKET_CUR}}/listening_history/'
IAM_ROLE '{{ROLE_ARN}}'
FORMAT AS PARQUET;

-- @ Controle de volumetrie
SELECT
  (SELECT count(*) FROM soundlab.dim_track)      AS lignes_dim_track,
  (SELECT count(*) FROM soundlab.fact_listening) AS lignes_fact_listening;
