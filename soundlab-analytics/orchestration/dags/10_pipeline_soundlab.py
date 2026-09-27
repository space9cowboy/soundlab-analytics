"""
Pipeline SoundLab Analytics - orchestration des traitements distribues.

    07_ingestion_music_info ──┐
                              ├──> 09_porte_qualite ──> 10_feature_engineering
    08_ingestion_listening ───┘

Les deux ingestions ne dependent pas l'une de l'autre : elles lisent deux
fichiers sources differents. C'est 09 qui a besoin des deux, parce qu'il
verifie l'integrite referentielle entre les deux tables.

Le graphe exprime les DEPENDANCES LOGIQUES. La contrainte de ressource -
le quota de 16 vCPU concurrents du compte - est exprimee separement par un
pool d'un seul emplacement. Inventer une fausse dependance 07 -> 08 pour
serialiser mentirait sur la nature du pipeline.

La cinquieme tache charge l'entrepot par COPY depuis la couche curated.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

journal = logging.getLogger(__name__)

# =============================================================================
#  Configuration
#
#  Ces valeurs refletent .soundlab.env. Elles ne sont pas secretes - ce sont
#  des identifiants de ressources, pas des identifiants d'acces. Un deploiement
#  reel les placerait dans des Variables Airflow ; ici, les garder visibles
#  dans le fichier rend le DAG lisible d'un coup d'oeil.
# =============================================================================
REGION = "eu-north-1"
COMPTE = "589276558852"

APP_EMR = "00g8l9brbs9e3f1d"
ROLE_EXECUTION = f"arn:aws:iam::{COMPTE}:role/SoundLabEMRServerlessExecutionRole"
SECRET_SEL = (
    f"arn:aws:secretsmanager:{REGION}:{COMPTE}:secret:"
    "soundlab/pseudonymisation-salt-wZrOBi"
)

B_RAW = "soundlab-raw-558852"
B_CUR = "soundlab-curated-558852"
B_SCR = "soundlab-scripts-558852"
B_LOG = "soundlab-logs-558852"
GLUE_DB = "soundlab_curated"

# Entrepot. Le role Redshift est celui attache au namespace : c'est Redshift
# qui l'assume pour lire S3, pas l'orchestrateur. Aucun iam:PassRole requis.
RS_WORKGROUP = "soundlab-wg"
RS_DB = "soundlab"
ROLE_REDSHIFT = f"arn:aws:iam::{COMPTE}:role/SoundLabRedshiftS3Role"
DELAI_SQL_S = 600
REPERTOIRE_SQL = Path(__file__).parent / "sql"

# Parametres Spark calibres sur le quota du compte.
#   3 executeurs x 4 coeurs + 2 coeurs de pilote = 14 vCPU, sous le plafond
#   de 16. Le plafond reel d'un traitement distribue n'est pas la capacite
#   declaree de l'application, mais le quota du compte (incident 10.3).
PARAMS_SPARK = " ".join([
    "--conf spark.executor.cores=4",
    "--conf spark.executor.memory=16g",
    "--conf spark.driver.cores=2",
    "--conf spark.driver.memory=8g",
    "--conf spark.executor.instances=3",
    "--conf spark.dynamicAllocation.enabled=true",
    "--conf spark.dynamicAllocation.minExecutors=1",
    "--conf spark.dynamicAllocation.initialExecutors=3",
    "--conf spark.dynamicAllocation.maxExecutors=3",
    "--conf spark.sql.shuffle.partitions=48",
    "--conf spark.sql.sources.partitionOverwriteMode=dynamic",
])

# Delai maximal accorde a un job. Les traitements mesures durent 36 a 45 s ;
# vingt minutes laissent une marge considerable tout en garantissant qu'un
# blocage ne puisse pas se facturer indefiniment. Sur une plateforme facturee
# a la seconde, le delai d'execution est une decision d'architecture
# (incident 10.6 : 41 minutes facturees pour une erreur de connexion).
DELAI_JOB_MINUTES = 20
INTERVALLE_SCRUTATION_S = 15

# Politique de reessai du client AWS. Les valeurs par defaut de botocore
# reessaient pendant des dizaines de minutes ; on borne explicitement.
CONFIG_AWS = Config(
    connect_timeout=5,
    read_timeout=30,
    retries={"max_attempts": 3, "mode": "standard"},
)

ETATS_TERMINES = {"SUCCESS", "FAILED", "CANCELLED"}


# =============================================================================
#  Outils
# =============================================================================
def _client(service: str):
    return boto3.client(service, region_name=REGION, config=CONFIG_AWS)


def _tracer_journal_spark(id_execution: str, lignes: int = 60) -> None:
    """Recopie la fin de la trace d'erreur Spark dans le journal Airflow.

    C'est la raison pour laquelle le role de l'orchestrateur a recu la lecture
    du compartiment de journaux. Sans cela, chaque echec obligerait a ouvrir
    la console AWS pour savoir ce qui s'est passe.
    """
    prefixe = f"emr-serverless/applications/{APP_EMR}/jobs/{id_execution}/"
    s3 = _client("s3")
    try:
        objets = s3.list_objects_v2(Bucket=B_LOG, Prefix=prefixe).get("Contents", [])
    except ClientError as err:
        journal.warning("Journaux Spark illisibles (%s)", err.response["Error"]["Code"])
        return

    cles = [o["Key"] for o in objets if o["Key"].endswith("SPARK_DRIVER/stderr.gz")]
    if not cles:
        journal.warning("Aucune trace SPARK_DRIVER sous s3://%s/%s", B_LOG, prefixe)
        return

    brut = s3.get_object(Bucket=B_LOG, Key=cles[0])["Body"].read()
    texte = gzip.GzipFile(fileobj=io.BytesIO(brut)).read().decode("utf-8", "replace")
    fin = texte.splitlines()[-lignes:]
    journal.error("--- %d dernieres lignes de la trace Spark ---", len(fin))
    for ligne in fin:
        journal.error("  %s", ligne)


def _tracer_rapport(chemin_s3: str) -> dict | None:
    """Recopie le rapport JSON produit par le job dans le journal Airflow."""
    sans_schema = chemin_s3.removeprefix("s3://")
    compartiment, _, cle = sans_schema.partition("/")
    try:
        corps = _client("s3").get_object(Bucket=compartiment, Key=cle)["Body"].read()
    except ClientError as err:
        journal.warning("Rapport illisible (%s)", err.response["Error"]["Code"])
        return None
    rapport = json.loads(corps)
    journal.info("--- rapport du job ---\n%s", json.dumps(rapport, indent=2, ensure_ascii=False))
    return rapport


def executer_job_emr(nom: str, script: str, arguments: list[str], **contexte) -> str:
    """Soumet un job EMR Serverless, attend sa fin, remonte ce qu'il a produit.

    Renvoie l'identifiant d'execution ; leve AirflowException si le job echoue.
    """
    emr = _client("emr-serverless")
    identifiant_run = contexte["run_id"].replace(":", "-").replace("+", "-")[:40]

    reponse = emr.start_job_run(
        applicationId=APP_EMR,
        executionRoleArn=ROLE_EXECUTION,
        name=f"{nom}-{identifiant_run}",
        jobDriver={
            "sparkSubmit": {
                "entryPoint": f"s3://{B_SCR}/jobs/{script}",
                "entryPointArguments": arguments,
                "sparkSubmitParameters": PARAMS_SPARK,
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": f"s3://{B_LOG}/emr-serverless/"}
            }
        },
        executionTimeoutMinutes=DELAI_JOB_MINUTES,
    )
    id_execution = reponse["jobRunId"]
    journal.info("Job %s soumis - jobRunId=%s", nom, id_execution)

    # L'identifiant part en XCom des la soumission : meme si la tache est tuee,
    # il reste tracable dans l'interface.
    contexte["ti"].xcom_push(key="job_run_id", value=id_execution)

    etat = "SUBMITTED"
    debut = time.monotonic()
    try:
        while etat not in ETATS_TERMINES:
            time.sleep(INTERVALLE_SCRUTATION_S)
            details = emr.get_job_run(applicationId=APP_EMR, jobRunId=id_execution)["jobRun"]
            etat = details["state"]
            journal.info("  %s : %s (%.0f s)", nom, etat, time.monotonic() - debut)
    except BaseException:
        # Interruption de la tache Airflow : on annule le job plutot que de
        # laisser du calcul facture tourner sans surveillance.
        journal.warning("Interruption - annulation du job %s", id_execution)
        try:
            emr.cancel_job_run(applicationId=APP_EMR, jobRunId=id_execution)
        except ClientError:
            journal.warning("Annulation impossible - verifier dans la console")
        raise

    duree = time.monotonic() - debut
    if etat != "SUCCESS":
        motif = details.get("stateDetails", "aucun detail")
        journal.error("Job %s termine en %s apres %.0f s : %s", nom, etat, duree, motif)
        _tracer_journal_spark(id_execution)
        raise AirflowException(f"{nom} : etat final {etat}")

    journal.info("Job %s reussi en %.0f s", nom, duree)
    return id_execution


def executer_et_rapporter(nom: str, script: str, arguments: list[str],
                          rapport: str, **contexte) -> str:
    id_execution = executer_job_emr(nom, script, arguments, **contexte)
    _tracer_rapport(rapport)
    return id_execution


def executer_porte_qualite(nom: str, script: str, arguments: list[str],
                           rapport: str, **contexte) -> str:
    """Identique, mais le rapport est trace AVANT de conclure.

    Le job 09 renvoie le code 1 si un controle bloquant echoue. EMR traduit
    ce code en etat FAILED, Airflow en tache en echec, et les taches en aval
    passent en upstream_failed sans jamais s'executer. La porte de qualite
    n'est pas une convention qu'on s'impose : c'est une propriete du graphe.

    On lit le rapport dans les deux cas, parce qu'en cas d'echec c'est
    justement lui qui dit QUEL controle a saute.
    """
    try:
        return executer_job_emr(nom, script, arguments, **contexte)
    finally:
        _tracer_rapport(rapport)


# =============================================================================
#  Chargement de l'entrepot
# =============================================================================
def _decouper_sql(texte: str) -> list[tuple[str, str]]:
    """Decoupe un fichier SQL en instructions, en conservant les libelles.

    La convention `-- @ Libelle` est celle deja utilisee par infra/rs.sh :
    chaque instruction porte un nom lisible, ce qui rend le journal Airflow
    intelligible sans avoir a y relire du SQL.
    """
    blocs: list[tuple[str, str]] = []
    libelle: str | None = None
    tampon: list[str] = []
    for ligne in texte.splitlines():
        nue = ligne.strip()
        if nue.startswith("-- @"):
            libelle = nue[4:].strip()
            continue
        if not nue or nue.startswith("--"):
            continue
        tampon.append(ligne)
        if nue.endswith(";"):
            instruction = "\n".join(tampon).rstrip().rstrip(";")
            blocs.append((libelle or "instruction", instruction))
            tampon, libelle = [], None
    return blocs


def _executer_instruction(client, sql: str, libelle: str) -> dict:
    reponse = client.execute_statement(
        WorkgroupName=RS_WORKGROUP, Database=RS_DB, Sql=sql,
        StatementName=libelle[:60],
    )
    identifiant = reponse["Id"]
    etat, details = "SUBMITTED", {}
    debut = time.monotonic()

    while etat not in {"FINISHED", "FAILED", "ABORTED"}:
        time.sleep(2)
        details = client.describe_statement(Id=identifiant)
        etat = details["Status"]
        if time.monotonic() - debut > DELAI_SQL_S:
            client.cancel_statement(Id=identifiant)
            raise AirflowException(f"{libelle} : delai de {DELAI_SQL_S} s depasse")

    if etat != "FINISHED":
        raise AirflowException(f"{libelle} : {etat} - {details.get('Error', 'sans detail')}")

    # Duration est exprimee en nanosecondes par l'API de donnees.
    journal.info("  %-42s %s en %.0f ms", libelle, etat, details.get("Duration", 0) / 1e6)
    details["_id"] = identifiant
    return details


def charger_redshift(fichier_sql: str, **contexte) -> dict:
    """Rejoue le fichier de chargement, instruction par instruction."""
    texte = (REPERTOIRE_SQL / fichier_sql).read_text(encoding="utf-8")
    texte = texte.replace("{{BUCKET_CUR}}", B_CUR).replace("{{ROLE_ARN}}", ROLE_REDSHIFT)

    instructions = _decouper_sql(texte)
    if not instructions:
        raise AirflowException(f"Aucune instruction lue dans {fichier_sql}")
    journal.info("%d instructions a executer", len(instructions))

    client = _client("redshift-data")
    derniere = None
    for libelle, sql in instructions:
        derniere = _executer_instruction(client, sql, libelle)

    # La derniere instruction est le controle de volumetrie.
    if not derniere.get("HasResultSet"):
        raise AirflowException("Le controle de volumetrie n'a renvoye aucun resultat")

    resultat = client.get_statement_result(Id=derniere["_id"])
    valeurs = [c.get("longValue") for c in resultat["Records"][0]]
    colonnes = [c["name"] for c in resultat["ColumnMetadata"]]
    volumetrie = dict(zip(colonnes, valeurs))
    journal.info("Volumetrie apres chargement : %s", volumetrie)

    vides = [nom for nom, n in volumetrie.items() if not n]
    if vides:
        raise AirflowException(f"Tables vides apres chargement : {', '.join(vides)}")

    return volumetrie


# =============================================================================
#  Chemins de sortie
# =============================================================================
def _rapport(tache: str) -> str:
    return f"s3://{B_CUR}/_rapports/{tache}/rapport.json"


ARGUMENTS_PAR_DEFAUT = {
    "owner": "soundlab",
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=1),
    # Le pool exprime la contrainte de ressource : au plus un job EMR a la
    # fois, parce que 2 x 14 vCPU depasserait le quota de 16 du compte.
    "pool": "emr_serverless",
}

with DAG(
    dag_id="10_pipeline_soundlab",
    description="Ingestion, qualite et construction des variables sur EMR Serverless",
    default_args=ARGUMENTS_PAR_DEFAUT,
    start_date=datetime(2026, 9, 1),
    # Les donnees sources n'ont aucun horodatage : aucune periodicite n'aurait
    # de sens. Le DAG se declenche manuellement, ou sera branche sur l'arrivee
    # de nouvelles donnees le jour ou il y en aura.
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["soundlab", "bloc3", "emr"],
) as dag:

    ingestion_music_info = PythonOperator(
        task_id="07_ingestion_music_info",
        python_callable=executer_et_rapporter,
        retries=1,
        op_kwargs={
            "nom": "ingestion-music-info",
            "script": "07_ingest_music_info.py",
            "rapport": _rapport("07_ingestion_music_info"),
            "arguments": [
                "--source", f"s3://{B_RAW}/msd/music_info.csv",
                "--cible", f"s3://{B_CUR}/music_info/",
                "--rapport", _rapport("07_ingestion_music_info"),
                "--glue-db", GLUE_DB,
                "--table", "music_info",
                "--region", REGION,
            ],
        },
    )

    ingestion_listening = PythonOperator(
        task_id="08_ingestion_listening_history",
        python_callable=executer_et_rapporter,
        retries=1,
        op_kwargs={
            "nom": "ingestion-listening-history",
            "script": "08_ingest_listening_history.py",
            "rapport": _rapport("08_ingestion_listening_history"),
            "arguments": [
                "--source", f"s3://{B_RAW}/msd/user_listening_history.csv",
                "--cible", f"s3://{B_CUR}/listening_history/",
                "--rapport", _rapport("08_ingestion_listening_history"),
                # L'orchestrateur transmet l'ARN du secret, il ne le lit pas.
                # C'est le role d'execution EMR qui a le droit de le resoudre :
                # le sel ne transite jamais par Airflow.
                "--secret-arn", SECRET_SEL,
                "--glue-db", GLUE_DB,
                "--table", "listening_history",
                "--fichiers", "12",
                "--bits-hachage", "128",
                "--region", REGION,
            ],
        },
    )

    porte_qualite = PythonOperator(
        task_id="09_porte_qualite",
        python_callable=executer_porte_qualite,
        # Aucun reessai. Un controle de qualite qui echoue rend un verdict
        # deterministe : le rejouer donnerait le meme resultat, en facturant
        # une seconde fois. Le reessai protege d'un alea, pas d'un constat.
        retries=0,
        op_kwargs={
            "nom": "tests-qualite",
            "script": "09_tests_qualite.py",
            "rapport": _rapport("09_porte_qualite"),
            "arguments": [
                "--music-info", f"s3://{B_CUR}/music_info/",
                "--listening-history", f"s3://{B_CUR}/listening_history/",
                "--rapport", _rapport("09_porte_qualite"),
                "--region", REGION,
            ],
        },
    )

    feature_engineering = PythonOperator(
        task_id="10_feature_engineering",
        python_callable=executer_et_rapporter,
        retries=1,
        op_kwargs={
            "nom": "feature-engineering",
            "script": "10_feature_engineering.py",
            "rapport": _rapport("10_feature_engineering"),
            "arguments": [
                "--music-info", f"s3://{B_CUR}/music_info/",
                "--listening-history", f"s3://{B_CUR}/listening_history/",
                "--cible-engagement", f"s3://{B_CUR}/track_engagement/",
                "--cible-features", f"s3://{B_CUR}/songs_features_labeled/",
                "--rapport", _rapport("10_feature_engineering"),
                "--glue-db", GLUE_DB,
                "--percentile", "0.75",
                "--region", REGION,
            ],
        },
    )

    chargement_redshift = PythonOperator(
        task_id="11_chargement_redshift",
        python_callable=charger_redshift,
        retries=1,
        # Cette tache ne consomme aucune capacite EMR : elle n'a rien a faire
        # dans le pool qui protege le quota de vCPU.
        pool="default_pool",
        op_kwargs={"fichier_sql": "03_chargement_redshift.sql"},
    )

    (
        [ingestion_music_info, ingestion_listening]
        >> porte_qualite
        >> feature_engineering
        >> chargement_redshift
    )
