"""Orquestra as três queries da arquitetura medalhão em uma sessão Spark."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from config import PipelineConfig
from bronze import start_bronze_query
from gold import materialize_gold_snapshot, start_gold_query
from silver import start_silver_query


def create_spark_session(config: PipelineConfig) -> SparkSession:
    """Cria e configura a sessão Spark utilizada pelo pipeline."""

    spark = (
        SparkSession.builder
        .appName("vehicle-financing-medallion-pipeline")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            config.minio_endpoint,
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            config.minio_access_key,
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            config.minio_secret_key,
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true",
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def create_local_directories(config: PipelineConfig) -> None:
    """Cria os diretórios locais utilizados pelo pipeline."""

    directories = (
        config.input_dir,
        config.bronze_checkpoint_dir,
        config.silver_checkpoint_dir,
        config.gold_dir,
        config.gold_checkpoint_dir,
    )

    for directory in directories:
        directory.mkdir(
            parents=True,
            exist_ok=True,
        )


def stop_query(
    query: StreamingQuery | None,
    name: str,
) -> None:
    """Encerra uma StreamingQuery caso ela ainda esteja ativa."""

    if query is None:
        return

    try:
        if query.isActive:
            print(
                f"[PIPELINE] Encerrando query {name}..."
            )
            query.stop()

    except Exception as exc:
        print(
            f"[PIPELINE] Erro ao encerrar {name}: {exc}"
        )


def run_medallion_pipeline(config: PipelineConfig) -> None:
    """Executa BRONZE, SILVER e GOLD como streaming."""

    print("=" * 70)
    print("VEHICLE FINANCING - STREAM PIPELINE")
    print("=" * 70)

    create_local_directories(config)

    print(
        f"[PIPELINE] Input monitorado: {config.input_dir}"
    )

    print(
        "[PIPELINE] Criando SparkSession..."
    )

    spark = create_spark_session(config)

    print(
        "[PIPELINE] SparkSession criada."
    )

    bronze_query: StreamingQuery | None = None
    silver_query: StreamingQuery | None = None
    gold_query: StreamingQuery | None = None

    try:
        # ==========================================================
        # BRONZE
        # financing_events.jsonl -> MinIO /bronze
        # ==========================================================

        print()
        print(
            "[PIPELINE] Iniciando BRONZE "
            "(data/input -> MinIO /bronze)"
        )

        bronze_query = start_bronze_query(
            spark,
            config,
        )

        print(
            "[PIPELINE] BRONZE iniciada."
        )

        # ==========================================================
        # SILVER
        # MinIO /bronze -> MinIO /silver
        # ==========================================================

        print()
        print(
            "[PIPELINE] Iniciando SILVER "
            "(MinIO /bronze -> MinIO /silver)"
        )

        silver_query = start_silver_query(
            spark,
            config,
        )

        print(
            "[PIPELINE] SILVER iniciada."
        )

        # ==========================================================
        # GOLD
        # MinIO /silver -> GOLD / PostgreSQL
        # ==========================================================

        print()
        print(
            "[PIPELINE] Iniciando GOLD "
            "(MinIO /silver -> GOLD/PostgreSQL)"
        )

        gold_query = start_gold_query(
            spark,
            config,
        )

        print(
            "[PIPELINE] GOLD iniciada."
        )

        # ==========================================================
        # STREAMING ATIVO
        # ==========================================================

        print()
        print("=" * 70)
        print("[PIPELINE] STREAMING ATIVO")
        print("=" * 70)

        print(
            "[PIPELINE] Aguardando eventos de financiamento..."
        )

        print(
            "[PIPELINE] Producer -> financing_events.jsonl "
            "-> BRONZE -> SILVER -> GOLD"
        )

        print()

        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        print()
        print(
            "[PIPELINE] Interrupção solicitada pelo usuário."
        )

    except Exception as exc:
        print()
        print(
            f"[PIPELINE] ERRO: {exc}"
        )
        raise

    finally:
        # ==========================================================
        # ENCERRAMENTO DAS QUERIES
        # ==========================================================

        print()
        print(
            "[PIPELINE] Encerrando queries..."
        )

        stop_query(
            gold_query,
            "GOLD",
        )

        stop_query(
            silver_query,
            "SILVER",
        )

        stop_query(
            bronze_query,
            "BRONZE",
        )

        # ==========================================================
        # SNAPSHOT FINAL DA GOLD
        # ==========================================================

        print()
        print(
            "[PIPELINE] Materializando snapshot final da GOLD..."
        )

        try:
            materialize_gold_snapshot(
                spark,
                config,
            )

            print(
                "[PIPELINE] Snapshot GOLD concluído."
            )

        except Exception as exc:
            print(
                "[PIPELINE] Não foi possível materializar "
                f"o snapshot GOLD: {exc}"
            )

        # ==========================================================
        # SPARK
        # ==========================================================

        print(
            "[PIPELINE] Encerrando SparkSession..."
        )

        spark.stop()

        print("=" * 70)
        print("[PIPELINE] Pipeline encerrado.")
        print("=" * 70)


if __name__ == "__main__":
    run_medallion_pipeline(
        PipelineConfig.from_environment()
    )
