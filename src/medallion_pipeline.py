
"""Orquestra as tres queries da arquitetura medalhao em uma sessao Spark."""

from __future__ import annotations

from pyspark.sql import SparkSession

from config import PipelineConfig
from bronze import start_bronze_query
from gold import materialize_gold_snapshot, start_gold_query
from silver import start_silver_query


def run_medallion_pipeline(config: PipelineConfig) -> None:
    """Executa BRONZE, SILVER e GOLD durante o tempo configurado."""

    # Diretorios que permanecem locais.
    #
    # BRONZE e SILVER sao armazenadas no MinIO.
    # Os checkpoints permanecem locais.
    for directory in (
        config.input_dir,
        config.bronze_checkpoint_dir,
        config.silver_checkpoint_dir,
        config.gold_dir,
        config.gold_checkpoint_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("vehicle-financing-medallion-pipeline")

        .config(
            "spark.sql.shuffle.partitions",
            "2",
        )

        # ==========================================================
        # Configuracao S3A / MinIO
        # ==========================================================

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

    queries = []

    try:
        # ==========================================================
        # Pipeline Medallion
        #
        # Input
        #   ↓
        # BRONZE
        #   ↓
        # SILVER
        #   ↓
        # GOLD
        # ==========================================================

        # BRONZE:
        # data/input → MinIO /bronze
        bronze_query = start_bronze_query(
            spark,
            config,
        )
        queries.append(bronze_query)

        # SILVER:
        # MinIO /bronze → MinIO /silver
        silver_query = start_silver_query(
            spark,
            config,
        )
        queries.append(silver_query)

        # GOLD:
        # MinIO /silver → Parquet local + PostgreSQL
        gold_query = start_gold_query(
            spark,
            config,
        )
        queries.append(gold_query)

        # A query BRONZE controla o tempo total da execucao.
        bronze_query.awaitTermination(
            config.runtime_seconds
        )

    finally:
        # Encerra as queries na ordem inversa.
        for query in reversed(queries):
            try:
                query.stop()
            except Exception:
                pass

        # ==========================================================
        # Snapshot final da GOLD
        #
        # A execucao local e finita. Depois que as queries
        # terminarem, le SILVER diretamente do MinIO e
        # fecha as janelas em batch.
        # ==========================================================

        try:
            materialize_gold_snapshot(
                spark,
                config,
            )
        except Exception as exc:
            print(
                f"ERRO ao materializar snapshot GOLD: {exc}"
            )
            raise

        spark.stop()


if __name__ == "__main__":
    run_medallion_pipeline(
        PipelineConfig.from_environment()
    )
