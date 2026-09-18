
"""Orquestra as tres queries da arquitetura medalhao em uma sessao Spark."""

from __future__ import annotations

from pyspark.sql import SparkSession

import subprocess
import sys
from pathlib import Path

from config import PipelineConfig
from bronze import start_bronze_query
from gold import materialize_gold_snapshot, start_gold_query
from silver import start_silver_query


def run_medallion_pipeline(config: PipelineConfig) -> None:
    """Executa BRONZE, SILVER e GOLD durante o tempo configurado."""

    # Gera eventos de teste localmente antes de iniciar as queries.
    # Use a variável de ambiente `INITIAL_EVENTS` para controlar a geração.
    if getattr(config, "initial_events", 0) > 0:
        try:
            script = (
                Path(__file__).resolve().parents[1] / "producer" / "generate_events.py"
            )
            cmd = [
                sys.executable,
                str(script),
                "--events",
                str(config.initial_events),
                "--interval",
                str(config.initial_interval),
                "--output",
                str(config.input_dir / "financing_events.jsonl"),
            ]
            print("Gerando eventos iniciais:", " ".join(cmd))
            subprocess.run(cmd, check=True)
        except Exception as exc:
            print(f"ERRO ao gerar eventos iniciais: {exc}")

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
        print("[PIPELINE] Iniciando BRONZE (data/input → MinIO /bronze)")
        bronze_query = start_bronze_query(
            spark,
            config,
        )
        print("[PIPELINE] BRONZE query iniciada; aguardando conclusão...")
        bronze_query.awaitTermination()
        print("[PIPELINE] BRONZE concluído: arquivos Parquet escritos no MinIO /bronze")

        # SILVER:
        # MinIO /bronze → MinIO /silver
        print("[PIPELINE] Iniciando SILVER (MinIO /bronze → MinIO /silver)")
        silver_query = start_silver_query(
            spark,
            config,
        )
        print("[PIPELINE] SILVER query iniciada; aguardando conclusão...")
        silver_query.awaitTermination()
        print("[PIPELINE] SILVER concluído: registros normalizados salvos no MinIO /silver")

        # GOLD:
        # MinIO /silver → Parquet local + PostgreSQL
        print("[PIPELINE] Iniciando GOLD (MinIO /silver → Parquet local + PostgreSQL)")
        gold_query = start_gold_query(
            spark,
            config,
        )
        print("[PIPELINE] GOLD query iniciada; aguardando conclusão...")
        gold_query.awaitTermination()
        print("[PIPELINE] GOLD concluído: agregações gravadas no Parquet local e no PostgreSQL")

    finally:
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
