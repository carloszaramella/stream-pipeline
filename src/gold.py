"""Camada GOLD: agrega financiamentos por janela e dimensoes de negocio."""

from __future__ import annotations

import psycopg2

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    round as spark_round,
    sum as spark_sum,
    window,
)

from config import PipelineConfig
from financing_schema import FINANCING_SCHEMA

SUMMARY_TABLE = "vehicle_financing_summary"


def aggregate_gold(
    silver_stream: DataFrame,
    window_duration: str,
    watermark_duration: str,
) -> DataFrame:
    """Calcula indicadores por segmento, regiao, tipo e modelo do veiculo."""
    return (
        silver_stream
        .withWatermark("created_at", watermark_duration)
        .groupBy(
            window(col("created_at"), window_duration),
            col("segment"),
            col("region"),
            col("vehicle_type"),
            col("vehicle_brand"),
            col("vehicle_model"),
        )
        .agg(
            count("financing_id").alias("financing_count"),
            spark_round(
                spark_sum("financed_amount"), 2
            ).alias("total_financed_amount"),
            spark_round(
                avg("monthly_installment"), 2
            ).alias("average_monthly_installment"),
            spark_round(
                avg("interest_rate_monthly"), 4
            ).alias("average_interest_rate_monthly"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "segment",
            "region",
            "vehicle_type",
            "vehicle_brand",
            "vehicle_model",
            "financing_count",
            "total_financed_amount",
            "average_monthly_installment",
            "average_interest_rate_monthly",
        )
    )


def aggregate_gold_batch(
    silver_df: DataFrame,
    window_duration: str,
) -> DataFrame:
    """Agrega a camada silver em batch para fechar as janelas."""
    return (
        silver_df
        .groupBy(
            window(col("created_at"), window_duration),
            col("segment"),
            col("region"),
            col("vehicle_type"),
            col("vehicle_brand"),
            col("vehicle_model"),
        )
        .agg(
            count("financing_id").alias("financing_count"),
            spark_round(
                spark_sum("financed_amount"), 2
            ).alias("total_financed_amount"),
            spark_round(
                avg("monthly_installment"), 2
            ).alias("average_monthly_installment"),
            spark_round(
                avg("interest_rate_monthly"), 4
            ).alias("average_interest_rate_monthly"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "segment",
            "region",
            "vehicle_type",
            "vehicle_brand",
            "vehicle_model",
            "financing_count",
            "total_financed_amount",
            "average_monthly_installment",
            "average_interest_rate_monthly",
        )
    )


def create_postgres_table(config: PipelineConfig) -> None:
    """Cria a tabela GOLD no PostgreSQL."""
    with psycopg2.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        database=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    ) as connection:

        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SUMMARY_TABLE} (
                    window_start TIMESTAMP,
                    window_end TIMESTAMP,
                    segment VARCHAR(100),
                    region VARCHAR(100),
                    vehicle_type VARCHAR(100),
                    vehicle_brand VARCHAR(100),
                    vehicle_model VARCHAR(100),
                    financing_count INTEGER,
                    total_financed_amount NUMERIC(18, 2),
                    average_monthly_installment NUMERIC(18, 2),
                    average_interest_rate_monthly NUMERIC(10, 4),

                    PRIMARY KEY (
                        window_start,
                        window_end,
                        segment,
                        region,
                        vehicle_type,
                        vehicle_brand,
                        vehicle_model
                    )
                )
                """
            )

        connection.commit()


def write_gold_batch(
    batch_df: DataFrame,
    batch_id: int,
    config: PipelineConfig,
    write_parquet: bool = True,
) -> None:
    """Grava cada microbatch em Parquet e PostgreSQL."""

    if write_parquet:
        (
            batch_df.write
            .mode("append")
            .format("parquet")
            .partitionBy(
                "segment",
                "region",
                "vehicle_type",
            )
            .save(str(config.gold_dir))
        )

    rows = [
        (
            row.window_start,
            row.window_end,
            row.segment,
            row.region,
            row.vehicle_type,
            row.vehicle_brand,
            row.vehicle_model,
            row.financing_count,
            row.total_financed_amount,
            row.average_monthly_installment,
            row.average_interest_rate_monthly,
        )
        for row in batch_df.toLocalIterator()
    ]

    if not rows:
        return

    with psycopg2.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        database=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    ) as connection:

        with connection.cursor() as cursor:

            cursor.executemany(
                f"""
                INSERT INTO {SUMMARY_TABLE} (
                    window_start,
                    window_end,
                    segment,
                    region,
                    vehicle_type,
                    vehicle_brand,
                    vehicle_model,
                    financing_count,
                    total_financed_amount,
                    average_monthly_installment,
                    average_interest_rate_monthly
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON CONFLICT (
                    window_start,
                    window_end,
                    segment,
                    region,
                    vehicle_type,
                    vehicle_brand,
                    vehicle_model
                )
                DO UPDATE SET
                    financing_count = EXCLUDED.financing_count,
                    total_financed_amount = EXCLUDED.total_financed_amount,
                    average_monthly_installment =
                        EXCLUDED.average_monthly_installment,
                    average_interest_rate_monthly =
                        EXCLUDED.average_interest_rate_monthly
                """,
                rows,
            )

        connection.commit()


def materialize_gold_snapshot(
    spark: SparkSession,
    config: PipelineConfig,
) -> None:
    """Fecha as janelas e grava o snapshot final."""

    silver_df = (
        spark.read
        .schema(FINANCING_SCHEMA)
        .parquet(config.silver_storage_path)
    )

    gold_df = aggregate_gold_batch(
        silver_df,
        config.window_duration,
    )

    # Mantem o Parquet como artefato analitico.
    (
        gold_df.write
        .mode("overwrite")
        .format("parquet")
        .partitionBy(
            "segment",
            "region",
            "vehicle_type",
        )
        .save(str(config.gold_dir))
    )

    # Garante que a tabela PostgreSQL exista.
    create_postgres_table(config)

    # Recria o snapshot no PostgreSQL.
    with psycopg2.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        database=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    ) as connection:

        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {SUMMARY_TABLE}")

        connection.commit()

    write_gold_batch(
        gold_df,
        -1,
        config,
        write_parquet=False,
    )


def start_gold_query(
    spark: SparkSession,
    config: PipelineConfig,
):
    """Lê SILVER do MinIO e grava GOLD em Parquet e PostgreSQL."""

    silver_stream = (
        spark.readStream
        .schema(FINANCING_SCHEMA)
        .parquet(config.silver_storage_path)
    )

    create_postgres_table(config)

    return (
        aggregate_gold(
            silver_stream,
            window_duration=config.window_duration,
            watermark_duration=config.watermark_duration,
        )
        .writeStream
        .outputMode("append")
        .foreachBatch(
            lambda batch, batch_id: write_gold_batch(
                batch,
                batch_id,
                config,
            )
        )
        .option(
            "checkpointLocation",
            str(config.gold_checkpoint_dir),
        )
        .trigger(availableNow=True)
        .start()
    )


def main() -> None:
    config = PipelineConfig.from_environment()

    spark = (
        SparkSession.builder
        .appName("vehicle-financing-gold")
        .getOrCreate()
    )

    try:
        query = start_gold_query(spark, config)
        query.awaitTermination(config.runtime_seconds)
        query.stop()

        # Fecha e materializa o snapshot final.
        materialize_gold_snapshot(spark, config)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()