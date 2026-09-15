"""Valida as camadas bronze, silver, gold e o DW PostgreSQL."""

from __future__ import annotations

from pathlib import Path
import sqlite3

from pyspark.sql import SparkSession

from config import PipelineConfig
from gold import SUMMARY_TABLE


def count_json_records(directory: Path) -> int:
    """Conta registros JSON em arquivos produzidos pela camada bronze."""
    total = 0
    for file_path in directory.glob("*.json"):
        with file_path.open(encoding="utf-8") as file:
            total += sum(1 for line in file if line.strip())
    return total


def main() -> None:
    config = PipelineConfig.from_environment()
    spark = (
        SparkSession.builder
        .appName("validate-medallion-layers")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        bronze_count = count_json_records(config.bronze_dir)
        silver = spark.read.parquet(str(config.silver_dir))
        gold = spark.read.parquet(str(config.gold_dir))

        print("=== VALIDACAO DAS CAMADAS ===")
        print(f"BRONZE  | registros JSON: {bronze_count}")
        print(f"SILVER  | registros Parquet: {silver.count()}")
        print(f"SILVER  | colunas: {', '.join(silver.columns)}")
        print(f"GOLD    | registros Parquet: {gold.count()}")
        print(f"GOLD    | colunas: {', '.join(gold.columns)}")

        with sqlite3.connect(config.sqlite_path) as connection:
            table_exists = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (SUMMARY_TABLE,),
            ).fetchone()
            if not table_exists:
                raise RuntimeError(f"Tabela SQLite ausente: {SUMMARY_TABLE}")

            rows = connection.execute(
                f"""
                SELECT region, vehicle_type, segment,
                       SUM(financing_count) AS financing_count,
                       ROUND(SUM(total_financed_amount), 2) AS total_financed_amount
                FROM {SUMMARY_TABLE}
                GROUP BY region, vehicle_type, segment
                ORDER BY total_financed_amount DESC
                """
            ).fetchall()

        print(f"SQLITE  | tabela: {SUMMARY_TABLE}")
        print("region | vehicle_type | segment | financing_count | total_financed_amount")
        for row in rows:
            print(" | ".join(str(value) for value in row))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
