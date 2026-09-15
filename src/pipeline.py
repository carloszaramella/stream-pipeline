"""Entrada principal do pipeline Spark Structured Streaming."""

from __future__ import annotations

from pyspark.sql import SparkSession

from config import PipelineConfig
from transformations import build_events_stream, summarize_events, validate_events


def create_spark_session() -> SparkSession:
    """Cria uma sessao local; em cluster, o master vem do spark-submit."""
    return (
        SparkSession.builder
        .appName("stream-pipeline-transactions")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def run_pipeline(config: PipelineConfig) -> None:
    """Executa ingestao, validacao, agregacao e escrita em Parquet."""
    config.output_dir.mkdir(parents=True, exist_ok=True)
    config.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        bronze_stream = (
            spark.readStream
            .format("rate")
            .option("rowsPerSecond", config.rows_per_second)
            .load()
        )
        events = build_events_stream(bronze_stream)
        valid_events, invalid_events = validate_events(events)
        summary = summarize_events(
            valid_events,
            window_duration=config.window_duration,
            watermark_duration=config.watermark_duration,
        )

        valid_query = (
            valid_events.writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", str(config.output_dir / "valid_transactions"))
            .option("checkpointLocation", str(config.checkpoint_dir / "valid"))
            .partitionBy("category")
            .trigger(processingTime=config.trigger_interval)
            .start()
        )
        invalid_query = (
            invalid_events.writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", str(config.output_dir / "invalid_transactions"))
            .option("checkpointLocation", str(config.checkpoint_dir / "invalid"))
            .trigger(processingTime=config.trigger_interval)
            .start()
        )
        summary_query = (
            summary.writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", str(config.output_dir / "transaction_summary"))
            .option("checkpointLocation", str(config.checkpoint_dir / "summary"))
            .trigger(processingTime=config.trigger_interval)
            .start()
        )

        queries = (valid_query, invalid_query, summary_query)
        try:
            valid_query.awaitTermination(config.runtime_seconds)
        finally:
            for query in queries:
                query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline(PipelineConfig.from_environment())
