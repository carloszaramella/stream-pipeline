
"""Transformacoes reutilizaveis do stream pipeline."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    concat,
    count,
    expr,
    lit,
    pmod,
    round as spark_round,
    sum as spark_sum,
    when,
    window,
)


VALID_VEHICLE_TYPES = ("CAR", "MOTORCYCLE")

# Regioes utilizadas pelos eventos sinteticos do pipeline.
# Tambem aceitamos siglas de estados para suportar dados de entrada
# como os utilizados nos testes.
VALID_REGIONS = (
    "NORTH",
    "NORTHEAST",
    "MIDWEST",
    "SOUTHEAST",
    "SOUTH",
    "SP",
    "RJ",
    "MG",
    "ES",
    "PR",
    "SC",
    "RS",
    "BA",
    "PE",
    "CE",
    "MA",
    "PI",
    "RN",
    "PB",
    "AL",
    "SE",
    "PA",
    "AM",
    "RO",
    "AC",
    "RR",
    "AP",
    "TO",
    "MT",
    "MS",
    "GO",
    "DF",
)


def build_events_stream(bronze_stream: DataFrame) -> DataFrame:
    """Converte a fonte rate em eventos sinteticos de financiamento."""

    return (
        bronze_stream
        .withColumn(
            "financing_id",
            concat(
                lit("FIN-"),
                expr("lpad(cast(value as string), 10, '0')"),
            ),
        )
        .withColumn(
            "customer_id",
            concat(
                lit("CUS-"),
                expr(
                    "lpad(cast(value % 100000000 as string), 8, '0')"
                ),
            ),
        )
        .withColumn(
            "vehicle_type",
            when(
                pmod(col("value"), lit(2)) == 0,
                lit("CAR"),
            ).otherwise(
                lit("MOTORCYCLE")
            ),
        )
        .withColumn(
            "vehicle_brand",
            when(
                col("vehicle_type") == "CAR",
                lit("Toyota"),
            ).otherwise(
                lit("Honda")
            ),
        )
        .withColumn(
            "vehicle_model",
            when(
                col("vehicle_type") == "CAR",
                lit("Corolla XEi"),
            ).otherwise(
                lit("CG 160 Titan")
            ),
        )
        .withColumn(
            "vehicle_year",
            (
                lit(2024)
                + pmod(col("value"), lit(3))
            ).cast("int"),
        )
        .withColumn(
            "segment",
            when(
                pmod(col("value"), lit(3)) == 0,
                lit("premium"),
            )
            .when(
                pmod(col("value"), lit(3)) == 1,
                lit("standard"),
            )
            .otherwise(
                lit("mass_market")
            ),
        )
        .withColumn(
            "region",
            expr(
                "array("
                "'NORTH', "
                "'NORTHEAST', "
                "'MIDWEST', "
                "'SOUTHEAST', "
                "'SOUTH'"
                ")[cast(pmod(value, 5) as int)]"
            ),
        )
        .withColumn(
            "financed_amount",
            expr(
                "cast(15000 + pmod(value, 120000) as double)"
            ),
        )
        .withColumn(
            "down_payment",
            expr(
                "cast(3000 + pmod(value, 20000) as double)"
            ),
        )
        .withColumn(
            "installment_count",
            expr(
                "cast(24 + pmod(value, 37) as int)"
            ),
        )
        .withColumn(
            "monthly_installment",
            expr(
                "cast(500 + pmod(value, 3000) as double)"
            ),
        )
        .withColumn(
            "interest_rate_monthly",
            expr(
                "cast("
                "1.2 + (pmod(value, 100) / 100.0)"
                " as double)"
            ),
        )
        .withColumn("status", lit("ACTIVE"))
        .withColumn("currency", lit("BRL"))
        .withColumn("created_at", col("timestamp"))
        .withColumn("event_time", col("timestamp"))
        .select(
            "financing_id",
            "customer_id",
            "vehicle_type",
            "vehicle_brand",
            "vehicle_model",
            "vehicle_year",
            "segment",
            "region",
            "financed_amount",
            "down_payment",
            "installment_count",
            "monthly_installment",
            "interest_rate_monthly",
            "status",
            "currency",
            "created_at",
            "event_time",
        )
    )


def validate_events(
    events: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Separa eventos validos e invalidos com o motivo da invalidacao."""

    reason = (
        when(
            col("financing_id").isNull(),
            lit("financing_id ausente"),
        )
        .when(
            ~col("vehicle_type").isin(*VALID_VEHICLE_TYPES),
            lit("vehicle_type invalido"),
        )
        .when(
            ~col("region").isin(*VALID_REGIONS),
            lit("region invalida"),
        )
        .when(
            col("financed_amount").isNull(),
            lit("financed_amount ausente"),
        )
        .when(
            col("financed_amount") <= 0,
            lit("financed_amount deve ser positivo"),
        )
        .when(
            col("monthly_installment") <= 0,
            lit("monthly_installment deve ser positivo"),
        )
    )

    classified = events.withColumn(
        "validation_reason",
        reason,
    )

    valid = (
        classified
        .filter(col("validation_reason").isNull())
        .drop("validation_reason")
    )

    invalid = classified.filter(
        col("validation_reason").isNotNull()
    )

    return valid, invalid