"""Schemas Spark do produto de dados de financiamento de veiculos."""

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType


FINANCING_SCHEMA = StructType(
    [
        StructField("financing_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("vehicle_type", StringType(), False),
        StructField("vehicle_id", StringType(), False),
        StructField("vehicle_brand", StringType(), False),
        StructField("vehicle_model", StringType(), False),
        StructField("vehicle_year", IntegerType(), False),
        StructField("segment", StringType(), False),
        StructField("region", StringType(), False),
        StructField("state", StringType(), False),
        StructField("city", StringType(), False),
        StructField("financed_amount", DoubleType(), False),
        StructField("down_payment", DoubleType(), False),
        StructField("installment_count", IntegerType(), False),
        StructField("monthly_installment", DoubleType(), False),
        StructField("interest_rate_monthly", DoubleType(), False),
        StructField("status", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("created_at", TimestampType(), False),
    ]
)

BRONZE_SCHEMA = StructType(
    FINANCING_SCHEMA.fields
    + [StructField("ingestion_time", TimestampType(), True)]
)

# Estas colunas sao gravadas como particoes e nao ficam no arquivo Parquet.
SILVER_PARQUET_SCHEMA = StructType(
    [field for field in FINANCING_SCHEMA.fields if field.name not in {"region", "vehicle_type"}]
)

VALID_VEHICLE_TYPES = ("CAR", "MOTORCYCLE")
VALID_REGIONS = ("NORTH", "NORTHEAST", "MIDWEST", "SOUTHEAST", "SOUTH")
VALID_STATUSES = (
    "SIMULATION",
    "PENDING_APPROVAL",
    "APPROVED",
    "ACTIVE",
    "PAID_OFF",
    "CANCELLED",
    "DEFAULTED",
)
