"""Configurações do pipeline e dos caminhos de entrada, saída e checkpoint."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


@dataclass(frozen=True)
class PipelineConfig:
    """Parâmetros do processamento, sobrescritos por variáveis de ambiente."""

    base_dir: Path = Path(__file__).resolve().parents[1]

    # ============================================================
    # PIPELINE
    # ============================================================

    rows_per_second: int = 5
    window_duration: str = "10 seconds"
    watermark_duration: str = "30 seconds"
    trigger_interval: str = "5 seconds"
    runtime_seconds: int = 30

    # ============================================================
    # MINIO
    # ============================================================

    # Comunicação entre containers Docker.
    # NÃO usar localhost/127.0.0.1 aqui.
    minio_endpoint: str = "http://minio:9000"
    minio_access_key: str = "minio"
    minio_secret_key: str = "minio123"
    minio_bucket: str = "stream-pipeline"

    # ============================================================
    # POSTGRESQL / WAREHOUSE
    # ============================================================

    # Dentro da rede Docker, o hostname é o nome do serviço:
    # postgres:5432
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "vehicle_financing"
    postgres_user: str = "dw_user"
    postgres_password: str = "dw_password"

    # ============================================================
    # DIRETÓRIOS LOCAIS
    # ============================================================

    @property
    def output_dir(self) -> Path:
        return self.base_dir / "data" / "output"

    @property
    def input_dir(self) -> Path:
        return self.base_dir / "data" / "input"

    @property
    def bronze_dir(self) -> Path:
        return self.base_dir / "data" / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.base_dir / "data" / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.base_dir / "data" / "gold"

    # ============================================================
    # SQLITE
    # ============================================================
    #
    # Mantido para compatibilidade com os testes e consultas locais.
    # O warehouse principal será o PostgreSQL.

    @property
    def sqlite_path(self) -> Path:
        return self.gold_dir / "vehicle_financing.sqlite"

    # ============================================================
    # CHECKPOINTS LOCAIS
    # ============================================================

    @property
    def checkpoint_dir(self) -> Path:
        return self.base_dir / "data" / "checkpoint"

    @property
    def bronze_checkpoint_dir(self) -> Path:
        return self.checkpoint_dir / "bronze"

    @property
    def silver_checkpoint_dir(self) -> Path:
        return self.checkpoint_dir / "silver"

    @property
    def gold_checkpoint_dir(self) -> Path:
        return self.checkpoint_dir / "gold"

    # ============================================================
    # MINIO / S3
    # ============================================================

    @property
    def bronze_storage_path(self) -> str:
        return f"s3a://{self.minio_bucket}/bronze"

    @property
    def silver_storage_path(self) -> str:
        return f"s3a://{self.minio_bucket}/silver"

    @property
    def gold_storage_path(self) -> str:
        return f"s3a://{self.minio_bucket}/gold"

    @property
    def bronze_storage_checkpoint(self) -> str:
        return f"s3a://{self.minio_bucket}/checkpoints/bronze"

    @property
    def silver_storage_checkpoint(self) -> str:
        return f"s3a://{self.minio_bucket}/checkpoints/silver"

    @property
    def gold_storage_checkpoint(self) -> str:
        return f"s3a://{self.minio_bucket}/checkpoints/gold"

    # ============================================================
    # POSTGRESQL / JDBC
    # ============================================================

    @property
    def postgres_jdbc_url(self) -> str:
        """Retorna a URL JDBC para conexão com o PostgreSQL."""

        return (
            f"jdbc:postgresql://"
            f"{self.postgres_host}:"
            f"{self.postgres_port}/"
            f"{self.postgres_db}"
        )

    @property
    def postgres_jdbc_properties(self) -> dict[str, str]:
        """Propriedades utilizadas pelo driver JDBC do PostgreSQL."""

        return {
            "user": self.postgres_user,
            "password": self.postgres_password,
            "driver": "org.postgresql.Driver",
        }

    # ============================================================
    # ENVIRONMENT
    # ============================================================

    @classmethod
    def from_environment(cls) -> "PipelineConfig":
        """Carrega parâmetros opcionais das variáveis de ambiente."""

        return cls(
            base_dir=Path(
                os.getenv(
                    "PIPELINE_BASE_DIR",
                    str(cls.base_dir),
                )
            ),

            rows_per_second=int(
                os.getenv(
                    "ROWS_PER_SECOND",
                    str(cls.rows_per_second),
                )
            ),

            window_duration=os.getenv(
                "WINDOW_DURATION",
                cls.window_duration,
            ),

            watermark_duration=os.getenv(
                "WATERMARK_DURATION",
                cls.watermark_duration,
            ),

            trigger_interval=os.getenv(
                "TRIGGER_INTERVAL",
                cls.trigger_interval,
            ),

            runtime_seconds=int(
                os.getenv(
                    "RUNTIME_SECONDS",
                    str(cls.runtime_seconds),
                )
            ),

            minio_endpoint=os.getenv(
                "MINIO_ENDPOINT",
                cls.minio_endpoint,
            ),

            minio_access_key=os.getenv(
                "MINIO_ACCESS_KEY",
                cls.minio_access_key,
            ),

            minio_secret_key=os.getenv(
                "MINIO_SECRET_KEY",
                cls.minio_secret_key,
            ),

            minio_bucket=os.getenv(
                "MINIO_BUCKET",
                cls.minio_bucket,
            ),

            postgres_host=os.getenv(
                "POSTGRES_HOST",
                cls.postgres_host,
            ),

            postgres_port=int(
                os.getenv(
                    "POSTGRES_PORT",
                    str(cls.postgres_port),
                )
            ),

            postgres_db=os.getenv(
                "POSTGRES_DB",
                cls.postgres_db,
            ),

            postgres_user=os.getenv(
                "POSTGRES_USER",
                cls.postgres_user,
            ),

            postgres_password=os.getenv(
                "POSTGRES_PASSWORD",
                cls.postgres_password,
            ),
        )