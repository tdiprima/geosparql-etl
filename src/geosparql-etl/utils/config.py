"""Centralized configuration management for ETL operations."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class MongoConfig:
    """MongoDB connection configuration.

    Attributes:
        uri: MongoDB connection URI
        db_name: Database name
        collection_name: Collection name (optional)
    """

    uri: str = "mongodb://localhost:27018/"
    db_name: str = "camic"
    collection_name: Optional[str] = None

    @classmethod
    def from_env(cls, prefix: str = "MONGO") -> "MongoConfig":
        """Load configuration from environment variables.

        Args:
            prefix: Environment variable prefix (default: MONGO)

        Returns:
            MongoConfig: Configuration loaded from environment

        Environment variables:
            MONGO_URI: MongoDB connection URI
            MONGO_DB_NAME: Database name
            MONGO_COLLECTION: Collection name
        """
        return cls(
            uri=os.getenv(f"{prefix}_URI", cls.uri),
            db_name=os.getenv(f"{prefix}_DB_NAME", cls.db_name),
            collection_name=os.getenv(f"{prefix}_COLLECTION"),
        )


@dataclass
class ETLConfig:
    """ETL operation configuration.

    Attributes:
        batch_size: Number of documents to process in each batch
        output_dir: Directory for output files
        checkpoint_file: File to store processing checkpoint state
        log_file: Log file path
        max_workers: Maximum number of parallel workers
    """

    batch_size: int = 5000
    output_dir: str = "ttl_output"
    checkpoint_file: str = "etl_checkpoint.json"
    log_file: str = "etl.log"
    max_workers: int = 4

    @classmethod
    def from_env(cls, prefix: str = "ETL") -> "ETLConfig":
        """Load configuration from environment variables.

        Args:
            prefix: Environment variable prefix (default: ETL)

        Returns:
            ETLConfig: Configuration loaded from environment

        Environment variables:
            ETL_BATCH_SIZE: Batch size for processing
            ETL_OUTPUT_DIR: Output directory path
            ETL_CHECKPOINT_FILE: Checkpoint file path
            ETL_LOG_FILE: Log file path
            ETL_MAX_WORKERS: Maximum number of workers
        """
        return cls(
            batch_size=int(os.getenv(f"{prefix}_BATCH_SIZE", cls.batch_size)),
            output_dir=os.getenv(f"{prefix}_OUTPUT_DIR", cls.output_dir),
            checkpoint_file=os.getenv(f"{prefix}_CHECKPOINT_FILE", cls.checkpoint_file),
            log_file=os.getenv(f"{prefix}_LOG_FILE", cls.log_file),
            max_workers=int(os.getenv(f"{prefix}_MAX_WORKERS", cls.max_workers)),
        )

    def ensure_output_dir(self) -> Path:
        """Create output directory if it doesn't exist.

        Returns:
            Path: Path object for output directory
        """
        output_path = Path(self.output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        return output_path


@dataclass
class AppConfig:
    """Complete application configuration combining all settings.

    Attributes:
        mongo: MongoDB configuration
        etl: ETL configuration
    """

    mongo: MongoConfig = field(default_factory=MongoConfig)
    etl: ETLConfig = field(default_factory=ETLConfig)

    @classmethod
    def from_env(cls) -> "AppConfig":
        """Load complete configuration from environment variables.

        Returns:
            AppConfig: Complete application configuration

        Example:
            from utils.config import AppConfig

            config = AppConfig.from_env()
            print(config.mongo.uri)
            print(config.etl.batch_size)
        """
        return cls(mongo=MongoConfig.from_env(), etl=ETLConfig.from_env())

    @classmethod
    def from_dict(cls, config_dict: dict) -> "AppConfig":
        """Load configuration from dictionary.

        Args:
            config_dict: Dictionary containing configuration

        Returns:
            AppConfig: Application configuration

        Example:
            config_dict = {
                "mongo": {
                    "uri": "mongodb://localhost:27018/",
                    "db_name": "camic"
                },
                "etl": {
                    "batch_size": 1000,
                    "output_dir": "output"
                }
            }
            config = AppConfig.from_dict(config_dict)
        """
        mongo_config = MongoConfig(**config_dict.get("mongo", {}))
        etl_config = ETLConfig(**config_dict.get("etl", {}))
        return cls(mongo=mongo_config, etl=etl_config)


def load_config_from_file(config_path: str) -> AppConfig:
    """Load configuration from JSON or YAML file.

    Args:
        config_path: Path to configuration file

    Returns:
        AppConfig: Loaded configuration

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If file format is unsupported

    Example:
        config = load_config_from_file("config.json")
    """
    import json

    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_file, "r") as f:
        if config_path.endswith(".json"):
            config_dict = json.load(f)
        elif config_path.endswith((".yaml", ".yml")):
            try:
                import yaml

                config_dict = yaml.safe_load(f)
            except ImportError:
                raise ImportError("PyYAML is required for YAML config files")
        else:
            raise ValueError(f"Unsupported config file format: {config_path}")

    return AppConfig.from_dict(config_dict)
