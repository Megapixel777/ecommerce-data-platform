import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class PipelineConfig:
    """Configuration for the Olist data pipeline."""

    input_path: Path
    output_dir: Path
    environment: str = "dev"


def load_config() -> PipelineConfig:
    """Load pipeline configuration from environment variables."""

    input_path = os.getenv("OLIST_DATA_PATH")
    output_dir = os.getenv("OUTPUT_DIR", "data/processed")
    environment = os.getenv("ENVIRONMENT", "dev")

    if not input_path:
        raise ValueError("OLIST_DATA_PATH environment variable is required")

    return PipelineConfig(
        input_path=Path(input_path),
        output_dir=Path(output_dir),
        environment=environment,
    )