"""Settings for every command in the project, read in one place.

Each value comes from the first source that has it:
    1. a keyword argument, e.g. Settings(landing_dir=...) in a test
    2. an environment variable with the field's name, in any case (LANDING_DIR or landing_dir)
    3. the .env file in the repo root
    4. the default below

Settings are built once, in a command's main(), and passed to the code that needs them.
Nothing here runs on import. Relative paths are taken from the repo root, so a command gives
the same result whichever folder it is started from.

Credentials are not settings. Google's libraries find them through Application Default
Credentials: your gcloud login locally, workload identity in CI, the attached service account
in production.

    uv run python -m ecom_pipeline.config      # print the values the commands would use
"""
from pathlib import Path
from typing import Literal

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# <repo>/src/ecom_pipeline/config.py -> parents[2] is the repo root
REPO_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    """Paths and Google Cloud names used by the snapshot, the runner and the loader."""

    model_config = SettingsConfigDict(
        env_file=REPO_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",  # other variables (e.g. ones only the notebook reads) are allowed
    )

    # Which environment this is. Used in logs and labels only; code never branches on it.
    app_env: Literal["dev", "ci", "prod"] = "dev"

    # Google Cloud. gcp_project is optional so that commands without BigQuery (the runner)
    # work without it; code that needs it calls require_gcp_project().
    gcp_project: str | None = None
    gcp_location: str = "US"
    source_dataset: str = "bigquery-public-data.thelook_ecommerce"
    raw_dataset: str = "ecommerce_raw"

    # Local files
    duckdb_path: Path = Path("data/snapshot.duckdb")
    landing_dir: Path = Path("data/landing")
    batch_log_path: Path = Path("data/batch_log.duckdb")

    @field_validator("duckdb_path", "landing_dir", "batch_log_path", "local_warehouse_path", mode="after")
    @classmethod
    def _from_repo_root(cls, path: Path) -> Path:
        """Resolve a relative path against the repo root instead of the current folder."""
        return path if path.is_absolute() else REPO_ROOT / path

    def require_gcp_project(self) -> str:
        """Return gcp_project, or raise a clear error for commands that need BigQuery."""
        if not self.gcp_project:
            raise ValueError("GCP_PROJECT is not set. Add it to .env or the environment.")
        return self.gcp_project


if __name__ == "__main__":
    for name, value in Settings().model_dump().items():
        print(f"{name:16} {value}")
