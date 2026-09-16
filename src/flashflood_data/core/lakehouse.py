"""Validated settings for MinIO and the Polaris Iceberg catalog."""

from pathlib import Path
from urllib.parse import urlsplit

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class LakehouseSettings(BaseSettings):
    """Runtime lakehouse endpoints and credentials loaded from the environment."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore", populate_by_name=True)

    minio_endpoint: str = "http://127.0.0.1:9000"
    minio_region: str = "us-east-1"
    raw_bucket: str = "raw"
    minio_access_key: SecretStr = Field(validation_alias="MINIO_ROOT_USER")
    minio_secret_key: SecretStr = Field(validation_alias="MINIO_ROOT_PASSWORD")
    polaris_uri: str = "http://127.0.0.1:8181/api/catalog"
    polaris_catalog: str = "flood_lakehouse"
    polaris_client_id: SecretStr = Field(validation_alias="POLARIS_CLIENT_ID")
    polaris_client_secret: SecretStr = Field(validation_alias="POLARIS_CLIENT_SECRET")
    project_root: Path = Field(default_factory=Path.cwd, validation_alias="FLASHFLOOD_PROJECT_ROOT")
    staging_root: Path = Field(
        default=Path("dataset/lakehouse/staging"),
        validation_alias="FLASHFLOOD_STAGING_ROOT",
    )

    @field_validator("minio_endpoint", "polaris_uri")
    @classmethod
    def endpoint_is_http(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("endpoint must be an absolute HTTP(S) URL")
        return value.rstrip("/")

    @field_validator("raw_bucket", "polaris_catalog")
    @classmethod
    def identifier_has_no_path(cls, value: str) -> str:
        if not value or "/" in value or value in {".", ".."}:
            raise ValueError("bucket and catalog names must be single non-empty segments")
        return value

    @field_validator("project_root", "staging_root")
    @classmethod
    def root_is_absolute(cls, value: Path) -> Path:
        return value.expanduser().resolve()
