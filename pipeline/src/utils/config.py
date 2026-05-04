"""
Application configuration — Wave 1 artifact.
All environment variables are loaded once at import time via pydantic-settings.
"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Storage
    S3_ENDPOINT: str = ""           # empty → real AWS S3; set for MinIO
    S3_BUCKET: str = "ph-lakehouse"
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_DEFAULT_REGION: str = "ap-southeast-1"

    # Database
    DATABASE_URL: str               # asyncpg DSN

    # Prefect
    PREFECT_API_URL: str

    # Environment
    ENV: str = "LOCAL"              # LOCAL | PROD

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
