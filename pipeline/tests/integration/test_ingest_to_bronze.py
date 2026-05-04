import os, pytest
from datetime import date

@pytest.mark.skipif(os.environ.get("ENV") != "TEST", reason="Requires TEST env")
@pytest.mark.asyncio
async def test_ingest_dedup_idempotent():
    """Two runs with identical content produce exactly one file_manifest row."""
    import asyncpg
    from unittest.mock import patch
    import moto, boto3

    with moto.mock_aws():
        s3 = boto3.client("s3", region_name="ap-southeast-1")
        s3.create_bucket(Bucket="ph-lakehouse-test",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-1"})

        db_url = os.environ.get("DATABASE_URL",
            "postgresql+asyncpg://lakehouse:lakehouse@localhost:5432/lakehouse_test")

        from src.ingest.landing import ingest_source
        with patch("src.utils.config.settings") as m:
            m.S3_BUCKET = "ph-lakehouse-test"
            m.S3_ENDPOINT = ""
            m.AWS_ACCESS_KEY_ID = "test"
            m.AWS_SECRET_ACCESS_KEY = "test"
            m.AWS_DEFAULT_REGION = "ap-southeast-1"
            m.DATABASE_URL = db_url

            path1 = await ingest_source("psa", date(2024, 1, 1))
            path2 = await ingest_source("psa", date(2024, 1, 1))

        assert path1 == path2
        conn = await asyncpg.connect(db_url.replace("postgresql+asyncpg://", "postgresql://"))
        try:
            count = await conn.fetchval("SELECT COUNT(*) FROM file_manifest WHERE source='psa'")
            assert count == 1
        finally:
            await conn.close()
