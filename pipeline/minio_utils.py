"""MinIO client helpers shared by landing, raw conversion, and API ingest."""

import minio

from pipeline.config import minio_settings_from_env


def get_minio_client() -> minio.Minio:
    settings = minio_settings_from_env()
    return minio.Minio(
        endpoint=settings.endpoint,
        access_key=settings.access_key,
        secret_key=settings.secret_key,
        secure=settings.secure,
    )


def ensure_bucket(client: minio.Minio, bucket_name: str) -> None:
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
