import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from minio.error import S3Error

from pipeline.config import DEFAULT_SOURCE_DIR
from pipeline.minio_utils import ensure_bucket, get_minio_client


def load_json_data_to_minio(
    bucket_name="rawjson", source_path=None, execution_date=None
):
    """Lands source JSON files in MinIO and writes a load manifest.

    Args:
        bucket_name: Destination raw bucket.
        source_path: Local directory containing source JSON files.
        execution_date: Logical load date in ``YYYY-MM-DD`` format.

    Returns:
        None. Loaded file metadata is written to the MinIO manifest object.

    Raises:
        FileNotFoundError: The source path is not a directory.
        RuntimeError: One or more files failed to upload.
    """
    source_dir = Path(source_path) if source_path else DEFAULT_SOURCE_DIR
    if not source_dir.is_dir():
        raise FileNotFoundError(
            f"'{source_dir}' is not a valid directory of JSON files."
        )

    client = get_minio_client()

    execution_date = execution_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    bucket_existed = client.bucket_exists(bucket_name)
    ensure_bucket(client, bucket_name)
    if not bucket_existed:
        print(f"Created bucket: '{bucket_name}'")

    partition_prefix = f"ingestion_date={execution_date}"
    loaded, failed = [], []

    for root, _, files in os.walk(source_dir):
        for file in files:
            if file.startswith(".") or not file.endswith(".json"):
                continue

            full_file_path = os.path.join(root, file)
            object_name = f"{partition_prefix}/{file}"

            try:
                client.fput_object(
                    bucket_name=bucket_name,
                    object_name=object_name,
                    file_path=full_file_path,
                    content_type="application/json",
                )
                loaded.append(
                    {
                        "file": file,
                        "size_bytes": os.path.getsize(full_file_path),
                        "object_path": f"s3://{bucket_name}/{object_name}",
                    }
                )
                print(f"Uploaded: {file} -> s3://{bucket_name}/{object_name}")
            except (OSError, S3Error) as e:
                failed.append({"file": file, "error": str(e)})
                print(f"FAILED: {file} -> {e}")

    # Store load metadata with the partition for downstream validation.
    manifest = {
        "execution_date": execution_date,
        "run_completed_at": datetime.now(timezone.utc).isoformat(),
        "bucket": bucket_name,
        "files_loaded": loaded,
        "files_failed": failed,
        "status": "success" if not failed else "partial_failure",
    }
    manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")
    manifest_object = f"{partition_prefix}/_manifest.json"

    client.put_object(
        bucket_name=bucket_name,
        object_name=manifest_object,
        data=io.BytesIO(manifest_bytes),
        length=len(manifest_bytes),
        content_type="application/json",
    )
    print(
        f"Manifest written: s3://{bucket_name}/{manifest_object} ({manifest['status']})"
    )

    if failed:
        raise RuntimeError(
            f"{len(failed)} file(s) failed to load: {[f['file'] for f in failed]}"
        )
