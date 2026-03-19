import gzip
import json
import os
import sys
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

from minio import Minio


URLHAUS_SRC = Path("/app/riskstream/services/ingestion/urlhaus/src")
if str(URLHAUS_SRC) not in sys.path:
    sys.path.insert(0, str(URLHAUS_SRC))

import feed_store  # noqa: E402


RAW_FEEDS_BUCKET = "raw-feeds"
ARCHIVES_BUCKET = "archives"


def _storage_client() -> Minio:
    return Minio(
        os.getenv("S3_ENDPOINT", "minio:9000"),
        access_key=os.getenv("S3_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("S3_SECRET_KEY", "minioadmin"),
        secure=os.getenv("S3_USE_SSL", "false").lower() in ("true", "1", "yes"),
    )


def _put_json_gzip(client: Minio, bucket: str, object_key: str, payload: dict) -> None:
    encoded = gzip.compress(json.dumps(payload, sort_keys=True).encode("utf-8"))
    client.put_object(
        bucket,
        object_key,
        BytesIO(encoded),
        len(encoded),
        content_type="application/gzip",
    )


def _remove_if_exists(client: Minio, bucket: str, object_key: str) -> None:
    objects = client.list_objects(bucket, prefix=object_key, recursive=True)
    if any(getattr(obj, "object_name", None) == object_key for obj in objects):
        client.remove_object(bucket, object_key)


def test_urlhaus_archive_lifecycle_moves_old_hot_objects_and_deletes_old_archives():
    client = _storage_client()
    old_checkpoint = "urlhaus/checkpoints/2026/01/01/000000Z.json.gz"
    old_delta = "urlhaus/deltas/2026/01/01/abc123.json.gz"
    latest_state = "urlhaus/state/latest.json.gz"
    stale_archive_delta = "urlhaus/deltas/2025/01/01/abc123.json.gz"

    for bucket, object_key in (
        (RAW_FEEDS_BUCKET, old_checkpoint),
        (RAW_FEEDS_BUCKET, old_delta),
        (RAW_FEEDS_BUCKET, latest_state),
        (ARCHIVES_BUCKET, stale_archive_delta),
    ):
        _remove_if_exists(client, bucket, object_key)

    try:
        _put_json_gzip(
            client,
            RAW_FEEDS_BUCKET,
            old_checkpoint,
            {"kind": "checkpoint", "raw_csv": "id,url\n1,https://bad.example\n"},
        )
        _put_json_gzip(
            client,
            RAW_FEEDS_BUCKET,
            old_delta,
            {"kind": "delta", "added": [{"id": "1"}], "updated": [], "removed": []},
        )
        _put_json_gzip(
            client,
            RAW_FEEDS_BUCKET,
            latest_state,
            {"content_hash": "statehash", "records_by_id": {"1": {"id": "1"}}},
        )
        _put_json_gzip(
            client,
            ARCHIVES_BUCKET,
            stale_archive_delta,
            {"kind": "delta", "added": [], "updated": [], "removed": []},
        )

        result = feed_store.run_archive_lifecycle(
            now=datetime(2026, 3, 20, tzinfo=timezone.utc),
            hot_retention_days=30,
            archive_retention_days=180,
        )

        assert result["archived_object_count"] >= 2
        assert result["pruned_hot_object_count"] >= 2
        assert result["deleted_archive_object_count"] >= 1

        raw_checkpoint_keys = {
            obj.object_name
            for obj in client.list_objects(
                RAW_FEEDS_BUCKET, prefix="urlhaus/checkpoints/", recursive=True
            )
        }
        raw_delta_keys = {
            obj.object_name
            for obj in client.list_objects(
                RAW_FEEDS_BUCKET, prefix="urlhaus/deltas/", recursive=True
            )
        }
        archive_checkpoint_keys = {
            obj.object_name
            for obj in client.list_objects(
                ARCHIVES_BUCKET, prefix="urlhaus/checkpoints/", recursive=True
            )
        }
        archive_delta_keys = {
            obj.object_name
            for obj in client.list_objects(
                ARCHIVES_BUCKET, prefix="urlhaus/deltas/", recursive=True
            )
        }
        raw_state_keys = {
            obj.object_name
            for obj in client.list_objects(
                RAW_FEEDS_BUCKET, prefix="urlhaus/state/", recursive=True
            )
        }

        assert old_checkpoint not in raw_checkpoint_keys
        assert old_delta not in raw_delta_keys
        assert old_checkpoint in archive_checkpoint_keys
        assert old_delta in archive_delta_keys
        assert stale_archive_delta not in archive_delta_keys
        assert latest_state in raw_state_keys
    finally:
        for bucket, object_key in (
            (RAW_FEEDS_BUCKET, old_checkpoint),
            (RAW_FEEDS_BUCKET, old_delta),
            (RAW_FEEDS_BUCKET, latest_state),
            (ARCHIVES_BUCKET, old_checkpoint),
            (ARCHIVES_BUCKET, old_delta),
            (ARCHIVES_BUCKET, stale_archive_delta),
        ):
            _remove_if_exists(client, bucket, object_key)
