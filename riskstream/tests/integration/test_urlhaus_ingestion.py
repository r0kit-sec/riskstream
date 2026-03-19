import json
import os
from urllib.request import Request, urlopen


def _base_url() -> str:
    return os.getenv("URLHAUS_BASE_URL", "http://urlhaus-ingestion").rstrip("/")


def test_urlhaus_recent_endpoint_returns_live_data():
    with urlopen(f"{_base_url()}/recent", timeout=30) as response:
        assert response.status == 200
        assert response.headers.get("Content-Type") == "application/json"

        payload = json.loads(response.read().decode("utf-8"))

    assert payload["service"] == "urlhaus-ingestion"
    assert payload["content_hash"]
    assert "urls_count" in payload
    assert "data" in payload

    data = payload["data"]
    assert isinstance(data, dict)
    assert data["content_hash"] == payload["content_hash"]

    urls = data.get("urls")
    if urls is not None:
        assert isinstance(urls, list)
        assert payload["urls_count"] == len(urls)


def test_urlhaus_ingest_recent_endpoint_persists_live_data():
    request = Request(f"{_base_url()}/ingestion/recent", method="POST")

    with urlopen(request, timeout=30) as response:
        assert response.status == 200
        assert response.headers.get("Content-Type") == "application/json"

        payload = json.loads(response.read().decode("utf-8"))

    assert payload["service"] == "urlhaus-ingestion"
    assert payload["feed"] == "recent"
    assert "changed" in payload
    assert "checkpoint_written" in payload
    assert "delta_written" in payload
    assert "delta_counts" in payload
    assert payload["checked_at"]
    assert "urls_count" in payload
    assert set(payload["delta_counts"]) == {"added", "updated", "removed"}
    if payload["checkpoint_written"]:
        assert payload["checkpoint_object_key"].startswith("urlhaus/checkpoints/")
    if payload["delta_written"]:
        assert payload["changed"] is True
        assert payload["delta_object_key"].startswith("urlhaus/deltas/")
        assert payload["state_object_key"] == "urlhaus/state/latest.json.gz"
