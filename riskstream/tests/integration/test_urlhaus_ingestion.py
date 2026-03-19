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
    assert "snapshot_written" in payload
    assert payload["checked_at"]
    assert "urls_count" in payload
    if payload["snapshot_written"]:
        assert payload["changed"] is True
        assert payload["bucket"] == "raw-feeds"
        assert payload["object_key"].startswith("urlhaus/recent/")
        assert payload["fetched_at"]
    else:
        assert payload["changed"] is False
        assert payload["last_object_key"].startswith("urlhaus/recent/")
