import json
import os
from urllib.request import urlopen


def _base_url() -> str:
    return os.getenv("THREATFOX_BASE_URL", "http://threatfox-ingestion").rstrip("/")


def test_threatfox_recent_endpoint_returns_live_data():
    with urlopen(f"{_base_url()}/recent", timeout=30) as response:
        assert response.status == 200
        assert response.headers.get("Content-Type") == "application/json"

        payload = json.loads(response.read().decode("utf-8"))

    assert payload["service"] == "threatfox-ingestion"
    assert "threats_count" in payload
    assert "data" in payload

    data = payload["data"]
    assert isinstance(data, dict)

    threats = data.get("data")
    if threats is not None:
        assert isinstance(threats, list)
        assert payload["threats_count"] == len(threats)
