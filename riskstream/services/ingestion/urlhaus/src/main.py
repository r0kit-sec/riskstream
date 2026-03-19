import json
import logging
import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[5]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from client import JsonFormatter, UrlhausClient
from feed_store import ingest_recent_feed

logger = logging.getLogger("urlhaus.main")


def configure_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())

    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(level)


def log_event(level: int, message: str, **fields) -> None:
    logger.log(level, message, extra={"fields": fields})


def ingest_recent_snapshot(recent_data: dict, storage=None) -> dict:
    return ingest_recent_feed(recent_data, storage=storage)


class Handler(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self.client = UrlhausClient()
        super().__init__(*args, **kwargs)

    def do_GET(self):
        environment = os.getenv("ENVIRONMENT", "unknown")
        self.log_request_started("GET", environment)

        if self.path == "/healthz":
            self.send_json_response(200, {"status": "ok"})
        elif self.path == "/recent":
            try:
                data = self.client.get_recent_urls()
                payload = {
                    "service": "urlhaus-ingestion",
                    "content_hash": data["content_hash"],
                    "urls_count": len(data.get("urls", [])),
                    "data": data,
                }
                self.send_json_response(200, payload)
            except Exception as e:
                log_event(
                    logging.ERROR,
                    "URLhaus recent request failed",
                    service="urlhaus-ingestion",
                    event="request_failed",
                    path=self.path,
                    method="GET",
                    environment=environment,
                    status_code=500,
                    error=str(e),
                )
                self.send_json_response(500, {"error": str(e)})
        else:
            payload = {
                "service": "urlhaus-ingestion",
                "environment": environment,
            }
            self.send_json_response(200, payload)

    def do_POST(self):
        environment = os.getenv("ENVIRONMENT", "unknown")
        self.log_request_started("POST", environment)

        if self.path == "/ingestion/recent":
            try:
                data = self.client.get_recent_urls()
                ingestion = ingest_recent_snapshot(data)
                log_event(
                    logging.INFO,
                    "URLhaus recent ingestion evaluated",
                    service="urlhaus-ingestion",
                    event="recent_ingestion_evaluated",
                    path=self.path,
                    method="POST",
                    environment=environment,
                    changed=ingestion["changed"],
                    checkpoint_written=ingestion["checkpoint_written"],
                    delta_written=ingestion["delta_written"],
                    content_hash=ingestion["content_hash"],
                    urls_count=ingestion["urls_count"],
                )
                payload = {
                    "service": "urlhaus-ingestion",
                    "feed": "recent",
                    **ingestion,
                }
                self.send_json_response(200, payload)
            except Exception as e:
                log_event(
                    logging.ERROR,
                    "URLhaus recent ingestion failed",
                    service="urlhaus-ingestion",
                    event="request_failed",
                    path=self.path,
                    method="POST",
                    environment=environment,
                    status_code=500,
                    error=str(e),
                )
                self.send_json_response(500, {"error": str(e)})
        else:
            self.send_json_response(404, {"error": "not found"})

    def log_request_started(self, method, environment):
        log_event(
            logging.INFO,
            "Handling HTTP request",
            service="urlhaus-ingestion",
            event="request_started",
            path=self.path,
            method=method,
            environment=environment,
        )

    def send_json_response(self, status_code, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        log_event(
            logging.INFO,
            "HTTP request completed",
            service="urlhaus-ingestion",
            event="request_completed",
            path=self.path,
            method=self.command,
            environment=os.getenv("ENVIRONMENT", "unknown"),
            status_code=status_code,
        )

    def log_message(self, format, *args):
        return


def run() -> None:
    configure_logging()
    port = int(os.getenv("PORT", "8083"))
    environment = os.getenv("ENVIRONMENT", "unknown")

    server = HTTPServer(("0.0.0.0", port), Handler)
    log_event(
        logging.INFO,
        "URLhaus ingestion service listening",
        service="urlhaus-ingestion",
        event="service_started",
        environment=environment,
        port=port,
    )
    server.serve_forever()


if __name__ == "__main__":
    run()
