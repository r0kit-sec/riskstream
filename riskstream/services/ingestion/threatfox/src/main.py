import json
import logging
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

from client import JsonFormatter, ThreatFoxClient


logger = logging.getLogger("threatfox.main")


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


class Handler(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self.client = ThreatFoxClient()
        super().__init__(*args, **kwargs)

    def do_GET(self):
        environment = os.getenv("ENVIRONMENT", "unknown")
        log_event(
            logging.INFO,
            "Handling HTTP request",
            service="threatfox-ingestion",
            event="request_started",
            path=self.path,
            method="GET",
            environment=environment,
        )

        if self.path == "/healthz":
            payload = {"status": "ok"}
            self.send_json_response(200, payload)
        elif self.path == "/recent":
            try:
                data = self.client.get_recent_threats(days=1)
                payload = {
                    "service": "threatfox-ingestion",
                    "threats_count": len(data.get("data", [])),
                    "data": data,
                }
                self.send_json_response(200, payload)
            except Exception as e:
                log_event(
                    logging.ERROR,
                    "ThreatFox recent request failed",
                    service="threatfox-ingestion",
                    event="request_failed",
                    path=self.path,
                    method="GET",
                    environment=environment,
                    status_code=500,
                    error=str(e),
                    days=1,
                )
                payload = {"error": str(e)}
                self.send_json_response(500, payload)
        else:
            payload = {
                "service": "threatfox-ingestion",
                "environment": environment,
            }
            self.send_json_response(200, payload)

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
            service="threatfox-ingestion",
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
    port = int(os.getenv("PORT", "8081"))
    environment = os.getenv("ENVIRONMENT", "unknown")

    # Enable remote debugging if DEBUG_MODE is set
    if os.getenv("DEBUG_MODE") == "true":
        import debugpy

        debug_port = int(os.getenv("DEBUG_PORT", "5678"))
        debugpy.listen(("0.0.0.0", debug_port))
        log_event(
            logging.INFO,
            "Debugger listening",
            service="threatfox-ingestion",
            event="debugger_listening",
            environment=environment,
            debug_port=debug_port,
        )
        log_event(
            logging.INFO,
            "Waiting for debugger to attach",
            service="threatfox-ingestion",
            event="debugger_waiting",
            environment=environment,
            debug_port=debug_port,
        )
        debugpy.wait_for_client()
        log_event(
            logging.INFO,
            "Debugger attached",
            service="threatfox-ingestion",
            event="debugger_attached",
            environment=environment,
            debug_port=debug_port,
        )

    server = HTTPServer(("0.0.0.0", port), Handler)
    log_event(
        logging.INFO,
        "ThreatFox ingestion service listening",
        service="threatfox-ingestion",
        event="service_started",
        environment=environment,
        port=port,
    )
    server.serve_forever()


if __name__ == "__main__":
    run()
