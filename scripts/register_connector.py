"""Registers connector on a local Kafka Connect REST API (no third-party deps)."""
import json
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ENDPOINT = "http://localhost:8083/connectors"
CONNECTOR = Path(__file__).resolve().parents[1] / "infra" / "debezium" / "connector.json"


def main():
    config = json.loads(CONNECTOR.read_text())
    for attempt in range(40):
        try:
            request = Request(ENDPOINT, data=json.dumps(config).encode(),
                              headers={"Content-Type": "application/json"}, method="POST")
            with urlopen(request, timeout=3) as response:
                print(response.read().decode()); return
        except HTTPError as exc:
            if exc.code == 409 and b"already exists" in exc.read().lower():
                print("Connector already registered; using existing definition."); return
            raise
        except (ConnectionError, URLError, TimeoutError):
            if attempt == 39:
                raise
            time.sleep(2)


if __name__ == "__main__":
    main()
