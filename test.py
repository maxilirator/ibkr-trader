from __future__ import annotations

import json
import os
import sys
from urllib.error import HTTPError
from urllib.error import URLError
from urllib.request import Request
from urllib.request import urlopen


API_BASE_URL = os.getenv("IBKR_TRADER_API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")


def _request_json(method: str, path: str, payload: dict | None = None) -> dict:
    request = Request(
        url=f"{API_BASE_URL}{path}",
        method=method,
        headers={"content-type": "application/json"},
        data=(json.dumps(payload).encode("utf-8") if payload is not None else None),
    )
    with urlopen(request, timeout=30) as response:  # noqa: S310
        return json.loads(response.read().decode("utf-8"))


def main() -> int:
    print(f"Checking local API at {API_BASE_URL}", flush=True)

    try:
        health = _request_json("GET", "/healthz")
        print("Health OK")
        print(json.dumps(health, indent=2))

        probe = _request_json("POST", "/v1/ibkr/probe")
        print("Probe OK")
        print(json.dumps(probe, indent=2))

        shortability = _request_json(
            "POST",
            "/v1/market-data/shortability-snapshot",
            {
                "symbols": ["VOLV-B"],
                "only_shortable": False,
            },
        )
        print("Shortability canary (VOLV-B):")
        print(json.dumps(shortability, indent=2))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        print(f"HTTP ERROR {exc.code}: {detail}", file=sys.stderr)
        return 1
    except URLError as exc:
        print(f"ERROR: failed to reach local API at {API_BASE_URL}: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
