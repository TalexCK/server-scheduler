#!/usr/bin/env python3
"""Read-only catalog diagnostics; run on the Scheduler host with Python 3."""

import argparse
import base64
import hashlib
import json
from pathlib import Path
import urllib.error
import urllib.request
import zipfile


def decode(value):
    return base64.b64decode(value + "=" * (-len(value) % 4)).decode("utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--game", default="survival")
    parser.add_argument("--jar", type=Path, help="Optional Lobby Paper Bridge jar on disk")
    args = parser.parse_args()
    config_path = args.config.absolute()
    config = json.loads(config_path.read_text(encoding="utf-8"))
    token = config["bridge"]["token"]

    # Never print the configuration, credentials, or arbitrary response bodies.
    servers_path = Path(config["paths"]["servers"])
    if not servers_path.is_absolute():
        servers_path = config_path.parent / servers_path
    external_path = servers_path / "external-servers.json"
    print("Config:", config_path)
    print("External file:", external_path)
    if external_path.is_file():
        external = json.loads(external_path.read_text(encoding="utf-8"))
        if not isinstance(external, dict):
            raise ValueError("external-servers.json must contain an object")
        matching = [key for key, value in external.items()
                    if isinstance(value, dict)
                    and isinstance(value.get("gamevoting"), dict)
                    and value["gamevoting"].get("id") == args.game]
        print("Configured external entries:", len(external))
        print("Requested game in file:", bool(matching))
    else:
        print("External file: MISSING")

    if args.jar:
        content = args.jar.read_bytes()
        with zipfile.ZipFile(args.jar) as archive:
            client = archive.read("com/schedulerbridge/common/BridgeHttpClient.class")
        print("Bridge jar SHA256:", hashlib.sha256(content).hexdigest())
        print("Bridge jar requests external catalog:", b"include_external=true" in client)
        print("Jar inspection describes the file on disk, not the running JVM.")

    url = "http://127.0.0.1:{}/bridge/v1/games?include_voting=true&include_external=true".format(
        int(config["api"]["port"]))
    request = urllib.request.Request(url, headers={"Authorization": "Bearer " + token})
    # The probe stays on loopback, including when HTTP proxy environment variables exist.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        with opener.open(request, timeout=10) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        print("Catalog HTTP status:", error.code)
        print("Check Scheduler logs at this time for the configuration error; no response body printed.")
        return 1
    except urllib.error.URLError:
        print("Cannot connect to the Scheduler API on the configured loopback port.")
        return 1
    rows = [line.split("\t") for line in body.splitlines() if line.strip()]
    print("Catalog HTTP status: 200")
    print("Catalog entries:", len(rows))
    print("Column counts:", sorted(set(len(row) for row in rows)))
    if any(len(row) not in (12, 17, 18, 20) for row in rows):
        print("Unexpected catalog format.")
        return 1
    matches = [row for row in rows if decode(row[1]) == args.game]
    print("Requested game in API:", bool(matches))
    for row in matches:
        print("Game/server:", decode(row[1]), decode(row[2]))
        print("Solo:", row[12] if len(row) >= 17 else "not supplied")
        print("Voting:", row[17] if len(row) >= 18 else "not supplied")
        print("External destination:", decode(row[18]) if len(row) == 20 else "not supplied")
        print("External version:", decode(row[19]) if len(row) == 20 else "not supplied")
    return 0


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, zipfile.BadZipFile) as error:
        print("Diagnostic input error:", type(error).__name__)
        raise SystemExit(1)
