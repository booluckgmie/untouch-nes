"""
Local server for NES data filter tool.
Auto-loads the latest nes_all_data_*.csv from the current directory.

Usage:
    python serve.py
    python serve.py --file nes_all_data_2026-01-01_2026-06-30_20260626.csv
    python serve.py --port 8888
"""

import argparse
import glob
import os
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

BASE_DIR = Path(__file__).parent


def find_latest_csv() -> Path | None:
    matches = sorted(
        glob.glob(str(BASE_DIR / "nes_all_data_*.csv")),
        key=os.path.getmtime,
        reverse=True,
    )
    return Path(matches[0]) if matches else None


class Handler(BaseHTTPRequestHandler):

    csv_path: Path | None = None

    def log_message(self, fmt, *args):
        pass  # silence access logs

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self._serve_file(BASE_DIR / "filter_data.html", "text/html; charset=utf-8")
        elif self.path == "/data":
            if not self.csv_path or not self.csv_path.exists():
                self.send_error(404, "CSV not found")
                return
            self._serve_file(self.csv_path, "text/csv; charset=utf-8")
        elif self.path == "/info":
            import json
            info = {
                "file": str(self.csv_path) if self.csv_path else None,
                "name": self.csv_path.name if self.csv_path else None,
            }
            body = json.dumps(info).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_error(404)

    def _serve_file(self, path: Path, content_type: str):
        try:
            data = path.read_bytes()
        except OSError:
            self.send_error(500, f"Cannot read {path}")
            return
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(data)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file",  default=None, help="CSV file to serve (default: latest nes_all_data_*.csv)")
    parser.add_argument("--port",  type=int, default=8765, help="Port (default 8765)")
    args = parser.parse_args()

    if args.file:
        csv = Path(args.file)
        if not csv.is_absolute():
            csv = BASE_DIR / csv
        if not csv.exists():
            print(f"File not found: {csv}")
            return
    else:
        csv = find_latest_csv()
        if not csv:
            print("No nes_all_data_*.csv found. Run fetch_all_data.py first.")
            return

    Handler.csv_path = csv

    server = HTTPServer(("127.0.0.1", args.port), Handler)
    url = f"http://127.0.0.1:{args.port}"
    print(f"Serving : {url}")
    print(f"CSV     : {csv.name}  ({csv.stat().st_size / 1024 / 1024:.1f} MB)")
    print(f"Press Ctrl+C to stop.")

    threading.Timer(0.5, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
