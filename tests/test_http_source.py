import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

from digital_publishing.sources import HttpJsonSource


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({"records": [{"source": "http", "observed_at": "2026-11-01T00:00:00Z", "payload": {"id": "1"}}]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        return


def test_http_source_reads_records_and_lands_raw_response(tmp_path):
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        source = HttpJsonSource(f"http://127.0.0.1:{server.server_port}/records", raw_dir=str(tmp_path / "raw"))
        records = list(source.read())
        assert len(records) == 1
        assert records[0]["payload"]["id"] == "1"
        assert list((tmp_path / "raw").glob("*.json"))
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
