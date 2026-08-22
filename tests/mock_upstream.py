#!/usr/bin/env python3
"""Mock upstream: 回显收到的 path/body/headers。"""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        n = int(self.headers.get("content-length", 0))
        body = self.rfile.read(n)
        payload = {"ok": True}
        try:
            payload["body"] = json.loads(body)
        except Exception:
            payload["body"] = body.decode("utf-8", "replace")
        payload["auth"] = self.headers.get("authorization")
        payload["x_req_id"] = self.headers.get("x-request-id")
        resp = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(resp)))
        self.end_headers()
        self.wfile.write(resp)
        self.wfile.flush()

    def log_message(self, *a):
        print("LOG", a, flush=True)


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", 9999), H).serve_forever()
