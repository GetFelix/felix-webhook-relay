# An endpoint for the install tests: answers 200 to every webhook and prints it.
from http.server import BaseHTTPRequestHandler, HTTPServer


class Hook(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers["content-length"] or 0))
        print(self.headers["webhook-id"], body.decode(), flush=True)
        self.send_response(200)
        self.end_headers()


HTTPServer(("", 9000), Hook).serve_forever()
