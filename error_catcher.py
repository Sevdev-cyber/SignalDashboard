import http.server, urllib.parse
class Handler(http.server.SimpleHTTPRequestHandler):
    def do_POST(self):
        content_len = int(self.headers.get('content-length', 0))
        post_body = self.rfile.read(content_len).decode('utf-8')
        print(f"JS ERROR/LOG: {post_body}")
        self.send_response(200)
        self.end_headers()

http.server.HTTPServer(('', 8082), Handler).serve_forever()
