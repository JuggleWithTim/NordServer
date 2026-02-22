from http.server import BaseHTTPRequestHandler, HTTPServer
import datetime
import os
import sys

XML_BODY = b'''<?xml version="1.0" encoding="UTF-8"?>
<events>
  <url id="splash_slx"
       swe="startup/ngpsplash_se.jpg"
       eng="startup/ngpsplash_eng.jpg"
       dan="startup/ngpsplash_se.jpg"
       nor="startup/ngpsplash_no.jpg"/>
  <url id="splash_ngp"
       swe="startup/ngpsplash_se.jpg"
       eng="startup/ngpsplash_eng.jpg"
       dan="startup/ngpsplash_se.jpg"
       nor="startup/ngpsplash_no.jpg"/>
  <url id="login_background"
       swe="startup/loginbackdrop.jpg"
       eng="startup/loginbackdrop_eng.jpg"
       dan="startup/loginbackdrop_eng.jpg"
       nor="startup/loginbackdrop_eng.jpg"/>
  <url id="login_newspic"
       swe="startup/accountbg.png"
       eng="startup/accountbg.png"
       dan="startup/accountbg.png"
       nor="startup/accountbg.png"/>
  <url id="login_foreground"
       swe="local/default.png"
       eng="local/default.png"
       dan="local/default.png"
       nor="local/default.png"/>
  <url id="newspic"
       swe="startup/accountbg.png"
       eng="startup/accountbg.png"
       dan="startup/accountbg.png"
       nor="startup/accountbg.png"/>
</events>
'''

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        sys.stdout.write(f"GET {self.path}\n")
        sys.stdout.flush()
        if self.path.startswith("/ServerTime.jsp"):
            now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S").encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(now)))
            self.end_headers()
            self.wfile.write(now)
            return

        # Default: return well-formed XML so XML parsers don't choke.
        self.send_response(200)
        self.send_header("Content-Type", "text/xml; charset=utf-8")
        self.send_header("Content-Length", str(len(XML_BODY)))
        self.end_headers()
        self.wfile.write(XML_BODY)

    def log_message(self, format, *args):
        return

if __name__ == "__main__":
    bind_host = os.environ.get("NORD_HTTP_BIND", "127.0.0.1")
    bind_port = int(os.environ.get("NORD_HTTP_PORT", "8080"))
    server = HTTPServer((bind_host, bind_port), Handler)
    print(f"Nord server listening on {bind_host}:{bind_port}")
    server.serve_forever()
