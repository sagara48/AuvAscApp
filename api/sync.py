import os
import json
from http.server import BaseHTTPRequestHandler

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self._respond()
    
    def do_POST(self):
        self._respond()
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def _respond(self):
        result = {
            "status": "ok",
            "message": "Sync diagnostic",
            "env_check": {
                "SUPABASE_URL": bool(os.environ.get('SUPABASE_URL')),
                "SUPABASE_KEY": bool(os.environ.get('SUPABASE_KEY')),
                "PROGILIFT_CODE": os.environ.get('PROGILIFT_CODE', 'NOT SET')
            }
        }
        
        body = json.dumps(result).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)
    
    def log_message(self, format, *args):
        pass
