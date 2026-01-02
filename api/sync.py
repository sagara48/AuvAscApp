import os
import json
from datetime import datetime
from http.server import BaseHTTPRequestHandler

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        
        result = {
            "status": "test",
            "supabase_url": os.environ.get('SUPABASE_URL', 'NOT SET')[:30],
            "progilift_code": os.environ.get('PROGILIFT_CODE', 'NOT SET'),
            "timestamp": datetime.now().isoformat()
        }
        self.wfile.write(json.dumps(result).encode())
    
    def do_POST(self):
        self.do_GET()
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
