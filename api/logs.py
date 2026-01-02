"""
Progilift Logs API
"""

import os
import json
import ssl
import urllib.request
from http.server import BaseHTTPRequestHandler

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')

try:
    ssl_context = ssl.create_default_context()
except:
    ssl_context = ssl._create_unverified_context()

def get_logs(limit=50):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []
    try:
        url = f"{SUPABASE_URL}/rest/v1/sync_logs?select=*&order=sync_date.desc&limit={limit}"
        headers = {
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {SUPABASE_KEY}'
        }
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10, context=ssl_context) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        return []

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            result = get_logs()
        except Exception as e:
            result = []
        
        body = json.dumps(result).encode('utf-8')
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def log_message(self, format, *args):
        pass
