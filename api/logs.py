"""
Progilift Sync - Logs API (sans dépendances)
"""

import os
import json
import ssl
import urllib.request
from http.server import BaseHTTPRequestHandler

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')

ssl_context = ssl.create_default_context()


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        
        try:
            headers = {
                'apikey': SUPABASE_KEY,
                'Authorization': f'Bearer {SUPABASE_KEY}'
            }
            req = urllib.request.Request(
                f"{SUPABASE_URL}/rest/v1/sync_logs?select=*&order=sync_date.desc&limit=50",
                headers=headers
            )
            with urllib.request.urlopen(req, timeout=10, context=ssl_context) as resp:
                logs = resp.read().decode('utf-8')
                self.wfile.write(logs.encode())
        except Exception as e:
            self.wfile.write(json.dumps({"error": str(e)}).encode())
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
