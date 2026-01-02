"""
Progilift Sync - Logs API
"""

import os
import json
from http.server import BaseHTTPRequestHandler
import requests

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')


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
            resp = requests.get(
                f"{SUPABASE_URL}/rest/v1/sync_logs?select=*&order=sync_date.desc&limit=50",
                headers=headers, timeout=10
            )
            logs = resp.json() if resp.status_code == 200 else []
            self.wfile.write(json.dumps(logs).encode())
        except Exception as e:
            self.wfile.write(json.dumps({"error": str(e)}).encode())
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
