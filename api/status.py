"""
Progilift Sync - Status API (sans dépendances)
"""

import os
import json
import ssl
import urllib.request
from datetime import datetime
from http.server import BaseHTTPRequestHandler

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')
PROGILIFT_CODE = os.environ.get('PROGILIFT_CODE', 'AUVNB1')

ssl_context = ssl.create_default_context()


def http_get(url: str, headers: dict) -> tuple:
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10, context=ssl_context) as resp:
            return resp.status, resp.read().decode('utf-8'), dict(resp.headers)
    except Exception as e:
        return 0, str(e), {}


def get_status():
    try:
        headers = {
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {SUPABASE_KEY}'
        }
        
        # Dernière sync
        status, body, _ = http_get(
            f"{SUPABASE_URL}/rest/v1/sync_logs?select=*&order=sync_date.desc&limit=1",
            headers
        )
        last_sync = json.loads(body)[0] if status == 200 and body.startswith('[') else None
        
        return {
            "status": "ok",
            "progilift_code": PROGILIFT_CODE,
            "supabase_url": SUPABASE_URL[:30] + "..." if SUPABASE_URL else "NOT SET",
            "last_sync": last_sync,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(get_status()).encode())
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
