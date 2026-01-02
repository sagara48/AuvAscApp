"""
Progilift Status API
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

def get_count(table):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return 0
    try:
        url = f"{SUPABASE_URL}/rest/v1/{table}?select=count"
        headers = {
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {SUPABASE_KEY}',
            'Prefer': 'count=exact'
        }
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10, context=ssl_context) as resp:
            content_range = resp.headers.get('content-range', '0/0')
            return int(content_range.split('/')[-1]) if '/' in content_range else 0
    except:
        return 0

def get_last_sync():
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    try:
        url = f"{SUPABASE_URL}/rest/v1/sync_logs?select=*&order=sync_date.desc&limit=1"
        headers = {
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {SUPABASE_KEY}'
        }
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10, context=ssl_context) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            return data[0] if data else None
    except:
        return None

def get_status():
    last_sync = get_last_sync()
    return {
        "status": "ok",
        "totals": {
            "equipements": get_count("equipements"),
            "pannes": get_count("pannes"),
            "appareils_arret": get_count("appareils_arret")
        },
        "last_sync": {
            "date": last_sync.get('sync_date') if last_sync else None,
            "status": last_sync.get('status') if last_sync else None,
            "duration": last_sync.get('duration_seconds') if last_sync else None
        } if last_sync else None
    }

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            result = get_status()
        except Exception as e:
            result = {"status": "error", "message": str(e)}
        
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
