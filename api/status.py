"""
Progilift Sync - Status API (Version simplifiée)
"""

import os
import json
from datetime import datetime
from http.server import BaseHTTPRequestHandler
import requests

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')
PROGILIFT_CODE = os.environ.get('PROGILIFT_CODE', 'AUVNB1')


def get_status():
    try:
        headers = {
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {SUPABASE_KEY}'
        }
        
        # Dernière sync
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/sync_logs?select=*&order=sync_date.desc&limit=1",
            headers=headers, timeout=10
        )
        last_sync = resp.json()[0] if resp.status_code == 200 and resp.json() else None
        
        # Counts
        equipements = requests.get(
            f"{SUPABASE_URL}/rest/v1/equipements?select=id_wsoucont",
            headers={**headers, 'Prefer': 'count=exact'}, timeout=10
        )
        
        pannes = requests.get(
            f"{SUPABASE_URL}/rest/v1/pannes?select=id_panne",
            headers={**headers, 'Prefer': 'count=exact'}, timeout=10
        )
        
        arrets = requests.get(
            f"{SUPABASE_URL}/rest/v1/appareils_arret?select=id",
            headers={**headers, 'Prefer': 'count=exact'}, timeout=10
        )
        
        return {
            "status": "ok",
            "progilift_code": PROGILIFT_CODE,
            "last_sync": {
                "date": last_sync['sync_date'] if last_sync else None,
                "status": last_sync['status'] if last_sync else None,
                "equipements": last_sync['equipements_count'] if last_sync else 0,
                "pannes": last_sync['pannes_count'] if last_sync else 0,
            } if last_sync else None,
            "totals": {
                "equipements": int(equipements.headers.get('content-range', '0-0/0').split('/')[-1]) if equipements.status_code == 200 else 0,
                "pannes": int(pannes.headers.get('content-range', '0-0/0').split('/')[-1]) if pannes.status_code == 200 else 0,
                "appareils_arret": int(arrets.headers.get('content-range', '0-0/0').split('/')[-1]) if arrets.status_code == 200 else 0,
            },
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
        
        result = get_status()
        self.wfile.write(json.dumps(result).encode())
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
