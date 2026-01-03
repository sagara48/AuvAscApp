"""
Endpoint Cron pour Vercel - Sync rapide toutes les heures
Synchronise: Arrêts + Pannes récentes
"""

import os
import json
import re
import ssl
import urllib.request
from datetime import datetime
from http.server import BaseHTTPRequestHandler

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')
PROGILIFT_CODE = os.environ.get('PROGILIFT_CODE', 'AUVNB1')

try:
    ssl_context = ssl.create_default_context()
except:
    ssl_context = ssl._create_unverified_context()

def safe_str(value, max_len=None):
    if value is None:
        return None
    try:
        s = str(value).strip()
        return s[:max_len] if max_len and s else s if s else None
    except:
        return None

def safe_int(value):
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except:
        return None

def http_request(url, method='GET', data=None, headers=None, timeout=30):
    try:
        req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
        with urllib.request.urlopen(req, timeout=timeout, context=ssl_context) as resp:
            return resp.status, resp.read().decode('utf-8')
    except urllib.error.HTTPError as e:
        return e.code, ''
    except Exception as e:
        return 0, str(e)

def supabase_upsert(table, data):
    if not SUPABASE_URL or not data:
        return False
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table}"
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=merge-duplicates'
    }
    status, _ = http_request(url, 'POST', json.dumps(data).encode(), headers, 30)
    return status in [200, 201, 204]

def supabase_insert(table, data):
    if not SUPABASE_URL or not data:
        return False
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table}"
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal'
    }
    status, _ = http_request(url, 'POST', json.dumps(data).encode(), headers, 30)
    return status in [200, 201, 204]

def supabase_delete(table):
    if not SUPABASE_URL:
        return False
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table}?id=gte.0"
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Prefer': 'return=minimal'
    }
    status, _ = http_request(url, 'DELETE', None, headers, 30)
    return status in [200, 204]

def progilift_call(method, params, wsid=None, timeout=30):
    ws_url = "https://ws.progilift.fr/WS_PROGILIFT_20230419_WEB/awws/WS_Progilift_20230419.awws"
    
    params_xml = ""
    if params:
        for k, v in params.items():
            if v is not None:
                v_esc = str(v).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                params_xml += f"<ws:{k}>{v_esc}</ws:{k}>"
    
    wsid_xml = f'<ws:WSID xsi:type="xsd:hexBinary" soap:mustUnderstand="1">{wsid}</ws:WSID>' if wsid else ""
    
    soap = f'''<?xml version="1.0" encoding="UTF-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ws="urn:WS_Progilift" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><soap:Header>{wsid_xml}</soap:Header><soap:Body><ws:{method}>{params_xml}</ws:{method}></soap:Body></soap:Envelope>'''
    
    headers = {'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': f'"urn:WS_Progilift/{method}"'}
    status, body = http_request(ws_url, 'POST', soap.encode('utf-8'), headers, timeout)
    
    return body if status == 200 and body and "Fault" not in body else None

def parse_items(xml, tag):
    items = []
    if not xml:
        return items
    for m in re.finditer(f'<{tag}>(.*?)</{tag}>', xml, re.DOTALL):
        item = {}
        for f in re.finditer(r'<([A-Za-z0-9_]+)>([^<]*)</\1>', m.group(1)):
            val = f.group(2).strip()
            item[f.group(1)] = int(val) if val and val.lstrip('-').isdigit() else (val if val else None)
        if item:
            items.append(item)
    return items

def run_cron_sync():
    """Sync rapide pour le cron horaire"""
    start = datetime.now()
    stats = {"arrets": 0, "pannes": 0, "errors": []}
    
    # Auth
    resp = progilift_call("IdentificationTechnicien", {"sSteCodeWeb": PROGILIFT_CODE}, None, 30)
    if not resp:
        return {"status": "error", "message": "Auth failed"}
    
    m = re.search(r'WSID[^>]*>([A-F0-9]+)<', resp, re.IGNORECASE)
    if not m:
        return {"status": "error", "message": "No WSID"}
    wsid = m.group(1)
    
    # 1. Arrêts
    try:
        resp = progilift_call("get_AppareilsArret", {}, wsid, 30)
        arrets = parse_items(resp, "tabListeArrets")
        supabase_delete('appareils_arret')
        for a in arrets:
            supabase_insert('appareils_arret', {
                'id_wsoucont': safe_int(a.get('nIDSOUCONT')),
                'id_panne': safe_int(a.get('nClepanne')),
                'date_appel': safe_str(a.get('sDateAppel'), 20),
                'heure_appel': safe_str(a.get('sHeureAppel'), 20),
                'motif': safe_str(a.get('sMotifAppel'), 500),
                'demandeur': safe_str(a.get('sDemandeur'), 100),
                'updated_at': datetime.now().isoformat()
            })
        stats["arrets"] = len(arrets)
    except Exception as e:
        stats["errors"].append(f"Arrets: {e}")
    
    # 2. Pannes récentes (30 derniers jours)
    try:
        # Date il y a 30 jours
        from datetime import timedelta
        date_30j = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00")
        
        resp = progilift_call("get_Synchro_Wpanne", {"dhDerniereMajFichier": date_30j}, wsid, 60)
        items = parse_items(resp, "tabListeWpanne")
        pannes_list = []
        for p in items:
            pid = safe_int(p.get('P0CLEUNIK'))
            if pid:
                pannes_list.append({
                    'id_panne': pid,
                    'id_wsoucont': safe_int(p.get('IDWSOUCONT')),
                    'date_panne': safe_str(p.get('DATE'), 20),
                    'depanneur': safe_str(p.get('DEPANNEUR'), 100),
                    'libelle': safe_str(p.get('PANNES'), 200),
                    'heure_inter': safe_str(p.get('INTER'), 20),
                    'heure_fin': safe_str(p.get('HRFININTER'), 20),
                    'data': json.dumps(p),
                    'updated_at': datetime.now().isoformat()
                })
        for i in range(0, len(pannes_list), 50):
            supabase_upsert('pannes', pannes_list[i:i+50])
        stats["pannes"] = len(pannes_list)
    except Exception as e:
        stats["errors"].append(f"Pannes: {e}")
    
    duration = (datetime.now() - start).total_seconds()
    
    # Log
    supabase_insert('sync_logs', {
        'sync_date': datetime.now().isoformat(),
        'status': 'cron' if not stats["errors"] else 'cron_partial',
        'equipements_count': 0,
        'pannes_count': stats["pannes"],
        'duration_seconds': round(duration, 1),
        'error_message': '; '.join(stats["errors"])[:500] if stats["errors"] else None
    })
    
    return {
        "status": "success" if not stats["errors"] else "partial",
        "mode": "cron",
        "stats": stats,
        "duration": round(duration, 1),
        "timestamp": datetime.now().isoformat()
    }

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self._respond()
    
    def do_POST(self):
        self._respond()
    
    def _respond(self):
        try:
            result = run_cron_sync()
        except Exception as e:
            result = {"status": "error", "message": str(e)}
        
        body = json.dumps(result).encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)
    
    def log_message(self, format, *args):
        pass
