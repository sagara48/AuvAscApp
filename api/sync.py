"""
Progilift Sync - Version par ETAPES
Fonctionne avec timeout court (10s)
Appeler plusieurs fois avec ?step=1, ?step=2, ?step=3
"""

import os
import json
import re
import ssl
import urllib.request
import traceback
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

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

def http_request(url, method='GET', data=None, headers=None, timeout=8):
    try:
        req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
        with urllib.request.urlopen(req, timeout=timeout, context=ssl_context) as resp:
            return resp.status, resp.read().decode('utf-8')
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode('utf-8') if e.fp else ''
    except Exception as e:
        return 0, str(e)

class SupabaseClient:
    def __init__(self):
        self.url = SUPABASE_URL.rstrip('/') if SUPABASE_URL else ''
        self.headers = {
            'apikey': SUPABASE_KEY or '',
            'Authorization': f'Bearer {SUPABASE_KEY}' if SUPABASE_KEY else '',
            'Content-Type': 'application/json'
        }
    
    def upsert(self, table, data):
        if not self.url:
            return False
        headers = {**self.headers, 'Prefer': 'resolution=merge-duplicates'}
        status, _ = http_request(f"{self.url}/rest/v1/{table}", 'POST', json.dumps(data).encode(), headers, 10)
        return status in [200, 201, 204]
    
    def insert(self, table, data):
        if not self.url:
            return False
        headers = {**self.headers, 'Prefer': 'return=minimal'}
        status, _ = http_request(f"{self.url}/rest/v1/{table}", 'POST', json.dumps(data).encode(), headers, 10)
        return status in [200, 201, 204]
    
    def delete(self, table):
        if not self.url:
            return False
        headers = {**self.headers, 'Prefer': 'return=minimal'}
        status, _ = http_request(f"{self.url}/rest/v1/{table}?id=gte.0", 'DELETE', None, headers, 10)
        return status in [200, 204]

class ProgiliftClient:
    WS_URL = "https://ws.progilift.fr/WS_PROGILIFT_20230419_WEB/awws/WS_Progilift_20230419.awws"
    SOAP = '''<?xml version="1.0" encoding="UTF-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ws="urn:WS_Progilift" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><soap:Header>{wsid}</soap:Header><soap:Body><ws:{method}>{params}</ws:{method}></soap:Body></soap:Envelope>'''

    def __init__(self):
        self.wsid = None
        self.sectors = []
    
    def _call(self, method, params=None, timeout=8):
        try:
            params_xml = ""
            if params:
                for k, v in params.items():
                    if v is not None:
                        v = str(v).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                        params_xml += f"<ws:{k}>{v}</ws:{k}>"
            wsid_h = f'<ws:WSID xsi:type="xsd:hexBinary" soap:mustUnderstand="1">{self.wsid}</ws:WSID>' if self.wsid else ""
            soap = self.SOAP.format(method=method, params=params_xml, wsid=wsid_h)
            headers = {'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': f'"urn:WS_Progilift/{method}"'}
            status, body = http_request(self.WS_URL, 'POST', soap.encode(), headers, timeout)
            return body if status == 200 and "Fault" not in body else None
        except:
            return None
    
    def _parse(self, xml, tag):
        items = []
        if not xml:
            return items
        try:
            for m in re.finditer(f'<{tag}>(.*?)</{tag}>', xml, re.DOTALL):
                item = {}
                for f in re.finditer(r'<([A-Za-z0-9_]+)>([^<]*)</\1>', m.group(1)):
                    val = f.group(2).strip()
                    item[f.group(1)] = int(val) if val.lstrip('-').isdigit() else val if val else None
                if item:
                    items.append(item)
        except:
            pass
        return items
    
    def authenticate(self):
        resp = self._call("IdentificationTechnicien", {"sSteCodeWeb": PROGILIFT_CODE}, 8)
        if resp:
            m = re.search(r'WSID[^>]*>([A-F0-9]+)<', resp, re.IGNORECASE)
            if m:
                self.wsid = m.group(1)
                return True
        return False
    
    def get_sectors(self):
        resp = self._call("get_Synchro_Wsect", {"dhDerniereMajFichier": "2000-01-01T00:00:00"}, 8)
        self.sectors = [s.strip() for s in re.findall(r'<SECTEUR>([^<]+)</SECTEUR>', resp or '') if s.strip()]
        return self.sectors

# =============================================================================
# STEP 1: Appareils à l'arrêt (rapide)
# =============================================================================
def sync_step1_arrets():
    pg = ProgiliftClient()
    sb = SupabaseClient()
    
    if not pg.authenticate():
        return {"status": "error", "message": "Auth failed"}
    
    resp = pg._call("get_AppareilsArret", {}, 8)
    arrets = pg._parse(resp, "tabListeArrets")
    
    sb.delete('appareils_arret')
    for a in arrets:
        sb.insert('appareils_arret', {
            'id_wsoucont': safe_int(a.get('nIDSOUCONT')),
            'id_panne': safe_int(a.get('nClepanne')),
            'date_appel': safe_str(a.get('sDateAppel'), 20),
            'heure_appel': safe_str(a.get('sHeureAppel'), 20),
            'motif': safe_str(a.get('sMotifAppel'), 500),
            'demandeur': safe_str(a.get('sDemandeur'), 100),
            'updated_at': datetime.now().isoformat()
        })
    
    return {"status": "success", "step": 1, "count": len(arrets), "next": "?step=2&sector=0"}

# =============================================================================
# STEP 2: Equipements par secteur
# =============================================================================
def sync_step2_equipements(sector_index):
    pg = ProgiliftClient()
    sb = SupabaseClient()
    
    if not pg.authenticate():
        return {"status": "error", "message": "Auth failed"}
    
    sectors = pg.get_sectors()
    if sector_index >= len(sectors):
        return {"status": "success", "step": 2, "message": "All sectors done", "next": "?step=3&period=0"}
    
    sector = sectors[sector_index]
    
    # Wsoucont
    resp = pg._call("get_Synchro_Wsoucont", {
        "dhDerniereMajFichier": "2000-01-01T00:00:00",
        "sListeSecteursTechnicien": sector
    }, 8)
    items = pg._parse(resp, "tabListeWsoucont")
    
    equip_list = []
    for e in items:
        id_ws = safe_int(e.get('IDWSOUCONT'))
        if id_ws:
            equip_list.append({
                'id_wsoucont': id_ws,
                'id_wcontrat': safe_int(e.get('IDWCONTRAT')),
                'secteur': safe_str(e.get('SECTEUR'), 20),
                'ascenseur': safe_str(e.get('ASCENSEUR'), 50),
                'adresse': safe_str(e.get('DES2'), 200),
                'ville': safe_str(e.get('DES3'), 200),
                'div1': safe_str(e.get('DIV1'), 100),
                'div2': safe_str(e.get('DIV2'), 100),
                'div6': safe_str(e.get('DIV6'), 100),
                'div15': safe_str(e.get('DIV15'), 200),
                'numappcli': safe_str(e.get('NUMAPPCLI'), 50),
                'jan': safe_int(e.get('JAN')),
                'fev': safe_int(e.get('FEV')),
                'mar': safe_int(e.get('MAR')),
                'avr': safe_int(e.get('AVR')),
                'mai': safe_int(e.get('MAI')),
                'jui': safe_int(e.get('JUI')),
                'jul': safe_int(e.get('JUL')),
                'aou': safe_int(e.get('AOU')),
                'sep': safe_int(e.get('SEP')),
                'oct': safe_int(e.get('OCT')),
                'nov': safe_int(e.get('NOV')),
                'dec': safe_int(e.get('DEC')),
                'data_wsoucont': json.dumps(e),
                'updated_at': datetime.now().isoformat()
            })
    
    for i in range(0, len(equip_list), 30):
        sb.upsert('equipements', equip_list[i:i+30])
    
    next_sector = sector_index + 1
    if next_sector < len(sectors):
        next_url = f"?step=2&sector={next_sector}"
    else:
        next_url = "?step=3&period=0"
    
    return {"status": "success", "step": 2, "sector": sector, "count": len(equip_list), "next": next_url}

# =============================================================================
# STEP 3: Pannes par période
# =============================================================================
PERIODS = [
    "2025-10-01T00:00:00",
    "2025-07-01T00:00:00",
    "2025-04-01T00:00:00",
    "2025-01-01T00:00:00",
    "2024-07-01T00:00:00",
    "2024-01-01T00:00:00",
    "2023-01-01T00:00:00",
    "2022-01-01T00:00:00",
    "2021-01-01T00:00:00",
    "2020-01-01T00:00:00"
]

def sync_step3_pannes(period_index):
    pg = ProgiliftClient()
    sb = SupabaseClient()
    
    if not pg.authenticate():
        return {"status": "error", "message": "Auth failed"}
    
    if period_index >= len(PERIODS):
        # Log final
        sb.insert('sync_logs', {
            'sync_date': datetime.now().isoformat(),
            'status': 'success',
            'equipements_count': 0,
            'pannes_count': 0,
            'duration_seconds': 0
        })
        return {"status": "success", "step": 3, "message": "All periods done - SYNC COMPLETE!"}
    
    period = PERIODS[period_index]
    
    resp = pg._call("get_Synchro_Wpanne", {"dhDerniereMajFichier": period}, 8)
    items = pg._parse(resp, "tabListeWpanne")
    
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
    
    for i in range(0, len(pannes_list), 30):
        sb.upsert('pannes', pannes_list[i:i+30])
    
    next_period = period_index + 1
    if next_period < len(PERIODS):
        next_url = f"?step=3&period={next_period}"
    else:
        next_url = None
    
    result = {"status": "success", "step": 3, "period": period, "count": len(pannes_list)}
    if next_url:
        result["next"] = next_url
    else:
        result["message"] = "SYNC COMPLETE!"
        sb.insert('sync_logs', {
            'sync_date': datetime.now().isoformat(),
            'status': 'success'
        })
    
    return result

# =============================================================================
# HANDLER
# =============================================================================
class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.handle_request()
    
    def do_POST(self):
        self.handle_request()
    
    def handle_request(self):
        try:
            # Parse query params
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            
            step = int(params.get('step', ['0'])[0])
            sector = int(params.get('sector', ['0'])[0])
            period = int(params.get('period', ['0'])[0])
            
            if step == 1:
                result = sync_step1_arrets()
            elif step == 2:
                result = sync_step2_equipements(sector)
            elif step == 3:
                result = sync_step3_pannes(period)
            else:
                # Step 0 = info
                result = {
                    "status": "ready",
                    "message": "Sync par étapes",
                    "usage": {
                        "step1": "/api/sync?step=1 (arrêts)",
                        "step2": "/api/sync?step=2&sector=N (équipements)",
                        "step3": "/api/sync?step=3&period=N (pannes)"
                    },
                    "start": "/api/sync?step=1"
                }
        
        except Exception as e:
            result = {"status": "error", "message": str(e), "traceback": traceback.format_exc()[:500]}
        
        body = json.dumps(result).encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def log_message(self, format, *args):
        pass
