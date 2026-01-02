import os
import json
import re
import ssl
import traceback
from datetime import datetime
from http.server import BaseHTTPRequestHandler

try:
    import urllib.request
    URLLIB_OK = True
except:
    URLLIB_OK = False

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

def http_request(url, method='GET', data=None, headers=None, timeout=60):
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
    try:
        url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table}"
        headers = {
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {SUPABASE_KEY}',
            'Content-Type': 'application/json',
            'Prefer': 'resolution=merge-duplicates'
        }
        status, _ = http_request(url, 'POST', json.dumps(data).encode(), headers, 60)
        return status in [200, 201, 204]
    except:
        return False

def supabase_insert(table, data):
    if not SUPABASE_URL or not data:
        return False
    try:
        url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table}"
        headers = {
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {SUPABASE_KEY}',
            'Content-Type': 'application/json',
            'Prefer': 'return=minimal'
        }
        status, _ = http_request(url, 'POST', json.dumps(data).encode(), headers, 30)
        return status in [200, 201, 204]
    except:
        return False

def supabase_delete(table):
    if not SUPABASE_URL:
        return False
    try:
        url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table}?id=gte.0"
        headers = {
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {SUPABASE_KEY}',
            'Content-Type': 'application/json',
            'Prefer': 'return=minimal'
        }
        status, _ = http_request(url, 'DELETE', None, headers, 30)
        return status in [200, 204]
    except:
        return False

def progilift_call(method, params, wsid=None, timeout=60):
    try:
        ws_url = "https://ws.progilift.fr/WS_PROGILIFT_20230419_WEB/awws/WS_Progilift_20230419.awws"
        
        params_xml = ""
        if params:
            for k, v in params.items():
                if v is not None:
                    v_esc = str(v).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                    params_xml += f"<ws:{k}>{v_esc}</ws:{k}>"
        
        wsid_xml = ""
        if wsid:
            wsid_xml = f'<ws:WSID xsi:type="xsd:hexBinary" soap:mustUnderstand="1">{wsid}</ws:WSID>'
        
        soap = f'''<?xml version="1.0" encoding="UTF-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ws="urn:WS_Progilift" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><soap:Header>{wsid_xml}</soap:Header><soap:Body><ws:{method}>{params_xml}</ws:{method}></soap:Body></soap:Envelope>'''
        
        headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': f'"urn:WS_Progilift/{method}"'
        }
        
        status, body = http_request(ws_url, 'POST', soap.encode('utf-8'), headers, timeout)
        
        if status == 200 and body and "Fault" not in body:
            return body
        return None
    except:
        return None

def parse_xml_items(xml, tag):
    items = []
    if not xml:
        return items
    try:
        for m in re.finditer(f'<{tag}>(.*?)</{tag}>', xml, re.DOTALL):
            item = {}
            for f in re.finditer(r'<([A-Za-z0-9_]+)>([^<]*)</\1>', m.group(1)):
                val = f.group(2).strip()
                if val:
                    item[f.group(1)] = int(val) if val.lstrip('-').isdigit() else val
                else:
                    item[f.group(1)] = None
            if item:
                items.append(item)
    except:
        pass
    return items

def run_sync():
    start = datetime.now()
    stats = {"equipements": 0, "pannes": 0, "appareils_arret": 0}
    errors = []
    
    # 1. Auth Progilift
    resp = progilift_call("IdentificationTechnicien", {"sSteCodeWeb": PROGILIFT_CODE}, None, 30)
    if not resp:
        return {"status": "error", "message": "Progilift auth failed"}
    
    m = re.search(r'WSID[^>]*>([A-F0-9]+)<', resp, re.IGNORECASE)
    if not m:
        return {"status": "error", "message": "No WSID in response"}
    wsid = m.group(1)
    
    # 2. Secteurs
    resp = progilift_call("get_Synchro_Wsect", {"dhDerniereMajFichier": "2000-01-01T00:00:00"}, wsid, 30)
    sectors = [s.strip() for s in re.findall(r'<SECTEUR>([^<]+)</SECTEUR>', resp or '') if s.strip()]
    if not sectors:
        return {"status": "error", "message": "No sectors"}
    
    # 3. Appareils arrêt
    try:
        resp = progilift_call("get_AppareilsArret", {}, wsid, 30)
        arrets = parse_xml_items(resp, "tabListeArrets")
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
        stats["appareils_arret"] = len(arrets)
    except Exception as e:
        errors.append(f"Arrets: {e}")
    
    # 4. Equipements
    try:
        equip_list = []
        for sector in sectors:
            resp = progilift_call("get_Synchro_Wsoucont", {
                "dhDerniereMajFichier": "2000-01-01T00:00:00",
                "sListeSecteursTechnicien": sector
            }, wsid, 90)
            items = parse_xml_items(resp, "tabListeWsoucont")
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
        
        for i in range(0, len(equip_list), 50):
            supabase_upsert('equipements', equip_list[i:i+50])
        stats["equipements"] = len(equip_list)
    except Exception as e:
        errors.append(f"Equip: {e}")
    
    # 5. Pannes
    try:
        all_pannes = []
        seen = set()
        periods = ["2025-01-01T00:00:00", "2024-01-01T00:00:00", "2023-01-01T00:00:00", "2022-01-01T00:00:00", "2020-01-01T00:00:00"]
        
        for period in periods:
            resp = progilift_call("get_Synchro_Wpanne", {"dhDerniereMajFichier": period}, wsid, 90)
            items = parse_xml_items(resp, "tabListeWpanne")
            for p in items:
                pid = safe_int(p.get('P0CLEUNIK'))
                if pid and pid not in seen:
                    seen.add(pid)
                    all_pannes.append({
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
        
        for i in range(0, len(all_pannes), 50):
            supabase_upsert('pannes', all_pannes[i:i+50])
        stats["pannes"] = len(all_pannes)
    except Exception as e:
        errors.append(f"Pannes: {e}")
    
    # Log
    duration = (datetime.now() - start).total_seconds()
    supabase_insert('sync_logs', {
        'sync_date': datetime.now().isoformat(),
        'status': 'success' if not errors else 'partial',
        'equipements_count': stats["equipements"],
        'pannes_count': stats["pannes"],
        'duration_seconds': round(duration, 1)
    })
    
    return {
        "status": "success" if not errors else "partial",
        "stats": stats,
        "duration": round(duration, 1),
        "sectors": len(sectors),
        "errors": errors if errors else None
    }

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
        try:
            result = run_sync()
        except Exception as e:
            result = {
                "status": "error",
                "message": str(e),
                "trace": traceback.format_exc()
            }
        
        body = json.dumps(result).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)
    
    def log_message(self, format, *args):
        pass
