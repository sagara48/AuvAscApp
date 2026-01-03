import os
import json
import re
import ssl
import traceback
import urllib.request
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')
PROGILIFT_CODE = os.environ.get('PROGILIFT_CODE', 'AUVNB1')

# Liste COMPLETE des 22 secteurs
SECTORS = ["1", "2", "3", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "17", "18", "19", "20", "71", "72", "73", "74"]

PERIODS = [
    "2025-10-01T00:00:00",
    "2025-07-01T00:00:00",
    "2025-01-01T00:00:00",
    "2024-01-01T00:00:00",
    "2023-01-01T00:00:00",
    "2022-01-01T00:00:00",
    "2020-01-01T00:00:00"
]

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

def supabase_update(table, id_field, id_value, data):
    if not SUPABASE_URL or not data:
        return False
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table}?{id_field}=eq.{id_value}"
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal'
    }
    status, _ = http_request(url, 'PATCH', json.dumps(data).encode(), headers, 30)
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

def get_auth():
    resp = progilift_call("IdentificationTechnicien", {"sSteCodeWeb": PROGILIFT_CODE}, None, 30)
    if resp:
        m = re.search(r'WSID[^>]*>([A-F0-9]+)<', resp, re.IGNORECASE)
        if m:
            return m.group(1)
    return None

# ==== STEP 1: Arrêts ====
def sync_arrets():
    wsid = get_auth()
    if not wsid:
        return {"status": "error", "message": "Auth failed"}
    
    resp = progilift_call("get_AppareilsArret", {}, wsid, 30)
    arrets = parse_items(resp, "tabListeArrets")
    
    supabase_delete('appareils_arret')
    inserted = 0
    for a in arrets:
        result = supabase_insert('appareils_arret', {
            'id_wsoucont': safe_int(a.get('nIDSOUCONT')),
            'id_panne': safe_int(a.get('nClepanne')),
            'date_appel': safe_str(a.get('sDateAppel'), 20),
            'heure_appel': safe_str(a.get('sHeureAppel'), 20),
            'motif': safe_str(a.get('sMotifAppel'), 500),
            'demandeur': safe_str(a.get('sDemandeur'), 100),
            'updated_at': datetime.now().isoformat()
        })
        if result:
            inserted += 1
    
    return {"status": "success", "step": 1, "arrets": len(arrets), "inserted": inserted, "next": "?step=2&sector=0"}

# ==== STEP 2: Équipements par secteur (Wsoucont) ====
def sync_equipements(sector_idx):
    wsid = get_auth()
    if not wsid:
        return {"status": "error", "message": "Auth failed"}
    
    if sector_idx >= len(SECTORS):
        return {"status": "success", "step": 2, "message": "All 22 sectors done", "next": "?step=2b&sector=0"}
    
    sector = SECTORS[sector_idx]
    resp = progilift_call("get_Synchro_Wsoucont", {
        "dhDerniereMajFichier": "2000-01-01T00:00:00",
        "sListeSecteursTechnicien": sector
    }, wsid, 90)
    
    items = parse_items(resp, "tabListeWsoucont")
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
                'des4': safe_str(e.get('DES4'), 200),
                'des6': safe_str(e.get('DES6'), 200),
                'des7': safe_str(e.get('DES7'), 200),
                'div1': safe_str(e.get('DIV1'), 100),
                'div2': safe_str(e.get('DIV2'), 100),
                'div3': safe_str(e.get('DIV3'), 100),
                'div4': safe_str(e.get('DIV4'), 100),
                'div5': safe_str(e.get('DIV5'), 100),
                'div6': safe_str(e.get('DIV6'), 100),
                'div7': safe_str(e.get('DIV7'), 100),
                'div8': safe_str(e.get('DIV8'), 100),
                'div9': safe_str(e.get('DIV9'), 100),
                'div10': safe_str(e.get('DIV10'), 100),
                'div11': safe_str(e.get('DIV11'), 200),
                'div12': safe_str(e.get('DIV12'), 200),
                'div13': safe_str(e.get('DIV13'), 200),
                'div14': safe_str(e.get('DIV14'), 200),
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
        supabase_upsert('equipements', equip_list[i:i+30])
    
    next_idx = sector_idx + 1
    next_url = f"?step=2&sector={next_idx}" if next_idx < len(SECTORS) else "?step=2b&sector=0"
    
    return {
        "status": "success",
        "step": 2,
        "sector": sector,
        "sector_index": f"{sector_idx + 1}/{len(SECTORS)}",
        "equipements": len(equip_list),
        "next": next_url
    }

# ==== STEP 2b: 10 derniers passages (Wsoucont2) ====
def sync_passages(sector_idx):
    wsid = get_auth()
    if not wsid:
        return {"status": "error", "message": "Auth failed"}
    
    if sector_idx >= len(SECTORS):
        return {"status": "success", "step": "2b", "message": "All passages done", "next": "?step=3&period=0"}
    
    sector = SECTORS[sector_idx]
    resp = progilift_call("get_Synchro_Wsoucont2", {
        "dhDerniereMajFichier": "2000-01-01T00:00:00",
        "sListeSecteursTechnicien": sector
    }, wsid, 90)
    
    items = parse_items(resp, "tabListeWsoucont2")
    updated = 0
    
    for e in items:
        id_ws = safe_int(e.get('IDWSOUCONT'))
        if id_ws:
            update_data = {'updated_at': datetime.now().isoformat()}
            for i in range(1, 11):
                update_data[f'lib{i}'] = safe_str(e.get(f'LIB{i}'), 100)
                update_data[f'datepass{i}'] = safe_int(e.get(f'DATEPASS{i}'))
            
            if supabase_update('equipements', 'id_wsoucont', id_ws, update_data):
                updated += 1
    
    next_idx = sector_idx + 1
    next_url = f"?step=2b&sector={next_idx}" if next_idx < len(SECTORS) else "?step=3&period=0"
    
    return {
        "status": "success",
        "step": "2b",
        "sector": sector,
        "sector_index": f"{sector_idx + 1}/{len(SECTORS)}",
        "passages_updated": updated,
        "next": next_url
    }

# ==== STEP 3: Pannes par période ====
def sync_pannes(period_idx):
    wsid = get_auth()
    if not wsid:
        return {"status": "error", "message": "Auth failed"}
    
    if period_idx >= len(PERIODS):
        supabase_insert('sync_logs', {
            'sync_date': datetime.now().isoformat(),
            'status': 'success'
        })
        return {"status": "success", "step": 3, "message": "SYNC COMPLETE!"}
    
    period = PERIODS[period_idx]
    resp = progilift_call("get_Synchro_Wpanne", {"dhDerniereMajFichier": period}, wsid, 90)
    items = parse_items(resp, "tabListeWpanne")
    
    pannes_list = []
    for p in items:
        pid = safe_int(p.get('P0CLEUNIK'))
        if pid:
            pannes_list.append({
                'id_panne': pid,
                'id_wsoucont': safe_int(p.get('IDWSOUCONT')),
                'date_panne': safe_str(p.get('DATE'), 20),
                'jour': safe_str(p.get('JOUR'), 20),
                'depanneur': safe_str(p.get('DEPANNEUR'), 100),
                'libelle': safe_str(p.get('PANNES'), 200),
                'heure_inter': safe_str(p.get('INTER'), 20),
                'heure_fin': safe_str(p.get('HRFININTER'), 20),
                'duree': safe_int(p.get('DUREE')),
                'ensemble': safe_int(p.get('ENSEMBLE')),
                'local_code': safe_int(p.get('LOCAL_')),
                'cause': safe_int(p.get('CAUSE')),
                'motif': safe_str(p.get('MOTIF'), 100),
                'note': safe_str(p.get('NOTE2'), 500),
                'data': json.dumps(p),
                'updated_at': datetime.now().isoformat()
            })
    
    for i in range(0, len(pannes_list), 30):
        supabase_upsert('pannes', pannes_list[i:i+30])
    
    next_idx = period_idx + 1
    next_url = f"?step=3&period={next_idx}" if next_idx < len(PERIODS) else None
    
    result = {
        "status": "success",
        "step": 3,
        "period": period,
        "period_index": f"{period_idx + 1}/{len(PERIODS)}",
        "pannes": len(pannes_list)
    }
    
    if next_url:
        result["next"] = next_url
    else:
        result["message"] = "SYNC COMPLETE!"
        supabase_insert('sync_logs', {'sync_date': datetime.now().isoformat(), 'status': 'success'})
    
    return result

# ==== CRON: Sync rapide (arrêts + pannes récentes) ====
def sync_cron():
    """Sync rapide pour le cron horaire - arrêts + pannes récentes uniquement"""
    start = datetime.now()
    stats = {"arrets": 0, "pannes": 0}
    
    wsid = get_auth()
    if not wsid:
        return {"status": "error", "message": "Auth failed"}
    
    # 1. Arrêts
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
    
    # 2. Pannes récentes (dernier mois)
    resp = progilift_call("get_Synchro_Wpanne", {"dhDerniereMajFichier": "2025-12-01T00:00:00"}, wsid, 60)
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
    
    duration = (datetime.now() - start).total_seconds()
    
    # Log
    supabase_insert('sync_logs', {
        'sync_date': datetime.now().isoformat(),
        'status': 'cron',
        'equipements_count': 0,
        'pannes_count': stats["pannes"],
        'duration_seconds': round(duration, 1)
    })
    
    return {
        "status": "success",
        "mode": "cron",
        "stats": stats,
        "duration": round(duration, 1)
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
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            
            step = params.get('step', ['0'])[0]
            sector = int(params.get('sector', ['0'])[0])
            period = int(params.get('period', ['0'])[0])
            mode = params.get('mode', [''])[0]
            
            if mode == 'cron':
                result = sync_cron()
            elif step == '1':
                result = sync_arrets()
            elif step == '2':
                result = sync_equipements(sector)
            elif step == '2b':
                result = sync_passages(sector)
            elif step == '3':
                result = sync_pannes(period)
            else:
                result = {
                    "status": "ready",
                    "message": "Progilift Sync API",
                    "sectors_count": len(SECTORS),
                    "periods_count": len(PERIODS),
                    "endpoints": {
                        "cron": "?mode=cron → Sync rapide (arrêts + pannes récentes)",
                        "step1": "?step=1 → Arrêts",
                        "step2": "?step=2&sector=0 → Équipements (0-21)",
                        "step2b": "?step=2b&sector=0 → 10 derniers passages (0-21)",
                        "step3": "?step=3&period=0 → Pannes (0-6)"
                    },
                    "start_full": "?step=1",
                    "start_cron": "?mode=cron"
                }
        except Exception as e:
            result = {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}
        
        body = json.dumps(result, ensure_ascii=False).encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)
    
    def log_message(self, format, *args):
        pass
