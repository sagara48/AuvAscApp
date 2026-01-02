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
    status, resp = http_request(url, 'POST', json.dumps(data).encode(), headers, 30)
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
    status, resp = http_request(url, 'POST', json.dumps(data).encode(), headers, 30)
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

def get_auth():
    resp = progilift_call("IdentificationTechnicien", {"sSteCodeWeb": PROGILIFT_CODE}, None, 30)
    if resp:
        m = re.search(r'WSID[^>]*>([A-F0-9]+)<', resp, re.IGNORECASE)
        if m:
            return m.group(1)
    return None

def get_sectors(wsid):
    resp = progilift_call("get_Synchro_Wsect", {"dhDerniereMajFichier": "2000-01-01T00:00:00"}, wsid, 30)
    return [s.strip() for s in re.findall(r'<SECTEUR>([^<]+)</SECTEUR>', resp or '') if s.strip()]

# ==== DEBUG: Voir les données brutes ====
def debug_info():
    wsid = get_auth()
    if not wsid:
        return {"status": "error", "message": "Auth failed"}
    
    # Secteurs
    sectors = get_sectors(wsid)
    
    # Arrêts - données brutes
    resp_arrets = progilift_call("get_AppareilsArret", {}, wsid, 30)
    arrets_raw = parse_items(resp_arrets, "tabListeArrets")
    
    # Essayer aussi d'autres tags possibles
    arrets_alt1 = parse_items(resp_arrets, "ArretAppareil")
    arrets_alt2 = parse_items(resp_arrets, "Arret")
    
    # Extrait du XML brut (premiers 2000 caractères)
    xml_sample = resp_arrets[:2000] if resp_arrets else "No response"
    
    return {
        "status": "debug",
        "wsid": wsid[:10] + "...",
        "sectors": sectors,
        "sectors_count": len(sectors),
        "arrets": {
            "count_tabListeArrets": len(arrets_raw),
            "count_ArretAppareil": len(arrets_alt1),
            "count_Arret": len(arrets_alt2),
            "sample_data": arrets_raw[:3] if arrets_raw else arrets_alt1[:3] if arrets_alt1 else arrets_alt2[:3] if arrets_alt2 else [],
            "xml_sample": xml_sample
        }
    }

# ==== STEP 1: Arrêts ====
def sync_arrets():
    wsid = get_auth()
    if not wsid:
        return {"status": "error", "message": "Auth failed"}
    
    resp = progilift_call("get_AppareilsArret", {}, wsid, 30)
    
    # Essayer plusieurs tags
    arrets = parse_items(resp, "tabListeArrets")
    if not arrets:
        arrets = parse_items(resp, "ArretAppareil")
    if not arrets:
        arrets = parse_items(resp, "Arret")
    
    # Debug: retourner le XML si vide
    if not arrets:
        return {
            "status": "warning",
            "step": 1,
            "arrets": 0,
            "message": "Aucun appareil à l'arrêt trouvé (ou parsing échoué)",
            "xml_sample": resp[:1500] if resp else "No response",
            "next": "?step=2&sector=0"
        }
    
    supabase_delete('appareils_arret')
    inserted = 0
    for a in arrets:
        # Essayer différents noms de champs
        id_ws = safe_int(a.get('nIDSOUCONT') or a.get('IDWSOUCONT') or a.get('IdWsoucont'))
        result = supabase_insert('appareils_arret', {
            'id_wsoucont': id_ws,
            'id_panne': safe_int(a.get('nClepanne') or a.get('CLEPANNE') or a.get('IdPanne')),
            'date_appel': safe_str(a.get('sDateAppel') or a.get('DATEAPPEL') or a.get('DateAppel'), 20),
            'heure_appel': safe_str(a.get('sHeureAppel') or a.get('HEUREAPPEL') or a.get('HeureAppel'), 20),
            'motif': safe_str(a.get('sMotifAppel') or a.get('MOTIFAPPEL') or a.get('MotifAppel') or a.get('Motif'), 500),
            'demandeur': safe_str(a.get('sDemandeur') or a.get('DEMANDEUR') or a.get('Demandeur'), 100),
            'updated_at': datetime.now().isoformat()
        })
        if result:
            inserted += 1
    
    return {"status": "success", "step": 1, "arrets": len(arrets), "inserted": inserted, "next": "?step=2&sector=0"}

# ==== STEP 2: Équipements par secteur ====
def sync_equipements(sector_idx):
    wsid = get_auth()
    if not wsid:
        return {"status": "error", "message": "Auth failed"}
    
    sectors = get_sectors(wsid)
    if sector_idx >= len(sectors):
        return {"status": "success", "step": 2, "message": "All sectors done", "next": "?step=3&period=0"}
    
    sector = sectors[sector_idx]
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
        supabase_upsert('equipements', equip_list[i:i+30])
    
    next_idx = sector_idx + 1
    next_url = f"?step=2&sector={next_idx}" if next_idx < len(sectors) else "?step=3&period=0"
    
    return {
        "status": "success",
        "step": 2,
        "sector": sector,
        "sector_index": f"{sector_idx + 1}/{len(sectors)}",
        "equipements": len(equip_list),
        "next": next_url
    }

# ==== STEP 3: Pannes par période ====
PERIODS = [
    "2025-10-01T00:00:00",
    "2025-07-01T00:00:00",
    "2025-01-01T00:00:00",
    "2024-01-01T00:00:00",
    "2023-01-01T00:00:00",
    "2022-01-01T00:00:00",
    "2020-01-01T00:00:00"
]

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
                'depanneur': safe_str(p.get('DEPANNEUR'), 100),
                'libelle': safe_str(p.get('PANNES'), 200),
                'heure_inter': safe_str(p.get('INTER'), 20),
                'heure_fin': safe_str(p.get('HRFININTER'), 20),
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
            
            step = int(params.get('step', ['0'])[0])
            sector = int(params.get('sector', ['0'])[0])
            period = int(params.get('period', ['0'])[0])
            debug = params.get('debug', ['0'])[0] == '1'
            
            if debug:
                result = debug_info()
            elif step == 1:
                result = sync_arrets()
            elif step == 2:
                result = sync_equipements(sector)
            elif step == 3:
                result = sync_pannes(period)
            else:
                wsid = get_auth()
                sectors = get_sectors(wsid) if wsid else []
                result = {
                    "status": "ready",
                    "message": "Sync par étapes",
                    "sectors": sectors,
                    "sectors_count": len(sectors),
                    "periods_count": len(PERIODS),
                    "usage": {
                        "debug": "?debug=1 → Voir données brutes",
                        "step1": "?step=1 → Arrêts",
                        "step2": "?step=2&sector=0 → Équipements",
                        "step3": "?step=3&period=0 → Pannes"
                    },
                    "start": "?step=1"
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
