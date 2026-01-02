"""
Progilift Sync - Version robuste avec gestion d'erreurs
"""

import os
import json
import re
import ssl
import urllib.request
import urllib.error
import traceback
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
        if not s:
            return None
        return s[:max_len] if max_len else s
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
            return resp.status, resp.read().decode('utf-8'), dict(resp.headers)
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode('utf-8') if e.fp else ''
        except:
            body = ''
        return e.code, body, {}
    except urllib.error.URLError as e:
        return 0, f"URLError: {str(e)}", {}
    except Exception as e:
        return 0, f"Error: {str(e)}", {}

class SupabaseClient:
    def __init__(self, url, key):
        self.url = url.rstrip('/') if url else ''
        self.headers = {
            'apikey': key or '',
            'Authorization': f'Bearer {key}' if key else '',
            'Content-Type': 'application/json',
            'Prefer': 'return=minimal'
        }
        self.last_error = None
    
    def insert(self, table, data):
        if not self.url:
            self.last_error = "No Supabase URL"
            return False
        try:
            status, body, _ = http_request(
                f"{self.url}/rest/v1/{table}",
                method='POST',
                data=json.dumps(data).encode('utf-8'),
                headers=self.headers,
                timeout=15
            )
            if status not in [200, 201, 204]:
                self.last_error = f"{table}: {status} - {body[:200]}"
                return False
            return True
        except Exception as e:
            self.last_error = f"{table}: {str(e)}"
            return False
    
    def upsert(self, table, data, on_conflict):
        if not self.url:
            self.last_error = "No Supabase URL"
            return False
        try:
            headers = {**self.headers, 'Prefer': 'resolution=merge-duplicates'}
            status, body, _ = http_request(
                f"{self.url}/rest/v1/{table}",
                method='POST',
                data=json.dumps(data).encode('utf-8'),
                headers=headers,
                timeout=60
            )
            if status not in [200, 201, 204]:
                self.last_error = f"{table}: {status} - {body[:300]}"
                return False
            return True
        except Exception as e:
            self.last_error = f"{table}: {str(e)}"
            return False
    
    def delete(self, table):
        if not self.url:
            return False
        try:
            status, _, _ = http_request(
                f"{self.url}/rest/v1/{table}?id=gte.0",
                method='DELETE',
                headers=self.headers,
                timeout=15
            )
            return status in [200, 204]
        except:
            return False

class ProgiliftClient:
    WS_URL = "https://ws.progilift.fr/WS_PROGILIFT_20230419_WEB/awws/WS_Progilift_20230419.awws"
    NS = "urn:WS_Progilift"
    SOAP = '''<?xml version="1.0" encoding="UTF-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ws="urn:WS_Progilift" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><soap:Header>{wsid}</soap:Header><soap:Body><ws:{method}>{params}</ws:{method}></soap:Body></soap:Envelope>'''

    def __init__(self, auth_code):
        self.auth_code = auth_code.upper() if auth_code else ''
        self.wsid = None
        self.sectors = []
        self.last_error = None
    
    def _call(self, method, params=None, timeout=60):
        try:
            params_xml = ""
            if params:
                for k, v in params.items():
                    if v is not None:
                        v = str(v).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                        params_xml += f"<ws:{k}>{v}</ws:{k}>"
            wsid_h = f'<ws:WSID xsi:type="xsd:hexBinary" soap:mustUnderstand="1">{self.wsid}</ws:WSID>' if self.wsid else ""
            soap = self.SOAP.format(method=method, params=params_xml, wsid=wsid_h)
            headers = {'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': f'"{self.NS}/{method}"'}
            status, body, _ = http_request(self.WS_URL, method='POST', data=soap.encode('utf-8'), headers=headers, timeout=timeout)
            if status == 200 and "Fault" not in body:
                return body
            self.last_error = f"{method}: status={status}"
            return None
        except Exception as e:
            self.last_error = f"{method}: {str(e)}"
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
        resp = self._call("IdentificationTechnicien", {"sSteCodeWeb": self.auth_code}, timeout=30)
        if resp:
            m = re.search(r'WSID[^>]*>([A-F0-9]+)<', resp, re.IGNORECASE)
            if m:
                self.wsid = m.group(1)
                return True
        return False
    
    def get_sectors(self):
        resp = self._call("get_Synchro_Wsect", {"dhDerniereMajFichier": "2000-01-01T00:00:00"}, timeout=30)
        self.sectors = [s.strip() for s in re.findall(r'<SECTEUR>([^<]+)</SECTEUR>', resp or '') if s.strip()]
        return self.sectors
    
    def get_wsoucont(self):
        items = []
        for sector in self.sectors:
            resp = self._call("get_Synchro_Wsoucont", {"dhDerniereMajFichier": "2000-01-01T00:00:00", "sListeSecteursTechnicien": sector}, timeout=90)
            items.extend(self._parse(resp, "tabListeWsoucont"))
        return items
    
    def get_wsoucont2(self):
        items = []
        for sector in self.sectors:
            resp = self._call("get_Synchro_Wsoucont2", {"dhDerniereMajFichier": "2000-01-01T00:00:00", "sListeSecteursTechnicien": sector}, timeout=90)
            items.extend(self._parse(resp, "tabListeWsoucont2"))
        return items
    
    def get_pannes(self):
        all_pannes = []
        seen = set()
        periods = ["2025-07-01T00:00:00", "2025-01-01T00:00:00", "2024-01-01T00:00:00", "2022-01-01T00:00:00"]
        for period in periods:
            resp = self._call("get_Synchro_Wpanne", {"dhDerniereMajFichier": period}, timeout=90)
            pannes = self._parse(resp, "tabListeWpanne")
            for p in pannes:
                pid = p.get('P0CLEUNIK')
                if pid and pid not in seen:
                    seen.add(pid)
                    all_pannes.append(p)
        return all_pannes
    
    def get_appareils_arret(self):
        resp = self._call("get_AppareilsArret", {}, timeout=30)
        return self._parse(resp, "tabListeArrets")

def run_sync(full_sync=False):
    start = datetime.now()
    stats = {"equipements": 0, "pannes": 0, "appareils_arret": 0}
    errors = []
    
    # Vérifier config
    if not SUPABASE_URL:
        return {"status": "error", "message": "SUPABASE_URL not configured"}
    if not SUPABASE_KEY:
        return {"status": "error", "message": "SUPABASE_KEY not configured"}
    if not PROGILIFT_CODE:
        return {"status": "error", "message": "PROGILIFT_CODE not configured"}
    
    try:
        pg = ProgiliftClient(PROGILIFT_CODE)
        sb = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
        
        # Auth
        if not pg.authenticate():
            return {"status": "error", "message": f"Progilift auth failed: {pg.last_error}"}
        
        # Sectors
        sectors = pg.get_sectors()
        if not sectors:
            return {"status": "error", "message": "No sectors found"}
        
        # EQUIPEMENTS
        wsoucont = pg.get_wsoucont()
        wsoucont2 = pg.get_wsoucont2()
        
        equip_map = {}
        for e in wsoucont:
            id_ws = safe_int(e.get('IDWSOUCONT'))
            if id_ws:
                equip_map[id_ws] = {
                    'id_wsoucont': id_ws,
                    'id_wcontrat': safe_int(e.get('IDWCONTRAT')),
                    'secteur': safe_str(e.get('SECTEUR'), 20),
                    'ascenseur': safe_str(e.get('ASCENSEUR'), 50),
                    'indice': safe_str(e.get('INDICE'), 20),
                    'genre': safe_int(e.get('GENRE')),
                    'type_appareil': safe_int(e.get('TYPE')),
                    'adresse': safe_str(e.get('DES2'), 200),
                    'ville': safe_str(e.get('DES3'), 200),
                    'des4': safe_str(e.get('DES4'), 200),
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
                    'div15': safe_str(e.get('DIV15'), 200),
                    'refcli': safe_str(e.get('REFCLI'), 50),
                    'numappcli': safe_str(e.get('NUMAPPCLI'), 50),
                    'nom_convivial': safe_str(e.get('NOM_CONVIVIAL'), 100),
                    'localisation': safe_str(e.get('LOCALISATION'), 200),
                    'telcabine': safe_str(e.get('TELCABINE'), 50),
                    'idtype_depannage': safe_int(e.get('IDTYPE_DEPANNAGE')),
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
                }
        
        for e in wsoucont2:
            id_ws = safe_int(e.get('IDWSOUCONT'))
            if id_ws:
                if id_ws in equip_map:
                    for i in range(1, 11):
                        equip_map[id_ws][f'lib{i}'] = safe_str(e.get(f'LIB{i}'), 100)
                        equip_map[id_ws][f'datepass{i}'] = safe_int(e.get(f'DATEPASS{i}'))
                    equip_map[id_ws]['data_wsoucont2'] = json.dumps(e)
                else:
                    equip_map[id_ws] = {
                        'id_wsoucont': id_ws,
                        'id_wcontrat': safe_int(e.get('IDWCONTRAT')),
                        'ascenseur': safe_str(e.get('ASCENSEUR'), 50),
                        'data_wsoucont2': json.dumps(e),
                        'updated_at': datetime.now().isoformat()
                    }
                    for i in range(1, 11):
                        equip_map[id_ws][f'lib{i}'] = safe_str(e.get(f'LIB{i}'), 100)
                        equip_map[id_ws][f'datepass{i}'] = safe_int(e.get(f'DATEPASS{i}'))
        
        records = list(equip_map.values())
        for i in range(0, len(records), 50):
            if not sb.upsert('equipements', records[i:i+50], 'id_wsoucont'):
                errors.append(sb.last_error)
        stats["equipements"] = len(records)
        
        # PANNES
        pannes = pg.get_pannes()
        records = []
        for p in pannes:
            r = {
                'id_panne': safe_int(p.get('P0CLEUNIK')),
                'id_wsoucont': safe_int(p.get('IDWSOUCONT')),
                'date_panne': safe_str(p.get('DATE'), 20),
                'jour': safe_str(p.get('JOUR'), 20),
                'heure_inter': safe_str(p.get('INTER'), 20),
                'heure_fin': safe_str(p.get('HRFININTER'), 20),
                'depanneur': safe_str(p.get('DEPANNEUR'), 100),
                'libelle': safe_str(p.get('PANNES') or p.get('Libelle'), 200),
                'ensemble': safe_int(p.get('ENSEMBLE')),
                'local': safe_int(p.get('LOCAL_')),
                'cause': safe_int(p.get('CAUSE')),
                'motif': safe_str(p.get('MOTIF'), 100),
                'note': safe_str(p.get('NOTE2'), 500),
                'data': json.dumps(p),
                'updated_at': datetime.now().isoformat()
            }
            if r['id_panne']:
                records.append(r)
        
        for i in range(0, len(records), 50):
            if not sb.upsert('pannes', records[i:i+50], 'id_panne'):
                errors.append(sb.last_error)
        stats["pannes"] = len(records)
        
        # ARRETS
        arrets = pg.get_appareils_arret()
        sb.delete('appareils_arret')
        for a in arrets:
            sb.insert('appareils_arret', {
                'id_wsoucont': safe_int(a.get('nIDSOUCONT')),
                'id_panne': safe_int(a.get('nClepanne')),
                'date_appel': safe_str(a.get('sDateAppel'), 20),
                'heure_appel': safe_str(a.get('sHeureAppel'), 20),
                'motif': safe_str(a.get('sMotifAppel'), 500),
                'demandeur': safe_str(a.get('sDemandeur'), 100),
                'note': safe_str(a.get('sNoteAppel'), 500),
                'data': json.dumps(a),
                'updated_at': datetime.now().isoformat()
            })
        stats["appareils_arret"] = len(arrets)
        
        # LOG
        duration = (datetime.now() - start).total_seconds()
        sb.insert('sync_logs', {
            'sync_date': datetime.now().isoformat(),
            'status': 'success' if not errors else 'partial',
            'equipements_count': stats["equipements"],
            'pannes_count': stats["pannes"],
            'duration_seconds': round(duration, 1),
            'error_message': '; '.join(errors[:3]) if errors else None
        })
        
        result = {"status": "success", "stats": stats, "duration": round(duration, 1)}
        if errors:
            result["errors"] = errors[:5]
        return result
        
    except Exception as e:
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        try:
            SupabaseClient(SUPABASE_URL, SUPABASE_KEY).insert('sync_logs', {
                'sync_date': datetime.now().isoformat(),
                'status': 'error',
                'error_message': error_msg[:500]
            })
        except:
            pass
        return {"status": "error", "message": str(e), "traceback": traceback.format_exc()[:500]}

class handler(BaseHTTPRequestHandler):
    def send_json(self, data):
        try:
            body = json.dumps(data).encode('utf-8')
        except Exception as e:
            body = json.dumps({"status": "error", "message": f"JSON encode error: {str(e)}"}).encode('utf-8')
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)
    
    def do_GET(self):
        try:
            result = run_sync()
        except Exception as e:
            result = {"status": "error", "message": str(e)}
        self.send_json(result)
    
    def do_POST(self):
        try:
            full = 'full' in self.path
            result = run_sync(full)
        except Exception as e:
            result = {"status": "error", "message": str(e)}
        self.send_json(result)
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def log_message(self, format, *args):
        pass  # Suppress logs
