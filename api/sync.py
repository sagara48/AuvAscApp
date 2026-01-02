"""
Progilift Sync - Version COMPLETE avec mapping corrigé
Les champs DIV1-DIV15 sont génériques (varient selon le type d'équipement)
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
ssl_context = ssl.create_default_context()

def safe_str(value, max_len=None):
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    return s[:max_len] if max_len else s

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
            return resp.status, resp.read().decode('utf-8'), dict(resp.headers)
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8') if e.fp else ''
        return e.code, body, {}
    except Exception as e:
        return 0, str(e), {}

class SupabaseClient:
    def __init__(self, url, key):
        self.url = url.rstrip('/')
        self.headers = {'apikey': key, 'Authorization': f'Bearer {key}', 'Content-Type': 'application/json', 'Prefer': 'return=minimal'}
        self.last_error = None
    
    def insert(self, table, data):
        status, body, _ = http_request(f"{self.url}/rest/v1/{table}", method='POST', data=json.dumps(data).encode('utf-8'), headers=self.headers, timeout=10)
        if status not in [200, 201, 204]:
            self.last_error = f"{table}: {status} - {body[:200]}"
        return status in [200, 201, 204]
    
    def upsert(self, table, data, on_conflict):
        headers = {**self.headers, 'Prefer': 'resolution=merge-duplicates'}
        status, body, _ = http_request(f"{self.url}/rest/v1/{table}", method='POST', data=json.dumps(data).encode('utf-8'), headers=headers, timeout=60)
        if status not in [200, 201, 204]:
            self.last_error = f"{table}: {status} - {body[:500]}"
            return False
        return True
    
    def delete(self, table):
        status, _, _ = http_request(f"{self.url}/rest/v1/{table}?id=gte.0", method='DELETE', headers=self.headers, timeout=10)
        return status in [200, 204]

class ProgiliftClient:
    WS_URL = "https://ws.progilift.fr/WS_PROGILIFT_20230419_WEB/awws/WS_Progilift_20230419.awws"
    NS = "urn:WS_Progilift"
    SOAP = '''<?xml version="1.0" encoding="UTF-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ws="urn:WS_Progilift" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><soap:Header>{wsid}</soap:Header><soap:Body><ws:{method}>{params}</ws:{method}></soap:Body></soap:Envelope>'''

    def __init__(self, auth_code):
        self.auth_code = auth_code.upper()
        self.wsid = None
        self.sectors = []
    
    def _call(self, method, params=None, timeout=60):
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
        return body if status == 200 and "Fault" not in body else None
    
    def _parse(self, xml, tag):
        items = []
        for m in re.finditer(f'<{tag}>(.*?)</{tag}>', xml or '', re.DOTALL):
            item = {}
            for f in re.finditer(r'<([A-Za-z0-9_]+)>([^<]*)</\1>', m.group(1)):
                val = f.group(2).strip()
                item[f.group(1)] = int(val) if val.lstrip('-').isdigit() else val if val else None
            if item:
                items.append(item)
        return items
    
    def authenticate(self):
        resp = self._call("IdentificationTechnicien", {"sSteCodeWeb": self.auth_code})
        if resp:
            m = re.search(r'WSID[^>]*>([A-F0-9]+)<', resp, re.IGNORECASE)
            if m:
                self.wsid = m.group(1)
            return True
        return False
    
    def get_sectors(self):
        resp = self._call("get_Synchro_Wsect", {"dhDerniereMajFichier": "2000-01-01T00:00:00"})
        self.sectors = [s.strip() for s in re.findall(r'<SECTEUR>([^<]+)</SECTEUR>', resp or '') if s.strip()]
        return self.sectors
    
    def get_wsoucont(self):
        items = []
        for sector in self.sectors:
            resp = self._call("get_Synchro_Wsoucont", {"dhDerniereMajFichier": "2000-01-01T00:00:00", "sListeSecteursTechnicien": sector}, timeout=120)
            items.extend(self._parse(resp, "tabListeWsoucont"))
        return items
    
    def get_wsoucont2(self):
        items = []
        for sector in self.sectors:
            resp = self._call("get_Synchro_Wsoucont2", {"dhDerniereMajFichier": "2000-01-01T00:00:00", "sListeSecteursTechnicien": sector}, timeout=120)
            items.extend(self._parse(resp, "tabListeWsoucont2"))
        return items
    
    def get_pannes(self):
        all_pannes = []
        seen = set()
        periods = [
            "2025-10-01T00:00:00", "2025-07-01T00:00:00", "2025-04-01T00:00:00", 
            "2025-01-01T00:00:00", "2024-07-01T00:00:00", "2024-01-01T00:00:00",
            "2023-01-01T00:00:00", "2022-01-01T00:00:00", "2020-01-01T00:00:00"
        ]
        for period in periods:
            resp = self._call("get_Synchro_Wpanne", {"dhDerniereMajFichier": period}, timeout=120)
            pannes = self._parse(resp, "tabListeWpanne")
            for p in pannes:
                pid = p.get('P0CLEUNIK')
                if pid and pid not in seen:
                    seen.add(pid)
                    all_pannes.append(p)
        return all_pannes
    
    def get_appareils_arret(self):
        resp = self._call("get_AppareilsArret", {})
        return self._parse(resp, "tabListeArrets")

def run_sync(full_sync=False):
    start = datetime.now()
    stats = {"equipements": 0, "pannes": 0, "appareils_arret": 0}
    errors = []
    
    try:
        pg = ProgiliftClient(PROGILIFT_CODE)
        sb = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
        
        if not pg.authenticate():
            raise Exception("Auth failed")
        pg.get_sectors()
        
        # =====================================================================
        # EQUIPEMENTS - Fusion Wsoucont + Wsoucont2
        # =====================================================================
        wsoucont = pg.get_wsoucont()
        wsoucont2 = pg.get_wsoucont2()
        
        equip_map = {}
        
        # Wsoucont (données de base)
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
                    # Adresse
                    'adresse': safe_str(e.get('DES2'), 200),
                    'ville': safe_str(e.get('DES3'), 200),
                    'des4': safe_str(e.get('DES4'), 200),
                    'des6': safe_str(e.get('DES6'), 200),
                    'des7': safe_str(e.get('DES7'), 200),
                    # Champs DIV génériques
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
                    'div16': safe_str(e.get('DIV16'), 200),
                    'div17': safe_str(e.get('DIV17'), 200),
                    'div18': safe_str(e.get('DIV18'), 200),
                    'div19': safe_str(e.get('DIV19'), 200),
                    'div20': safe_str(e.get('DIV20'), 200),
                    # Autres champs
                    'refcli': safe_str(e.get('REFCLI'), 50),
                    'refcli2': safe_str(e.get('REFCLI2'), 50),
                    'refcli3': safe_str(e.get('REFCLI3'), 50),
                    'numappcli': safe_str(e.get('NUMAPPCLI'), 50),
                    'nom_convivial': safe_str(e.get('NOM_CONVIVIAL'), 100),
                    'localisation': safe_str(e.get('LOCALISATION'), 200),
                    'telcabine': safe_str(e.get('TELCABINE'), 50),
                    'idtype_depannage': safe_int(e.get('IDTYPE_DEPANNAGE')),
                    'securite': safe_int(e.get('SECURITE')),
                    'securite2': safe_int(e.get('SECURITE2')),
                    'typeplanning': safe_str(e.get('TYPEPLANNING'), 50),
                    'wordre': safe_int(e.get('WORDRE')),
                    'ordre2': safe_int(e.get('ORDRE2')),
                    'code_acquittement': safe_str(e.get('CODE_ACQUITTEMENT'), 50),
                    'date_heure_modif': safe_str(e.get('DATE_HEURE_MODIF'), 30),
                    # Planning mensuel
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
                    # JSON complet
                    'data_wsoucont': json.dumps(e),
                    'updated_at': datetime.now().isoformat()
                }
        
        # Wsoucont2 (10 derniers passages)
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
        
        # =====================================================================
        # PANNES
        # =====================================================================
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
        
        # =====================================================================
        # APPAREILS A L'ARRET
        # =====================================================================
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
        
        # Log
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
        try:
            SupabaseClient(SUPABASE_URL, SUPABASE_KEY).insert('sync_logs', {
                'sync_date': datetime.now().isoformat(),
                'status': 'error',
                'error_message': str(e)[:500]
            })
        except:
            pass
        return {"status": "error", "message": str(e)}

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(run_sync()).encode())
    
    def do_POST(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(run_sync('full' in self.path)).encode())
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
