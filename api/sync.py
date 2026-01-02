"""
Progilift Sync - Vercel Serverless Function
Version sans dépendances externes (urllib uniquement)
"""

import os
import json
import re
import ssl
import urllib.request
import urllib.parse
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from typing import Optional, Dict, Any, List

# Configuration
SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')
PROGILIFT_CODE = os.environ.get('PROGILIFT_CODE', 'AUVNB1')

# SSL context
ssl_context = ssl.create_default_context()


def http_request(url: str, method: str = 'GET', data: bytes = None, headers: dict = None, timeout: int = 60) -> tuple:
    """Requête HTTP simple avec urllib"""
    try:
        req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
        with urllib.request.urlopen(req, timeout=timeout, context=ssl_context) as resp:
            return resp.status, resp.read().decode('utf-8'), dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode('utf-8') if e.fp else '', {}
    except Exception as e:
        return 0, str(e), {}


# ============================================================================
# CLIENT SUPABASE
# ============================================================================

class SupabaseClient:
    def __init__(self, url: str, key: str):
        self.url = url.rstrip('/')
        self.headers = {
            'apikey': key,
            'Authorization': f'Bearer {key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=minimal'
        }
    
    def insert(self, table: str, data: dict) -> bool:
        status, body, _ = http_request(
            f"{self.url}/rest/v1/{table}",
            method='POST',
            data=json.dumps(data).encode('utf-8'),
            headers=self.headers,
            timeout=10
        )
        return status in [200, 201, 204]
    
    def upsert(self, table: str, data: list, on_conflict: str) -> bool:
        headers = {**self.headers, 'Prefer': 'resolution=merge-duplicates'}
        status, body, _ = http_request(
            f"{self.url}/rest/v1/{table}",
            method='POST',
            data=json.dumps(data).encode('utf-8'),
            headers=headers,
            timeout=30
        )
        return status in [200, 201, 204]
    
    def delete(self, table: str) -> bool:
        status, _, _ = http_request(
            f"{self.url}/rest/v1/{table}?id=gte.0",
            method='DELETE',
            headers=self.headers,
            timeout=10
        )
        return status in [200, 204]


# ============================================================================
# CLIENT PROGILIFT
# ============================================================================

class ProgiliftClient:
    WS_URL = "https://ws.progilift.fr/WS_PROGILIFT_20230419_WEB/awws/WS_Progilift_20230419.awws"
    NS = "urn:WS_Progilift"
    OLD_DATE = "2000-01-01T00:00:00"
    
    SOAP_TEMPLATE = '''<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" 
               xmlns:ws="urn:WS_Progilift"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <soap:Header>{wsid_header}</soap:Header>
    <soap:Body>
        <ws:{method}>{params}</ws:{method}>
    </soap:Body>
</soap:Envelope>'''

    def __init__(self, auth_code: str):
        self.auth_code = auth_code.upper()
        self.wsid = None
        self.tech_id = None
        self.sectors = []
    
    def _call(self, method: str, params: Dict = None, timeout: int = 60) -> Optional[str]:
        params_xml = ""
        if params:
            for key, value in params.items():
                if value is not None:
                    if isinstance(value, str):
                        value = value.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                    params_xml += f"<ws:{key}>{value}</ws:{key}>"
        
        wsid_header = ""
        if self.wsid:
            wsid_header = f'<ws:WSID xsi:type="xsd:hexBinary" soap:mustUnderstand="1">{self.wsid}</ws:WSID>'
        
        soap = self.SOAP_TEMPLATE.format(method=method, params=params_xml, wsid_header=wsid_header)
        headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': f'"{self.NS}/{method}"',
            'User-Agent': 'ProgiliftSync/1.0'
        }
        
        status, body, _ = http_request(self.WS_URL, method='POST', data=soap.encode('utf-8'), headers=headers, timeout=timeout)
        
        if status == 200 and "Fault" not in body:
            return body
        return None
    
    def _convert(self, value: str) -> Any:
        if not value or not value.strip():
            return None
        value = value.strip()
        if value.lstrip('-').isdigit():
            return int(value)
        return value
    
    def _parse_items(self, xml: str, tag: str) -> List[Dict]:
        items = []
        pattern = f'<{tag}>(.*?)</{tag}>'
        for match in re.finditer(pattern, xml, re.DOTALL):
            block = match.group(1)
            item = {}
            for field_match in re.finditer(r'<([A-Za-z0-9_]+)>([^<]*)</\1>', block):
                item[field_match.group(1)] = self._convert(field_match.group(2))
            if item:
                items.append(item)
        return items
    
    def authenticate(self) -> bool:
        response = self._call("IdentificationTechnicien", {"sSteCodeWeb": self.auth_code})
        if response:
            match = re.search(r'<[^>]*WSID[^>]*>([A-F0-9]+)</[^>]*WSID>', response, re.IGNORECASE)
            if match:
                self.wsid = match.group(1)
            id_match = re.search(r'<ID>(\d+)</ID>', response)
            if id_match:
                self.tech_id = int(id_match.group(1))
            return True
        return False
    
    def get_sectors(self) -> List[str]:
        response = self._call("get_Synchro_Wsect", {"dhDerniereMajFichier": self.OLD_DATE})
        if response:
            secteurs = re.findall(r'<SECTEUR>([^<]+)</SECTEUR>', response)
            self.sectors = [s.strip() for s in secteurs if s.strip()]
        return self.sectors
    
    def get_equipements(self) -> tuple:
        wsoucont = []
        wsoucont2 = []
        
        for sector in self.sectors:
            response = self._call("get_Synchro_Wsoucont", {
                "dhDerniereMajFichier": self.OLD_DATE,
                "sListeSecteursTechnicien": sector
            }, timeout=90)
            if response:
                wsoucont.extend(self._parse_items(response, "tabListeWsoucont"))
            
            response = self._call("get_Synchro_Wsoucont2", {
                "dhDerniereMajFichier": self.OLD_DATE,
                "sListeSecteursTechnicien": sector
            }, timeout=90)
            if response:
                wsoucont2.extend(self._parse_items(response, "tabListeWsoucont2"))
        
        return wsoucont, wsoucont2
    
    def get_pannes(self, since: str = "2025-07-01T00:00:00") -> List[Dict]:
        response = self._call("get_Synchro_Wpanne", {"dhDerniereMajFichier": since}, timeout=90)
        if response:
            return self._parse_items(response, "tabListeWpanne")
        return []
    
    def get_appareils_arret(self) -> List[Dict]:
        response = self._call("get_AppareilsArret", {})
        if response:
            return self._parse_items(response, "tabListeArrets")
        return []


# ============================================================================
# SYNC
# ============================================================================

def run_sync(full_sync: bool = False) -> Dict:
    start_time = datetime.now()
    stats = {"equipements": 0, "pannes": 0, "appareils_arret": 0}
    
    try:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise Exception("SUPABASE_URL et SUPABASE_KEY requis")
        
        progilift = ProgiliftClient(PROGILIFT_CODE)
        supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
        
        if not progilift.authenticate():
            raise Exception("Échec authentification Progilift")
        
        progilift.get_sectors()
        
        # Équipements
        wsoucont, wsoucont2 = progilift.get_equipements()
        
        equip_records = []
        for item in wsoucont2:
            id_ws = item.get('IDWSOUCONT')
            if id_ws:
                record = {
                    'id_wsoucont': id_ws,
                    'id_wcontrat': item.get('IDWCONTRAT'),
                    'ascenseur': (item.get('ASCENSEUR') or '').strip()[:50],
                    'updated_at': datetime.now().isoformat()
                }
                for i in range(1, 11):
                    record[f'lib{i}'] = (item.get(f'LIB{i}') or '')[:100]
                    record[f'datepass{i}'] = item.get(f'DATEPASS{i}')
                equip_records.append(record)
        
        if equip_records:
            for i in range(0, len(equip_records), 50):
                supabase.upsert('equipements', equip_records[i:i+50], 'id_wsoucont')
        stats["equipements"] = len(equip_records)
        
        # Pannes
        pannes = progilift.get_pannes()
        panne_records = []
        for item in pannes:
            panne_records.append({
                'id_panne': item.get('P0CLEUNIK'),
                'id_wsoucont': item.get('IDWSOUCONT'),
                'date_panne': str(item.get('DATE', '')),
                'depanneur': (item.get('DEPANNEUR') or '').strip()[:100],
                'libelle': (item.get('PANNES') or '')[:200],
                'updated_at': datetime.now().isoformat()
            })
        
        if panne_records:
            for i in range(0, len(panne_records), 50):
                supabase.upsert('pannes', panne_records[i:i+50], 'id_panne')
        stats["pannes"] = len(panne_records)
        
        # Appareils arrêt
        arrets = progilift.get_appareils_arret()
        supabase.delete('appareils_arret')
        
        for item in arrets:
            supabase.insert('appareils_arret', {
                'id_wsoucont': item.get('nIDSOUCONT'),
                'date_appel': (item.get('sDateAppel') or '').strip(),
                'heure_appel': (item.get('sHeureAppel') or '').strip(),
                'motif': (item.get('sMotifAppel') or '').strip(),
                'updated_at': datetime.now().isoformat()
            })
        stats["appareils_arret"] = len(arrets)
        
        # Log
        duration = (datetime.now() - start_time).total_seconds()
        supabase.insert('sync_logs', {
            'sync_date': datetime.now().isoformat(),
            'status': 'success',
            'equipements_count': stats["equipements"],
            'pannes_count': stats["pannes"],
            'duration_seconds': round(duration, 1)
        })
        
        return {"status": "success", "stats": stats, "duration": round(duration, 1)}
        
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


# ============================================================================
# HANDLER
# ============================================================================

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        result = run_sync(full_sync=False)
        self.wfile.write(json.dumps(result).encode())
    
    def do_POST(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        result = run_sync(full_sync='full' in self.path)
        self.wfile.write(json.dumps(result).encode())
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
