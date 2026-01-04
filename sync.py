"""
Progilift Sync API - Synchronisation complète vers Supabase
===========================================================
Endpoints:
  ?step=0           → Types planning (table référence nb_visites)
  ?step=1           → Arrêts en cours
  ?step=2&sector=X  → Équipements Wsoucont (0-21)
  ?step=2b&sector=X → Wsoucont2: passages, DAT, TXT (0-21)
  ?step=3&period=X  → Pannes (0-6)
  ?step=4           → Mise à jour nb_visites_an
  ?mode=cron        → Sync rapide (arrêts + pannes récentes)
"""

import os
import json
import re
import ssl
import traceback
import urllib.request
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

# Configuration
SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')
PROGILIFT_CODE = os.environ.get('PROGILIFT_CODE', 'AUVNB1')
WS_URL = "https://ws.progilift.fr/WS_PROGILIFT_20230419_WEB/awws/WS_Progilift_20230419.awws"

# Liste des 22 secteurs
SECTORS = ["1", "2", "3", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "17", "18", "19", "20", "71", "72", "73", "74"]

# Périodes pour les pannes
PERIODS = [
    "2025-10-01T00:00:00",
    "2025-07-01T00:00:00",
    "2025-01-01T00:00:00",
    "2024-01-01T00:00:00",
    "2023-01-01T00:00:00",
    "2022-01-01T00:00:00",
    "2020-01-01T00:00:00"
]

# SSL Context
try:
    ssl_context = ssl.create_default_context()
except:
    ssl_context = ssl._create_unverified_context()

# ============================================================
# UTILITAIRES
# ============================================================

def safe_str(value, max_len=None):
    """Convertit en string sécurisé"""
    if value is None:
        return None
    try:
        s = str(value).strip()
        return s[:max_len] if max_len and s else s if s else None
    except:
        return None

def safe_int(value):
    """Convertit en entier sécurisé"""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except:
        return None

def http_request(url, method='GET', data=None, headers=None, timeout=30):
    """Requête HTTP générique"""
    headers = headers or {}
    if data and isinstance(data, (dict, list)):
        data = json.dumps(data).encode('utf-8')
        headers.setdefault('Content-Type', 'application/json')
    elif data and isinstance(data, str):
        data = data.encode('utf-8')
    
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ssl_context) as resp:
            return resp.status, resp.read().decode('utf-8')
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode('utf-8') if e.fp else str(e)
    except Exception as e:
        return 0, str(e)

# ============================================================
# PROGILIFT API
# ============================================================

def progilift_call(method, params, wsid=None, timeout=60):
    """Appel SOAP à Progilift"""
    wsid_xml = f'<ws:WSID xsi:type="xsd:hexBinary" soap:mustUnderstand="1">{wsid}</ws:WSID>' if wsid else ""
    
    params_xml = ""
    for k, v in params.items():
        params_xml += f"<ws:{k}>{v}</ws:{k}>"
    
    soap = f'''<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" 
               xmlns:ws="urn:WS_Progilift" 
               xmlns:xsd="http://www.w3.org/2001/XMLSchema" 
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <soap:Header>{wsid_xml}</soap:Header>
    <soap:Body>
        <ws:{method}>{params_xml}</ws:{method}>
    </soap:Body>
</soap:Envelope>'''
    
    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': f'"urn:WS_Progilift/{method}"'
    }
    
    status, body = http_request(WS_URL, 'POST', soap, headers, timeout)
    return body if status == 200 else ""

def get_auth():
    """Authentification Progilift"""
    resp = progilift_call("IdentificationTechnicien", {"sSteCodeWeb": PROGILIFT_CODE}, None, 15)
    if resp:
        m = re.search(r'WSID[^>]*>([A-F0-9]+)<', resp, re.IGNORECASE)
        if m:
            return m.group(1)
    return None

def parse_items(xml, tag):
    """Parse les items XML"""
    items = []
    pattern = f'<{tag}>(.*?)</{tag}>'
    for m in re.finditer(pattern, xml, re.DOTALL | re.IGNORECASE):
        item = {}
        for f in re.finditer(r'<([A-Za-z0-9_]+)>([^<]*)</\1>', m.group(1)):
            item[f.group(1)] = f.group(2).strip() if f.group(2).strip() else None
        if item:
            items.append(item)
    return items

# ============================================================
# SUPABASE API
# ============================================================

def supabase_headers():
    """Headers Supabase"""
    return {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal'
    }

def supabase_insert(table, data):
    """Insert dans Supabase"""
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table}"
    headers = supabase_headers()
    headers['Prefer'] = 'resolution=merge-duplicates,return=minimal'
    status, _ = http_request(url, 'POST', data, headers, 15)
    return status in [200, 201]

def supabase_upsert(table, data):
    """Upsert dans Supabase"""
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table}"
    headers = supabase_headers()
    headers['Prefer'] = 'resolution=merge-duplicates,return=minimal'
    status, _ = http_request(url, 'POST', data, headers, 15)
    return status in [200, 201]

def supabase_update(table, key_col, key_val, data):
    """Update dans Supabase"""
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table}?{key_col}=eq.{key_val}"
    status, _ = http_request(url, 'PATCH', data, supabase_headers(), 15)
    return status in [200, 204]

def supabase_delete(table, filter_str=None):
    """Delete dans Supabase"""
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table}"
    if filter_str:
        url += f"?{filter_str}"
    else:
        url += "?id=gt.0"  # Delete all
    status, _ = http_request(url, 'DELETE', None, supabase_headers(), 30)
    return status in [200, 204]

def supabase_get(table, select="*", filter_str=None, limit=None):
    """Get depuis Supabase"""
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table}?select={select}"
    if filter_str:
        url += f"&{filter_str}"
    if limit:
        url += f"&limit={limit}"
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}'
    }
    status, body = http_request(url, 'GET', None, headers, 30)
    if status == 200:
        return json.loads(body)
    return []

# ============================================================
# STEP 0: Types de planning
# ============================================================

def sync_type_planning():
    """Synchronise la table de référence type_planning depuis Wtypepla"""
    wsid = get_auth()
    if not wsid:
        return {"status": "error", "message": "Auth failed"}
    
    resp = progilift_call("get_Synchro_Wtypepla", {"dhDerniereMajFichier": "2000-01-01T00:00:00"}, wsid, 30)
    
    # Parser les items
    items = parse_items(resp, "tabListeWtypepla")
    if not items:
        items = parse_items(resp, "ST_Wtypepla")
    if not items:
        items = parse_items(resp, "Wtypepla")
    
    if not items:
        return {"status": "error", "message": "No data in Wtypepla response", "response_size": len(resp)}
    
    # Supprimer et recréer
    supabase_delete('type_planning')
    inserted = 0
    
    for item in items:
        code = safe_str(item.get('TYPEPLANNING') or item.get('typeplanning'), 50)
        nb_visites = safe_int(item.get('NB_VISITES') or item.get('nb_visites'))
        libelle = safe_str(item.get('LIBELLEPLAN') or item.get('libelleplan'), 200)
        id_type = safe_int(item.get('IDWTYPEPLA') or item.get('idwtypepla'))
        
        if code:
            if supabase_insert('type_planning', {
                'id_wtypepla': id_type,
                'code': code,
                'nb_visites': nb_visites,
                'libelle': libelle,
                'updated_at': datetime.now().isoformat()
            }):
                inserted += 1
    
    return {
        "status": "success",
        "step": 0,
        "type_planning_found": len(items),
        "inserted": inserted,
        "next": "?step=1"
    }

# ============================================================
# STEP 1: Arrêts en cours
# ============================================================

def sync_arrets():
    """Synchronise les appareils à l'arrêt"""
    wsid = get_auth()
    if not wsid:
        return {"status": "error", "message": "Auth failed"}
    
    resp = progilift_call("get_AppareilsArret", {}, wsid, 30)
    arrets = parse_items(resp, "tabListeArrets")
    
    supabase_delete('appareils_arret')
    inserted = 0
    
    for a in arrets:
        if supabase_insert('appareils_arret', {
            'id_wsoucont': safe_int(a.get('nIDSOUCONT')),
            'id_panne': safe_int(a.get('nClepanne')),
            'date_appel': safe_str(a.get('sDateAppel'), 20),
            'heure_appel': safe_str(a.get('sHeureAppel'), 20),
            'motif': safe_str(a.get('sMotifAppel'), 500),
            'demandeur': safe_str(a.get('sDemandeur'), 100),
            'updated_at': datetime.now().isoformat()
        }):
            inserted += 1
    
    return {
        "status": "success",
        "step": 1,
        "arrets_found": len(arrets),
        "inserted": inserted,
        "next": "?step=2&sector=0"
    }

# ============================================================
# STEP 2: Équipements (Wsoucont)
# ============================================================

def sync_equipements(sector_idx):
    """Synchronise les équipements pour un secteur"""
    if sector_idx >= len(SECTORS):
        return {"status": "done", "message": "All sectors completed", "next": "?step=2b&sector=0"}
    
    sector = SECTORS[sector_idx]
    wsid = get_auth()
    if not wsid:
        return {"status": "error", "message": "Auth failed"}
    
    resp = progilift_call("get_Synchro_Wsoucont", {
        "dhDerniereMajFichier": "2000-01-01T00:00:00",
        "sListeSecteursTechnicien": sector
    }, wsid, 120)
    
    items = parse_items(resp, "tabListeWsoucont")
    upserted = 0
    
    for e in items:
        id_wsoucont = safe_int(e.get('IDWSOUCONT'))
        if not id_wsoucont:
            continue
        
        data = {
            'id_wsoucont': id_wsoucont,
            'id_wcontrat': safe_int(e.get('IDWCONTRAT')),
            'secteur': safe_int(e.get('SECTEUR')),
            'ascenseur': safe_str(e.get('ASCENSEUR'), 50),
            'indice': safe_int(e.get('INDICE')),
            'adresse': safe_str(e.get('DES2'), 200),
            'ville': safe_str(e.get('DES3'), 200),
            'code_postal': safe_str(e.get('DES3', '')[:5] if e.get('DES3') else None, 10),
            'genre': safe_int(e.get('GENRE')),
            'type_appareil': safe_str(e.get('TYPE'), 50),
            'marque': safe_str(e.get('DIV1'), 100),
            'modele': safe_str(e.get('DIV2'), 100),
            'num_serie': safe_str(e.get('DIV7'), 100),
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
            'div11': safe_str(e.get('DIV11'), 100),
            'div12': safe_str(e.get('DIV12'), 100),
            'div13': safe_str(e.get('DIV13'), 100),
            'div14': safe_str(e.get('DIV14'), 100),
            'div15': safe_str(e.get('DIV15'), 100),
            'refcli': safe_str(e.get('REFCLI'), 100),
            'refcli2': safe_str(e.get('REFCLI2'), 100),
            'refcli3': safe_str(e.get('REFCLI3'), 100),
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
        
        if supabase_upsert('equipements', data):
            upserted += 1
    
    next_sector = sector_idx + 1
    return {
        "status": "success",
        "step": 2,
        "sector": sector,
        "sector_idx": sector_idx,
        "equipements_found": len(items),
        "upserted": upserted,
        "next": f"?step=2&sector={next_sector}" if next_sector < len(SECTORS) else "?step=2b&sector=0"
    }

# ============================================================
# STEP 2b: Passages et données complémentaires (Wsoucont2)
# ============================================================

def sync_passages(sector_idx):
    """Synchronise les passages (Wsoucont2) pour un secteur"""
    if sector_idx >= len(SECTORS):
        return {"status": "done", "message": "All sectors completed", "next": "?step=3&period=0"}
    
    sector = SECTORS[sector_idx]
    wsid = get_auth()
    if not wsid:
        return {"status": "error", "message": "Auth failed"}
    
    resp = progilift_call("get_Synchro_Wsoucont2", {
        "dhDerniereMajFichier": "2000-01-01T00:00:00",
        "sListeSecteursTechnicien": sector
    }, wsid, 120)
    
    items = parse_items(resp, "tabListeWsoucont2")
    updated = 0
    
    for e in items:
        id_wsoucont = safe_int(e.get('IDWSOUCONT'))
        if not id_wsoucont:
            continue
        
        data = {
            'lib1': safe_str(e.get('LIB1'), 100),
            'lib2': safe_str(e.get('LIB2'), 100),
            'lib3': safe_str(e.get('LIB3'), 100),
            'lib4': safe_str(e.get('LIB4'), 100),
            'lib5': safe_str(e.get('LIB5'), 100),
            'lib6': safe_str(e.get('LIB6'), 100),
            'lib7': safe_str(e.get('LIB7'), 100),
            'lib8': safe_str(e.get('LIB8'), 100),
            'lib9': safe_str(e.get('LIB9'), 100),
            'lib10': safe_str(e.get('LIB10'), 100),
            'datepass1': safe_int(e.get('DATEPASS1')),
            'datepass2': safe_int(e.get('DATEPASS2')),
            'datepass3': safe_int(e.get('DATEPASS3')),
            'datepass4': safe_int(e.get('DATEPASS4')),
            'datepass5': safe_int(e.get('DATEPASS5')),
            'datepass6': safe_int(e.get('DATEPASS6')),
            'datepass7': safe_int(e.get('DATEPASS7')),
            'datepass8': safe_int(e.get('DATEPASS8')),
            'datepass9': safe_int(e.get('DATEPASS9')),
            'datepass10': safe_int(e.get('DATEPASS10')),
            'dat1': safe_int(e.get('DAT1')),
            'dat2': safe_int(e.get('DAT2')),
            'dat3': safe_int(e.get('DAT3')),
            'dat4': safe_int(e.get('DAT4')),
            'dat5': safe_int(e.get('DAT5')),
            'dat6': safe_int(e.get('DAT6')),
            'dat7': safe_int(e.get('DAT7')),
            'dat8': safe_int(e.get('DAT8')),
            'dat9': safe_int(e.get('DAT9')),
            'dat10': safe_int(e.get('DAT10')),
            'dat11': safe_int(e.get('DAT11')),
            'dat12': safe_int(e.get('DAT12')),
            'dat13': safe_int(e.get('DAT13')),
            'dat14': safe_int(e.get('DAT14')),
            'dat15': safe_int(e.get('DAT15')),
            'txt1': safe_str(e.get('TXT1'), 500),
            'txt2': safe_str(e.get('TXT2'), 500),
            'txt3': safe_str(e.get('TXT3'), 500),
            'txt4': safe_str(e.get('TXT4'), 500),
            'txt5': safe_str(e.get('TXT5'), 500),
            'data_wsoucont2': json.dumps(e),
            'updated_at': datetime.now().isoformat()
        }
        
        if supabase_update('equipements', 'id_wsoucont', id_wsoucont, data):
            updated += 1
    
    next_sector = sector_idx + 1
    return {
        "status": "success",
        "step": "2b",
        "sector": sector,
        "sector_idx": sector_idx,
        "passages_found": len(items),
        "updated": updated,
        "next": f"?step=2b&sector={next_sector}" if next_sector < len(SECTORS) else "?step=3&period=0"
    }

# ============================================================
# STEP 3: Pannes
# ============================================================

def sync_pannes(period_idx):
    """Synchronise les pannes pour une période"""
    if period_idx >= len(PERIODS):
        return {"status": "done", "message": "All periods completed", "next": "?step=4"}
    
    since_date = PERIODS[period_idx]
    wsid = get_auth()
    if not wsid:
        return {"status": "error", "message": "Auth failed"}
    
    resp = progilift_call("get_Synchro_Wpanne", {
        "dhDerniereMajFichier": since_date
    }, wsid, 180)
    
    items = parse_items(resp, "tabListeWpanne")
    upserted = 0
    
    for p in items:
        id_panne = safe_int(p.get('IDWPANNE'))
        if not id_panne:
            continue
        
        data = {
            'id_panne': id_panne,
            'id_wsoucont': safe_int(p.get('IDWSOUCONT')),
            'ascenseur': safe_str(p.get('ASCENSEUR'), 50),
            'adresse': safe_str(p.get('ADRES'), 200),
            'code_postal': safe_str(p.get('NUM'), 10),
            'date_appel': safe_str(p.get('DATEAPP'), 20),
            'heure_appel': safe_str(p.get('HEUREAPP'), 20),
            'date_arrivee': safe_str(p.get('DATEARR'), 20),
            'heure_arrivee': safe_str(p.get('HEUREARR'), 20),
            'date_depart': safe_str(p.get('DATEDEP'), 20),
            'heure_depart': safe_str(p.get('HEUREDEP'), 20),
            'motif': safe_str(p.get('MOTIF'), 500),
            'cause': safe_str(p.get('CAUSE'), 500),
            'travaux': safe_str(p.get('TRAVAUX'), 1000),
            'depanneur': safe_str(p.get('DEPANNEUR'), 100),
            'duree': safe_int(p.get('DUREE')),
            'type_panne': safe_str(p.get('TYPEPANNE'), 100),
            'etat': safe_str(p.get('ETAT'), 50),
            'demandeur': safe_str(p.get('DEMANDEUR'), 100),
            'personnes_bloquees': safe_str(p.get('PERSBLOQ'), 10),
            'data_wpanne': json.dumps(p),
            'updated_at': datetime.now().isoformat()
        }
        
        if supabase_upsert('pannes', data):
            upserted += 1
    
    next_period = period_idx + 1
    return {
        "status": "success",
        "step": 3,
        "period": since_date,
        "period_idx": period_idx,
        "pannes_found": len(items),
        "upserted": upserted,
        "next": f"?step=3&period={next_period}" if next_period < len(PERIODS) else "?step=4"
    }

# ============================================================
# STEP 4: Mise à jour nb_visites_an
# ============================================================

def update_nb_visites():
    """Met à jour nb_visites_an dans equipements via type_planning"""
    
    # Récupérer la table type_planning
    type_planning = supabase_get('type_planning', 'code,nb_visites')
    if not type_planning:
        return {"status": "error", "message": "type_planning table is empty. Run ?step=0 first."}
    
    type_map = {tp['code']: tp['nb_visites'] for tp in type_planning if tp.get('code')}
    
    # Récupérer les équipements avec typeplanning
    equipements = supabase_get('equipements', 'id_wsoucont,typeplanning', 'typeplanning=not.is.null')
    
    updated = 0
    for eq in equipements:
        typeplanning = eq.get('typeplanning')
        if typeplanning and typeplanning in type_map:
            nb_visites = type_map[typeplanning]
            if supabase_update('equipements', 'id_wsoucont', eq['id_wsoucont'], {'nb_visites_an': nb_visites}):
                updated += 1
    
    return {
        "status": "success",
        "step": 4,
        "type_planning_codes": len(type_map),
        "equipements_with_planning": len(equipements),
        "updated": updated,
        "message": "nb_visites_an updated!"
    }

# ============================================================
# CRON: Sync rapide
# ============================================================

def sync_cron():
    """Sync rapide pour cron job (arrêts + pannes récentes)"""
    results = {}
    
    # Arrêts
    r1 = sync_arrets()
    results['arrets'] = r1.get('inserted', 0)
    
    # Pannes récentes (première période seulement)
    r2 = sync_pannes(0)
    results['pannes'] = r2.get('upserted', 0)
    
    return {
        "status": "success",
        "mode": "cron",
        "results": results,
        "timestamp": datetime.now().isoformat()
    }

# ============================================================
# HANDLER HTTP (Vercel)
# ============================================================

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
            
            step = params.get('step', [''])[0]
            sector = int(params.get('sector', ['0'])[0])
            period = int(params.get('period', ['0'])[0])
            mode = params.get('mode', [''])[0]
            
            if mode == 'cron':
                result = sync_cron()
            elif step == '0':
                result = sync_type_planning()
            elif step == '1':
                result = sync_arrets()
            elif step == '2':
                result = sync_equipements(sector)
            elif step == '2b':
                result = sync_passages(sector)
            elif step == '3':
                result = sync_pannes(period)
            elif step == '4':
                result = update_nb_visites()
            else:
                result = {
                    "status": "ready",
                    "message": "Progilift Sync API v2",
                    "config": {
                        "sectors": len(SECTORS),
                        "periods": len(PERIODS)
                    },
                    "endpoints": {
                        "step0": "?step=0 → Types planning (référentiel nb_visites)",
                        "step1": "?step=1 → Arrêts en cours",
                        "step2": "?step=2&sector=0..21 → Équipements (Wsoucont)",
                        "step2b": "?step=2b&sector=0..21 → Passages (Wsoucont2)",
                        "step3": "?step=3&period=0..6 → Pannes",
                        "step4": "?step=4 → Mise à jour nb_visites_an",
                        "cron": "?mode=cron → Sync rapide"
                    },
                    "full_sync_order": "0 → 1 → 2 (x22) → 2b (x22) → 3 (x7) → 4"
                }
        
        except Exception as e:
            result = {
                "status": "error",
                "message": str(e),
                "trace": traceback.format_exc()[:500]
            }
        
        body = json.dumps(result, ensure_ascii=False).encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)
    
    def log_message(self, format, *args):
        pass
