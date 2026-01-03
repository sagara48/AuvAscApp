"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                     PROGILIFT SYNC API v2.0 - Vercel                         ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Synchronisation des données ProgiLift vers Supabase                         ║
║  API serverless pour Vercel avec support SOAP vers REST                       ║
╚══════════════════════════════════════════════════════════════════════════════╝

ENDPOINTS PROGILIFT DISPONIBLES (découverts par reverse-engineering):
=====================================================================

AUTHENTIFICATION:
  - IdentificationTechnicien          → Authentification, retourne WSID

ÉQUIPEMENTS (Wsoucont):
  - get_Synchro_Wsoucont              → Liste des équipements par secteur
  - get_Synchro_Wsoucont2             → 10 derniers passages par équipement
  - get_WSoucontCS_IDWSOUCONT         → Détails d'un équipement par ID
  - get_InfosSoucontPourUnCBLu        → Infos équipement par code-barres

PANNES:
  - get_AppareilsArret                → Appareils actuellement à l'arrêt
  - get_Synchro_Wpanne                → Historique des pannes
  - get_Synchro_Wpanop2               → Détails opérations pannes
  - get_Synchro_Wpanop2WP             → Détails opérations pannes (v2)
  - get_Synchro_Retourne              → Retours/rappels pannes
  - get_Synchro_MotifsRendusPannes    → Référentiel motifs de rendu

DEVIS:
  - get_Synchro_Devis                 → Liste des devis
  - get_Synchro_SuiviDemandesDevis    → Suivi des demandes de devis

MISSIONS / INTERVENTIONS:
  - get_Synchro_Histo_Mission         → Historique des missions
  - get_Encours_NonSynchronises_PourUnTech → Missions en cours non sync
  - get_Superviseur_Etat_Interventions_ParTech → État interventions

CONTRÔLES / ÉTATS:
  - get_Synchro_Wcontrol              → Contrôles réglementaires
  - get_Synchro_EtatDesAppareils      → États des appareils
  - get_Synchro_Etat_Appareil         → État d'un appareil
  - get_Synchro_Motifs_Etat_Appareil  → Référentiel motifs état

STOCK:
  - get_Synchro_WsorstockWP           → Sorties de stock

CLOUD / DIVERS:
  - get_Synchro_Cloud                 → Données cloud
  - get_Synchro_MO                    → Main d'œuvre
  - get_Synchro_EDSRubrique           → Rubriques EDS
  - get_Synchro_EDSType               → Types EDS
  - get_Synchro_TagNFC                → Tags NFC
  - get_Droits                        → Droits utilisateur
  - get_RDV_PourUnTechnicien_AjdEtDemain → RDV aujourd'hui/demain

STRUCTURE DES DONNÉES:
======================

Wsoucont (Équipements):
  - IDWSOUCONT (PK), IDWCONTRAT, SECTEUR, ASCENSEUR
  - DES2 (adresse), DES3 (ville), DES4-DES7
  - DIV1-DIV15 (champs personnalisables)
  - NUMAPPCLI, JAN-DEC (planning mensuel)

Wsoucont2 (10 derniers passages):
  - IDWSOUCONT, LIB1-LIB10, DATEPASS1-DATEPASS10

Wpanne (Pannes):
  - P0CLEUNIK (PK), IDWSOUCONT, DATE, JOUR
  - DEPANNEUR, PANNES (libellé), INTER, HRFININTER
  - DUREE, ENSEMBLE, LOCAL_, CAUSE, MOTIF, NOTE2

Devis:
  - IDDEVIS (PK), IDWSOUCONT, NUMERO, DATE
  - OBJET, MONTANTHT, MONTANTTTC, TVA
  - STATUT, DATESTATUT, CLIENT, ADRESSE, COMMENTAIRE

Histo_Mission:
  - IDMISSION (PK), IDWSOUCONT, DATE, TYPE
  - TECHNICIEN, DUREE, OBSERVATIONS

Wcontrol (Contrôles):
  - IDCONTROL (PK), IDWSOUCONT, DATE_CONTROLE
  - TYPE_CONTROLE, RESULTAT, OBSERVATIONS

WsorstockWP (Stock):
  - IDSORSTOCK (PK), IDWSOUCONT, DATE
  - ARTICLE, QUANTITE, PRIX

PARAMÈTRES API:
===============
  - dhDerniereMajFichier : Date ISO pour sync incrémentale
  - sListeSecteursTechnicien : Secteur(s) à synchroniser
  - nIDWSoucont : ID équipement spécifique
  - nIdTechnicien : ID technicien
  - sCBLu : Code-barres lu

URL SOAP:
=========
  https://ws.progilift.fr/WS_PROGILIFT_20230419_WEB/awws/WS_Progilift_20230419.awws
  Namespace: urn:WS_Progilift
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

# ============================================================================
# CONFIGURATION
# ============================================================================

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')
PROGILIFT_CODE = os.environ.get('PROGILIFT_CODE', 'AUVNB1')

# URL du WebService SOAP ProgiLift
WS_URL = "https://ws.progilift.fr/WS_PROGILIFT_20230419_WEB/awws/WS_Progilift_20230419.awws"
WS_NAMESPACE = "urn:WS_Progilift"

# Liste COMPLETE des 22 secteurs Auvergne Ascenseurs
SECTORS = [
    "1", "2", "3", "5", "6", "7", "8", "9", "10", "11", "12", 
    "13", "14", "15", "17", "18", "19", "20", "71", "72", "73", "74"
]

# Périodes pour les pannes (du plus récent au plus ancien)
PERIODS = [
    "2025-10-01T00:00:00",
    "2025-07-01T00:00:00",
    "2025-01-01T00:00:00",
    "2024-01-01T00:00:00",
    "2023-01-01T00:00:00",
    "2022-01-01T00:00:00",
    "2020-01-01T00:00:00"
]

# Périodes pour les devis
DEVIS_PERIODS = [
    "2025-01-01T00:00:00",
    "2024-01-01T00:00:00",
    "2023-01-01T00:00:00",
    "2020-01-01T00:00:00"
]

# Périodes pour l'historique des missions
MISSIONS_PERIODS = [
    "2025-01-01T00:00:00",
    "2024-01-01T00:00:00",
    "2023-01-01T00:00:00"
]

# Périodes pour les contrôles
CONTROLES_PERIODS = [
    "2024-01-01T00:00:00",
    "2020-01-01T00:00:00"
]

# ============================================================================
# UTILITAIRES
# ============================================================================

try:
    ssl_context = ssl.create_default_context()
except:
    ssl_context = ssl._create_unverified_context()

def safe_str(value, max_len=None):
    """Convertit une valeur en string safe, avec longueur max optionnelle"""
    if value is None:
        return None
    try:
        s = str(value).strip()
        return s[:max_len] if max_len and s else s if s else None
    except:
        return None

def safe_int(value):
    """Convertit une valeur en entier safe"""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except:
        return None

def safe_float(value):
    """Convertit une valeur en float safe (gère la virgule française)"""
    if value is None:
        return None
    try:
        return float(str(value).strip().replace(',', '.'))
    except:
        return None

def http_request(url, method='GET', data=None, headers=None, timeout=30):
    """Effectue une requête HTTP générique"""
    try:
        req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
        with urllib.request.urlopen(req, timeout=timeout, context=ssl_context) as resp:
            return resp.status, resp.read().decode('utf-8')
    except urllib.error.HTTPError as e:
        return e.code, ''
    except Exception as e:
        return 0, str(e)

# ============================================================================
# SUPABASE HELPERS
# ============================================================================

def supabase_upsert(table, data):
    """Insert ou update dans Supabase (nécessite une contrainte unique)"""
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
    """Insert simple dans Supabase"""
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

def supabase_delete(table, condition="id=gte.0"):
    """Supprime des enregistrements dans Supabase"""
    if not SUPABASE_URL:
        return False
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table}?{condition}"
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Prefer': 'return=minimal'
    }
    status, _ = http_request(url, 'DELETE', None, headers, 30)
    return status in [200, 204]

def supabase_update(table, id_field, id_value, data):
    """Met à jour un enregistrement dans Supabase"""
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

# ============================================================================
# PROGILIFT SOAP CLIENT
# ============================================================================

def progilift_call(method, params, wsid=None, timeout=30):
    """
    Appelle une méthode SOAP ProgiLift
    
    Args:
        method: Nom de la méthode (ex: get_Synchro_Wsoucont)
        params: Dict des paramètres
        wsid: Session ID (obtenu après authentification)
        timeout: Timeout en secondes
    
    Returns:
        str: Corps de la réponse XML ou None si erreur
    """
    params_xml = ""
    if params:
        for k, v in params.items():
            if v is not None:
                v_esc = str(v).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                params_xml += f"<ws:{k}>{v_esc}</ws:{k}>"
    
    wsid_xml = f'<ws:WSID xsi:type="xsd:hexBinary" soap:mustUnderstand="1">{wsid}</ws:WSID>' if wsid else ""
    
    soap = f'''<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" 
               xmlns:ws="{WS_NAMESPACE}" 
               xmlns:xsd="http://www.w3.org/2001/XMLSchema" 
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <soap:Header>{wsid_xml}</soap:Header>
    <soap:Body>
        <ws:{method}>{params_xml}</ws:{method}>
    </soap:Body>
</soap:Envelope>'''
    
    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': f'"{WS_NAMESPACE}/{method}"'
    }
    status, body = http_request(WS_URL, 'POST', soap.encode('utf-8'), headers, timeout)
    
    return body if status == 200 and body and "Fault" not in body else None

def parse_items(xml, tag):
    """
    Parse une liste d'items XML
    
    Args:
        xml: Réponse XML brute
        tag: Nom du tag contenant les items (ex: tabListeWsoucont)
    
    Returns:
        list: Liste de dictionnaires
    """
    items = []
    if not xml:
        return items
    for m in re.finditer(f'<{tag}>(.*?)</{tag}>', xml, re.DOTALL):
        item = {}
        for f in re.finditer(r'<([A-Za-z0-9_]+)>([^<]*)</\1>', m.group(1)):
            val = f.group(2).strip()
            # Conversion automatique en int si possible
            item[f.group(1)] = int(val) if val and val.lstrip('-').isdigit() else (val if val else None)
        if item:
            items.append(item)
    return items

def get_auth():
    """
    Authentification ProgiLift
    
    Returns:
        str: WSID (session ID) ou None si échec
    """
    resp = progilift_call("IdentificationTechnicien", {"sSteCodeWeb": PROGILIFT_CODE}, None, 30)
    if resp:
        m = re.search(r'WSID[^>]*>([A-F0-9]+)<', resp, re.IGNORECASE)
        if m:
            return m.group(1)
    return None

# ============================================================================
# STEP 1: ARRÊTS (Appareils actuellement à l'arrêt)
# ============================================================================

def sync_arrets():
    """Synchronise les appareils actuellement à l'arrêt"""
    wsid = get_auth()
    if not wsid:
        return {"status": "error", "message": "Auth failed"}
    
    resp = progilift_call("get_AppareilsArret", {}, wsid, 30)
    arrets = parse_items(resp, "tabListeArrets")
    
    # Vider et repeupler la table (données temps réel)
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
    
    return {
        "status": "success", 
        "step": 1, 
        "arrets": len(arrets), 
        "inserted": inserted, 
        "next": "?step=2&sector=0"
    }

# ============================================================================
# STEP 2: ÉQUIPEMENTS (Wsoucont par secteur)
# ============================================================================

def sync_equipements(sector_idx):
    """Synchronise les équipements d'un secteur"""
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
                # Champs personnalisables DIV1-DIV15
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
                # Planning mensuel des visites
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
                # Données brutes JSON
                'data_wsoucont': json.dumps(e),
                'updated_at': datetime.now().isoformat()
            })
    
    # Upsert par batches de 30
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

# ============================================================================
# STEP 2b: 10 DERNIERS PASSAGES (Wsoucont2)
# ============================================================================

def sync_passages(sector_idx):
    """Synchronise les 10 derniers passages d'un secteur"""
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
            # LIB1-LIB10: Libellés des 10 derniers passages
            # DATEPASS1-DATEPASS10: Dates des passages (format numérique)
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

# ============================================================================
# STEP 3: PANNES (Wpanne par période)
# ============================================================================

def sync_pannes(period_idx):
    """Synchronise les pannes d'une période"""
    wsid = get_auth()
    if not wsid:
        return {"status": "error", "message": "Auth failed"}
    
    if period_idx >= len(PERIODS):
        return {"status": "success", "step": 3, "message": "All pannes done", "next": "?step=4&period=0"}
    
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
    next_url = f"?step=3&period={next_idx}" if next_idx < len(PERIODS) else "?step=4&period=0"
    
    return {
        "status": "success",
        "step": 3,
        "period": period,
        "period_index": f"{period_idx + 1}/{len(PERIODS)}",
        "pannes": len(pannes_list),
        "next": next_url
    }

# ============================================================================
# STEP 4: DEVIS (par période)
# ============================================================================

def sync_devis(period_idx):
    """Synchronise les devis d'une période"""
    wsid = get_auth()
    if not wsid:
        return {"status": "error", "message": "Auth failed"}
    
    if period_idx >= len(DEVIS_PERIODS):
        # Sync terminée !
        supabase_insert('sync_logs', {
            'sync_date': datetime.now().isoformat(),
            'status': 'success'
        })
        return {"status": "success", "step": 4, "message": "SYNC COMPLETE!"}
    
    period = DEVIS_PERIODS[period_idx]
    resp = progilift_call("get_Synchro_Devis", {"dhDerniereMajFichier": period}, wsid, 90)
    items = parse_items(resp, "tabListeDevis")
    
    devis_list = []
    for d in items:
        did = safe_int(d.get('IDDEVIS')) or safe_int(d.get('ID'))
        if did:
            devis_list.append({
                'id_devis': did,
                'id_wsoucont': safe_int(d.get('IDWSOUCONT')),
                'numero': safe_str(d.get('NUMERO'), 50),
                'date_devis': safe_str(d.get('DATE'), 20),
                'objet': safe_str(d.get('OBJET'), 500),
                'montant_ht': safe_float(d.get('MONTANTHT')),
                'montant_ttc': safe_float(d.get('MONTANTTTC')),
                'tva': safe_float(d.get('TVA')),
                'statut': safe_str(d.get('STATUT'), 50),
                'date_statut': safe_str(d.get('DATESTATUT'), 20),
                'client': safe_str(d.get('CLIENT'), 200),
                'adresse': safe_str(d.get('ADRESSE'), 300),
                'commentaire': safe_str(d.get('COMMENTAIRE'), 1000),
                'data': json.dumps(d),
                'updated_at': datetime.now().isoformat()
            })
    
    for i in range(0, len(devis_list), 30):
        supabase_upsert('devis', devis_list[i:i+30])
    
    next_idx = period_idx + 1
    next_url = f"?step=4&period={next_idx}" if next_idx < len(DEVIS_PERIODS) else None
    
    result = {
        "status": "success",
        "step": 4,
        "period": period,
        "period_index": f"{period_idx + 1}/{len(DEVIS_PERIODS)}",
        "devis": len(devis_list)
    }
    
    if next_url:
        result["next"] = next_url
    else:
        result["message"] = "SYNC COMPLETE!"
        supabase_insert('sync_logs', {'sync_date': datetime.now().isoformat(), 'status': 'success'})
    
    return result

# ============================================================================
# ENDPOINTS OPTIONNELS (pour extensions futures)
# ============================================================================

def sync_missions(period_idx):
    """Synchronise l'historique des missions (optionnel)"""
    wsid = get_auth()
    if not wsid:
        return {"status": "error", "message": "Auth failed"}
    
    if period_idx >= len(MISSIONS_PERIODS):
        return {"status": "success", "step": "missions", "message": "All missions done"}
    
    period = MISSIONS_PERIODS[period_idx]
    resp = progilift_call("get_Synchro_Histo_Mission", {"dhDerniereMajFichier": period}, wsid, 90)
    items = parse_items(resp, "tabListeHistoMission")
    
    missions_list = []
    for m in items:
        mid = safe_int(m.get('IDMISSION')) or safe_int(m.get('ID'))
        if mid:
            missions_list.append({
                'id_mission': mid,
                'id_wsoucont': safe_int(m.get('IDWSOUCONT')),
                'date_mission': safe_str(m.get('DATE'), 20),
                'type_mission': safe_str(m.get('TYPE'), 50),
                'technicien': safe_str(m.get('TECHNICIEN'), 100),
                'duree': safe_int(m.get('DUREE')),
                'observations': safe_str(m.get('OBSERVATIONS'), 1000),
                'data': json.dumps(m),
                'updated_at': datetime.now().isoformat()
            })
    
    for i in range(0, len(missions_list), 30):
        supabase_upsert('missions', missions_list[i:i+30])
    
    next_idx = period_idx + 1
    
    return {
        "status": "success",
        "step": "missions",
        "period": period,
        "period_index": f"{period_idx + 1}/{len(MISSIONS_PERIODS)}",
        "missions": len(missions_list),
        "next": f"?step=missions&period={next_idx}" if next_idx < len(MISSIONS_PERIODS) else None
    }

def sync_controles(period_idx):
    """Synchronise les contrôles réglementaires (optionnel)"""
    wsid = get_auth()
    if not wsid:
        return {"status": "error", "message": "Auth failed"}
    
    if period_idx >= len(CONTROLES_PERIODS):
        return {"status": "success", "step": "controles", "message": "All controles done"}
    
    period = CONTROLES_PERIODS[period_idx]
    resp = progilift_call("get_Synchro_Wcontrol", {"dhDerniereMajFichier": period}, wsid, 90)
    items = parse_items(resp, "tabListeWcontrol")
    
    controles_list = []
    for c in items:
        cid = safe_int(c.get('IDCONTROL')) or safe_int(c.get('ID'))
        if cid:
            controles_list.append({
                'id_controle': cid,
                'id_wsoucont': safe_int(c.get('IDWSOUCONT')),
                'date_controle': safe_str(c.get('DATE_CONTROLE') or c.get('DATE'), 20),
                'type_controle': safe_str(c.get('TYPE_CONTROLE') or c.get('TYPE'), 100),
                'resultat': safe_str(c.get('RESULTAT'), 50),
                'observations': safe_str(c.get('OBSERVATIONS'), 1000),
                'data': json.dumps(c),
                'updated_at': datetime.now().isoformat()
            })
    
    for i in range(0, len(controles_list), 30):
        supabase_upsert('controles', controles_list[i:i+30])
    
    next_idx = period_idx + 1
    
    return {
        "status": "success",
        "step": "controles",
        "period": period,
        "period_index": f"{period_idx + 1}/{len(CONTROLES_PERIODS)}",
        "controles": len(controles_list),
        "next": f"?step=controles&period={next_idx}" if next_idx < len(CONTROLES_PERIODS) else None
    }

def get_equipement_detail(id_wsoucont):
    """Récupère les détails complets d'un équipement"""
    wsid = get_auth()
    if not wsid:
        return {"status": "error", "message": "Auth failed"}
    
    resp = progilift_call("get_WSoucontCS_IDWSOUCONT", {"nIDWSoucont": id_wsoucont}, wsid, 30)
    items = parse_items(resp, "WSoucontCS")
    
    if items:
        return {"status": "success", "equipement": items[0]}
    return {"status": "error", "message": "Équipement non trouvé"}

# ============================================================================
# CRON: SYNC RAPIDE (arrêts + pannes récentes)
# ============================================================================

def sync_cron():
    """Sync rapide pour le cron horaire - arrêts + pannes récentes uniquement"""
    start = datetime.now()
    stats = {"arrets": 0, "pannes": 0}
    
    wsid = get_auth()
    if not wsid:
        return {"status": "error", "message": "Auth failed"}
    
    # 1. Arrêts (temps réel)
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

# ============================================================================
# VERCEL HANDLER
# ============================================================================

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
            id_equip = params.get('id', [''])[0]
            
            # Router
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
            elif step == '4':
                result = sync_devis(period)
            # Endpoints optionnels
            elif step == 'missions':
                result = sync_missions(period)
            elif step == 'controles':
                result = sync_controles(period)
            elif step == 'detail' and id_equip:
                result = get_equipement_detail(int(id_equip))
            else:
                # Documentation
                result = {
                    "status": "ready",
                    "message": "Progilift Sync API v2.0",
                    "version": "2.0.0",
                    "config": {
                        "sectors_count": len(SECTORS),
                        "periods_count": len(PERIODS),
                        "devis_periods_count": len(DEVIS_PERIODS),
                        "missions_periods_count": len(MISSIONS_PERIODS),
                        "controles_periods_count": len(CONTROLES_PERIODS)
                    },
                    "endpoints": {
                        "status": "GET / → Cette documentation",
                        "cron": "GET ?mode=cron → Sync rapide (arrêts + pannes récentes)",
                        "step1": "GET ?step=1 → Arrêts",
                        "step2": "GET ?step=2&sector=0 → Équipements (0-21)",
                        "step2b": "GET ?step=2b&sector=0 → 10 derniers passages (0-21)",
                        "step3": "GET ?step=3&period=0 → Pannes (0-6)",
                        "step4": "GET ?step=4&period=0 → Devis (0-3)",
                        "missions": "GET ?step=missions&period=0 → Historique missions (optionnel)",
                        "controles": "GET ?step=controles&period=0 → Contrôles (optionnel)",
                        "detail": "GET ?step=detail&id=1234 → Détail équipement (optionnel)"
                    },
                    "full_sync_sequence": "?step=1 → ?step=2&sector=0..21 → ?step=2b&sector=0..21 → ?step=3&period=0..6 → ?step=4&period=0..3",
                    "progilift_info": {
                        "ws_url": WS_URL,
                        "available_operations": [
                            "IdentificationTechnicien",
                            "get_AppareilsArret",
                            "get_Synchro_Wsoucont",
                            "get_Synchro_Wsoucont2", 
                            "get_Synchro_Wpanne",
                            "get_Synchro_Devis",
                            "get_Synchro_Histo_Mission",
                            "get_Synchro_Wcontrol",
                            "get_WSoucontCS_IDWSOUCONT",
                            "get_Synchro_WsorstockWP",
                            "get_Synchro_Cloud",
                            "get_Synchro_EtatDesAppareils",
                            "get_Synchro_MotifsRendusPannes",
                            "get_Synchro_SuiviDemandesDevis",
                            "get_Droits",
                            "get_RDV_PourUnTechnicien_AjdEtDemain"
                        ]
                    }
                }
        except Exception as e:
            result = {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}
        
        body = json.dumps(result, ensure_ascii=False, indent=2).encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)
    
    def log_message(self, format, *args):
        pass
