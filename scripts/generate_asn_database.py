#!/usr/bin/env python3
"""
generate_asn_database.py
Fetches ASN data from PeeringDB (primary) or RIPE NCC (fallback) and generates asn_database.json
"""

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

# API endpoints
PEERINGDB_API = "https://www.peeringdb.com/api/net"
RIPE_RIS_ASNS_API = "https://stat.ripe.net/data/ris-asns/data.json?list_asns=true&asn_types=o"

# Configuration
OUTPUT_FILE = Path(__file__).parent.parent / "asn_database.json"
MIN_EXPECTED_ENTRIES = 1000
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 10


def get_timestamp() -> str:
    """Generate ISO 8601 timestamp."""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def fetch_with_retry(url: str, source_name: str) -> Optional[dict]:
    """Fetch URL with retry logic and exponential backoff."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[{source_name}] Attempt {attempt}/{MAX_RETRIES}...")
            response = requests.get(url, timeout=120)
            response.raise_for_status()
            print(f"[{source_name}] Success!")
            return response.json()
        except requests.exceptions.Timeout:
            print(f"[{source_name}] Timeout after 120 seconds")
        except requests.exceptions.RequestException as e:
            print(f"[{source_name}] Request failed: {e}")

        if attempt < MAX_RETRIES:
            wait_time = RETRY_DELAY_SECONDS * (2 ** (attempt - 1))  # Exponential backoff
            print(f"[{source_name}] Retrying in {wait_time} seconds...")
            time.sleep(wait_time)

    print(f"[{source_name}] All {MAX_RETRIES} attempts failed")
    return None


def fetch_peeringdb() -> Optional[dict]:
    """Fetch network data from PeeringDB API."""
    print("\n=== Trying PeeringDB (Primary Source) ===")
    return fetch_with_retry(PEERINGDB_API, "PeeringDB")


def fetch_ripe_ris() -> Optional[dict]:
    """Fetch ASN data from RIPE NCC RIS API."""
    print("\n=== Trying RIPE NCC RIS (Fallback Source) ===")
    return fetch_with_retry(RIPE_RIS_ASNS_API, "RIPE RIS")


def transform_peeringdb_data(response: dict) -> dict:
    """Transform PeeringDB response to our schema."""
    entries = {}

    for network in response.get("data", []):
        asn = network.get("asn")
        if asn is None:
            continue

        name = network.get("name") or f"AS{asn}"
        info_type = network.get("info_type") or ""

        entries[str(asn)] = {
            "name": name,
            "type": info_type if info_type else "Unknown"
        }

    return {
        "version": "1.0.0",
        "updated_at": get_timestamp(),
        "source": "PeeringDB",
        "entry_count": len(entries),
        "entries": entries
    }


def transform_ripe_data(response: dict) -> dict:
    """Transform RIPE NCC RIS response to our schema."""
    entries = {}

    asns = response.get("data", {}).get("asns", [])
    for asn in asns:
        if asn is None:
            continue
        # RIPE RIS only provides ASN numbers, not names
        # We use a generic name format
        entries[str(asn)] = {
            "name": f"AS{asn}",
            "type": "Unknown"
        }

    return {
        "version": "1.0.0",
        "updated_at": get_timestamp(),
        "source": "RIPE NCC RIS",
        "entry_count": len(entries),
        "entries": entries
    }


def load_existing_database() -> Optional[dict]:
    """Load existing database file if present."""
    if not OUTPUT_FILE.exists():
        return None

    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            print(f"Loaded existing database with {data.get('entry_count', 0)} entries")
            return data
    except (json.JSONDecodeError, IOError) as e:
        print(f"WARNING: Could not load existing database: {e}")
        return None


def merge_databases(primary: dict, fallback: dict) -> dict:
    """Merge fallback data into primary, keeping primary entries where available."""
    merged_entries = {**fallback.get("entries", {}), **primary.get("entries", {})}

    return {
        "version": "1.0.0",
        "updated_at": get_timestamp(),
        "source": f"{primary.get('source', 'Unknown')} + {fallback.get('source', 'Unknown')}",
        "entry_count": len(merged_entries),
        "entries": merged_entries
    }


def validate_database(database: dict) -> bool:
    """Validate the generated database meets minimum requirements."""
    entry_count = database.get("entry_count", 0)

    if entry_count < MIN_EXPECTED_ENTRIES:
        print(f"ERROR: Only {entry_count} entries found, expected at least {MIN_EXPECTED_ENTRIES}")
        return False

    # Check for well-known ASNs that should always be present
    # Comprehensive list covering major providers across all categories worldwide
    known_asns = {
        # ===========================================
        # CLOUD PROVIDERS
        # ===========================================
        "15169": "Google",
        "16509": "Amazon (AWS)",
        "8075": "Microsoft (Azure)",
        "14618": "Amazon (AWS)",
        "36492": "Google (Cloud)",
        "45566": "Alibaba Cloud",
        "37963": "Alibaba (China)",
        "132203": "Tencent Cloud",
        "13238": "Yandex",
        "60068": "Datacamp (DigitalOcean)",
        "14061": "DigitalOcean",
        "63949": "Linode (Akamai)",
        "20473": "Vultr",
        "16276": "OVH",
        "24940": "Hetzner",
        "51167": "Contabo",
        "213230": "Hetzner (Finland)",
        "212317": "Hetzner",
        "8560": "IONOS (1&1)",
        "29838": "Atlantic.Net",
        "398324": "Censys",
        "135377": "UCloud",
        "55967": "Beijing Baidu",
        "38365": "Baidu",
        "58466": "Chinanet Cloud",
        "136907": "Huawei Cloud",
        "55990": "Huawei Cloud",
        "132591": "Naver Cloud (Korea)",
        "131477": "Kakao (Korea)",
        "16510": "Amazon (AWS)",
        "19047": "Amazon (AWS)",
        "38895": "Amazon (AWS)",
        "62785": "Oracle Cloud",
        "31898": "Oracle Cloud",
        "7160": "Oracle",
        "140034": "Google Cloud",

        # ===========================================
        # CDN & EDGE NETWORKS
        # ===========================================
        "13335": "Cloudflare",
        "20940": "Akamai",
        "54113": "Fastly",
        "46489": "Twitch",
        "16625": "Akamai",
        "21342": "Akamai",
        "35994": "Akamai",
        "393234": "Cloudflare (WARP)",
        "209242": "Cloudflare",
        "22822": "Limelight",
        "15133": "Edgecast (Verizon)",
        "14153": "Edgecast",
        "30675": "Verizon Digital Media",
        "18717": "Verizon Digital Media",
        "19551": "Incapsula",
        "55429": "Microsoft CDN",
        "8068": "Microsoft",
        "198047": "Bunny CDN",
        "60626": "Bunny CDN",
        "136787": "TATA CDN",
        "133877": "Kingsoft Cloud CDN",
        "395747": "Imperva",
        "202623": "Sucuri",
        "30148": "Sucuri",
        "200856": "KeyCDN",
        "203898": "CDNetworks",
        "36408": "CDNetworks",

        # ===========================================
        # CONTENT PROVIDERS & STREAMING
        # ===========================================
        "2906": "Netflix",
        "40027": "Netflix",
        "55095": "Netflix",
        "32934": "Meta (Facebook)",
        "63293": "Meta (Facebook)",
        "54115": "Meta (Facebook)",
        "32787": "Meta (Facebook)",
        "714": "Apple",
        "6185": "Apple",
        "2709": "Apple",
        "36183": "Akamai",
        "23286": "Hulu",
        "19679": "Dropbox",
        "62041": "Telegram",
        "44907": "Spotify",
        "8403": "Spotify",
        "36040": "YouTube",
        "43515": "YouTube",
        "36561": "YouTube",
        "13414": "Twitter/X",
        "35995": "Twitter/X",
        "54888": "Twitter/X",
        "63179": "Twitter/X",
        "132892": "TikTok (Bytedance)",
        "138699": "TikTok (Bytedance)",
        "396986": "TikTok (Bytedance)",
        "13767": "TikTok",
        "16591": "Google Fiber",
        "41264": "Google Fiber",
        "139190": "TikTok",
        "2687": "AT&T",
        "395973": "Disney Streaming",
        "40428": "Disney",
        "393414": "Disney Streaming",
        "7377": "DIRECTV",
        "23059": "Sling TV",
        "46264": "Hulu",
        "36259": "Hulu",
        "11664": "Technorati",
        "14907": "Wikipedia",
        "43821": "Wikipedia",
        "22394": "Vimeo",
        "10310": "Yahoo",
        "14510": "Verizon Media",
        "26101": "Yahoo",
        "10753": "Yahoo Japan",
        "17506": "Yahoo Japan",

        # ===========================================
        # SOCIAL MEDIA & COMMUNICATION
        # ===========================================
        "14413": "LinkedIn",
        "40793": "LinkedIn",
        "33425": "Pinterest",
        "63223": "Pinterest",
        "36631": "Snapchat",
        "394536": "Snapchat",
        "16509": "Snapchat (AWS)",
        "394406": "Discord",
        "49544": "Discord",
        "19437": "Reddit",
        "395502": "Reddit",
        "13414": "Reddit",
        "20057": "AT&T",
        "14340": "Salesforce",
        "133493": "WeChat (Tencent)",
        "132203": "WeChat (Tencent)",
        "45090": "Tencent",
        "132591": "KakaoTalk (Korea)",
        "17858": "LINE (Japan)",
        "131965": "LINE",
        "9370": "Sakura Internet (Japan)",
        "14618": "WhatsApp (Meta/AWS)",
        "25820": "IT7 Networks",

        # ===========================================
        # VIDEO CONFERENCING & COLLABORATION
        # ===========================================
        "52347": "Zoom",
        "209197": "Zoom",
        "36351": "Zoom (IBM)",
        "14618": "Zoom (AWS)",
        "54098": "Slack",
        "395831": "Slack",
        "27176": "WebEx (Cisco)",
        "109": "Cisco",
        "16550": "Cisco",
        "5765": "Microsoft Teams",
        "8068": "Microsoft Teams",
        "21574": "Microsoft Teams",
        "45634": "Zoho",
        "136907": "DingTalk",
        "16509": "Webex (AWS)",

        # ===========================================
        # MAJOR TRANSIT PROVIDERS
        # ===========================================
        "3356": "Lumen (Level3)",
        "1299": "Arelion (Telia)",
        "174": "Cogent",
        "6939": "Hurricane Electric",
        "2914": "NTT",
        "3257": "GTT",
        "6453": "TATA Communications",
        "6461": "Zayo",
        "6762": "Telecom Italia Sparkle",
        "1239": "Sprint",
        "701": "Verizon Business",
        "7018": "AT&T",
        "3491": "PCCW Global",
        "9002": "RETN",
        "4637": "Telstra Global",
        "5511": "Orange",
        "12956": "Telefonica",
        "2828": "XO Communications",
        "6830": "Liberty Global",
        "1273": "Vodafone (Cable & Wireless)",
        "3549": "Lumen (Level3)",
        "4323": "T-Mobile (Sprint)",
        "6327": "Shaw",
        "577": "Bell Canada",
        "6327": "Shaw",
        "852": "Telus",
        "812": "Rogers",
        "22652": "Fibrenoire",
        "5769": "Videotron",
        "6327": "Shaw",
        "30036": "Mediacom",
        "209": "Qwest (CenturyLink)",
        "20912": "ASN-PANSERVICE",
        "8220": "Colt",
        "9121": "Turk Telekom",
        "8345": "MaxNet",
        "31133": "PJSC MegaFon",
        "42610": "Rostelecom",
        "12389": "Rostelecom",
        "3216": "PJSC VimpelCom (Beeline)",
        "8359": "MTS Russia",
        "8402": "Corbina (Vimpelcom)",

        # ===========================================
        # US ISPs & CARRIERS
        # ===========================================
        "7922": "Comcast",
        "7015": "Comcast",
        "33650": "Comcast",
        "33651": "Comcast",
        "33652": "Comcast",
        "33489": "Comcast",
        "33490": "Comcast",
        "33491": "Comcast",
        "20001": "Charter (Spectrum)",
        "11351": "Charter",
        "11427": "Charter",
        "20115": "Charter",
        "33363": "Charter",
        "10796": "Charter",
        "11426": "Charter",
        "7843": "Charter",
        "12271": "Charter",
        "22773": "Cox",
        "6848": "Cox",
        "5650": "Frontier",
        "6128": "Cablevision (Altice)",
        "12271": "Optimum (Altice)",
        "6167": "Verizon Business",
        "701": "Verizon",
        "702": "Verizon",
        "22394": "Verizon Wireless",
        "6167": "Verizon",
        "19262": "Verizon Wireless",
        "22394": "Cellco (Verizon)",
        "21928": "T-Mobile",
        "20057": "AT&T Mobility",
        "7018": "AT&T",
        "6389": "AT&T",
        "2386": "AT&T",
        "5688": "EarthLink",
        "11486": "AT&T Wireless",
        "5693": "CenturyLink",
        "22561": "CenturyLink",
        "6347": "Windstream",
        "7029": "Windstream",
        "26827": "EPB Fiber",
        "30036": "Mediacom",
        "11232": "Midco",
        "11404": "Wave Broadband",
        "7029": "Windstream",
        "7065": "Starlink",

        # ===========================================
        # EUROPEAN ISPs
        # ===========================================
        "3320": "Deutsche Telekom",
        "6805": "Telefonica Germany",
        "3209": "Vodafone Germany",
        "8881": "Versatel Germany",
        "31334": "Kabel Deutschland (Vodafone)",
        "6830": "Liberty Global (UPC)",
        "8422": "NetCologne",
        "15600": "Quickline (Switzerland)",
        "3303": "Swisscom",
        "6730": "Sunrise (Switzerland)",
        "12322": "Free (France)",
        "15557": "SFR (France)",
        "3215": "Orange (France)",
        "5410": "Bouygues (France)",
        "6461": "Zayo France",
        "2856": "BT (UK)",
        "5089": "Virgin Media (UK)",
        "6871": "Plusnet (UK)",
        "2529": "Sky UK",
        "5607": "Sky UK",
        "13285": "TalkTalk (UK)",
        "12576": "EE (UK)",
        "15533": "Three UK",
        "8468": "Entanet (UK)",
        "20712": "Andrews & Arnold (UK)",
        "3269": "Telecom Italia",
        "12874": "Fastweb (Italy)",
        "30722": "Vodafone Italy",
        "1267": "Wind Italy",
        "29286": "Iliad Italy",
        "3352": "Telefonica (Spain)",
        "12479": "Orange Spain",
        "12715": "Jazz Telecom (Spain)",
        "12430": "Vodafone Spain",
        "6739": "Vodafone Spain",
        "3324": "Vodafone Spain",
        "6848": "Telenet (Belgium)",
        "5432": "Proximus (Belgium)",
        "12392": "Orange Belgium",
        "21156": "Digi (Romania)",
        "6830": "UPC Romania",
        "9050": "Orange Romania",
        "6663": "Tiscali (Italy)",
        "5588": "GTS Poland",
        "5617": "Orange Poland",
        "12741": "Netia (Poland)",
        "12968": "UPC Poland",
        "29314": "Vectra (Poland)",
        "20940": "Akamai (Poland)",
        "6855": "A1 Telekom Austria",
        "8447": "A1 Telekom Austria",
        "12605": "Magenta Telekom (Austria)",
        "21334": "Hutchison Drei (Austria)",
        "5588": "T-Mobile Austria",
        "44034": "Hi3G (Sweden)",
        "3301": "Telia Sweden",
        "2119": "Telenor Sweden",
        "29518": "Bredband2 (Sweden)",
        "1759": "Telenor Norway",
        "2116": "GET Norway",
        "15659": "NextGenTel (Norway)",
        "12929": "Telia Finland",
        "1759": "Elisa Finland",
        "719": "Elisa Finland",
        "6667": "Eunet Finland",
        "42473": "Anexia (Austria)",
        "60068": "CDN77",
        "197540": "Netcup",
        "136620": "Google",
        "45102": "Alibaba (China)",
        "34984": "Superonline (Turkey)",
        "9121": "Turk Telekom",
        "12735": "TurkNet",
        "47331": "TTNet (Turkey)",

        # ===========================================
        # ASIA-PACIFIC ISPs
        # ===========================================
        # India
        "9498": "Bharti Airtel (India)",
        "18101": "Reliance Jio (India)",
        "4755": "TATA (India)",
        "9829": "BSNL (India)",
        "17488": "Hathway (India)",
        "24560": "Bharti Airtel",
        "45609": "Airtel (India)",
        "55836": "Reliance Jio",
        "132335": "ACT Fibernet (India)",
        "134810": "GTPL (India)",
        "17426": "Tikona (India)",
        "38266": "MTNL (India)",
        "45528": "TATA Teleservices",
        "58678": "VI (Vodafone Idea)",
        "55644": "Vodafone India",

        # China
        "4134": "China Telecom",
        "4812": "China Telecom",
        "4837": "China Unicom",
        "4808": "China Unicom",
        "9808": "China Mobile",
        "56040": "China Mobile",
        "9929": "China Telecom (CN2)",
        "24445": "China Telecom Americas",
        "10099": "China Unicom Global",
        "58453": "China Mobile International",
        "4538": "CERNET (Education)",
        "23724": "CERNET2",
        "23910": "China Telecom",

        # Japan
        "17676": "SoftBank (Japan)",
        "2516": "KDDI (Japan)",
        "2527": "Sony Network (Japan)",
        "2497": "IIJ (Japan)",
        "9605": "NTT DoCoMo",
        "4713": "NTT OCN",
        "2514": "NTT (Japan)",
        "7679": "Tokyo QKD",
        "4725": "ODN (Japan)",
        "23623": "Rakuten Mobile",
        "18126": "Chubu Telecom (Japan)",
        "9370": "Sakura Internet",
        "7506": "GMO Internet",
        "59103": "GMO Internet",

        # Korea
        "4766": "Korea Telecom",
        "9318": "SK Broadband",
        "3786": "LG Dacom (Korea)",
        "9644": "SK Telecom",
        "9316": "Dreamline (Korea)",
        "17858": "LG U+ (Korea)",
        "38091": "Naver",
        "10036": "Korea Telecom",
        "17853": "Korea Telecom",
        "9848": "Sejong Telecom",

        # Southeast Asia
        "7473": "Singapore Telecom",
        "3758": "SingNet",
        "9506": "Singtel Optus",
        "4657": "StarHub (Singapore)",
        "132537": "M1 (Singapore)",
        "4844": "SuperInternet (Singapore)",
        "45430": "Viewqwest (Singapore)",
        "4788": "TM (Malaysia)",
        "4818": "DiGi (Malaysia)",
        "10030": "TM (Malaysia)",
        "38466": "U Mobile (Malaysia)",
        "7713": "Telkom Indonesia",
        "17974": "Telkomnet",
        "4761": "Indosat (Indonesia)",
        "131090": "Indosat",
        "45727": "XL Axiata (Indonesia)",
        "23679": "Biznet (Indonesia)",
        "9299": "PLDT (Philippines)",
        "4775": "Globe (Philippines)",
        "132199": "Globe",
        "18139": "Converge ICT (Philippines)",
        "4787": "Viettel (Vietnam)",
        "45899": "VNPT (Vietnam)",
        "7552": "Viettel",
        "131429": "FPT Telecom (Vietnam)",
        "4750": "True Internet (Thailand)",
        "7470": "TRUE (Thailand)",
        "9931": "CAT Telecom (Thailand)",
        "45629": "AIS (Thailand)",
        "23969": "TOT (Thailand)",
        "132061": "DTAC (Thailand)",
        "45494": "CMRU (Thailand)",

        # Australia & New Zealand
        "1221": "Telstra (Australia)",
        "4804": "Optus (Australia)",
        "4739": "Internode (Australia)",
        "7545": "TPG Telecom (Australia)",
        "4826": "Vocus (Australia)",
        "9268": "iiNet (Australia)",
        "10148": "Telstra Internet",
        "17408": "Exetel (Australia)",
        "9790": "Vodafone Australia",
        "45671": "Aussie Broadband",
        "2764": "Spark NZ",
        "9500": "Vodafone NZ",
        "9790": "2degrees (NZ)",
        "38022": "Orcon (NZ)",
        "17746": "Orcon",
        "45177": "Vocus (NZ)",
        "24446": "Netspace (Australia)",

        # ===========================================
        # MIDDLE EAST ISPs
        # ===========================================
        "8781": "Saudi Telecom (STC)",
        "25019": "Saudi Telecom",
        "35753": "Mobily (Saudi)",
        "39386": "Saudi Telecom",
        "5384": "Etisalat (UAE)",
        "15802": "Du (UAE)",
        "8376": "Emirates Telecom",
        "50710": "Virgin Mobile (UAE)",
        "12880": "Ooredoo (Qatar)",
        "8781": "Ooredoo",
        "42298": "Batelco (Bahrain)",
        "5416": "Batelco",
        "8452": "Turkcell",
        "16135": "Turkcell",
        "12978": "Turkcell",
        "34984": "Superonline",
        "44217": "Zain (Kuwait)",
        "3225": "Zain",
        "8529": "Omantel",
        "21050": "Ooredoo Oman",
        "12400": "Partner (Israel)",
        "1680": "Bezeq (Israel)",
        "378": "Bezeq",
        "8551": "Bezeq",
        "12849": "Hot Telecom (Israel)",
        "9116": "012.net (Israel)",
        "48832": "Pelephone (Israel)",
        "50463": "Cellcom (Israel)",
        "44709": "HOT Mobile (Israel)",
        "16116": "Pelephone",

        # ===========================================
        # AFRICA ISPs
        # ===========================================
        "36992": "Etisalat (Egypt)",
        "8452": "Telecom Egypt",
        "24863": "Link Egypt",
        "37069": "Orange Egypt",
        "37558": "Vodafone Egypt",
        "36903": "MTN (South Africa)",
        "37457": "Telkom SA",
        "327693": "Afrihost (SA)",
        "37153": "Vodacom (SA)",
        "16637": "MTN",
        "29975": "Vodacom",
        "3741": "Internet Solutions (SA)",
        "5713": "SAIX (South Africa)",
        "37611": "Liquid Telecom",
        "36874": "Safaricom (Kenya)",
        "15399": "Kenya Education Network",
        "33771": "Safaricom",
        "37349": "Airtel Kenya",
        "36925": "Vodafone Ghana",
        "30985": "MTN Ghana",
        "37148": "MTN Nigeria",
        "36873": "Airtel Nigeria",
        "37282": "MainOne (Nigeria)",
        "29465": "MTN",
        "327786": "Smile (Nigeria)",
        "29571": "CITelecom",
        "8513": "Maroc Telecom",
        "6713": "IAM (Morocco)",
        "36947": "Telecom Algeria",
        "327912": "Spectranet (Nigeria)",
        "328512": "Comsats",
        "37075": "Ooredoo (Tunisia)",
        "2609": "Tunisie Telecom",
        "24758": "Orange Tunisia",

        # ===========================================
        # LATIN AMERICA ISPs
        # ===========================================
        "7738": "Telemar (Brazil)",
        "26615": "Tim Brasil",
        "16735": "Vivo (Brazil)",
        "4230": "Claro (Brazil)",
        "28573": "NET (Claro Brazil)",
        "18881": "Telefonica Brazil",
        "53013": "Algar Telecom (Brazil)",
        "19353": "Nextel Brazil",
        "27699": "Telefonica Brazil",
        "28283": "Net Virtua (Brazil)",
        "52320": "GlobeNet",
        "6057": "Administracion Nacional de Telecomunicaciones (Uruguay)",
        "6535": "Telmex (Mexico)",
        "11172": "Alestra (Mexico)",
        "8151": "Uninet (Telmex)",
        "28403": "TotalPlay (Mexico)",
        "18734": "Megacable (Mexico)",
        "13999": "Megared (Mexico)",
        "11888": "Axtel (Mexico)",
        "28548": "Cablevision (Mexico)",
        "7303": "Telecom Argentina",
        "10481": "Prima (Argentina)",
        "7908": "BT Argentina",
        "27747": "Telecentro (Argentina)",
        "11315": "IPLAN (Argentina)",
        "19037": "Metrotel (Argentina)",
        "6471": "Entel Chile",
        "7418": "Telefonica Chile (Movistar)",
        "27651": "Entel PCS (Chile)",
        "27678": "VTR (Chile)",
        "52468": "Mundo Pacifico (Chile)",
        "14080": "Cantv (Venezuela)",
        "8048": "CANTV",
        "19429": "ETB (Colombia)",
        "10299": "ETB Colombia",
        "13489": "EPM Telecomunicaciones (Colombia)",
        "262186": "Movistar Colombia",
        "52458": "Tigo (Colombia)",
        "27839": "Claro Colombia",
        "6147": "Telefonica (Peru)",
        "12252": "Claro Peru",
        "6147": "Movistar Peru",

        # ===========================================
        # CANADA ISPs
        # ===========================================
        "577": "Bell Canada",
        "6327": "Shaw",
        "812": "Rogers",
        "852": "Telus",
        "5769": "Videotron",
        "22652": "Fibrenoire",
        "855": "Bell Aliant",
        "803": "SaskTel",
        "4589": "Eastlink",
        "15290": "Allstream (Bell)",
        "11260": "Eastlink",
        "3602": "Rogers",
        "14135": "Teksavvy",
        "16395": "Distributel",
        "577": "Bell Residential",
        "813": "Eastlink",
        "6799": "Telus Internet",
        "11426": "Spectrum",
        "14061": "DigitalOcean",

        # ===========================================
        # DNS & SECURITY
        # ===========================================
        "36692": "OpenDNS (Cisco)",
        "19281": "Quad9",
        "30148": "Sucuri",
        "13335": "Cloudflare DNS",
        "15169": "Google DNS",
        "3356": "Level3 DNS",
        "19551": "Incapsula (Imperva)",
        "395747": "Imperva",
        "395954": "Imperva",
        "54994": "Prolexic (Akamai)",
        "396356": "Maxmind",
        "62597": "NSFocus",
        "40027": "DoS mitigation",
        "14618": "AWS Shield",
        "12222": "Verisign",
        "7342": "Verisign",
        "26415": "Verisign",
        "30060": "Verisign",
        "16509": "AWS WAF",

        # ===========================================
        # VPN PROVIDERS
        # ===========================================
        "9009": "M247",
        "136787": "TATA",
        "174": "Cogent (VPN)",
        "60068": "DataCamp",
        "212238": "Datacamp",
        "25369": "Hydra Communications",
        "62563": "GTHost",
        "9370": "Sakura (VPN)",
        "51167": "Contabo",
        "53667": "FranTech",
        "202425": "Private Internet Access",
        "394711": "Private Internet Access",
        "62041": "Telegram (Proxy)",
        "60404": "Leaseweb",
        "28753": "Leaseweb",
        "60781": "Leaseweb",
        "16265": "Leaseweb",
        "16276": "OVH (VPN)",
        "24940": "Hetzner (VPN)",
        "58065": "Packet Exchange",
        "49981": "WorldStream",
        "32475": "SingleHop",
        "22612": "Namecheap",
        "47583": "Hostinger",

        # ===========================================
        # GAMING & ESPORTS
        # ===========================================
        "57976": "Blizzard",
        "57469": "Valve (Steam)",
        "32590": "Valve (Steam)",
        "200990": "Twitch",
        "398355": "Riot Games",
        "394639": "Riot Games",
        "6507": "Sony PlayStation",
        "2914": "PlayStation Network",
        "18978": "Xbox Live",
        "6461": "Microsoft Xbox",
        "8075": "Xbox Live",
        "132203": "Tencent Games",
        "45090": "Tencent Games",
        "32934": "Meta (Oculus)",
        "11404": "Electronic Arts",
        "3561": "Savvis (EA)",
        "19994": "Rackspace (Epic Games)",
        "16509": "Epic Games (AWS)",
        "136787": "PUBG Mobile",
        "38266": "PUBG",
        "36040": "YouTube Gaming",
        "46489": "Twitch Streaming",
        "54113": "Fastly (Gaming CDN)",
        "20940": "Akamai Gaming",
        "57695": "Multiplay (UK)",
        "396982": "Google Stadia",
        "8075": "Xbox Cloud Gaming",
        "15169": "GeForce NOW",
        "14618": "Luna (Amazon)",

        # ===========================================
        # ENTERPRISE TECH
        # ===========================================
        "3598": "Microsoft",
        "8068": "Microsoft",
        "8069": "Microsoft",
        "8070": "Microsoft",
        "8071": "Microsoft",
        "8072": "Microsoft",
        "8073": "Microsoft",
        "8074": "Microsoft",
        "8075": "Microsoft",
        "36351": "SoftLayer (IBM)",
        "19994": "Rackspace",
        "26496": "GoDaddy",
        "46606": "Unified Layer",
        "33070": "Rackspace",
        "27357": "Rackspace",
        "12876": "Scaleway",
        "14340": "Salesforce",
        "62931": "Salesforce",
        "22612": "Namecheap",
        "29169": "Gandi",
        "21321": "Aruba (Italy)",
        "14061": "DigitalOcean",
        "30633": "Leaseweb",
        "21859": "Zenlayer",
        "199524": "G-Core Labs",
        "203214": "HugeServer",
        "24961": "myLoc",
        "31898": "Oracle (Dyn)",
        "209": "CenturyLink",
        "40676": "Psychz Networks",
        "46562": "Performive",
        "30081": "Cachefly",
        "20738": "VKontakte",
        "17501": "VKontakte",
        "47541": "VKontakte",

        # ===========================================
        # FINANCIAL & E-COMMERCE
        # ===========================================
        "19905": "Mastercard",
        "22509": "PayPal",
        "23394": "Visa",
        "15169": "Google Pay",
        "14618": "Stripe (AWS)",
        "14618": "Shopify (AWS)",
        "13335": "Square (Cloudflare)",
        "16509": "Robinhood",
        "14618": "Coinbase",
        "16509": "Binance",
        "14340": "Workday",
        "62567": "Workday",
        "701": "Bloomberg",
        "7018": "NYSE",
        "16509": "NASDAQ (AWS)",
        "14618": "eBay",
        "23640": "eBay",
        "16509": "Etsy (AWS)",
        "14618": "Airbnb",
        "396998": "Airbnb",
        "14618": "DoorDash",
        "16509": "Uber",
        "46200": "Uber",
        "14618": "Lyft",
        "16509": "Instacart",
        "16509": "Grubhub",
        "14153": "Edgecast (Walmart)",
        "16509": "Target (AWS)",
        "14618": "Costco (AWS)",
        "14340": "ServiceNow",

        # ===========================================
        # SATELLITE & RURAL INTERNET
        # ===========================================
        "14593": "Starlink",
        "7065": "Starlink (SpaceX)",
        "394141": "Starlink",
        "16974": "HughesNet",
        "33363": "HughesNet",
        "36236": "Viasat",
        "20055": "Viasat",
        "4839": "Hughes Network",
        "7029": "Windstream",
        "11963": "Rise Broadband",
        "393581": "GeoLinks",
        "54540": "Fixed Wireless",
        "13591": "Wisper ISP",
        "25973": "Starry",
        "395381": "Common Networks",
        "18530": "Webpass (Google Fiber)",

        # ===========================================
        # EDUCATION & RESEARCH
        # ===========================================
        "11537": "Internet2",
        "87": "Indiana University",
        "3701": "MIT",
        "26": "Cornell",
        "217": "Stanford",
        "32": "Stanford",
        "13": "DNIC-AS",
        "14": "Columbia University",
        "3": "MIT",
        "73": "University of Washington",
        "568": "UC San Diego",
        "2551": "NSFNet",
        "14": "Columbia",
        "2152": "Cenic (California)",
        "5765": "Merit Network",
        "6395": "Virginia Tech",
        "40": "MIT Lincoln Lab",
        "20130": "University of Pennsylvania",
        "5765": "Pennsylvania State",
        "88": "Princeton",
        "23": "NASA",
        "297": "NASA",
        "7046": "University of Oregon",
        "589": "Georgia Tech",
        "10455": "NC State",
        "46": "Rutgers",
        "71": "Harvard",
        "11": "Harvard",
        "3128": "University of Wisconsin",
        "786": "ONENet (Oklahoma)",
        "600": "OARnet (Ohio)",
        "14": "NYU",
        "12": "NYU",
        "31": "DNIC-AS",
        "20965": "GEANT (Europe)",
        "680": "DFN (Germany)",
        "2200": "RENATER (France)",
        "1103": "SURFnet (Netherlands)",
        "137": "GARR (Italy)",
        "1930": "SWITCH (Switzerland)",
        "2603": "NORDUnet",
        "513": "CERN",

        # ===========================================
        # HOSTING PROVIDERS
        # ===========================================
        "46606": "Bluehost/HostGator",
        "19871": "Network Solutions",
        "8551": "Bezeq",
        "29802": "HIVELOCITY",
        "46475": "Limestone Networks",
        "133752": "Leaseweb Singapore",
        "63128": "Coresite",
        "29791": "Voxel",
        "7992": "Cogecodata",
        "36352": "ColoCrossing",
        "174": "Cogent DC",
        "19624": "Alchemy Communications",
        "394256": "CloudFlare Inc",
        "209197": "Stark Industries",
        "51430": "AltusHost",
        "43234": "Dediserve",
        "62904": "Eonix",
        "49544": "i3D.net",
        "47869": "Netrouting",
        "35425": "Bytemark",
        "51207": "Infomaniak",
        "9304": "Hutchison Global",
        "133480": "Intergrid Group",
        "328320": "Kuroit",
        "50673": "Serverius",
        "60117": "20i",

        # ===========================================
        # MOBILE CARRIERS (ADDITIONAL)
        # ===========================================
        "22394": "Verizon Wireless",
        "21928": "T-Mobile",
        "20057": "AT&T Mobility",
        "7018": "AT&T",
        "7922": "Xfinity Mobile (Comcast)",
        "6167": "Verizon Wireless",
        "23479": "Dish Wireless",
        "11260": "Eastlink Wireless",
        "16591": "Google Fi",
        "25899": "US Cellular",
        "20001": "Spectrum Mobile",
        "5769": "Videotron Mobile",
        "812": "Rogers Wireless",
        "852": "Telus Mobility",
        "16395": "Fizz (Videotron)",
        "45143": "Telstra Mobile",
        "9790": "Vodafone Mobile (AU)",
        "133612": "Optus Mobile",
        "2516": "au by KDDI",
        "17676": "SoftBank Mobile",
        "2527": "So-net (NURO)",
        "9848": "T-Mobile Netherlands",
        "12322": "Free Mobile (France)",
        "15557": "SFR Mobile",
        "3215": "Orange Mobile",
        "5410": "Bouygues Mobile",
        "12430": "Vodafone Mobile (ES)",
        "3352": "Movistar Mobile",
        "6739": "ONO/Vodafone",
        "12576": "EE Mobile",
        "15533": "Three Mobile (UK)",
        "2529": "Sky Mobile",
        "5089": "Virgin Mobile (UK)",
        "30722": "Vodafone Mobile (IT)",
        "12874": "Fastweb Mobile",
        "21334": "Drei Mobile (Austria)",
        "6855": "A1 Mobile (Austria)",
        "44034": "Hi3G (3 Mobile Sweden)",
        "12929": "Telia Mobile",
        "8452": "Turkcell Mobile",
        "16135": "Turkcell",
        "12978": "Superonline Mobile",
        "3216": "Beeline Mobile",
        "8359": "MTS Mobile",
        "31133": "MegaFon Mobile",
        "12389": "Rostelecom Mobile",
        "45609": "Airtel Mobile (India)",
        "55836": "Jio Mobile",
        "58678": "Vi Mobile (India)",
        "9808": "China Mobile",
        "4837": "China Unicom Mobile",
        "4134": "China Telecom Mobile",
        "4766": "KT Mobile",
        "9318": "SK Telecom",
        "17858": "LG U+ Mobile",
        "7713": "Telkomsel Mobile",
        "4761": "Indosat Mobile",
        "9299": "Smart Mobile (PH)",
        "4775": "Globe Mobile",
        "4787": "Viettel Mobile",
        "45899": "VinaPhone",
        "7552": "MobiFone",
        "45629": "AIS Mobile",
        "132061": "DTAC Mobile",
        "4750": "True Mobile",
        "4818": "DiGi Mobile (MY)",
        "4788": "Celcom (TM)",
        "38466": "U Mobile",
        "17676": "SoftBank",
        "4657": "StarHub Mobile",
        "132537": "M1 Mobile",
        "8781": "STC Mobile",
        "5384": "Etisalat Mobile",
        "15802": "Du Mobile",
    }

    entries = database.get("entries", {})
    missing_asns = []
    found_asns = []

    for asn, expected_name in known_asns.items():
        if asn in entries:
            found_asns.append(asn)
        else:
            missing_asns.append((asn, expected_name))

    # Report results
    total_known = len(known_asns)
    found_count = len(found_asns)
    missing_count = len(missing_asns)
    coverage_pct = (found_count / total_known) * 100

    print(f"Known ASN check: {found_count}/{total_known} found ({coverage_pct:.1f}% coverage)")

    if missing_asns:
        print(f"Missing {missing_count} well-known ASNs:")
        for asn, name in missing_asns[:10]:  # Show first 10
            print(f"  - AS{asn}: {name}")
        if missing_count > 10:
            print(f"  ... and {missing_count - 10} more")

    # Fail if coverage is too low (less than 50% of known ASNs found)
    if coverage_pct < 50:
        print(f"ERROR: Only {coverage_pct:.1f}% of known ASNs found, data may be incomplete")
        return False

    print(f"Validation passed: {entry_count} entries")
    return True


def main():
    """Main entry point with fallback logic."""
    print("=" * 50)
    print("ASN Database Generator")
    print("=" * 50)

    asn_database = None
    existing_database = load_existing_database()

    # Strategy 1: Try PeeringDB (best source)
    peeringdb_data = fetch_peeringdb()
    if peeringdb_data:
        print("Processing PeeringDB data...")
        asn_database = transform_peeringdb_data(peeringdb_data)

        if validate_database(asn_database):
            print("PeeringDB data is valid")
        else:
            print("PeeringDB data failed validation")
            asn_database = None

    # Strategy 2: Try RIPE NCC RIS (fallback - less detailed but reliable)
    if asn_database is None:
        ripe_data = fetch_ripe_ris()
        if ripe_data:
            print("Processing RIPE NCC RIS data...")
            asn_database = transform_ripe_data(ripe_data)

            # If we have existing data, merge to preserve names
            if existing_database:
                print("Merging with existing database to preserve names...")
                asn_database = merge_databases(existing_database, asn_database)

            if validate_database(asn_database):
                print("RIPE NCC RIS data is valid")
            else:
                print("RIPE NCC RIS data failed validation")
                asn_database = None

    # Strategy 3: Keep existing database (last resort)
    if asn_database is None:
        print("\n=== All sources failed, checking existing database ===")
        if existing_database and validate_database(existing_database):
            print("WARNING: Using existing database (data may be stale)")
            # Don't overwrite - just exit successfully to keep existing file
            print(f"Keeping existing {OUTPUT_FILE}")
            print(f"Last updated: {existing_database.get('updated_at', 'Unknown')}")
            sys.exit(0)
        else:
            print("ERROR: No valid data source available and no valid existing database")
            sys.exit(1)

    # Write output
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(asn_database, f, indent=2, ensure_ascii=False)

    print("\n" + "=" * 50)
    print(f"Generated {OUTPUT_FILE}")
    print(f"  Source: {asn_database['source']}")
    print(f"  Entries: {asn_database['entry_count']}")
    print(f"  Updated: {asn_database['updated_at']}")
    print("=" * 50)


if __name__ == "__main__":
    main()
