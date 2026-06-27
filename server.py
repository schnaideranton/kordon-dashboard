"""
КОРДОН — Border Dashboard Backend
Scrapes real data from DPSU, eCherha, granica.gov.pl, financnasprava.sk
Serves API + static frontend
"""

import asyncio
import json
import math
import re
import time
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("kordon")

app = FastAPI(title="Кордон API")

# ── Cache ──────────────────────────────────────
cache = {
    "dpsu": {},        # checkpoint_id → data
    "echerha": {},     # checkpoint_id → truck/bus data
    "poland": {},      # checkpoint_id → polish side data
    "slovakia": {},    # checkpoint_id → slovak side data
    "romania": {},     # checkpoint_id → romanian side data
    "hungary": {},     # checkpoint_id → hungarian side data
    "scraped_at": {},  # source_name → iso timestamp of last SUCCESSFUL non-empty scrape
    "last_update": None,
    "errors": [],
}

SCRAPE_INTERVAL = 300  # 5 minutes
STALE_AFTER_MIN = 90   # data older than this is flagged stale (DPSU itself lags ~40min)
_scrape_lock = asyncio.Lock()  # serialize scrape_all (background loop vs /api/refresh)


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _age_minutes(iso_str):
    """Age in minutes of an ISO-8601 UTC timestamp; None if unparseable."""
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0, (datetime.now(timezone.utc) - dt).total_seconds() / 60)
    except Exception:
        return None


def _dpsu_age_minutes(s):
    """DPSU timestamps look like '2026-06-05 06:15:55' in Kyiv time (UTC+3 summer).
    Returns age in minutes, or None if unparseable."""
    if not s:
        return None
    try:
        dt = datetime.strptime(s.strip()[:19], "%Y-%m-%d %H:%M:%S")
        # Treat as Kyiv local (UTC+3); convert to UTC for comparison
        dt = dt.replace(tzinfo=timezone.utc)
        from datetime import timedelta
        dt = dt - timedelta(hours=3)
        return max(0, (datetime.now(timezone.utc) - dt).total_seconds() / 60)
    except Exception:
        return None


# ── Crossing definitions ───────────────────────
CROSSINGS = [
    {"id": "krakovets",  "name": "Краковець — Корчова",    "country": "PL", "lat": 49.975, "lng": 23.157},
    {"id": "shehyni",    "name": "Шегині — Медика",        "country": "PL", "lat": 49.792, "lng": 22.888},
    {"id": "rava_ruska", "name": "Рава-Руська — Гребенне", "country": "PL", "lat": 50.228, "lng": 23.647},
    {"id": "ustyluh",    "name": "Устилуг — Зосін",        "country": "PL", "lat": 50.879, "lng": 24.033},
    {"id": "yahodyn",    "name": "Ягодин — Дорогуськ",     "country": "PL", "lat": 51.524, "lng": 23.822},
    {"id": "hrushiv",    "name": "Грушів — Будомеж",       "country": "PL", "lat": 49.965, "lng": 23.005},
    {"id": "smilnytsia", "name": "Смільниця — Кросценко",  "country": "PL", "lat": 49.377, "lng": 22.486},
    {"id": "uzhhorod",   "name": "Ужгород — В.Немецьке",   "country": "SK", "lat": 48.658, "lng": 22.214},
    {"id": "ubla",       "name": "Убля — Убля",            "country": "SK", "lat": 48.930, "lng": 22.381},
    {"id": "chop",       "name": "Чоп — Загонь",           "country": "HU", "lat": 48.433, "lng": 22.195},
    {"id": "tysa",       "name": "Тиса — Тисабеч",        "country": "HU", "lat": 48.100, "lng": 23.575},
    {"id": "porubne",    "name": "Порубне — Сірет",        "country": "RO", "lat": 48.218, "lng": 25.953},
    {"id": "dyakove",    "name": "Дякове — Галмеу",        "country": "RO", "lat": 48.003, "lng": 23.318},
    {"id": "solotvyno",  "name": "Солотвино — С.Мармацієй","country": "RO", "lat": 47.950, "lng": 23.830},
]

# Name matching patterns for DPSU HTML scraping
# DPSU uses Ukrainian transliteration that differs from common usage:
#   "Краківець" (not "Краковець"), "Будомєж" (not "Будомеж"), "Кросьценко" (not "Кросценко")
DPSU_PATTERNS = {
    "краківець": "krakovets",
    "краковець": "krakovets",  # legacy fallback
    "шегині": "shehyni",
    "рава-руська": "rava_ruska",
    "устилуг": "ustyluh",
    "ягодин": "yahodyn",
    "грушів": "hrushiv",
    "смільниця": "smilnytsia",
    "ужгород": "uzhhorod",
    "убля": "ubla",
    "чоп": "chop",
    "тиса": "tysa",
    "порубне": "porubne",
    "дякове": "dyakove",
    "солотвино": "solotvyno",
}


# ── Scrapers ───────────────────────────────────

async def scrape_dpsu(client: httpx.AsyncClient):
    """Scrape DPSU interactive map for passenger car queue data."""
    try:
        resp = await client.get(
            "https://dpsu.gov.ua/uk/map",
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"},
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        results = {}
        for opt in soup.select("option[data-state_of_busy]"):
            name_raw = opt.get_text(strip=True).lower()
            busy_html = opt.get("data-state_of_busy", "")
            color = opt.get("data-color", "grey")
            state = opt.get("data-state", "")
            lat = opt.get("data-latitute", "")
            lng = opt.get("data-longitute", "")
            updated = opt.get("data-created_at", "")
            video = opt.get("data-video_out", "")
            cp_type = opt.get("data-type", "")

            # Only car crossings
            if cp_type != "car":
                continue

            # Match to our crossing IDs
            matched_id = None
            for pattern, cid in DPSU_PATTERNS.items():
                if pattern in name_raw:
                    matched_id = cid
                    break

            if not matched_id:
                continue

            # Parse busy info
            cars = None
            speed = None
            trucks = None

            # "Кількість легкових авто перед ППр: 42"
            car_match = re.search(r"легков\w+\s+авто.*?:\s*(\d+)", busy_html)
            if car_match:
                cars = int(car_match.group(1))

            # "Швидкість оформлення легкових авто: 30 авто/год"
            speed_match = re.search(r"швидкість.*?:\s*(\d+)", busy_html, re.IGNORECASE)
            if speed_match:
                speed = int(speed_match.group(1))

            # "Кількість вантажних авто перед ППр: 150"
            truck_match = re.search(r"вантажн\w+\s+авто.*?:\s*(\d+)", busy_html)
            if truck_match:
                trucks = int(truck_match.group(1))

            # Estimate wait time from cars and speed
            wait_minutes = None
            if cars is not None and speed and speed > 0:
                wait_minutes = round(cars / speed * 60)
            elif cars is not None:
                # Rough estimate: 2 min per car
                wait_minutes = cars * 2

            # Map color to status
            status_map = {"green": "green", "blue": "yellow", "red": "red", "grey": "grey"}
            status = status_map.get(color, "grey")
            if wait_minutes is not None:
                if wait_minutes < 30:
                    status = "green"
                elif wait_minutes < 90:
                    status = "yellow"
                elif wait_minutes < 180:
                    status = "orange"
                else:
                    status = "red"

            # Normalize open-state: tolerate wording/case changes
            # ("відкритий" / "Відкрито" / "відкрито" all mean open)
            is_open = "відкрит" in (state or "").lower()

            results[matched_id] = {
                "cars": cars,
                "trucks": trucks,
                "speed": speed,
                "waitMinutes": wait_minutes,
                "status": status,
                "isOpen": is_open,
                "webcam": video if video else None,
                "updatedAt": updated,
                "source": "ДПСУ",
            }

        # Only overwrite cache on a successful non-empty scrape — never blank
        # out good data because the site hiccuped or changed shape.
        if results:
            cache["dpsu"] = results
            cache["scraped_at"]["dpsu"] = _now_iso()
            log.info(f"DPSU: scraped {len(results)} crossings")
            return True
        log.warning("DPSU: parsed 0 crossings — keeping previous cache")
        cache["errors"].append("DPSU: parsed 0 crossings (site shape changed?)")
        return False

    except Exception as e:
        log.error(f"DPSU scrape failed: {e}")
        cache["errors"].append(f"DPSU: {e}")
        return False


async def scrape_echerha(client: httpx.AsyncClient):
    """Fetch eCherha API for truck/bus queue data."""
    results = {}
    for type_id, type_name in [(1, "truck"), (2, "bus")]:
        try:
            resp = await client.get(
                f"https://back.echerha.gov.ua/api/v4/workload/{type_id}",
                timeout=15,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "x-client-locale": "uk",
                    "x-user-agent": "UABorder/3.2.2 Web/1.1.0 User/guest",
                },
            )
            resp.raise_for_status()
            data = resp.json()

            for item in data.get("data", []):
                lat = item.get("lat")
                lng = item.get("lng")
                # Treat missing/zero coords as unusable (0,0 is in the ocean)
                if not lat or not lng:
                    continue

                # Match to the single nearest crossing within a tight radius
                best_id = None
                best_dist = 999.0
                for c in CROSSINGS:
                    d = abs(c["lat"] - lat) + abs(c["lng"] - lng)
                    if d < best_dist:
                        best_dist = d
                        best_id = c["id"]

                if best_dist > 0.35 or not best_id:
                    continue

                if best_id not in results:
                    results[best_id] = {}

                wait_sec = item.get("wait_time", 0) or 0
                count = item.get("vehicle_in_active_queues_counts", 0)
                is_paused = item.get("is_paused", False)
                wait_min = round(wait_sec / 60)
                # Clamp implausible values (upstream sometimes returns multi-day garbage)
                if wait_min > 3000:  # >50h is almost certainly stale/garbage
                    wait_min = None

                results[best_id][type_name] = {
                    "count": count,
                    "waitMinutes": wait_min,
                    "isPaused": is_paused,
                    "freeSlots": item.get("free_slots_today"),
                }

        except Exception as e:
            log.error(f"eCherha type {type_id} failed: {e}")
            cache["errors"].append(f"eCherha({type_name}): {e}")

    if results:
        cache["echerha"] = results
        cache["scraped_at"]["echerha"] = _now_iso()
        log.info(f"eCherha: scraped {len(results)} crossings")
        return True
    log.warning("eCherha: 0 crossings — keeping previous cache")
    return False


async def scrape_poland(client: httpx.AsyncClient):
    """Scrape granica.gov.pl for Polish side wait times.
    Page has 6 tables; the 1st table for UA border has 9 columns:
    Dorohusk, Zosin, Dołhobyczów, Hrebenne, Budomierz, Korczowa, Medyka, Malhowice, Krościenko
    Row 2 = car wait times in 'H:MM' format (e.g. '0:00', '1:30').
    """
    pl_names = {
        "dorohusk": "yahodyn",
        "zosin": "ustyluh",
        "hrebenne": "rava_ruska",
        "budomierz": "hrushiv",
        "korczowa": "krakovets",
        "medyka": "shehyni",
        "krościenko": "smilnytsia",
        "krosciensko": "smilnytsia",
    }
    BROWSER_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,pl;q=0.8,uk;q=0.7",
    }
    results = {}

    def parse_hmm(s):
        m = re.match(r"\s*(\d+):(\d+)", s.strip())
        if m:
            return int(m.group(1)) * 60 + int(m.group(2))
        return None

    for direction, k in [("exit", "w"), ("enter", "wj")]:
        try:
            resp = await client.get(
                f"https://granica.gov.pl/index_wait.php?p=u&v=en&k={k}",
                timeout=15,
                headers=BROWSER_HEADERS,
            )
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            for table in soup.find_all("table"):
                txt = table.get_text()
                if "Korczowa" not in txt or "Medyka" not in txt:
                    continue
                rows = table.find_all("tr")
                # Find header row with crossing names
                header_idx = None
                col_to_cid = {}
                for ri, r in enumerate(rows):
                    cells = [c.get_text(strip=True).lower() for c in r.find_all(["th", "td"])]
                    matches = sum(1 for c in cells for pl in pl_names if pl in c)
                    if matches >= 5:
                        header_idx = ri
                        for ci, c in enumerate(cells):
                            for pl, cid in pl_names.items():
                                if pl in c:
                                    col_to_cid[ci] = cid
                                    break
                        break
                if header_idx is None:
                    continue
                # Find passenger-cars row. Skip truck rows with weight markers ">7,5T".
                # Some cells may be plain "0" without H:MM colon — treat as 0.
                def cell_to_min(s):
                    s = s.strip()
                    if re.fullmatch(r"\d+:\d+", s):
                        h, mm = s.split(":")
                        return int(h) * 60 + int(mm)
                    if re.fullmatch(r"\d+", s):
                        return int(s)  # bare integer = minutes
                    return None

                for r in rows[header_idx + 1:]:
                    cells = [c.get_text(strip=True) for c in r.find_all(["th", "td"])]
                    if any(">7,5" in c or "≤7,5" in c for c in cells):
                        continue
                    # Identify columns that contain valid time/number values
                    parsed = [cell_to_min(c) for c in cells]
                    valid_count = sum(1 for v in parsed if v is not None)
                    if valid_count < 5:
                        continue
                    # First valid column = data start. Map header column 0 to first valid index.
                    first_valid = next(i for i, v in enumerate(parsed) if v is not None)
                    for col_idx, cid in col_to_cid.items():
                        data_idx = first_valid + col_idx
                        if 0 <= data_idx < len(parsed) and parsed[data_idx] is not None:
                            if cid not in results:
                                results[cid] = {}
                            results[cid][f"pl_{direction}"] = parsed[data_idx]
                    break
                break  # only first matching UA table
        except Exception as e:
            log.error(f"Poland scrape ({direction}) failed: {e}")
            cache["errors"].append(f"Poland({direction}): {e}")

    if results:
        cache["poland"] = results
        cache["scraped_at"]["poland"] = _now_iso()
        log.info(f"Poland: scraped {len(results)} crossings")
        return True
    log.warning("Poland: 0 crossings — keeping previous cache")
    return False


async def scrape_slovakia(client: httpx.AsyncClient):
    """Scrape Slovak border data."""
    try:
        resp = await client.get(
            "https://www.financnasprava.sk/sk/infoservis/hranicne-priechody",
            timeout=15,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "sk,en;q=0.9",
            },
        )
        resp.raise_for_status()
        text = resp.text.lower()

        results = {}
        sk_names = {"vysne nemecke": "uzhhorod", "vyšné nemecké": "uzhhorod", "ubla": "ubla", "ubľa": "ubla"}

        for sk_name, cid in sk_names.items():
            if sk_name in text:
                idx = text.index(sk_name)
                snippet = text[idx:idx+500]
                # Look for minutes
                time_matches = re.findall(r"(\d+)\s*min", snippet)
                if time_matches:
                    results[cid] = {"sk_wait": int(time_matches[0])}

        if results:
            cache["slovakia"] = results
            cache["scraped_at"]["slovakia"] = _now_iso()
            log.info(f"Slovakia: scraped {len(results)} crossings")
            return True
        log.warning("Slovakia: 0 crossings — keeping previous cache")
        cache["errors"].append("Slovakia: parsed 0 crossings")
        return False

    except Exception as e:
        log.error(f"Slovakia scrape failed: {e}")
        cache["errors"].append(f"Slovakia: {e}")
        return False


async def scrape_romania(client: httpx.AsyncClient):
    """Scrape politiadefrontiera.ro — data is inline JS array on page.
    They render markers with title + description containing 'Timp de așteptare X min.'
    """
    # Romanian crossing name → our ID. Their names use diacritics.
    ro_names = {
        "halmeu": "dyakove",          # Halmeu — Дякове
        "siret": "porubne",           # Siret — Порубне
        "sighetu marmației": "solotvyno",  # Sighetu — Солотвино
        "sighetul marmatiei": "solotvyno",
    }
    results = {}
    BROWSER_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ro,en;q=0.9",
    }
    # vt=1 cars, dt=2 exit (UA→RO direction has dt=1 for entry to RO)
    for direction, dt in [("exit", "1"), ("enter", "2")]:
        try:
            resp = await client.get(
                f"https://www.politiadefrontiera.ro/ro/traficonline/?vt=1&dt={dt}",
                timeout=15,
                headers=BROWSER_HEADERS,
            )
            resp.raise_for_status()
            html = resp.text
            # Markers are JS objects: {"title": 'Halmeu', ... "description": '...Timp de așteptare 39 min...'}
            # Find each marker block
            import re
            blocks = re.findall(r'\{\s*"title":\s*\'([^\']+)\'.*?"description":\s*\'(.*?)\',', html, re.DOTALL)
            for title, desc in blocks:
                key = title.lower()
                cid = None
                for ro_name, our_id in ro_names.items():
                    if ro_name in key:
                        cid = our_id
                        break
                if not cid:
                    continue
                # Extract wait: "Timp de așteptare 39 min"
                m = re.search(r"timp de a[șs]teptare\s+(\d+)\s*min", desc, re.IGNORECASE)
                if m:
                    wait = int(m.group(1))
                    if cid not in results:
                        results[cid] = {}
                    results[cid][f"ro_{direction}"] = wait
        except Exception as e:
            log.error(f"Romania scrape ({direction}) failed: {e}")
            cache["errors"].append(f"Romania({direction}): {e}")
    if results:
        cache["romania"] = results
        cache["scraped_at"]["romania"] = _now_iso()
        log.info(f"Romania: scraped {len(results)} crossings")
        return True
    log.warning("Romania: 0 crossings — keeping previous cache")
    return False


async def scrape_hungary(client: httpx.AsyncClient):
    """Best-effort: parse text bulletin from police.hu.
    Returns wait minutes for any of 3 named crossings if mentioned."""
    hu_names = {
        "tisz": "tysa",          # Tisza/Tiszabecs near Tisa
        "tiszabecs": "tysa",
        "záhony": "chop",         # Záhony — Чоп
        "zahony": "chop",
        "beregsuran": "yahodyn",  # Closest to Beregsurány — but not in our list directly
    }
    # we only map to our crossings. Beregsurány doesn't match our list cleanly; skip.
    hu_names = {"tiszabecs": "tysa", "tisz": "tysa", "záhony": "chop", "zahony": "chop"}
    results = {}
    BROWSER_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,*/*;q=0.8",
        "Accept-Language": "en,hu;q=0.8",
    }
    try:
        resp = await client.get(
            "https://www.police.hu/en/content/border-traffic-situation-at-the-hungarian-ukrainian-border",
            timeout=15,
            headers=BROWSER_HEADERS,
        )
        resp.raise_for_status()
        text = re.sub(r"<[^>]+>", " ", resp.text).lower()
        text = re.sub(r"\s+", " ", text)
        # find segments with crossing names + wait patterns
        # Patterns: "X hour", "X hours", "X minutes", "two hours"
        word2num = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8}
        for hu_name, cid in hu_names.items():
            idx = text.find(hu_name)
            if idx < 0:
                continue
            seg = text[max(0, idx-100):idx+400]
            wait_min = None
            # numeric hours
            m = re.search(r"(\d+)\s*hour", seg)
            if m:
                wait_min = int(m.group(1)) * 60
            # word hours
            if wait_min is None:
                for w, n in word2num.items():
                    if re.search(rf"\b{w}\s+hour", seg):
                        wait_min = n * 60
                        break
            # minutes
            if wait_min is None:
                m = re.search(r"(\d+)\s*minute", seg)
                if m:
                    wait_min = int(m.group(1))
            # phrase "less than 15 minutes" → ~10
            if wait_min is None and "less than 15 minute" in seg:
                wait_min = 10
            # phrase "normal" → ~20
            if wait_min is None and ("normal" in seg or "usual" in seg):
                wait_min = 20
            if wait_min is not None:
                if cid not in results:
                    results[cid] = {}
                # use as estimate
                results[cid]["hu_wait"] = wait_min
        if results:
            cache["hungary"] = results
            cache["scraped_at"]["hungary"] = _now_iso()
            log.info(f"Hungary: scraped {len(results)} crossings")
            return True
        log.warning("Hungary: 0 crossings — keeping previous cache")
        return False
    except Exception as e:
        log.error(f"Hungary scrape failed: {e}")
        cache["errors"].append(f"Hungary: {e}")
        return False


# ── Merge all sources ──────────────────────────

def merge_data():
    """Combine all sources into unified crossing data."""
    merged = {}

    for c in CROSSINGS:
        cid = c["id"]
        entry = {
            "id": cid,
            "name": c["name"],
            "country": c["country"],
            "lat": c["lat"],
            "lng": c["lng"],
            "cars": None,
            "trucks": None,
            "speed": None,
            "waitMinutes": None,
            "status": "grey",
            "isOpen": True,
            "webcam": None,
            "sources": [],
            "updatedAt": None,
        }

        # DPSU data (primary for passenger cars)
        dpsu = cache["dpsu"].get(cid)
        if dpsu:
            entry["cars"] = dpsu.get("cars")
            entry["trucks"] = dpsu.get("trucks")
            entry["speed"] = dpsu.get("speed")
            entry["waitMinutes"] = dpsu.get("waitMinutes")
            entry["status"] = dpsu.get("status", "grey")
            entry["isOpen"] = dpsu.get("isOpen", True)
            entry["webcam"] = dpsu.get("webcam")
            entry["updatedAt"] = dpsu.get("updatedAt")
            entry["sources"].append("ДПСУ")

        # eCherha (trucks/buses)
        ech = cache["echerha"].get(cid)
        if ech:
            truck_data = ech.get("truck")
            if truck_data:
                entry["trucks"] = truck_data.get("count", entry["trucks"])
                entry["truckWaitMinutes"] = truck_data.get("waitMinutes")
            bus_data = ech.get("bus")
            if bus_data:
                entry["busCount"] = bus_data.get("count")
                entry["busWaitMinutes"] = bus_data.get("waitMinutes")
                entry["busFreeSlots"] = bus_data.get("freeSlots")
            entry["sources"].append("eCherha")

        # Polish side
        pl = cache["poland"].get(cid)
        if pl:
            entry["plExitMinutes"] = pl.get("pl_exit")
            entry["plEnterMinutes"] = pl.get("pl_enter")
            entry["sources"].append("granica.gov.pl")

        # Slovakia
        sk = cache["slovakia"].get(cid)
        if sk:
            entry["skWaitMinutes"] = sk.get("sk_wait")
            entry["sources"].append("financnasprava.sk")

        # Romania
        ro = cache["romania"].get(cid)
        if ro:
            entry["roExitMinutes"] = ro.get("ro_exit")
            entry["roEnterMinutes"] = ro.get("ro_enter")
            entry["sources"].append("politiadefrontiera.ro")

        # Hungary (best-effort text parsing)
        hu = cache["hungary"].get(cid)
        if hu:
            entry["huWaitMinutes"] = hu.get("hu_wait")
            entry["sources"].append("police.hu")

        # ── Two-sided wait: UA exit (DPSU) + EU entry (host country) ──
        # For UA→EU travel: queue at Ukrainian checkpoint + queue at EU checkpoint.
        # `pl_enter` is the wait to ENTER Poland (UA→PL); same for ro_enter etc.
        ua_wait = entry["waitMinutes"]  # from DPSU (Ukrainian side)
        # `or` would treat 0 as falsy and skip valid zero waits — use first-not-None
        def first_not_none(*vals):
            for v in vals:
                if v is not None:
                    return v
            return None
        # Note: granica.gov.pl only publishes pl_exit (PL→UA direction).
        # Use it as the best PL-side estimate for either direction since
        # cross-border congestion is largely symmetric in real time.
        eu_wait = first_not_none(
            entry.get("plEnterMinutes"),
            entry.get("plExitMinutes"),
            entry.get("roEnterMinutes"),
            entry.get("huWaitMinutes"),
            entry.get("skWaitMinutes"),
        )
        eu_wait_exit = first_not_none(
            entry.get("plExitMinutes"),
            entry.get("roExitMinutes"),
            entry.get("huWaitMinutes"),
            entry.get("skWaitMinutes"),
        )

        entry["uaWaitMinutes"] = ua_wait
        entry["euWaitMinutes"] = eu_wait
        entry["euExitMinutes"] = eu_wait_exit

        # Combined wait for default UA→EU travel: sum both sides if available
        if ua_wait is not None and eu_wait is not None:
            entry["waitMinutes"] = ua_wait + eu_wait
        elif ua_wait is not None:
            entry["waitMinutes"] = ua_wait
        elif eu_wait is not None:
            entry["waitMinutes"] = eu_wait
        # else: stays None

        # ── Data freshness ──────────────────────────────────
        # Report how old the QUEUE numbers actually are — not when we last polled.
        # DPSU carries a real measurement timestamp per crossing (catches the
        # "shows 0 min but it's 14 days old" trap). EU sources are live at fetch,
        # so their age = when we last scraped that country.
        country_src = {"PL": "poland", "SK": "slovakia", "RO": "romania", "HU": "hungary"}
        ua_age = _dpsu_age_minutes(dpsu.get("updatedAt")) if dpsu else None
        eu_age = _age_minutes(cache["scraped_at"].get(country_src.get(c["country"])))

        contributing = []  # ages of the sources that actually formed waitMinutes
        if ua_wait is not None and ua_age is not None:
            contributing.append(ua_age)
        if eu_wait is not None and eu_age is not None:
            contributing.append(eu_age)
        # "At least this stale" — the most pessimistic of the contributing sources.
        data_age = round(max(contributing)) if contributing else None
        ua_age_r = round(ua_age) if ua_age is not None else None
        entry["uaAgeMinutes"] = ua_age_r
        entry["dataAgeMinutes"] = data_age
        entry["stale"] = bool(data_age is not None and data_age > STALE_AFTER_MIN)

        if entry["waitMinutes"] is not None:
            wm = entry["waitMinutes"]
            if wm < 30:
                entry["status"] = "green"
            elif wm < 90:
                entry["status"] = "yellow"
            elif wm < 180:
                entry["status"] = "orange"
            else:
                entry["status"] = "red"

        if not entry["isOpen"]:
            entry["status"] = "grey"

        merged[cid] = entry

    return merged


# ── Background scraper ─────────────────────────

async def scrape_all():
    """Run all scrapers and merge results. Serialized via a lock so the
    background loop and /api/refresh never mutate the cache concurrently."""
    async with _scrape_lock:
        cache["errors"] = []
        async with httpx.AsyncClient(follow_redirects=True) as client:
            results = await asyncio.gather(
                scrape_dpsu(client),
                scrape_echerha(client),
                scrape_poland(client),
                scrape_slovakia(client),
                scrape_romania(client),
                scrape_hungary(client),
                return_exceptions=True,
            )
            for i, r in enumerate(results):
                if isinstance(r, Exception):
                    log.error(f"Scraper {i} exception: {r}")
        cache["last_update"] = _now_iso()
        log.info(f"All scrapers done. Errors: {len(cache['errors'])}")


def sources_status():
    """Per-source health: live flag + age in minutes of last good scrape."""
    out = {}
    for key in ("dpsu", "echerha", "poland", "slovakia", "romania", "hungary"):
        ts = cache["scraped_at"].get(key)
        age = _age_minutes(ts)
        out[key] = {
            "live": len(cache[key]) > 0,
            "ageMinutes": round(age) if age is not None else None,
        }
    return out


async def scrape_loop():
    """Background loop that scrapes every SCRAPE_INTERVAL seconds. Supervised:
    if it ever crashes, log and keep going rather than dying silently."""
    while True:
        try:
            await scrape_all()
        except Exception as e:
            log.error(f"Scrape loop error: {e}")
        await asyncio.sleep(SCRAPE_INTERVAL)


@asynccontextmanager
async def lifespan(app):
    # Warm the cache before serving, then keep a reference to the loop task.
    task = asyncio.create_task(scrape_loop())
    app.state.scrape_task = task
    yield
    task.cancel()


app.router.lifespan_context = lifespan


# ── API Endpoints ──────────────────────────────

def _flat_sources():
    """Backwards-compatible flat bool map (frontend source dots rely on this)."""
    return {k: v["live"] for k, v in sources_status().items()}


@app.get("/api/crossings")
async def get_crossings():
    """Return merged crossing data from all sources."""
    merged = merge_data()
    return JSONResponse(
        {
            "crossings": merged,
            "lastUpdate": cache["last_update"],
            "sources": _flat_sources(),
            "sourcesDetail": sources_status(),
            "errors": cache["errors"][-10:],  # Last 10 errors
        },
        headers={"Cache-Control": "public, max-age=30, stale-while-revalidate=120"},
    )


@app.get("/api/health")
async def health():
    return {"status": "ok", "lastUpdate": cache["last_update"], "sources": sources_status()}


@app.post("/api/refresh")
async def refresh():
    """Force refresh all data. If a scrape is already running, just return
    the current snapshot instead of piling on a second concurrent scrape."""
    if not _scrape_lock.locked():
        await scrape_all()
    merged = merge_data()
    return JSONResponse({
        "crossings": merged,
        "lastUpdate": cache["last_update"],
        "sources": _flat_sources(),
        "sourcesDetail": sources_status(),
    })


async def _osrm_route(client, from_lng, from_lat, to_lng, to_lat, sem):
    # 'simplified' geometry cuts the payload dramatically vs 'full' while
    # still drawing a road-shaped line.
    url = (f"https://router.project-osrm.org/route/v1/driving/"
           f"{from_lng},{from_lat};{to_lng},{to_lat}"
           f"?overview=simplified&geometries=geojson")
    async with sem:
        for attempt in range(3):
            try:
                resp = await client.get(url)
                if resp.status_code == 429:
                    await asyncio.sleep(1 + attempt)
                    continue
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("code") == "Ok" and data.get("routes"):
                        r = data["routes"][0]
                        return {
                            "distKm": round(r["distance"] / 1000),
                            "driveMin": round(r["duration"] / 60),
                            "geometry": r["geometry"]["coordinates"],
                        }
                return None
            except Exception as e:
                log.warning(f"OSRM failed (attempt {attempt+1}): {e}")
                await asyncio.sleep(0.5)
        return None


def _valid_coord(lat, lng):
    return (lat is not None and lng is not None
            and -90 <= lat <= 90 and -180 <= lng <= 180)


# A point roughly in the middle of Ukraine. Used only to decide which side of a
# crossing is "Ukraine" vs "the EU country".
UA_CENTROID = (49.0, 32.0)


def _approach_points(lat, lng, km=15.0):
    """Two points ~km on each side of a border crossing: one nudged toward the
    Ukrainian interior, one toward the EU country. Routing each leg to the point
    on the SAME side as that leg's endpoint stops OSRM from "cheating" across a
    different open border to reach a point that sits on the borderline."""
    coslat = math.cos(math.radians(lat)) or 1e-6
    # Direction toward Ukraine, in km-space (scale lng by cos(lat))
    vy = UA_CENTROID[0] - lat
    vx = (UA_CENTROID[1] - lng) * coslat
    norm = math.hypot(vx, vy) or 1.0
    off = km / 111.0  # km → degrees of latitude
    dlat = (vy / norm) * off
    dlng = (vx / norm) * off / coslat
    ua = (lat + dlat, lng + dlng)   # toward Ukraine
    eu = (lat - dlat, lng - dlng)   # toward the EU country
    return ua, eu


def _l1(a, b_lat, b_lng):
    return abs(a[0] - b_lat) + abs(a[1] - b_lng)


@app.get("/api/routes")
async def get_routes(from_lat: float, from_lng: float, to_lat: float = None, to_lng: float = None):
    """Fetch OSRM driving routes: origin→border, and border→destination if to_* set.
    Each leg is routed to the crossing's approach point on the SAME side as the
    leg's endpoint, so the path can't shortcut through another country's crossing."""
    if not _valid_coord(from_lat, from_lng):
        return JSONResponse({"error": "invalid from coordinates"}, status_code=422)
    has_dest = to_lat is not None and to_lng is not None
    if has_dest and not _valid_coord(to_lat, to_lng):
        return JSONResponse({"error": "invalid to coordinates"}, status_code=422)

    sem = asyncio.Semaphore(5)  # be gentle with the public OSRM demo server
    routes = {}
    routes_after = {}
    async with httpx.AsyncClient(timeout=12) as client:
        async def leg_to_border(cp):
            ua, eu = _approach_points(cp["lat"], cp["lng"])
            # approach point on the origin's side
            o = ua if _l1(ua, from_lat, from_lng) < _l1(eu, from_lat, from_lng) else eu
            r = await _osrm_route(client, from_lng, from_lat, o[1], o[0], sem)
            if r:
                routes[cp["id"]] = r

        async def leg_after(cp):
            ua, eu = _approach_points(cp["lat"], cp["lng"])
            # approach point on the destination's side
            d = ua if _l1(ua, to_lat, to_lng) < _l1(eu, to_lat, to_lng) else eu
            r = await _osrm_route(client, d[1], d[0], to_lng, to_lat, sem)
            if r:
                routes_after[cp["id"]] = r

        tasks = [leg_to_border(cp) for cp in CROSSINGS]
        if has_dest:
            tasks += [leg_after(cp) for cp in CROSSINGS]
        await asyncio.gather(*tasks)

    return JSONResponse(
        {"routes": routes, "routesAfter": routes_after},
        headers={"Cache-Control": "public, max-age=120"},
    )


# ── Serve frontend ─────────────────────────────

@app.get("/")
async def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")
