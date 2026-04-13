"""
КОРДОН — Border Dashboard Backend
Scrapes real data from DPSU, eCherha, granica.gov.pl, financnasprava.sk
Serves API + static frontend
"""

import asyncio
import json
import re
import time
import logging
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
    "last_update": None,
    "errors": [],
}

SCRAPE_INTERVAL = 300  # 5 minutes


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
DPSU_PATTERNS = {
    "краковець": "krakovets",
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

            results[matched_id] = {
                "cars": cars,
                "trucks": trucks,
                "speed": speed,
                "waitMinutes": wait_minutes,
                "status": status,
                "isOpen": state == "відкритий",
                "webcam": video if video else None,
                "updatedAt": updated,
                "source": "ДПСУ",
            }

        cache["dpsu"] = results
        log.info(f"DPSU: scraped {len(results)} crossings")
        return True

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
                lat = item.get("lat", 0)
                lng = item.get("lng", 0)

                # Match by proximity
                best_id = None
                best_dist = 999
                if lat is None or lng is None:
                    continue
                for c in CROSSINGS:
                    d = abs(c["lat"] - lat) + abs(c["lng"] - lng)
                    if d < best_dist:
                        best_dist = d
                        best_id = c["id"]

                if best_dist > 0.5 or not best_id:
                    continue

                if best_id not in results:
                    results[best_id] = {}

                wait_sec = item.get("wait_time", 0)
                count = item.get("vehicle_in_active_queues_counts", 0)
                is_paused = item.get("is_paused", False)

                results[best_id][type_name] = {
                    "count": count,
                    "waitMinutes": round(wait_sec / 60) if wait_sec else 0,
                    "isPaused": is_paused,
                    "freeSlots": item.get("free_slots_today"),
                }

        except Exception as e:
            log.error(f"eCherha type {type_id} failed: {e}")
            cache["errors"].append(f"eCherha({type_name}): {e}")

    cache["echerha"] = results
    log.info(f"eCherha: scraped {len(results)} crossings")
    return len(results) > 0


async def scrape_poland(client: httpx.AsyncClient):
    """Scrape granica.gov.pl for Polish side wait times."""
    results = {}

    # Polish crossing name → our ID
    pl_names = {
        "korczowa": "krakovets",
        "medyka": "shehyni",
        "hrebenne": "rava_ruska",
        "zosin": "ustyluh",
        "dorohusk": "yahodyn",
        "budomierz": "hrushiv",
        "krościenko": "smilnytsia",
        "kroscien": "smilnytsia",
    }

    for direction, k in [("exit", "w"), ("enter", "wj")]:
        try:
            resp = await client.get(
                f"https://granica.gov.pl/index_wait.php?p=u&v=en&k={k}",
                timeout=15,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            resp.raise_for_status()
            text = resp.text.lower()

            for pl_name, cid in pl_names.items():
                if pl_name in text:
                    # Try to extract wait time near the name
                    # Pattern: "X:XXh" or "Xh" near the crossing name
                    idx = text.index(pl_name)
                    snippet = text[idx:idx+300]
                    time_match = re.search(r"(\d+):(\d+)\s*h", snippet)
                    if time_match:
                        hours = int(time_match.group(1))
                        mins = int(time_match.group(2))
                        total_mins = hours * 60 + mins
                        if cid not in results:
                            results[cid] = {}
                        results[cid][f"pl_{direction}"] = total_mins

        except Exception as e:
            log.error(f"Poland scrape ({direction}) failed: {e}")
            cache["errors"].append(f"Poland({direction}): {e}")

    cache["poland"] = results
    log.info(f"Poland: scraped {len(results)} crossings")
    return len(results) > 0


async def scrape_slovakia(client: httpx.AsyncClient):
    """Scrape Slovak border data."""
    try:
        resp = await client.get(
            "https://www.financnasprava.sk/sk/infoservis/hranicne-priechody",
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"},
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

        cache["slovakia"] = results
        log.info(f"Slovakia: scraped {len(results)} crossings")
        return True

    except Exception as e:
        log.error(f"Slovakia scrape failed: {e}")
        cache["errors"].append(f"Slovakia: {e}")
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

        # If no DPSU data but have other sources, estimate status
        if entry["waitMinutes"] is None:
            # Try Polish data
            pw = entry.get("plExitMinutes") or entry.get("skWaitMinutes")
            if pw:
                entry["waitMinutes"] = pw

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
    """Run all scrapers and merge results."""
    cache["errors"] = []
    async with httpx.AsyncClient(follow_redirects=True) as client:
        results = await asyncio.gather(
            scrape_dpsu(client),
            scrape_echerha(client),
            scrape_poland(client),
            scrape_slovakia(client),
            return_exceptions=True,
        )
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                log.error(f"Scraper {i} exception: {r}")

    cache["last_update"] = datetime.now(timezone.utc).isoformat()
    log.info(f"All scrapers done. Errors: {len(cache['errors'])}")


async def scrape_loop():
    """Background loop that scrapes every SCRAPE_INTERVAL seconds."""
    while True:
        try:
            await scrape_all()
        except Exception as e:
            log.error(f"Scrape loop error: {e}")
        await asyncio.sleep(SCRAPE_INTERVAL)


@app.on_event("startup")
async def startup():
    asyncio.create_task(scrape_loop())


# ── API Endpoints ──────────────────────────────

@app.get("/api/crossings")
async def get_crossings():
    """Return merged crossing data from all sources."""
    merged = merge_data()
    return JSONResponse({
        "crossings": merged,
        "lastUpdate": cache["last_update"],
        "sources": {
            "dpsu": len(cache["dpsu"]) > 0,
            "echerha": len(cache["echerha"]) > 0,
            "poland": len(cache["poland"]) > 0,
            "slovakia": len(cache["slovakia"]) > 0,
        },
        "errors": cache["errors"][-10:],  # Last 10 errors
    })


@app.get("/api/health")
async def health():
    return {"status": "ok", "lastUpdate": cache["last_update"]}


@app.post("/api/refresh")
async def refresh():
    """Force refresh all data."""
    await scrape_all()
    merged = merge_data()
    return JSONResponse({
        "crossings": merged,
        "lastUpdate": cache["last_update"],
        "sources": {
            "dpsu": len(cache["dpsu"]) > 0,
            "echerha": len(cache["echerha"]) > 0,
            "poland": len(cache["poland"]) > 0,
            "slovakia": len(cache["slovakia"]) > 0,
        },
    })


# ── Serve frontend ─────────────────────────────

@app.get("/")
async def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")
