import os, json, math, asyncio, csv
from datetime import datetime, date
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Optional

import httpx
import websockets
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from discord.ext import commands
from discord import app_commands
import discord
from dotenv import load_dotenv

# ---------- Paths / env ----------
BASE_DIR = Path(__file__).parent.resolve()
load_dotenv(BASE_DIR / ".env")

TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))
EODHD_KEY = os.getenv("EODHD_API_KEY")
TZ = ZoneInfo(os.getenv("TZ", "America/New_York"))
DATA_FILE = BASE_DIR / "positions.json"

# Strongly recommended for instant slash sync in your server
GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "0"))
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

# ---------- REST endpoints ----------
BASE_INTRADAY = "https://eodhd.com/api/intraday/{s}?api_token={k}&interval=1m&fmt=json"
BASE_EOD      = "https://eodhd.com/api/eod/{s}?api_token={k}&from={d}&to={d}&fmt=json"
BASE_MP_OPT   = "https://eodhd.com/api/market-params/options?{q}"

# ---------- Simple state ----------
PRICE_CACHE: dict[str, float] = {}
_SUBS: set[str] = set()
_WS: Optional[websockets.WebSocketClientProtocol] = None
_WS_TASK: Optional[asyncio.Task] = None
_RESUB_REQUESTED = asyncio.Event()

# ---------- JSON persistence ----------
def _load():
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            text = f.read().encode("utf-8", errors="ignore").decode("utf-8", errors="ignore").strip()
        if not text:
            raise ValueError("empty")
        data = json.loads(text)
        if not isinstance(data, dict):
            raise ValueError("bad schema")
        if "users" not in data:
            cc = data.get("cc", []); closed = data.get("closed", [])
            data = {"users": {"legacy": {"cc": cc, "csp": [], "closed": closed}}}
        # ensure per-user buckets exist
        for uid, bucket in list(data.get("users", {}).items()):
            if not isinstance(bucket, dict):
                data["users"][uid] = {"cc": [], "csp": [], "closed": []}
            else:
                bucket.setdefault("cc", [])
                bucket.setdefault("csp", [])
                bucket.setdefault("closed", [])
        return data
    except Exception:
        return {"users": {}}

def _save(data):
    tmp = DATA_FILE.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, DATA_FILE)

def _ensure_user(uid: str):
    d = _load()
    users = d.setdefault("users", {})
    if uid not in users:
        users[uid] = {"cc": [], "csp": [], "closed": []}
        if "legacy" in users:
            users[uid] = users.pop("legacy")
            users[uid].setdefault("csp", [])
    else:
        users[uid].setdefault("csp", [])
        users[uid].setdefault("closed", [])
    _save(d)
    return d

# ---------- Date helpers ----------
def parse_exp_str(s: str) -> date:
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%m-%d-%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        raise ValueError(f"Unrecognized expiry format: {s!r}. Use YYYY-MM-DD or MM-DD-YYYY.")

def iso_exp_str(s: str) -> str:
    return parse_exp_str(s).strftime("%Y-%m-%d")

def display_mdy(s: str) -> str:
    return parse_exp_str(s).strftime("%m-%d-%Y")

def dte(exp_str: str, now_dt: datetime) -> int:
    return (parse_exp_str(exp_str) - now_dt.date()).days

def _compute_next_id(bucket: dict) -> int:
    # Look across open CC, CSP, and CLOSED to avoid reuse
    ids = [p.get("id", 0) for p in bucket.get("cc", [])]
    ids += [p.get("id", 0) for p in bucket.get("csp", [])]
    ids += [p.get("id", 0) for p in bucket.get("closed", [])]
    return (max(ids) if ids else 0) + 1

def _next_id(uid: str) -> int:
    d, bucket = _get_user_bucket(uid)
    nid = bucket.get("_next_id")
    if not isinstance(nid, int) or nid < 1:
        nid = _compute_next_id(bucket)
    pid = nid
    bucket["_next_id"] = nid + 1
    _save(d)
    return pid

# ---------- HTTP helpers ----------
async def fetch_json(cli, url, params=None):
    r = await cli.get(url, params=params or {}, timeout=20)
    r.raise_for_status()
    return r.json()

async def get_intraday_last(cli, sym: str) -> Optional[float]:
    key = sym
    if key in PRICE_CACHE:
        return PRICE_CACHE[key]
    u = BASE_INTRADAY.format(s=sym, k=EODHD_KEY)
    try:
        j = await fetch_json(cli, u)
        if isinstance(j, list) and j:
            last_px = float(j[-1]["close"])
            PRICE_CACHE[key] = last_px
            return last_px
    except Exception:
        return None
    return None

async def get_official_close(cli, ticker: str) -> Optional[float]:
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    try:
        j = await fetch_json(cli, BASE_EOD.format(s=ticker, k=EODHD_KEY, d=today))
        if isinstance(j, list) and j:
            return float(j[-1]["close"])
    except Exception:
        return None
    return None

async def get_call_mid(cli, ticker: str, exp_yyyy_mm_dd: str, strike: float) -> Optional[float]:
    params = {
        "filter": f"ticker:eq:{ticker},type:eq:call,exp_date:ge:{exp_yyyy_mm_dd}",
        "fields": "ticker,bid,ask,last,exp_date,strike,type",
        "page[limit]": "5",
        "sort": "-exp_date",
        "api_token": EODHD_KEY,
    }
    try:
        j = await fetch_json(cli, BASE_MP_OPT, params=params)
    except Exception:
        return None

    data = j.get("data") or []
    if not isinstance(data, list) or not data:
        return None

    def mid_from(attrs):
        bid = attrs.get("bid"); ask = attrs.get("ask"); last = attrs.get("last")
        mids = []
        if isinstance(bid, (int,float)) and isinstance(ask, (int,float)) and ask>0:
            mids.append((bid+ask)/2)
        if isinstance(last, (int,float)) and last>0:
            mids.append(last)
        return sum(mids)/len(mids) if mids else None

    for item in data:
        attrs = item.get("attributes", {})
        if str(attrs.get("exp_date","")).startswith(exp_yyyy_mm_dd) and abs(float(attrs.get("strike",0))-float(strike))<1e-6:
            return mid_from(attrs)
    return None

# ---------- Position helpers ----------
def _norm_us(s: str) -> str:
    s = s.strip().upper()
    return f"{s}.US"

def dist_pct(u, strike):
    try:
        return (strike - u) / u * 100
    except Exception:
        return float("nan")

def btc_hit(credit, mid):
    try:
        if credit and mid:
            pct = (credit - mid) / credit * 100
            return pct >= 50.0, pct
    except Exception:
        pass
    return False, None

def request_ws_resub():
    _RESUB_REQUESTED.set()

# ---------- Discord ----------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------- Per-user data ----------
def _get_user_bucket(uid: str):
    d = _ensure_user(uid)
    return d, d["users"][uid]

def positions(uid: str):
    return _get_user_bucket(uid)[1]["cc"]

def _find_pos(uid: str, pid: int):
    bucket = _get_user_bucket(uid)[1]
    for p in bucket["cc"]:
        if p["id"] == pid:
            return p
    return None

def add_pos(uid: str, t, s, c, e, cr):
    d, bucket = _get_user_bucket(uid)
    next_id = (max([p["id"] for p in bucket["cc"]], default=0) + 1)
    bucket["cc"].append({
        "id": next_id,
        "ticker": t.upper(),
        "strike": float(s),
        "contracts": int(c),
        "expiry": iso_exp_str(e),
        "entry_credit": float(cr),
        "created_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
    })
    _save(d); request_ws_resub()
    return next_id

def add_csp(uid: str, t, s, c, e, cr):
    d, bucket = _get_user_bucket(uid)
    next_id = (max([p["id"] for p in bucket.get("csp", [])], default=0) + 1)
    bucket["csp"].append({
        "id": next_id,
        "ticker": t.upper(),
        "strike": float(s),
        "contracts": int(c),
        "expiry": iso_exp_str(e),
        "entry_credit": float(cr),
        "created_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
    })
    _save(d); request_ws_resub()
    return next_id

def rm_pos(uid: str, pid: int):
    d, bucket = _get_user_bucket(uid)
    before = len(bucket["cc"])
    bucket["cc"] = [p for p in bucket["cc"] if p["id"] != pid]
    _save(d); request_ws_resub()
    return len(bucket["cc"]) < before

def edit_pos(uid: str, pid: int, *, ticker=None, strike=None, contracts=None, expiry=None, credit=None):
    d, bucket = _get_user_bucket(uid)
    p = _find_pos(uid, pid)
    if not p: return False
    if ticker is not None:    p["ticker"] = str(ticker).upper()
    if strike is not None:    p["strike"] = float(strike)
    if contracts is not None: p["contracts"] = int(contracts)
    if expiry is not None:    p["expiry"] = iso_exp_str(expiry)
    if credit is not None:    p["entry_credit"] = float(credit)
    _save(d); request_ws_resub()
    return True

def close_pos(uid: str, pid: int, btc_price: float | None):
    d, bucket = _get_user_bucket(uid)
    idx = next((i for i, p in enumerate(bucket["cc"]) if p["id"] == pid), None)
    if idx is None: return None
    p = bucket["cc"].pop(idx)
    pnl_pct = None
    if btc_price is not None:
        try: pnl_pct = (p["entry_credit"] - float(btc_price)) / p["entry_credit"] * 100.0
        except Exception: pnl_pct = None
    archived = {
        **p,
        "closed_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "btc_price": btc_price if btc_price is not None else None,
        "pnl_pct": round(pnl_pct, 2) if pnl_pct is not None else None,
    }
    bucket["closed"].append(archived)
    _save(d); request_ws_resub()
    return archived

def user_closed(uid: str, n: int = 10):
    d, bucket = _get_user_bucket(uid)
    return bucket["closed"][-n:]

# ---------- Trade Log Utilities ----------
from pathlib import Path as _Path
from datetime import timezone as _timezone

# New schema (append-only, Excel friendly):
# - trade_id: stable identifier for the trade (we use position_id)
# - expiration: YYYY-MM-DD (Excel parses as date)
# - timestamp_local: human-friendly local time
TRADE_LOG_FIELDS = [
    "event",            # OPEN_CC | CLOSE_CC | (later: OPEN_CSP, CLOSE_CSP)
    "trade_id",         # == position_id for now (stable per open/close pair)
    "user_id",
    "position_id",
    "timestamp_utc",    # ISO 8601 Zulu
    "timestamp_local",  # YYYY-MM-DD HH:MM:SS AM/PM TZ
    "ticker",
    "option_type",      # C or P
    "strike",
    "contracts",
    "expiration",       # YYYY-MM-DD
    "premium_credit",   # open credit
    "premium_debit",    # close debit (BTC)
    "pnl",              # credit - debit
    "delta",
    "gamma",
    "theta",
    "vega",
    "iv"
]

# Support migration from the prior, smaller header
OLD_FIELDS_VARIANTS = [
    ["event","user_id","position_id","timestamp_utc","ticker","option_type","strike","contracts","expiration","premium_credit","premium_debit","pnl","delta","gamma","theta","vega","iv"],
]

_LOG_DIR = _Path("data")
_LOG_DIR.mkdir(exist_ok=True)

def _user_log_path(user_id: str) -> _Path:
    p = _LOG_DIR / f"{user_id}_trades.csv"
    if not p.exists():
        with p.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=TRADE_LOG_FIELDS)
            writer.writeheader()
        return p
    # If exists, ensure header is upgraded
    _ensure_log_schema(p)
    return p

def _read_csv_rows(path: _Path) -> tuple[list[str], list[dict]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        hdr = reader.fieldnames or []
        return hdr, list(reader)

def _write_csv_rows(path: _Path, rows: list[dict]):
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TRADE_LOG_FIELDS)
        w.writeheader()
        for r in rows:
            # Fill missing fields
            out = {k: r.get(k, "") for k in TRADE_LOG_FIELDS}
            w.writerow(out)

def _ensure_log_schema(path: _Path):
    try:
        hdr, rows = _read_csv_rows(path)
        if hdr == TRADE_LOG_FIELDS:
            return  # already new schema
        # If the existing header is one of the old variants, migrate
        if hdr in OLD_FIELDS_VARIANTS:
            migrated = []
            for r in rows:
                migrated.append({
                    "event": r.get("event",""),
                    "trade_id": r.get("position_id",""),  # new
                    "user_id": r.get("user_id",""),
                    "position_id": r.get("position_id",""),
                    "timestamp_utc": r.get("timestamp_utc",""),
                    "timestamp_local": "",                # unknown historically
                    "ticker": r.get("ticker",""),
                    "option_type": r.get("option_type",""),
                    "strike": r.get("strike",""),
                    "contracts": r.get("contracts",""),
                    # Normalize old expiration (may be yyyymmdd) -> YYYY-MM-DD if possible
                    "expiration": _normalize_any_expiration(r.get("expiration","")),
                    "premium_credit": r.get("premium_credit",""),
                    "premium_debit": r.get("premium_debit",""),
                    "pnl": r.get("pnl",""),
                    "delta": r.get("delta",""),
                    "gamma": r.get("gamma",""),
                    "theta": r.get("theta",""),
                    "vega": r.get("vega",""),
                    "iv": r.get("iv",""),
                })
            _write_csv_rows(path, migrated)
        else:
            # Unknown header: keep file but append using current schema (header mismatch is non-fatal for us)
            pass
    except Exception:
        # Fail-soft: don't block the bot
        pass

def _normalize_any_expiration(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    # Already ISO?
    try:
        return datetime.strptime(s, "%Y-%m-%d").strftime("%Y-%m-%d")
    except Exception:
        pass
    # YYYYMMDD -> YYYY-MM-DD
    if len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    # Try parse via helpers
    try:
        return parse_exp_str(s).strftime("%Y-%m-%d")
    except Exception:
        return s

def _now_iso() -> str:
    return datetime.now(_timezone.utc).isoformat(timespec="seconds")

def _now_local_str() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d %I:%M:%S %p %Z")

def append_trade_log(user_id: str, row: dict):
    path = _user_log_path(user_id)
    clean = {k: row.get(k, "") for k in TRADE_LOG_FIELDS}
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TRADE_LOG_FIELDS)
        writer.writerow(clean)

def get_option_greeks_snapshot(ticker: str, option_type: str, strike: float, expiry_yyyymmdd: str) -> dict:
    # Placeholder; wire to your data source later
    return {"delta":"","gamma":"","theta":"","vega":"","iv":""}

def log_btc_close(uid: str, position_id: int, ticker: str, strike: float, contracts: int, expiry_yyyy_mm_dd: str, debit_paid: float | None, credit_received_at_open: float | None):
    greeks = get_option_greeks_snapshot(ticker, "C", float(strike), expiry_yyyy_mm_dd.replace("-", ""))
    pnl = ""
    if debit_paid is not None and credit_received_at_open is not None:
        try:
            pnl = round(float(credit_received_at_open) - float(debit_paid), 2)
        except Exception:
            pnl = ""
    append_trade_log(uid, {
        "event": "CLOSE_CC",
        "trade_id": position_id,
        "user_id": uid,
        "position_id": position_id,
        "timestamp_utc": _now_iso(),
        "timestamp_local": _now_local_str(),
        "ticker": str(ticker).upper(),
        "option_type": "C",
        "strike": float(strike) if strike is not None else "",
        "contracts": int(contracts) if contracts is not None else "",
        "expiration": _normalize_any_expiration(expiry_yyyy_mm_dd),
        "premium_credit": credit_received_at_open if credit_received_at_open is not None else "",
        "premium_debit": debit_paid if debit_paid is not None else "",
        "pnl": pnl,
        "delta": greeks["delta"],
        "gamma": greeks["gamma"],
        "theta": greeks["theta"],
        "vega": greeks["vega"],
        "iv": greeks["iv"],
    })

# ---------- Embeds ----------
def _fmt_line(p: dict, u: float | None, o: float | None, days: int) -> tuple[str, bool]:
    exp_disp = display_mdy(p["expiry"])
    line = f"{p['ticker']} {p['strike']:.2f}C ×{p['contracts']} — exp {exp_disp} ({days} DTE)"
    if u is not None:
        line += f" | Px ${u:.2f} | {dist_pct(u, p['strike']):.1f}% to strike"
    if o is not None:
        line += f" | Opt ~${o:.2f}"
    hit, pct = btc_hit(p['entry_credit'], o)
    if hit:
        line += f" | ✅ BTC {pct:.0f}%"
    return line, hit

def _decision_label(hit: bool, days: int, px: Optional[float], strike: float) -> str:
    if hit: return "BTC ✅"
    if days <= 7: return "Watch ⏳"
    if px is not None and strike:
        away = dist_pct(px, strike)
        if away <= 4: return "Roll-watch ↔"
    return "Hold 🧊"

async def build_positions_embed(uid: str, now: datetime) -> discord.Embed:
    emb = discord.Embed(title="Premium Pilot — Position Summary", color=0x2b90d9, timestamp=now)
    emb.set_footer(text=f"As of {now.strftime('%I:%M %p %Z')}")
    async with httpx.AsyncClient() as cli:
        user_obj = await bot.fetch_user(int(uid))
        name = f"User {uid}"
        try:
            if user_obj:
                name = getattr(user_obj, "display_name", None) or user_obj.name
        except Exception:
            pass

        ps = positions(uid)
        if not ps:
            emb.add_field(name=name, value="No covered call positions.", inline=False)
            return emb

        lines = []
        for p in ps:
            sym_us = _norm_us(p["ticker"])
            u = PRICE_CACHE.get(sym_us)
            if u is None:
                u = await get_intraday_last(cli, p["ticker"]) or await get_official_close(cli, p["ticker"])
            o = await get_call_mid(cli, p["ticker"], p["expiry"], p["strike"])
            days = dte(p["expiry"], now)
            line, hit = _fmt_line(p, u, o, days)
            decision = _decision_label(hit, days, u, p["strike"])
            lines.append(f"**{decision}**\n{line}")

        emb.add_field(name=name, value="\n".join(lines), inline=False)

    emb.set_footer(text="Rules: BTC @50–60% profit & ≤7 DTE • Roll-watch if ≤4% to strike or ∆≈strike")
    return emb

# ---------- EoD + Intraday dispatch ----------
async def send_public_eod():
    ch = bot.get_channel(CHANNEL_ID)
    if not ch: return
    now = datetime.now(TZ)
    embs = []
    d = _load()
    for uid in d.get("users", {}):
        emb = await build_positions_embed(uid, now)
        embs.append(emb)
    if embs:
        await ch.send(embeds=embs)

async def send_dm_eods():
    now = datetime.now(TZ)
    d = _load()
    for uid in d.get("users", {}):
        try:
            user = await bot.fetch_user(int(uid))
            if not user: continue
            emb = await build_positions_embed(uid, now)
            await user.send(embed=emb)
        except Exception:
            pass

async def eod_job():
    await send_public_eod()
    await send_dm_eods()

async def intraday_rules_tick():
    return

# ---------- WS subscription mgmt ----------
def symbol_variants(s: str) -> list[str]:
    s = s.strip().upper()
    return [s, f"{s}.US", f"US.{s}"]

def all_symbols_in_positions() -> set[str]:
    d = _load()
    users = d.get("users", {})
    syms = {_norm_us(p["ticker"]) for u in users.values() for p in u.get("cc", [])}
    return {s for s in syms if s}

async def refresh_ws_subscriptions(ws):
    global _SUBS
    want = all_symbols_in_positions()
    to_add = want - _SUBS
    to_del = _SUBS - want
    if to_add:
        await ws.send(json.dumps({"action": "subscribe", "symbols": list(to_add)}))
        _SUBS |= to_add
    if to_del:
        await ws.send(json.dumps({"action": "unsubscribe", "symbols": list(to_del)}))
        _SUBS -= to_del

async def ws_loop():
    global _WS
    while True:
        try:
            async with websockets.connect("wss://ws.eodhd.com/ws/real-time?api_token="+EODHD_KEY) as ws:
                _WS = ws
                await refresh_ws_subscriptions(ws)
                while True:
                    msg = await ws.recv()
                    try:
                        j = json.loads(msg)
                        sym = j.get("s") or j.get("code") or j.get("symbol")
                        px = j.get("p") or j.get("close")
                        if sym and isinstance(px, (int,float)):
                            PRICE_CACHE[sym] = float(px)
                    except Exception:
                        pass
        except Exception:
            await asyncio.sleep(5)

def ensure_ws_task():
    global _WS_TASK
    if _WS_TASK is None or _WS_TASK.done():
        _WS_TASK = asyncio.create_task(ws_loop())

async def ensure_ws_started():
    ensure_ws_task()

# ---------- Modals ----------
class CoveredCallModal(discord.ui.Modal, title="Add New Position — Covered Call"):
    ticker    = discord.ui.TextInput(label="Ticker", placeholder="SOFI", max_length=10)
    strike    = discord.ui.TextInput(label="Strike", placeholder="32")
    contracts = discord.ui.TextInput(label="Contracts", placeholder="2")
    expiry    = discord.ui.TextInput(label="Expiry (MM-DD-YYYY or YYYY-MM-DD)", placeholder="11-14-2025")
    credit    = discord.ui.TextInput(label="Entry Credit ($)", placeholder="0.56")

    async def on_submit(self, interaction: discord.Interaction):
        uid = str(interaction.user.id)
        try:
            pid = add_pos(uid, self.ticker.value, float(self.strike.value), int(self.contracts.value),
                          self.expiry.value, float(self.credit.value))

            # Log OPEN_CC to per-user CSV (export-friendly)
            try:
                iso_exp = iso_exp_str(self.expiry.value)            # YYYY-MM-DD
                iso_exp_compact = iso_exp.replace("-", "")          # for greeks placeholder
                greeks = get_option_greeks_snapshot(self.ticker.value, "C", float(self.strike.value), iso_exp_compact)
                append_trade_log(uid, {
                    "event": "OPEN_CC",
                    "trade_id": pid,
                    "user_id": uid,
                    "position_id": pid,
                    "timestamp_utc": _now_iso(),
                    "timestamp_local": _now_local_str(),
                    "ticker": self.ticker.value.upper(),
                    "option_type": "C",
                    "strike": float(self.strike.value),
                    "contracts": int(self.contracts.value),
                    "expiration": iso_exp,                       # Excel-friendly
                    "premium_credit": float(self.credit.value),
                    "premium_debit": "",
                    "pnl": "",
                    "delta": greeks["delta"],
                    "gamma": greeks["gamma"],
                    "theta": greeks["theta"],
                    "vega": greeks["vega"],
                    "iv": greeks["iv"],
                })
            except Exception as _e:
                print(f"⚠️ trade log append failed: {_e}")

            await interaction.response.send_message(
                f"✅ Added CC (ID {pid}): {self.ticker.value.upper()} {self.strike.value}C ×{self.contracts.value} "
                f"exp {self.expiry.value} credit ${self.credit.value}", ephemeral=True
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ Failed: {e}", ephemeral=True)

class CashSecuredPutModal(discord.ui.Modal, title="Add New Position — Cash Secured Put"):
    ticker    = discord.ui.TextInput(label="Ticker", placeholder="SOFI", max_length=10)
    strike    = discord.ui.TextInput(label="Strike", placeholder="15")
    contracts = discord.ui.TextInput(label="Contracts", placeholder="1")
    expiry    = discord.ui.TextInput(label="Expiry (MM-DD-YYYY or YYYY-MM-DD)", placeholder="12-20-2025")
    credit    = discord.ui.TextInput(label="Entry Credit ($)", placeholder="0.45")

    async def on_submit(self, interaction: discord.Interaction):
        uid = str(interaction.user.id)
        try:
            pid = add_csp(
                uid,
                self.ticker.value,
                float(self.strike.value),
                int(self.contracts.value),
                self.expiry.value,
                float(self.credit.value)
            )
            await interaction.response.send_message(
                f"✅ Added CSP (ID {pid}): {self.ticker.value.upper()} {self.strike.value}P ×{self.contracts.value} "
                f"exp {self.expiry.value} credit ${self.credit.value}",
                ephemeral=True
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ Failed: {e}", ephemeral=True)

class EditPositionModal(discord.ui.Modal, title="Edit Position"):
    pid       = discord.ui.TextInput(label="Position ID", style=discord.TextStyle.short)
    ticker    = discord.ui.TextInput(label="Ticker", required=False)
    strike    = discord.ui.TextInput(label="Strike", required=False)
    contracts = discord.ui.TextInput(label="Contracts", required=False)
    expiry    = discord.ui.TextInput(label="Expiry (MM-DD-YYYY or YYYY-MM-DD)", required=False)
    credit    = discord.ui.TextInput(label="Entry Credit ($)", required=False)

    def __init__(self, p: Optional[dict] = None):
        super().__init__()
        if p:
            self.pid.default = str(p["id"])
            self.ticker.default = p["ticker"]
            self.strike.default = f"{p['strike']:.2f}"
            self.contracts.default = str(p["contracts"])
            self.expiry.default = display_mdy(p["expiry"])
            self.credit.default = f"{p['entry_credit']:.2f}"

    async def on_submit(self, interaction: discord.Interaction):
        uid = str(interaction.user.id)
        try:
            pid_i = int(self.pid.value)
            ok = edit_pos(
                uid, pid_i,
                ticker=self.ticker.value or None,
                strike=float(self.strike.value) if self.strike.value else None,
                contracts=int(self.contracts.value) if self.contracts.value else None,
                expiry=self.expiry.value or None,
                credit=float(self.credit.value) if self.credit.value else None,
            )
            if not ok:
                return await interaction.response.send_message(f"ID {pid_i} not found.", ephemeral=True)
            await interaction.response.send_message("✅ Updated.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ Failed: {e}", ephemeral=True)

# ---------- Slash command groups ----------
# /add covered-call  (modal)
add_group = app_commands.Group(name="add", description="Add New Position")
@add_group.command(name="covered-call", description="Add a Covered Call (opens a modal)")
async def add_covered_call(interaction: discord.Interaction):
    await interaction.response.send_modal(CoveredCallModal())

@add_group.command(name="cash-secured-put", description="Add a Cash Secured Put (opens a modal)")
async def add_cash_secured_put(interaction: discord.Interaction):
    await interaction.response.send_modal(CashSecuredPutModal())

# /log ...
log_group = app_commands.Group(name="log", description="Trade log utilities")

@log_group.command(name="show", description="Preview the last N trade log entries (ephemeral)")
@app_commands.describe(limit="How many recent entries to show (default 10)")
async def log_show(interaction: discord.Interaction, limit: int = 10):
    uid = str(interaction.user.id)
    path = _user_log_path(uid)
    rows = []
    try:
        with path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except FileNotFoundError:
        rows = []
    if not rows:
        return await interaction.response.send_message("No trades logged yet.", ephemeral=True)
    tail = rows[-limit:]
    lines = []
    for r in tail:
        try:
            strike = f"{float(r['strike']):.2f}" if r['strike'] else ""
        except Exception:
            strike = r.get('strike','')
        lines.append(
            f"{r['event']:<8} id:{r.get('trade_id', r.get('position_id','')):<4} {r['timestamp_local'] or r['timestamp_utc']:<22} "
            f"{r['ticker']:<6} {r['option_type']}{strike:>7}  exp {r['expiration']:<10} "
            f"cr {r['premium_credit'] or '':>6}  db {r['premium_debit'] or '':>6}  pnl {r['pnl'] or '':>7}"
        )
    msg = "```\n" + "\n".join(lines) + "\n```"
    await interaction.response.send_message(msg, ephemeral=True)

@log_group.command(name="export", description="DM yourself the full trade log CSV")
async def log_export(interaction: discord.Interaction):
    uid = str(interaction.user.id)
    path = _user_log_path(uid)
    try:
        dm = await interaction.user.create_dm()
        await dm.send(file=discord.File(str(path), filename=f"premium_pilot_trades_{uid}.csv"))
        await interaction.response.send_message("I’ve DMed you your CSV. 📩", ephemeral=True)
    except discord.Forbidden:
        await interaction.response.send_message(
            "Your DMs are closed—uploading the CSV here (visible to others in this channel).",
            ephemeral=True
        )
        await interaction.followup.send(file=discord.File(str(path), filename=f"premium_pilot_trades_{uid}.csv"))

# /position ...
position_group = app_commands.Group(name="position", description="Manage your positions")

@position_group.command(name="update", description="Show your current positions (ephemeral)")
async def position_update(interaction: discord.Interaction):
    uid = str(interaction.user.id)
    now = datetime.now(TZ)
    emb = await build_positions_embed(uid, now)
    await interaction.response.send_message(embed=emb, ephemeral=True)

@position_group.command(name="edit", description="Edit a position via modal (prefilled)")
@app_commands.describe(id="Position ID from /position update or /log show")
async def position_edit(interaction: discord.Interaction, id: int):
    uid = str(interaction.user.id)
    p = _find_pos(uid, id)
    if not p:
        return await interaction.response.send_message(f"ID {id} not found.", ephemeral=True)
    await interaction.response.send_modal(EditPositionModal(p))

@position_group.command(name="remove", description="Remove a position by ID")
@app_commands.describe(id="Position ID")
async def position_remove(interaction: discord.Interaction, id: int):
    uid = str(interaction.user.id)
    ok = rm_pos(uid, id)
    await interaction.response.send_message("Removed." if ok else "Not found.", ephemeral=True)

@position_group.command(name="close", description="Close a position by ID (optional BTC price)")
@app_commands.describe(id="Position ID", btc_price="Buy-to-close price (optional)")
async def position_close(interaction: discord.Interaction, id: int, btc_price: Optional[float] = None):
    uid = str(interaction.user.id)
    archived = close_pos(uid, id, btc_price)
    if archived is None:
        return await interaction.response.send_message(f"ID {id} not found.", ephemeral=True)

    # Append CLOSE_CC to trade log (with export-friendly fields)
    try:
        log_btc_close(uid, id, archived['ticker'], archived['strike'], archived['contracts'],
                      archived['expiry'], archived.get('btc_price'), archived.get('entry_credit'))
    except Exception as _e:
        print(f"⚠️ trade log close append failed: {_e}")

    msg = (
        f"Closed ID {id}: {archived['ticker']} {archived['strike']:.2f}C x{archived['contracts']} "
        f"exp {display_mdy(archived['expiry'])}"
    )
    if archived.get("pnl_pct") is not None and archived.get("btc_price") is not None:
        msg += f" | BTC ${archived['btc_price']:.2f} | PnL ~{archived['pnl_pct']:.1f}%"
    await interaction.response.send_message(msg, ephemeral=True)

# ---------- EoD commands ----------
eod_group = app_commands.Group(name="eod", description="End-of-day actions")

@eod_group.command(name="update", description="Post public EoD and DM user summaries")
async def eod_update(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    await send_public_eod()
    await send_dm_eods()
    await interaction.followup.send("EoD dispatched. ✅", ephemeral=True)

# Register slash groups (guild-scoped for instant availability if GUILD provided)
if GUILD:
    bot.tree.add_command(add_group, guild=GUILD)
    bot.tree.add_command(position_group, guild=GUILD)
    bot.tree.add_command(eod_group, guild=GUILD)
    bot.tree.add_command(log_group, guild=GUILD)
else:
    bot.tree.add_command(add_group)
    bot.tree.add_command(position_group)
    bot.tree.add_command(eod_group)
    bot.tree.add_command(log_group)

# ---------- Text commands (legacy) ----------
@bot.command(help="Add covered call: !addcc SOFI 10 2 2025-11-22 0.56")
async def addcc(ctx, t: str, s: float, c: int, e: str, cr: float):
    uid = str(ctx.author.id)
    pid = add_pos(uid, t, s, c, e, cr)
    await ctx.reply(f"Added ID {pid}.")

@bot.command(help="List covered calls.")
async def listcc(ctx):
    uid = str(ctx.author.id)
    d = _load()
    bucket = d["users"].get(uid, {"cc": []})
    if not bucket["cc"]:
        return await ctx.reply("No positions.")
    lines = [f"[{p['id']}] {p['ticker']} {p['strike']}C x{p['contracts']} exp {p['expiry']} credit {p['entry_credit']}" for p in bucket["cc"]]
    await ctx.reply("```\n" + "\n".join(lines) + "\n```")

@bot.command(help="Remove by ID")
async def rmcc(ctx, pid: int):
    uid = str(ctx.author.id); ok = rm_pos(uid, pid)
    await ctx.reply("Removed." if ok else "Not found.")

@bot.command(help="Close by ID; optional BTC price. Ex: !closecc 3 0.07")
async def closecc(ctx, pid: int, btc_price: float = None):
    uid = str(ctx.author.id); archived = close_pos(uid, pid, btc_price)
    if archived is None: return await ctx.reply(f"ID {pid} not found.")
    msg = f"Closed ID {pid}: {archived['ticker']} {archived['strike']:.2f}C x{archived['contracts']} exp {display_mdy(archived['expiry'])}"
    if archived.get("pnl_pct") is not None and archived.get("btc_price") is not None:
        msg += f" | BTC ${archived['btc_price']:.2f} | PnL ~{archived['pnl_pct']:.1f}%"
    await ctx.reply(msg)

@bot.command(help="Run EoD now (public + DMs)")
async def run_eod(ctx):
    await send_public_eod(); await send_dm_eods()
    await ctx.reply("OK")

# ---------- Bot lifecycle ----------
@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user} ({bot.user.id})")
    try:
        await ensure_ws_started()
    except Exception as e:
        print(f"⚠️ ensure_ws_started failed: {e}")

    # Slash sync
    try:
        if GUILD:
            # Make sure global-only commands are visible in-guild too
            bot.tree.copy_global_to(guild=GUILD)
            await bot.tree.sync(guild=GUILD)
            print(f"🌲 Slash commands synced to guild {GUILD_ID}")
        else:
            await bot.tree.sync()
            print("🌎 Slash commands synced globally")
    except Exception as e:
        print(f"❌ Slash sync failed: {e}")

    # Schedulers
    sch = AsyncIOScheduler(timezone=TZ)
    sch.add_job(eod_job, "cron", day_of_week="mon-fri", hour=16, minute=15)
    sch.add_job(intraday_rules_tick, "cron", day_of_week="mon-fri", hour="9-15", minute="*")
    sch.start()

@bot.tree.command(name="helpme", description="Show a quick guide to Premium Pilot features")
async def helpme_slash(interaction: discord.Interaction):
    guide = (
        "**Premium Pilot — Quick Guide**\n"
        "\n"
        "__Add Positions__\n"
        "• `/add covered-call` → modal (Ticker, Strike, Contracts, Expiration, Entry Credit)\n"
        "• `/add cash-secured-put` → modal\n"
        "\n"
        "__Manage Positions__\n"
        "• `/position update` → live summary (ephemeral)\n"
        "• `/position edit id:ID` → modal to edit\n"
        "• `/position close id:ID btc_price:0.07` → close (optional BTC price)\n"
        "\n"
        "__Trade Log__\n"
        "• `/log show [limit]` → preview last N entries (shows trade_id)\n"
        "• `/log export` → DM CSV (Excel-friendly dates)\n"
        "\n"
        "__EoD__\n"
        "• `/eod update` → public + DMs\n"
        "_Scheduler runs at market close._\n"
        "\n"
        "__Rules__\n"
        "• **BTC** at **50–60% profit** and **≤ 7 DTE**\n"
        "• **Roll-watch** when underlying is **within ~4% of strike**\n"
        "• Otherwise **hold** and let theta decay\n"
        "\n"
        "__Roadmap__\n"
        "• Live WebSocket prices & option mids • Real-time BTC/Roll alerts • CSP + Wheel analytics\n"
        "• Morning chain screen & rankings • Premium compounding planner • Stats dashboard\n"
    )
    await interaction.response.send_message(guide, ephemeral=True)

# ---------- Boot ----------
if __name__ == "__main__":
    if not TOKEN or CHANNEL_ID == 0:
        raise SystemExit("Missing DISCORD_TOKEN or DISCORD_CHANNEL_ID in .env")
    bot.run(TOKEN)
