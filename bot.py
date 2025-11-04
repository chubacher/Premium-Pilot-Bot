import os, json, math, asyncio, itertools
from datetime import datetime, date
from zoneinfo import ZoneInfo
from pathlib import Path

import httpx
import websockets
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from discord.ext import commands
import discord
from dotenv import load_dotenv

# ---------- Paths / env ----------
BASE_DIR = Path(__file__).parent.resolve()
load_dotenv(BASE_DIR / ".env")  # load .env next to bot.py

TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))
EODHD_KEY = os.getenv("EODHD_API_KEY")
TZ = ZoneInfo(os.getenv("TZ", "America/New_York"))
DATA_FILE = BASE_DIR / "positions.json"  # always use the file next to bot.py

# REST endpoints
BASE_RT = "https://eodhd.com/api/real-time/{s}?api_token={k}&fmt=json"
BASE_INTRADAY = "https://eodhd.com/api/intraday/{s}?api_token={k}&interval=1m&fmt=json"
BASE_EOD = "https://eodhd.com/api/eod/{s}?api_token={k}&order=d&limit=1&fmt=json"
BASE_MP_OPT = "https://eodhd.com/api/mp/unicornbay/options/contracts"

# ---------- Live price caches (WebSocket) ----------
PRICE_CACHE: dict[str, float] = {}    # last trade price by US ticker (e.g., "SOFI")
QUOTE_CACHE: dict[str, dict] = {}     # not used yet; room for bid/ask
_SUBS: set[str] = set()               # subscribed symbols on socket
_RESUB_EVENT = asyncio.Event()        # signal reader to resubscribe
_WS_TASK: asyncio.Task | None = None

def _norm_us(sym: str) -> str:
    s = sym.upper().strip()
    if s.endswith(".US"): s = s[:-3]
    if s.startswith("US."): s = s[3:]
    return s

def _cands(sym: str) -> list[str]:
    s = sym.upper().strip()
    return [s, f"{s}.US", f"US.{s}"]

def all_symbols_in_positions() -> set[str]:
    d = _load()
    users = d.get("users", {})
    syms = {_norm_us(p["ticker"]) for u in users.values() for p in u.get("cc", [])}
    return {s for s in syms if s}

async def refresh_ws_subscriptions(ws):
    """Subscribe/unsubscribe to match current positions."""
    global _SUBS
    want = all_symbols_in_positions()
    to_add = want - _SUBS
    to_del = _SUBS - want
    if to_add:
        await ws.send(json.dumps({"action": "subscribe", "symbols": ",".join(sorted(to_add))}))
        _SUBS |= to_add
    if to_del:
        await ws.send(json.dumps({"action": "unsubscribe", "symbols": ",".join(sorted(to_del))}))
        _SUBS -= to_del

async def _ws_reader():
    """US trades stream; updates PRICE_CACHE. Reconnects on errors and handles resub requests."""
    backoff = 1
    while True:
        try:
            url = f"wss://ws.eodhistoricaldata.com/ws/us?api_token={EODHD_KEY}"
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=10) as ws:
                await refresh_ws_subscriptions(ws)
                backoff = 1
                while True:
                    # Wait for either a message or a resubscription signal
                    done, _ = await asyncio.wait(
                        {
                            asyncio.create_task(ws.recv()),
                            asyncio.create_task(_RESUB_EVENT.wait()),
                        },
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for task in done:
                        if task is None:
                            continue
                        # Resubscribe request
                        if task._coro.__name__ == '_wait':  # internal name of Event.wait()
                            _RESUB_EVENT.clear()
                            await refresh_ws_subscriptions(ws)
                            continue
                        # WebSocket data
                        raw = task.result()
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        s = msg.get("s")
                        if not s:
                            continue
                        if "p" in msg:  # trade tick
                            try:
                                PRICE_CACHE[s] = float(msg["p"])
                            except Exception:
                                pass
        except Exception:
            await asyncio.sleep(min(30, backoff))
            backoff = min(30, backoff * 2)

async def ensure_ws_started():
    global _WS_TASK
    if _WS_TASK is None or _WS_TASK.done():
        _WS_TASK = asyncio.create_task(_ws_reader())

def request_ws_resub():
    # Called after add/remove/close positions
    try:
        _RESUB_EVENT.set()
    except Exception:
        pass

# ---------- Robust storage ----------
def _load():
    """
    Multi-user schema:
    {"users": { "<uid>": {"cc":[...], "closed":[...] } } }
    """
    try:
        raw = b""
        if DATA_FILE.exists():
            with open(DATA_FILE, "rb") as f:
                raw = f.read()
        if raw.startswith(b"\xef\xbb\xbf"):
            raw = raw[3:]
        text = raw.decode("utf-8", errors="ignore").strip()
        if not text:
            raise ValueError("empty")
        data = json.loads(text)
        if not isinstance(data, dict):
            raise ValueError("bad schema")
        if "users" not in data:
            cc = data.get("cc", [])
            closed = data.get("closed", [])
            data = {"users": {}}
            if cc or closed:
                # legacy => attach to a placeholder; will migrate on first user call
                data["users"]["legacy"] = {"cc": cc, "closed": closed}
        if not isinstance(data.get("users", {}), dict):
            data["users"] = {}
    except Exception:
        data = {"users": {}}
        _save(data)
    return data

def _save(data):
    tmp = DATA_FILE.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, DATA_FILE)

def _ensure_user(uid: str):
    d = _load()
    users = d.setdefault("users", {})
    if uid not in users:
        users[uid] = {"cc": [], "closed": []}
        # migrate legacy if present
        if "legacy" in users:
            users[uid] = users.pop("legacy")
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

# ---------- HTTP helpers ----------
async def fetch_json(cli, url, params=None):
    r = await cli.get(url, params=params or {}, timeout=20)
    r.raise_for_status()
    return r.json()

async def get_intraday_last(cli, sym: str) -> float | None:
    """1m intraday: take last bar close (pre/post included)."""
    for s in _cands(sym):
        try:
            js = await fetch_json(cli, BASE_INTRADAY.format(s=s, k=EODHD_KEY))
            if isinstance(js, list) and js:
                last = js[-1]
                px = last.get("close") or last.get("last")
                if px is not None:
                    return float(px)
        except Exception:
            continue
    return None

async def get_official_close(cli, sym: str) -> float | None:
    """Official exchange close from /eod (last trading day)."""
    for s in _cands(sym):
        try:
            j = await fetch_json(cli, BASE_EOD.format(s=s, k=EODHD_KEY))
            if isinstance(j, list) and j:
                close = j[0].get("close")
                if close is not None:
                    return float(close)
        except Exception:
            continue
    return None

def _underlying_for_mp(sym: str) -> str:
    s = sym.upper().strip()
    if s.endswith(".US"): s = s[:-3]
    if s.startswith("US."): s = s[3:]
    return s

async def get_call_mid(cli, sym, exp, strike):
    """MP UnicornBay options contracts mid (bid/ask -> mid; else last)."""
    underlying = _underlying_for_mp(sym)
    exp_iso = iso_exp_str(exp)
    params = {
        "filter[underlying_symbol]": underlying,
        "filter[strike_eq]": f"{float(strike):.2f}".rstrip("0").rstrip("."),
        "filter[type]": "call",
        "filter[exp_date_eq]": exp_iso,
        "fields[options-contracts]": "contract,bid,ask,last,exp_date,strike,type",
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
        if isinstance(bid, (int, float)) and isinstance(ask, (int, float)) and bid > 0 and ask > 0:
            return (bid + ask) / 2
        if isinstance(last, (int, float)):
            return float(last)
        return None

    for item in data:
        attrs = item.get("attributes") or item
        try:
            k = float(attrs.get("strike")); t = (attrs.get("type") or "").lower()
            e = (attrs.get("exp_date") or "")
        except Exception:
            continue
        if abs(k - float(strike)) < 1e-6 and t == "call" and e.startswith(exp_iso):
            m = mid_from(attrs)
            if m is not None:
                return float(m)

    attrs = (data[0].get("attributes") or data[0]) if data else {}
    m = mid_from(attrs)
    return float(m) if m is not None else None

# ---------- Strategy helpers ----------
def dist_pct(px, strike):
    return (strike - px) / px * 100.0

def btc_hit(credit, now_price):
    if not credit or now_price is None:
        return (False, float("nan"))
    pct = (credit - now_price) / credit * 100
    return (pct >= 50.0, pct)

def roll_watch(px, strike):
    return dist_pct(px, strike) <= 4.0  # within 4% to strike

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

def add_pos(uid: str, t, s, c, e, cr):
    d, bucket = _get_user_bucket(uid)
    next_id = (max([p["id"] for p in bucket["cc"]], default=0) + 1)
    bucket["cc"].append({
        "id": next_id,
        "ticker": t.upper(),
        "strike": float(s),
        "contracts": int(c),
        "expiry": iso_exp_str(e),  # store ISO
        "entry_credit": float(cr)
    })
    _save(d)
    request_ws_resub()
    return next_id

def rm_pos(uid: str, pid: int):
    d, bucket = _get_user_bucket(uid)
    before = len(bucket["cc"])
    bucket["cc"] = [p for p in bucket["cc"] if p["id"] != pid]
    _save(d)
    request_ws_resub()
    return len(bucket["cc"]) < before

def close_pos(uid: str, pid: int, btc_price: float | None):
    d, bucket = _get_user_bucket(uid)
    idx = next((i for i, p in enumerate(bucket["cc"]) if p["id"] == pid), None)
    if idx is None:
        return None
    p = bucket["cc"].pop(idx)
    pnl_pct = None
    if btc_price is not None:
        try:
            pnl_pct = (p["entry_credit"] - float(btc_price)) / p["entry_credit"] * 100.0
        except Exception:
            pnl_pct = None
    archived = {
        **p,
        "closed_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "btc_price": btc_price if btc_price is not None else None,
        "pnl_pct": round(pnl_pct, 2) if pnl_pct is not None else None,
    }
    bucket["closed"].append(archived)
    _save(d)
    request_ws_resub()
    return archived

def user_closed(uid: str, n: int = 10):
    d, bucket = _get_user_bucket(uid)
    return bucket["closed"][-n:]

# ---------- Embeds ----------
def _fmt_line(p: dict, u: float | None, o: float | None, days: int) -> tuple[str, bool]:
    exp_disp = display_mdy(p["expiry"])
    line = f"{p['ticker']} {p['strike']:.2f}C ×{p['contracts']} — exp {exp_disp} ({days} DTE)"
    if u is not None:
        line += f" | Px ${u:.2f} | {dist_pct(u, p['strike']):.1f}% to strike"
    if o is not None:
        line += f" | Opt ~${o:.2f}"
    hit, pct = btc_hit(p['entry_credit'], o if o is not None else float('nan'))
    if not math.isnan(pct):
        line += f" | PnL ~{pct:.0f}%"
    return line, hit

def _decision_label(hit: bool, days: int, u: float | None, strike: float) -> str:
    if hit and days <= 7:
        return "✅ BTC (50–60% gain hit) → resell"
    if u is not None and roll_watch(u, strike):
        return "⚠️ Roll-watch"
    return "💤 Hold; let theta work"

async def build_user_embed(uid: str, user_name: str) -> discord.Embed:
    ps = positions(uid)
    now = datetime.now(TZ)
    emb = discord.Embed(
        title=f"{user_name}",
        description=f"EoD Covered Call Check — {now.strftime('%m-%d-%Y')}",
        color=discord.Color.blurple()
    )
    if not ps:
        emb.add_field(name="No positions", value="Use `!addcc ...` to add one.", inline=False)
        return emb

    async with httpx.AsyncClient() as cli:
        for p in ps:
            # live price first; fallback to intraday/EoD
            sym_us = _norm_us(p["ticker"])
            u = PRICE_CACHE.get(sym_us)
            if u is None:
                u = await get_intraday_last(cli, p["ticker"]) or await get_official_close(cli, p["ticker"])
            o = await get_call_mid(cli, p["ticker"], p["expiry"], p["strike"])
            days = dte(p["expiry"], now)
            line, hit = _fmt_line(p, u, o, days)
            decision = _decision_label(hit, days, u, p["strike"])
            emb.add_field(name=decision, value=line, inline=False)

    emb.set_footer(text="Rules: BTC @50–60% profit & ≤7 DTE • Roll-watch if ≤4% to strike or ∆≈strike")
    return emb

async def build_public_embed():
    d = _load()
    users = d.get("users", {})
    now = datetime.now(TZ)
    emb = discord.Embed(
        title=f"EoD Covered Call Check — {now:%m-%d-%Y}",
        color=discord.Color.dark_teal()
    )
    if not users:
        emb.description = "No users yet. Add with `!addcc TICK STRIKE CONTRACTS MM-DD-YYYY CREDIT`"
        return emb

    async with httpx.AsyncClient() as cli:
        for uid in users.keys():
            # resolve display name
            name = f"User {uid}"
            try:
                ch = bot.get_channel(CHANNEL_ID)
                member = ch.guild.get_member(int(uid)) if isinstance(ch, discord.TextChannel) else None
                user_obj = member or bot.get_user(int(uid)) or await bot.fetch_user(int(uid))
                if user_obj:
                    name = getattr(user_obj, "display_name", None) or user_obj.name
            except Exception:
                pass

            ps = positions(uid)
            if not ps:
                emb.add_field(name=name, value="No covered call positions.", inline=False)
                continue

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
    if not ch:
        return
    try:
        emb = await build_public_embed()
        await ch.send(embed=emb)
    except Exception as e:
        try:
            await ch.send(f"EoD failed: {e}")
        except Exception:
            pass

async def send_dm_eods():
    d = _load()
    users = d.get("users", {})
    for uid in users.keys():
        try:
            user_obj = bot.get_user(int(uid)) or await bot.fetch_user(int(uid))
            if user_obj:
                emb = await build_user_embed(uid, getattr(user_obj, "display_name", None) or user_obj.name)
                await user_obj.send(embed=emb)
        except Exception:
            continue

async def eod_job():
    await send_public_eod()
    await send_dm_eods()

async def intraday_rules_tick():
    """Once per minute during market hours: DM alerts for BTC-hit and roll-watch."""
    d = _load()
    users = d.get("users", {})
    now = datetime.now(TZ)
    async with httpx.AsyncClient() as cli:
        for uid, bucket in users.items():
            user_obj = bot.get_user(int(uid)) or await bot.fetch_user(int(uid))
            if not user_obj:
                continue
            notes = []
            for p in bucket.get("cc", []):
                sym = _norm_us(p["ticker"])
                px = PRICE_CACHE.get(sym)
                opt_mid = await get_call_mid(cli, p["ticker"], p["expiry"], p["strike"])
                days = dte(p["expiry"], now)
                # BTC rule
                hit, _ = btc_hit(p["entry_credit"], opt_mid if opt_mid is not None else None)
                if hit and days <= 7:
                    notes.append(
                        f"✅ BTC alert: {p['ticker']} {p['strike']}C ×{p['contracts']} — "
                        f"exp {display_mdy(p['expiry'])} ({days} DTE) | Opt ~${(opt_mid or 0):.2f}"
                    )
                # Roll watch
                if px is not None and roll_watch(px, p["strike"]):
                    notes.append(
                        f"⚠️ Roll-watch: {p['ticker']} | Px ${px:.2f} within 4% of {p['strike']}C strike"
                    )
            if notes:
                try:
                    await user_obj.send("\n".join(notes))
                except Exception:
                    pass

# ---------- Bot events & commands ----------
@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user}")
    await ensure_ws_started()
    sch = AsyncIOScheduler(timezone=TZ)
    # EoD @ 4:15pm ET (after most equity options settle)
    sch.add_job(eod_job, "cron", day_of_week="mon-fri", hour=16, minute=15)
    # Intraday alert tick: once a minute during market hours (9:00–15:59 ET)
    sch.add_job(intraday_rules_tick, "cron", day_of_week="mon-fri", hour="9-15", minute="*")
    sch.start()
@bot.command(help="Show bot usage & strategy guide")
async def helpme(ctx):
    embed = discord.Embed(
        title="📈 Premium Pilot — Command Guide",
        description="The bot helps you systematically track & manage covered calls.\n\n**v1.0 — CC Tracking & EOD Reporting**",
        color=discord.Color.green()
    )

    embed.add_field(
        name="🎯 Core Commands",
        value=(
            "`!addcc TICK STRIKE CONTRACTS EXPIRY CREDIT`\n"
            "Add a covered-call position\n"
            "• Example: `!addcc SOFI 30 2 2025-11-15 0.66`\n\n"
            "`!listcc`\n"
            "Show your open covered calls\n\n"
            "`!closecc ID PRICE`\n"
            "Close a call (buy-to-close)\n"
            "• Example: `!closecc 3 0.05`\n\n"
            "`!rmcc ID`\n"
            "Remove position w/out closing (use if entry mistake)\n\n"
            "`!run_eod`\n"
            "Manually trigger EOD analysis & signals"
        ),
        inline=False
    )

    embed.add_field(
        name="📊 Strategy Logic (Current Rules)",
        value=(
            "• Buy-to-close at **50–60% profit** if **<7 DTE**\n"
            "• **Roll** if price is within **4% of strike**\n"
            "• Otherwise **hold and let theta decay**"
        ),
        inline=False
    )

    embed.add_field(
        name="🚧 Coming Soon",
        value=(
            "• Live price + options data stream\n"
            "• Real-time roll + BTC alerts\n"
            "• CSP & Wheel tracking\n"
            "• Weekly strike selection engine\n"
            "• Automated premium compounding plan\n"
            "• Strategy backtesting + stats dashboard"
        ),
        inline=False
    )

    embed.add_field(
        name="⚠️ Note",
        value="This bot provides rules-based decision support — *you place trades.*",
        inline=False
    )

    embed.set_footer(text="Premium Pilot — Building the elite retail quant wheel engine ✨")

    await ctx.reply(embed=embed)


@bot.command(help="Add: !addcc TICKER STRIKE CONTRACTS MM-DD-YYYY CREDIT (or YYYY-MM-DD)")
async def addcc(ctx, ticker, strike: float, contracts: int, expiry, credit: float):
    uid = str(ctx.author.id)
    pid = add_pos(uid, ticker, strike, contracts, expiry, credit)
    await ctx.reply(
        f"Added ID {pid}: {ticker.upper()} {float(strike):.2f}C x{contracts} "
        f"exp {display_mdy(iso_exp_str(expiry))} credit ${float(credit):.2f}"
    )

@bot.command(help="List your positions")
async def listcc(ctx):
    uid = str(ctx.author.id)
    ps = positions(uid)
    if not ps:
        return await ctx.reply("No positions yet.")
    lines = [
        f"ID {p['id']}: {p['ticker']} {p['strike']:.2f}C x{p['contracts']} "
        f"exp {display_mdy(p['expiry'])} credit ${p['entry_credit']:.2f}"
        for p in ps
    ]
    await ctx.reply("```\n" + "\n".join(lines) + "\n```")

@bot.command(help="Remove your position by ID (hard delete)")
async def rmcc(ctx, pid: int):
    uid = str(ctx.author.id)
    ok = rm_pos(uid, pid)
    await ctx.reply("Removed." if ok else "Not found.")

@bot.command(help="Close by ID; optional BTC price. Ex: !closecc 3 0.07")
async def closecc(ctx, pid: int, btc_price: float = None):
    uid = str(ctx.author.id)
    archived = close_pos(uid, pid, btc_price)
    if archived is None:
        return await ctx.reply(f"ID {pid} not found.")
    msg = (
        f"Closed ID {pid}: {archived['ticker']} {archived['strike']:.2f}C x{archived['contracts']} "
        f"exp {display_mdy(archived['expiry'])}"
    )
    if archived.get("pnl_pct") is not None and archived.get("btc_price") is not None:
        msg += f" | BTC ${archived['btc_price']:.2f} | PnL ~{archived['pnl_pct']:.1f}%"
    await ctx.reply(msg)

@bot.command(help="Show your last N closed items (default 10). Ex: !history 15")
async def history(ctx, n: int = 10):
    uid = str(ctx.author.id)
    closed = user_closed(uid, n)
    if not closed:
        return await ctx.reply("No closed positions yet.")
    lines = []
    for p in closed:
        base = (
            f"ID {p['id']}: {p['ticker']} {p['strike']:.2f}C x{p['contracts']} "
            f"exp {display_mdy(p['expiry'])} | closed {p.get('closed_at','')}"
        )
        if p.get("btc_price") is not None:
            base += f" | BTC ${p['btc_price']:.2f}"
        if p.get("pnl_pct") is not None:
            base += f" | PnL ~{p['pnl_pct']:.1f}%"
        lines.append(base)
    await ctx.reply("```\n" + "\n".join(lines) + "\n```")

@bot.command(help="Run EoD now (public post + DMs)")
async def run_eod(ctx):
    await send_public_eod()
    await send_dm_eods()
    await ctx.message.add_reaction("✅")

# ---------- Boot ----------
if __name__ == "__main__":
    if not TOKEN or CHANNEL_ID == 0:
        raise SystemExit("Missing DISCORD_TOKEN or DISCORD_CHANNEL_ID in .env")
    bot.run(TOKEN)
