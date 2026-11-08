import os, re, asyncio, random, logging, json, time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiohttp
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import (
    FloodWaitError, PeerFloodError,
    UserDeactivatedError, UserDeactivatedBanError,
    AuthKeyUnregisteredError, AuthKeyDuplicatedError, SessionRevokedError,
)

# ---------- CONFIG ----------
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_ID = int(os.environ["ADMIN_ID"])
TARGET_BOT = os.environ["TARGET_BOT"]

DAILY_MIN = int(os.getenv("DAILY_MIN", "50"))
DAILY_MAX = int(os.getenv("DAILY_MAX", "100"))
REPLY_TIMEOUT = int(os.getenv("REPLY_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))
COOLDOWN_HOURS = float(os.getenv("COOLDOWN_HOURS", "4"))
MIN_DELAY = int(os.getenv("MIN_DELAY", "20"))
MAX_DELAY = int(os.getenv("MAX_DELAY", "90"))
PROGRESS_EVERY = int(os.getenv("PROGRESS_EVERY", "5"))
CONN_TRIES = int(os.getenv("CONN_TRIES", "4"))
CONN_DELAYS = [10, 30, 60, 120]
STAGGER = int(os.getenv("STAGGER", "0"))

GEMINI_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
QUESTION_TOPIC = os.getenv("QUESTION_TOPIC", "everyday general topics")

IST = timezone(timedelta(hours=5, minutes=30))
FIXED_TARGET: Optional[int] = int(os.getenv("FIXED_TARGET", "0")) or None
PREMIUM_ON = os.getenv("PREMIUM_EMOJI", "1") == "1"


def new_client(session):
    return TelegramClient(StringSession(session), API_ID, API_HASH,
                          connection_retries=5, retry_delay=3, auto_reconnect=True)


# ---------- SESSION LOADING ----------
def clean_session(s):
    return re.sub(r"[^A-Za-z0-9_\-=]", "", s)


def parse_entry(default_label, raw):
    raw = raw.strip().strip("\"'").strip()
    label = default_label
    if ":" in raw:
        lab, raw = raw.split(":", 1)
        lab = re.sub(r"[^A-Za-z0-9_\-]", "", lab)
        if lab:
            label = lab
    return label, clean_session(raw)


ENTRIES = []
_n = 0
for _s in os.environ.get("SESSIONS", "").replace("\n", ",").split(","):
    if _s.strip():
        _n += 1
        ENTRIES.append((f"SESSIONS#{_n}", _s))
for _k, _v in sorted(os.environ.items(), key=lambda x: (len(x[0]), x[0])):
    if _k.startswith("SESSION_") and _v.strip():
        ENTRIES.append((_k, _v))

# ---------- LAST KNOWN NAMES ----------
NAMES_FILE = "names.json"


def load_names():
    try:
        with open(NAMES_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_names(d):
    try:
        with open(NAMES_FILE, "w", encoding="utf-8") as f:
            json.dump(d, f)
    except Exception:
        pass


NAMES = load_names()

# ---------- LOGGING ----------
alert_q: asyncio.Queue = asyncio.Queue()
_recent: dict = {}


class AdminAlertHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = record.getMessage()
            key = re.sub(r"\(\d+/\d+\)|#\d+/\d+|\b\d+s\b", "", msg)
            now_t = time.time()
            if now_t - _recent.get(key, 0) < 3600:
                return
            _recent[key] = now_t
            alert_q.put_nowait(self.format(record))
        except Exception:
            pass


log = logging.getLogger("worker")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s %(message)s")
_sh = logging.StreamHandler()
_sh.setFormatter(_fmt)
log.addHandler(_sh)
_ah = AdminAlertHandler(level=logging.ERROR)
_ah.setFormatter(_fmt)
log.addHandler(_ah)

# ---------- STATE ----------
REPLY_RATE = float(os.getenv("REPLY_RATE", "0.003"))
DATA_DIR = os.getenv("DATA_DIR", ".")
STATE_FILE = os.path.join(DATA_DIR, "state.json")
BRAND = "▰▰ 𝗖𝗟𝗘𝗫𝗘𝗥 ▰▰"
AWAIT_BALANCE = False
AWAIT_SCHEDULE = False
AWAIT_BROADCAST = False


def today_str():
    return datetime.now(IST).strftime("%Y-%m-%d")


def load_state():
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


STATE = load_state()
STATE.setdefault("date", today_str())
STATE.setdefault("daily", {})
STATE.setdefault("targets", {})
STATE.setdefault("history", {})
STATE.setdefault("base", 0.0)
STATE.setdefault("since", 0)
STATE.setdefault("auto_run", False)
STATE.setdefault("run_time", None)
STATE.setdefault("last_auto_date", "")
STATE.setdefault("seen", [])


def save_state():
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(STATE, f)
    except Exception as e:
        log.warning(f"state save failed: {e}")


def ensure_today():
    t = today_str()
    if STATE["date"] != t:
        STATE["history"][STATE["date"]] = sum(STATE["daily"].values())
        STATE["history"] = dict(sorted(STATE["history"].items())[-30:])
        STATE["daily"], STATE["targets"], STATE["date"], STATE["seen"] = {}, {}, t, []
        save_state()
        return True
    return False


def bump(uid):
    ensure_today()
    k = str(uid)
    STATE["daily"][k] = STATE["daily"].get(k, 0) + 1
    STATE["since"] += 1
    save_state()


def day_done(uid):
    ensure_today()
    return STATE["daily"].get(str(uid), 0)


def daily_target(uid):
    ensure_today()
    if FIXED_TARGET:
        return FIXED_TARGET
    k = str(uid)
    if k not in STATE["targets"]:
        STATE["targets"][k] = random.randint(DAILY_MIN, DAILY_MAX)
        save_state()
    return STATE["targets"][k]


def money(x):
    return f"${x:,.3f}"


def dashboard():
    ensure_today()
    today = sum(STATE["daily"].values())
    life = STATE["base"] + STATE["since"] * REPLY_RATE
    lines = [
        "💰 Earnings Dashboard",
        f"Rate: {money(REPLY_RATE)} per reply\n",
        f"📅 Today ({STATE['date']}): {today} replies → {money(today * REPLY_RATE)}",
        f"🏦 Lifetime balance: {money(life)}",
        f"   (inserted {money(STATE['base'])} + {STATE['since']} replies × {money(REPLY_RATE)})",
    ]
    hist = list(STATE["history"].items())[-7:]
    if hist:
        lines.append("\n🗓 Previous days:")
        lines += [f"   {d}: {n} replies → {money(n * REPLY_RATE)}" for d, n in hist]
    lines.append("\n👤 Today per account:")
    for a in primaries():
        n = day_done(a.user_id)
        lines.append(f"   @{a.username}: {n}/{daily_target(a.user_id)} → {money(n * REPLY_RATE)}")
    return "\n".join(lines)


def parse_time(text):
    text = text.strip().lower()
    m = re.match(r"^(\d{1,2})(?::(\d{2}))?\s*(am|pm)?$", text)
    if not m:
        return None
    h, mi, ap = int(m.group(1)), int(m.group(2) or 0), m.group(3)
    if mi > 59:
        return None
    if ap:
        if not (1 <= h <= 12):
            return None
        h = (0 if h == 12 else h) if ap == "am" else (12 if h == 12 else h + 12)
    else:
        if not (0 <= h <= 23):
            return None
    return f"{h:02d}:{mi:02d}"


# ---------- QUESTIONS ----------
_Q_TOPICS = [
    "your service", "this bot", "your features", "pricing", "getting started", "your team",
    "updates", "support", "your product", "integrations", "security", "privacy",
    "the mobile app", "your community", "feedback", "your roadmap", "tips", "best practices",
    "troubleshooting", "your API", "documentation", "your plans", "a free trial",
    "your company", "your mission", "case studies", "your blog", "your newsletter",
    "your socials", "your website", "this platform", "automation", "your workflow",
    "customization", "your dashboard", "notifications", "your settings", "account setup",
    "your onboarding", "your FAQ",
]
_Q_TEMPLATES = [
    "What is {t}?", "Can you explain {t}?", "Tell me about {t}", "How does {t} work?",
    "Why should I care about {t}?", "Any tips on {t}?", "What's new with {t}?",
    "Can you help me understand {t}?", "Where can I learn about {t}?",
    "What do you think about {t}?",
]


def build_fallback():
    out = [tpl.format(t=t) for t in _Q_TOPICS for tpl in _Q_TEMPLATES]
    random.shuffle(out)
    return out[:150]


FALLBACK = build_fallback()
pool: list = []
pool_lock = asyncio.Lock()


async def gemini_questions(avoid=None) -> list:
    if not GEMINI_KEY:
        return []
    avoid = list(avoid or [])[-20:]
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_KEY}"
    prompt = (f"Generate 30 different short, casual questions a normal user might ask a chatbot about "
              f"{QUESTION_TOPIC}. Vary length and wording.")
    if avoid:
        prompt += " Do not repeat any of these already-used questions: " + " | ".join(avoid)
    prompt += " Return only a JSON array of strings."
    body = {"contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"responseMimeType": "application/json"}}
    async with aiohttp.ClientSession() as s:
        async with s.post(url, json=body, timeout=aiohttp.ClientTimeout(total=30)) as r:
            data = await r.json()
            if r.status != 200:
                raise RuntimeError(f"Gemini HTTP {r.status}: {str(data)[:200]}")
    text = data["candidates"][0]["content"]["parts"][0]["text"]
    return [q.strip() for q in json.loads(text) if isinstance(q, str) and q.strip()]


async def next_question() -> str:
    async with pool_lock:
        ensure_today()
        seen = set(STATE.get("seen", []))
        q = None
        for _ in range(300):
            if not pool:
                qs = []
                try:
                    qs = await gemini_questions(STATE.get("seen"))
                except Exception as e:
                    log.warning(f"Gemini failed, using fallback: {e}")
                if not qs:
                    qs = FALLBACK[:]
                random.shuffle(qs)
                pool.extend(qs)
            cand = pool.pop()
            if cand not in seen:
                q = cand
                break
        if q is None:
            q = random.choice(FALLBACK)
        STATE["seen"] = (STATE.get("seen", []) + [q])[-500:]
        save_state()
        return q


# ---------- ACCOUNT STATE ----------
@dataclass
class Acc:
    idx: int
    label: str
    session: str
    group: str = ""
    client: Optional[TelegramClient] = None
    user_id: int = 0
    username: str = ""
    last_user: str = ""
    dup_of: str = ""
    role: str = "new"
    ready: bool = False
    error: str = ""
    target: int = 0
    done: int = 0
    fails: int = 0
    cycles: int = 0
    status: str = "idle"
    task: Optional[asyncio.Task] = None
    next_retry: float = 0.0

    def tag(self):
        u = self.username or self.last_user
        return f"{self.label} (@{u})" if u else self.label


accs = []
for i, (lab, raw) in enumerate(ENTRIES):
    label, sess = parse_entry(lab, raw)
    accs.append(Acc(i, label, sess, group=label.split("__")[0], last_user=NAMES.get(label, "")))

GROUPS: dict = {}
bot = TelegramClient(StringSession(), API_ID, API_HASH)
progress_task: Optional[asyncio.Task] = None

FATAL = (UserDeactivatedError, UserDeactivatedBanError, AuthKeyUnregisteredError,
         AuthKeyDuplicatedError, SessionRevokedError)


def primaries():
    return [a for a in accs if a.role == "primary"]


def validate(s):
    if not s:
        return "empty value"
    try:
        StringSession(s)
    except Exception as e:
        return f"corrupted string ({len(s)} chars): {e}"
    return None


async def connect(acc: Acc, seen: dict):
    err = validate(acc.session)
    if not err and acc.session in seen:
        acc.dup_of = seen[acc.session]
        err = f"exact duplicate of {acc.dup_of}"
    if err:
        acc.error = err
        acc.status = "error"
        log.warning(f"[{acc.label}] {err}")
        return
    seen[acc.session] = acc.label
    try:
        acc.client = new_client(acc.session)
        await acc.client.connect()
        if not await acc.client.is_user_authorized():
            acc.error = "session dead/revoked (not authorized)"
            acc.status = "error"
            log.warning(f"[{acc.label}] {acc.error}")
            return
        me = await acc.client.get_me()
        acc.user_id = me.id
        acc.username = me.username or str(me.id)
        NAMES[acc.label] = acc.username
        save_names(NAMES)
        acc.ready = True
        acc.error = ""
        log.info(f"[{acc.tag()}] connected")
    except Exception as e:
        acc.error = repr(e)[:150]
        acc.status = "error"
        log.warning(f"[{acc.label}] connect failed: {acc.error}")


async def organize():
    GROUPS.clear()
    for a in accs:
        if a.ready:
            GROUPS.setdefault(a.user_id, []).append(a)
        else:
            a.role = "broken"
    for uid, lst in GROUPS.items():
        lst.sort(key=lambda a: a.idx)
        lst[0].role = "primary"
        for sp in lst[1:]:
            sp.role = "spare"
            try:
                await sp.client.disconnect()
            except Exception:
                pass


def owner_of(a: Acc):
    if a.dup_of:
        o = next((x for x in accs if x.label == a.dup_of and x.user_id), None)
        if o:
            return o.user_id
    for x in accs:
        if x.user_id and x is not a and x.group == a.group:
            return x.user_id
    if a.last_user:
        for x in accs:
            if x.user_id and x.username == a.last_user:
                return x.user_id
    return None


def describe_broken(a: Acc):
    uid = owner_of(a)
    if uid:
        good = GROUPS.get(uid, [])
        bad = [x for x in accs if not x.ready and x is not a and owner_of(x) == uid] + [a]
        user = good[0].username if good else (a.last_user or "?")
        return (f"❌ {a.label} — account @{user} has {len(good) + len(bad)} string(s): "
                f"{len(good)} valid, {len(bad)} corrupt. This one: {a.error}")
    who = f" (last known @{a.last_user})" if a.last_user else ""
    return f"❌ {a.label}{who} — owner unknown. {a.error}"


def accounts_report():
    lines = [f"👥 {len(accs)} strings → {len(GROUPS)} unique working account(s)"]
    bad_by_owner, unknown = {}, []
    for a in accs:
        if a.ready:
            continue
        uid = owner_of(a)
        if uid:
            bad_by_owner.setdefault(uid, []).append(a)
        else:
            unknown.append(a)
    for uid, lst in GROUPS.items():
        bad = bad_by_owner.get(uid, [])
        total = len(lst) + len(bad)
        if total == 1:
            lines.append(f"\n👤 @{lst[0].username} — 1 string ✅")
            continue
        head = f"\n👤 @{lst[0].username} — {total} strings: {len(lst)} valid"
        head += f", {len(bad)} CORRUPT" if bad else ""
        lines.append(head)
        for a in lst:
            lines.append(f"   {'✅ in use' if a.role == 'primary' else '⚪ spare'}: {a.label}")
        for a in bad:
            lines.append(f"   ❌ CORRUPT: {a.label} — {a.error}")
    if unknown:
        lines.append("\n❌ Broken strings, owner unknown (rename extra strings NAME__2 to match):")
        for a in unknown:
            who = f" (last known @{a.last_user})" if a.last_user else ""
            lines.append(f"   • {a.label}{who} — {a.error}")
    return "\n".join(lines)


async def promote(dead: Acc):
    for sp in GROUPS.get(dead.user_id, []):
        if sp is dead or sp.role != "spare":
            continue
        try:
            sp.client = new_client(sp.session)
            await sp.client.connect()
            if not await sp.client.is_user_authorized():
                raise RuntimeError("spare string not authorized")
            sp.role = "primary"
            sp.status = "idle"
            return sp
        except Exception as e:
            sp.role = "broken"
            sp.ready = False
            sp.error = f"spare failed: {e!r}"[:150]
            log.error(f"SPARE BROKEN -> {sp.tag()}: {sp.error}")
    return None


# ---------- BOT API SENDER ----------
BOT_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
_http: Optional[aiohttp.ClientSession] = None

PREMIUM = {
    "💰": "5359797475177691478", "🏦": "5332455502917949981", "🗓": "6025878226260202192",
    "👤": "5258362837411045098", "📅": "5028418466000930064", "🚨": "6053367550257928793",
    "🏁": "5408906741125490282", "🕒": "5215484787325676090", "⏳": "5370561264516865667",
    "📊": "6053234303192537471", "➕": "6339079894458179573", "🎯": "5310278924616356636",
    "🎲": "6172496405312051278", "⚪": "5820913139830492948", "🟢": "5039928501612839813",
    "🟡": "4974280407529818183", "✅": "5940635490645449104", "🔴": "5042042652019655612",
    "❌": "6052869226677410910",
}


async def api(method, **params):
    global _http
    if _http is None or _http.closed:
        _http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
    for _ in range(3):
        async with _http.post(f"{BOT_API}/{method}", json=params) as r:
            data = await r.json()
        if data.get("ok"):
            return data["result"]
        if data.get("error_code") == 429:
            await asyncio.sleep(data.get("parameters", {}).get("retry_after", 5) + 1)
            continue
        raise RuntimeError(f"{method}: {data.get('description')}")
    raise RuntimeError(f"{method}: rate limited")


def with_premium(text):
    ents, off, i = [], 0, 0
    while i < len(text):
        ch = text[i]
        n = 1
        units = 2 if ord(ch) > 0xFFFF else 1
        if ch in PREMIUM:
            if i + 1 < len(text) and text[i + 1] == "\ufe0f":
                n, units = 2, units + 1
            ents.append({"type": "custom_emoji", "offset": off, "length": units,
                         "custom_emoji_id": PREMIUM[ch]})
        off += units
        i += n
    return ents


def strip_icons(m):
    if isinstance(m, dict):
        return {k: strip_icons(v) for k, v in m.items() if k != "icon_custom_emoji_id"}
    if isinstance(m, list):
        return [strip_icons(x) for x in m]
    return m


def chunk_text(text, limit=3500):
    if len(text) <= limit:
        return [text]
    parts, cur = [], ""
    for line in text.split("\n"):
        if len(cur) + len(line) + 1 > limit:
            if cur:
                parts.append(cur)
            cur = line
            while len(cur) > limit:
                parts.append(cur[:limit])
                cur = cur[limit:]
        else:
            cur = f"{cur}\n{line}" if cur else line
    if cur:
        parts.append(cur)
    return parts


def _prep(text, markup):
    p = {"text": text[:4000]}
    if markup:
        p["reply_markup"] = markup
    if PREMIUM_ON:
        ents = with_premium(p["text"])
        if ents:
            p["entities"] = ents
    return p


async def _call(method, p):
    global PREMIUM_ON
    try:
        return await api(method, **p)
    except RuntimeError as e:
        err = str(e)
        if "not modified" in err.lower():
            return None
        if PREMIUM_ON and any(w in err.lower() for w in ("emoji", "entit", "icon", "premium")):
            PREMIUM_ON = False
            log.warning(f"Premium emoji not allowed for this bot, using normal emojis: {err}")
            p = dict(p)
            p.pop("entities", None)
            if "reply_markup" in p:
                p["reply_markup"] = strip_icons(p["reply_markup"])
            return await api(method, **p)
        raise


async def say(text, buttons=None):
    full = f"{BRAND}\n{text}"
    chunks = chunk_text(full, 3900)
    last_id = None
    for i, c in enumerate(chunks):
        p = _prep(c, buttons if i == len(chunks) - 1 else None)
        p["chat_id"] = ADMIN_ID
        r = await _call("sendMessage", p)
        last_id = r["message_id"] if r else last_id
    return last_id


async def edit_msg(msg_id, text, buttons=None):
    full = f"{BRAND}\n{text}"
    if len(full) > 3900:
        full = full[:3850] + "\n…(trimmed, see /status for full list)"
    p = _prep(full, buttons)
    p.update(chat_id=ADMIN_ID, message_id=msg_id)
    await _call("editMessageText", p)
    return msg_id


async def safe_delete(msg_id):
    try:
        if msg_id:
            await api("deleteMessage", chat_id=ADMIN_ID, message_id=msg_id)
    except Exception:
        pass


# ---------- WORKER ----------
async def run_account(acc: Acc):
    conn_fails = 0
    conn_msg = None
    try:
        if acc.done == 0 and STAGGER > 0:
            await asyncio.sleep(random.uniform(0, STAGGER))
        acc.status = "running"
        errors = 0
        while acc.done < acc.target:
            if not acc.client.is_connected():
                try:
                    await acc.client.connect()
                except Exception:
                    pass
            q = await next_question()
            try:
                async with acc.client.conversation(TARGET_BOT, timeout=REPLY_TIMEOUT) as conv:
                    await conv.send_message(q)
                    reply = await conv.get_response()
                acc.done += 1
                bump(acc.user_id)
                acc.fails = 0
                acc.cycles = 0
                errors = 0
                conn_fails = 0
                if conn_msg:
                    await safe_delete(conn_msg)
                    conn_msg = None
                log.info(f"[{acc.tag()}] #{acc.done}/{acc.target} q='{q[:40]}' reply='{(reply.text or '')[:40]}'")
            except asyncio.TimeoutError:
                acc.fails += 1
                log.info(f"[{acc.tag()}] no reply in {REPLY_TIMEOUT}s ({acc.fails}/{MAX_RETRIES})")
                if acc.fails >= MAX_RETRIES:
                    acc.cycles += 1
                    acc.fails = 0
                    if acc.cycles >= 2:
                        acc.status = "stopped"
                        log.error(f"[{acc.tag()}] still no reply after cooldown. Stopped. Send /run to retry.")
                        return
                    acc.status = "cooldown"
                    acc.next_retry = time.time() + COOLDOWN_HOURS * 3600
                    log.info(f"[{acc.tag()}] {MAX_RETRIES} fails, cooling down {COOLDOWN_HOURS}h")
                    await asyncio.sleep(COOLDOWN_HOURS * 3600)
                    acc.status = "running"
                continue
            except FloodWaitError as e:
                log.error(f"[{acc.tag()}] FloodWait {e.seconds}s")
                await asyncio.sleep(e.seconds + 5)
                continue
            except PeerFloodError:
                acc.status = "error"
                acc.error = "PeerFlood (limited by Telegram)"
                log.error(f"[{acc.tag()}] PeerFlood (Telegram limited this account). Stopped.")
                return
            except FATAL as e:
                acc.status = "error"
                acc.ready = False
                acc.role = "dead"
                acc.error = f"{type(e).__name__} (session dead/banned)"
                log.error(f"BROKEN SESSION -> {acc.tag()}: {acc.error}")
                sp = await promote(acc)
                if sp:
                    sp.target, sp.done = acc.target, acc.done
                    sp.task = asyncio.create_task(run_account(sp))
                    log.error(f"[{acc.label}] switched to spare string {sp.label} for @{sp.username}, continuing at {sp.done}/{sp.target}")
                else:
                    log.error(f"[{acc.label}] no spare string left for @{acc.username}. Stopped.")
                return
            except (ConnectionError, OSError) as e:
                conn_fails += 1
                log.warning(f"[{acc.tag()}] connection error {conn_fails}/{CONN_TRIES}: {e!r}")
                if conn_fails == 1:
                    conn_msg = await say(f"⚠️ {acc.tag()} lost connection. Retrying in the background…")
                if conn_fails >= CONN_TRIES:
                    await safe_delete(conn_msg)
                    conn_msg = None
                    acc.status = "error"
                    acc.error = f"connection failed {CONN_TRIES} times ({type(e).__name__})"
                    await say(
                        f"❌ {acc.tag()} still not connecting after {CONN_TRIES} tries.\n"
                        f"Reason: {type(e).__name__}: {str(e)[:120]}\n"
                        f"This account is stopped. Other accounts keep running.\n"
                        f"Tap Run to try it again.",
                        inline_main(),
                    )
                    return
                await asyncio.sleep(CONN_DELAYS[min(conn_fails - 1, len(CONN_DELAYS) - 1)])
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                errors += 1
                log.error(f"[{acc.tag()}] error ({errors}/5): {e!r}")
                if errors >= 5:
                    acc.status = "error"
                    acc.error = f"too many errors: {e!r}"[:150]
                    log.error(f"[{acc.tag()}] too many errors. Stopped.")
                    return
                await asyncio.sleep(30)
                continue

            await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

        acc.status = "done"
        log.info(f"[{acc.tag()}] finished {acc.done}/{acc.target}")
    except asyncio.CancelledError:
        acc.status = "idle"
        raise


# ---------- BROADCAST ----------
async def broadcast_one(acc: Acc, text: str):
    if not acc.ready or acc.role != "primary":
        return False
    try:
        if not acc.client.is_connected():
            await acc.client.connect()
        async with acc.client.conversation(TARGET_BOT, timeout=REPLY_TIMEOUT) as conv:
            await conv.send_message(text)
            await conv.get_response()
        return True
    except Exception as e:
        log.warning(f"[{acc.tag()}] broadcast failed: {e!r}")
        return False


async def do_broadcast(text: str):
    targets = [a for a in primaries() if a.ready]
    if not targets:
        return await say("No working accounts to broadcast to.", inline_main())
    await say(f"📣 Sending to {len(targets)} account(s) now…")
    results = await asyncio.gather(*(broadcast_one(a, text) for a in targets))
    ok = sum(1 for r in results if r)
    await say(f"📣 Broadcast done: {ok}/{len(targets)} accounts got a reply.", inline_main())


# ---------- KEYBOARDS ----------
BTN = {
    "run": ("▶️", "Run", "success"),
    "stop": ("🛑", "Stop", "danger"),
    "status": ("📊", "Status", "primary"),
    "ping": ("🏓", "Ping", "primary"),
    "dashboard": ("💰", "Dashboard", "success"),
    "balance": ("➕", "Insert balance", "primary"),
    "accounts": ("👥", "Accounts", "primary"),
    "set": ("🎯", "Set target", "primary"),
    "autorun": ("🔁", "Auto-Run", "primary"),
    "schedule": ("⏰", "Schedule", "primary"),
    "broadcast": ("📣", "Broadcast", "primary"),
    "reset": ("🔄", "Reset Today", "danger"),
    "hide": ("⌨️", "Hide", None),
}
TEXT_MAP = {}
for _c, (_e, _l, _s) in BTN.items():
    TEXT_MAP[_l] = _c
    TEXT_MAP[f"{_e} {_l}"] = _c

REMOVE_KB = {"remove_keyboard": True}


def mk(emoji, label, style=None, **extra):
    b = dict(extra)
    key = emoji.replace("\ufe0f", "")
    if PREMIUM_ON and key in PREMIUM:
        b["text"] = label
        b["icon_custom_emoji_id"] = PREMIUM[key]
    else:
        b["text"] = f"{emoji} {label}" if emoji else label
    if style:
        b["style"] = style
    return b


def rb(cmd):
    e, l, s = BTN[cmd]
    return mk(e, l, s)


def reply_kb():
    rows = [["run", "stop"], ["status", "ping"], ["dashboard", "balance"],
            ["accounts", "set"], ["autorun", "schedule"], ["broadcast", "reset"], ["hide"]]
    return {"keyboard": [[rb(c) for c in r] for r in rows], "resize_keyboard": True}


def inline_main():
    auto_label = "🔁 Auto-Run: ON" if STATE["auto_run"] else "🔁 Auto-Run: OFF"
    auto_style = "success" if STATE["auto_run"] else "danger"
    return {"inline_keyboard": [
        [mk("▶️", "Run", "success", callback_data="run"), mk("🛑", "Stop", "danger", callback_data="stop")],
        [mk("📊", "Status", "primary", callback_data="status"), mk("🏓", "Ping", "primary", callback_data="ping")],
        [mk("💰", "Dashboard", "success", callback_data="dashboard"),
         mk("➕", "Insert balance", "primary", callback_data="balance")],
        [mk("👥", "Accounts", "primary", callback_data="accounts"),
         mk("🎯", "Set target", "primary", callback_data="settarget")],
        [{"text": auto_label, "style": auto_style, "callback_data": "autorun"},
         mk("⏰", "Schedule", "primary", callback_data="schedule")],
        [mk("📣", "Broadcast", "primary", callback_data="broadcast")],
        [mk("🔄", "Reset Today", "danger", callback_data="reset")],
        [mk("⌨️", "Hide keyboard", callback_data="kbhide"), mk("⌨️", "Show keyboard", callback_data="kbshow")],
    ]}


def inline_run():
    return {"inline_keyboard": [
        [mk("🛑", "Stop", "danger", callback_data="stop"), mk("🏓", "Ping", "primary", callback_data="ping")],
    ]}


def inline_targets():
    return {"inline_keyboard": [
        [{"text": str(n), "callback_data": f"set:{n}", "style": "primary"} for n in (10, 25, 50, 100)],
        [mk("🎲", "Random", callback_data="set:off"), mk("⬅️", "Back", callback_data="back")],
    ]}


# ---------- PROGRESS ----------
ICON = {"idle": "⚪", "running": "🟢", "cooldown": "🟡", "done": "✅", "stopped": "🔴", "error": "❌"}


def bar(done, total, w=20):
    p = (done / total) if total else 0
    f = int(p * w)
    return "█" * f + "░" * (w - f) + f" {int(p * 100)}%"


def render():
    active = primaries()
    tot = sum(a.target for a in active)
    done = sum(a.done for a in active)
    mode = f"fixed {FIXED_TARGET}" if FIXED_TARGET else f"random {DAILY_MIN}-{DAILY_MAX}"
    sched = f"\n⏰ Auto-Run: {'ON' if STATE['auto_run'] else 'OFF'}" + (f" at {STATE['run_time']} IST" if STATE["run_time"] else "")
    lines = [f"📊 Progress ({mode}){sched}\n{bar(done, tot)}  {done}/{tot}\n"]

    detail, idle_n, done_n = [], 0, 0
    for a in accs:
        if a.role in ("spare", "dead"):
            continue
        if a.role != "primary":
            detail.append(f"❌ {a.label} — {a.error or 'not connected'}")
            continue
        if a.status == "idle":
            idle_n += 1
            continue
        if a.status == "done":
            done_n += 1
            continue
        extra = ""
        if a.status == "cooldown":
            extra = f" ⏳ retry in {max(0, int((a.next_retry - time.time()) / 60))}m"
        elif a.status == "error":
            extra = f" ⚠️ {a.error}"
        elif a.fails:
            extra = f" ⚠️ fail {a.fails}/{MAX_RETRIES}"
        dn, dt = day_done(a.user_id), daily_target(a.user_id)
        detail.append(f"{ICON.get(a.status, '⚪')} {a.tag()} {bar(a.done, a.target, 10)} {a.done}/{a.target} · day {dn}/{dt}{extra}")

    MAX_LINES = 40
    if len(detail) > MAX_LINES:
        shown, hidden = detail[:MAX_LINES], len(detail) - MAX_LINES
        lines += shown
        lines.append(f"…and {hidden} more active account(s)")
    else:
        lines += detail

    if idle_n:
        lines.append(f"⚪ {idle_n} account(s) queued, not started yet")
    if done_n:
        lines.append(f"✅ {done_n} account(s) finished this run")
    spare = sum(1 for a in accs if a.role == "spare")
    if spare:
        lines.append(f"⚪ {spare} spare string(s) on standby")
    lines.append(f"\n🕒 {datetime.now(IST).strftime('%H:%M:%S')} IST")
    return "\n".join(lines)


def is_running():
    return any(a.task and not a.task.done() for a in accs)


async def progress_loop(msg):
    last = ""
    while True:
        running = is_running()
        text = render()
        if text != last:
            try:
                msg = await edit_msg(msg, text, inline_run() if running else inline_main())
                last = text
            except Exception as e:
                log.warning(f"progress edit failed: {e!r}")
        if not running:
            break
        await asyncio.sleep(PROGRESS_EVERY)
    await say("🏁 Run finished.", inline_main())


# ---------- PING ----------
async def ping_one(a: Acc):
    try:
        if not a.client.is_connected():
            await a.client.connect()
        t = time.perf_counter()
        await asyncio.wait_for(a.client.get_me(), 10)
        ms = int((time.perf_counter() - t) * 1000)
        return True, f"✅ {a.tag()} — {ms}ms · {a.status} {a.done}/{a.target}"
    except FATAL as e:
        a.ready = False
        a.role = "dead"
        a.error = f"{type(e).__name__} (session dead/banned)"
        log.error(f"BROKEN SESSION -> {a.tag()}: {a.error}")
        sp = await promote(a)
        if sp:
            return True, f"🔁 {a.tag()} died, switched to spare {sp.label}"
        return False, f"❌ {a.tag()} — {a.error}, no spare left"
    except Exception as e:
        return False, f"❌ {a.tag()} — no response ({type(e).__name__})"


async def act_ping():
    prim = primaries()
    if not prim:
        return await say("No working accounts.\n\n" + accounts_report(), inline_main())
    results = await asyncio.gather(*(ping_one(a) for a in prim))
    alive = sum(1 for ok, _ in results if ok)
    spare = sum(1 for a in accs if a.role == "spare")
    lines = [f"🏓 Ping: {alive}/{len(prim)} accounts alive ({len(accs)} strings, {spare} spare)\n"]
    lines += [t for _, t in results]
    bad = [a for a in accs if a.role == "broken"]
    if bad:
        lines.append("\n⚠️ Corrupt/broken strings:")
        lines += [describe_broken(a) for a in bad]
    lines.append(f"\n🕒 {datetime.now(IST).strftime('%H:%M:%S')} IST")
    await say("\n".join(lines), inline_main())


# ---------- ACTIONS ----------
async def act_run():
    global progress_task
    if is_running():
        return await say("Already running.", inline_run())
    ensure_today()
    started, skipped = 0, []
    for a in primaries():
        if not a.ready:
            continue
        tgt, dn = daily_target(a.user_id), day_done(a.user_id)
        remaining = tgt - dn
        a.error = ""
        a.fails = a.cycles = 0
        if remaining <= 0:
            a.target, a.done, a.status = 0, 0, "done"
            skipped.append(f"{a.tag()} {dn}/{tgt}")
            continue
        a.target, a.done, a.status = remaining, 0, "idle"
        a.task = asyncio.create_task(run_account(a))
        started += 1
    if not started:
        return await say("✅ All accounts already completed today's limit.\n" + "\n".join(skipped), inline_main())
    note = ("\n\n⏭ Already done today:\n" + "\n".join(skipped)) if skipped else ""
    msg = await say(render() + note, inline_run())
    progress_task = asyncio.create_task(progress_loop(msg))


async def act_stop():
    n = 0
    for a in accs:
        if a.task and not a.task.done():
            a.task.cancel()
            n += 1
    await say(f"🛑 Stopped {n} account(s)." if n else "Nothing is running.", inline_main())


async def act_status():
    await say(render(), inline_run() if is_running() else inline_main())


async def act_set(arg=None):
    global FIXED_TARGET
    if not arg:
        cur = f"{FIXED_TARGET} per account" if FIXED_TARGET else f"random {DAILY_MIN}-{DAILY_MAX}"
        return await say(f"🎯 Current target: {cur}\nPick one, or send /set 50", inline_targets())
    if arg.lower() in ("off", "random"):
        FIXED_TARGET = None
        return await say(f"🎲 Target is random ({DAILY_MIN}-{DAILY_MAX}) from the next run.", inline_main())
    if arg.isdigit() and int(arg) > 0:
        FIXED_TARGET = int(arg)
        live = 0
        for a in accs:
            if a.task and not a.task.done():
                a.target = max(0, FIXED_TARGET - day_done(a.user_id) + a.done)
                live += 1
        note = f" Updated {live} running account(s)." if live else " Applies from the next run."
        return await say(f"🎯 Target set: {FIXED_TARGET} replies per account.{note}", inline_main())
    await say("Usage: /set 50  or  /set off", inline_targets())


async def act_balance(arg=None):
    global AWAIT_BALANCE
    if arg is None:
        AWAIT_BALANCE = True
        return await say("➕ Send your current total balance in $ (example: 12.50)")
    try:
        val = float(arg.replace("$", "").replace(",", ""))
    except ValueError:
        return await say("Send a number, example: 12.50")
    AWAIT_BALANCE = False
    STATE["base"], STATE["since"] = val, 0
    save_state()
    await say(f"✅ Balance set to {money(val)}. Each new reply adds {money(REPLY_RATE)}.\n\n" + dashboard(), inline_main())


async def act_reset_today():
    n = 0
    for a in accs:
        if a.task and not a.task.done():
            a.task.cancel()
            n += 1
    ensure_today()
    STATE["daily"] = {}
    STATE["targets"] = {}
    save_state()
    for a in accs:
        a.done = a.target = a.fails = a.cycles = 0
        a.status = "idle"
        a.error = ""
    await say(f"🔄 Reset done. Stopped {n} running account(s). All accounts back to day 0/50.", inline_main())


async def act_autorun_toggle():
    STATE["auto_run"] = not STATE["auto_run"]
    save_state()
    if STATE["auto_run"] and not STATE["run_time"]:
        await say("🔁 Auto-Run is ON, but no time is set yet. Tap ⏰ Schedule to pick a time.", inline_main())
    else:
        state_txt = "ON" if STATE["auto_run"] else "OFF"
        time_txt = f" Scheduled for {STATE['run_time']} IST daily." if STATE["run_time"] else ""
        await say(f"🔁 Auto-Run is now {state_txt}.{time_txt}", inline_main())


async def act_schedule(arg=None):
    global AWAIT_SCHEDULE
    if not arg:
        AWAIT_SCHEDULE = True
        cur = f"\nCurrent: {STATE['run_time']} IST" if STATE["run_time"] else ""
        return await say(f"⏰ Send the time to auto-run daily, example: 2:00 am or 5:00 pm{cur}")
    t = parse_time(arg)
    if not t:
        return await say("Couldn't read that time. Try: 2:00 am, 5 pm, or 14:00")
    AWAIT_SCHEDULE = False
    STATE["run_time"] = t
    save_state()
    extra = "" if STATE["auto_run"] else "\nAuto-Run is currently OFF — tap 🔁 Auto-Run to turn it on."
    await say(f"⏰ Scheduled daily run at {t} IST.{extra}", inline_main())


async def act_broadcast(arg=None):
    global AWAIT_BROADCAST
    if not arg:
        AWAIT_BROADCAST = True
        return await say("📣 Send the message to broadcast to all accounts now (doesn't count toward 50/day).")
    AWAIT_BROADCAST = False
    await do_broadcast(arg)


HELP = ("Commands:\n/run /stop /status /ping\n/dashboard - earnings today and lifetime\n"
        "/balance 12.50 - insert current balance\n/accounts - strings per account, corrupt ones\n"
        "/set 50 - replies per account\n/set off - random target\n"
        "/autorun - toggle scheduled daily run\n/schedule 2:00 am - set the daily run time\n"
        "/broadcast <msg> - send one message to all accounts right now\n"
        "/reset - reset today's counts to 0/50\n/menu - show keyboard\n/hide - hide keyboard")


async def dispatch(cmd, arg=None):
    if cmd in ("start", "help", "menu"):
        await say("⌨️ Keyboard on." if cmd == "menu" else HELP, reply_kb())
        return await say("Controls:", inline_main())
    if cmd == "hide":
        return await say("⌨️ Keyboard hidden. Send /menu to show it again.", REMOVE_KB)
    if cmd == "run":
        return await act_run()
    if cmd == "stop":
        return await act_stop()
    if cmd == "status":
        return await act_status()
    if cmd == "ping":
        return await act_ping()
    if cmd == "accounts":
        return await say(accounts_report(), inline_main())
    if cmd == "dashboard":
        return await say(dashboard(), inline_main())
    if cmd == "balance":
        return await act_balance(arg)
    if cmd == "set":
        return await act_set(arg)
    if cmd == "autorun":
        return await act_autorun_toggle()
    if cmd == "schedule":
        return await act_schedule(arg)
    if cmd == "broadcast":
        return await act_broadcast(arg)
    if cmd == "reset":
        return await act_reset_today()


# ---------- HANDLERS ----------
@bot.on(events.NewMessage(from_users=ADMIN_ID))
async def h_msg(ev):
    global AWAIT_BALANCE, AWAIT_SCHEDULE, AWAIT_BROADCAST
    t = (ev.raw_text or "").strip()
    if AWAIT_BROADCAST and t and not t.startswith("/") and t not in TEXT_MAP:
        AWAIT_BROADCAST = False
        return await do_broadcast(t)
    if AWAIT_BALANCE and t and not t.startswith("/") and t not in TEXT_MAP:
        return await act_balance(t)
    if AWAIT_SCHEDULE and t and not t.startswith("/") and t not in TEXT_MAP:
        return await act_schedule(t)
    AWAIT_BALANCE = AWAIT_SCHEDULE = AWAIT_BROADCAST = False
    m = re.match(r"^/(\w+)(?:@\w+)?(?:\s+(.+))?$", t)
    if m:
        cmd, arg = m.group(1).lower(), m.group(2)
    else:
        cmd, arg = TEXT_MAP.get(t), None
    if cmd:
        await dispatch(cmd, arg)


@bot.on(events.CallbackQuery(func=lambda e: e.sender_id == ADMIN_ID))
async def h_cb(ev):
    d = ev.data.decode()
    await ev.answer()
    if d.startswith("set:"):
        return await act_set(d[4:])
    if d == "settarget":
        return await act_set(None)
    if d == "back":
        return await say("Controls:", inline_main())
    if d == "kbhide":
        return await dispatch("hide")
    if d == "kbshow":
        return await say("⌨️ Keyboard on.", reply_kb())
    await dispatch(d)


# ---------- ALERTS ----------
async def alert_sender():
    while True:
        text = await alert_q.get()
        try:
            await say(f"🚨 {text[:3800]}")
        except Exception as e:
            print("alert send failed:", e)
        await asyncio.sleep(1)


async def midnight_loop():
    while True:
        n = datetime.now(IST)
        nxt = (n + timedelta(days=1)).replace(hour=0, minute=0, second=5, microsecond=0)
        await asyncio.sleep((nxt - n).total_seconds())
        prev = STATE["date"]
        if ensure_today():
            y = STATE["history"].get(prev, 0)
            stopped = 0
            for a in accs:
                if a.task and not a.task.done():
                    a.task.cancel()
                    stopped += 1
            for a in accs:
                a.done = a.target = a.fails = a.cycles = 0
                a.status = "idle"
                a.error = ""
            await say(
                f"🌅 New day (IST). Yesterday: {y} replies → {money(y * REPLY_RATE)}\n"
                f"Stopped {stopped} leftover run(s). All accounts reset to day 0/50.",
                inline_main(),
            )


async def schedule_loop():
    while True:
        await asyncio.sleep(30)
        if not STATE.get("auto_run") or not STATE.get("run_time"):
            continue
        now = datetime.now(IST)
        if now.strftime("%H:%M") == STATE["run_time"] and STATE.get("last_auto_date") != today_str():
            STATE["last_auto_date"] = today_str()
            save_state()
            log.info(f"Auto-Run triggered at {STATE['run_time']} IST")
            await act_run()


async def main():
    await bot.start(bot_token=BOT_TOKEN)
    asyncio.create_task(alert_sender())
    asyncio.create_task(midnight_loop())
    asyncio.create_task(schedule_loop())
    try:
        await bot.unpin_message(ADMIN_ID)
        cap = f"{BRAND}\n𝗖𝗟𝗘𝗫𝗘𝗥 — auto worker"
        if os.path.exists("logo.png"):
            m = await bot.send_file(ADMIN_ID, "logo.png", caption=cap)
        else:
            m = await bot.send_message(ADMIN_ID, cap)
        await bot.pin_message(ADMIN_ID, m, notify=False)
    except Exception as e:
        log.warning(f"pin failed: {e}")
    seen = {}
    for a in accs:
        await connect(a, seen)
    await organize()
    for a in accs:
        if a.role == "broken":
            log.error(describe_broken(a))
    text = (f"✅ Online. {len(GROUPS)} unique account(s) ready from {len(accs)} string(s).\n\n"
            f"{accounts_report()}\n\nTap Run, or send /set 50 then /run.")
    await say(text, reply_kb())
    await say("Controls:", inline_main())
    await bot.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())