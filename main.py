import os, re, asyncio, random, logging, json, time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiohttp
from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.errors import (
    FloodWaitError, MessageNotModifiedError, PeerFloodError,
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

GEMINI_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
QUESTION_TOPIC = os.getenv("QUESTION_TOPIC", "everyday general topics")

IST = timezone(timedelta(hours=5, minutes=30))
FIXED_TARGET: Optional[int] = int(os.getenv("FIXED_TARGET", "0")) or None


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

# ---------- LOGGING (ERROR -> admin DM) ----------
alert_q: asyncio.Queue = asyncio.Queue()


class AdminAlertHandler(logging.Handler):
    def emit(self, record):
        try:
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

# ---------- STATE (daily counts, earnings) ----------
REPLY_RATE = float(os.getenv("REPLY_RATE", "0.003"))
DATA_DIR = os.getenv("DATA_DIR", ".")
STATE_FILE = os.path.join(DATA_DIR, "state.json")
BRAND = "▰▰ 𝗖𝗟𝗘𝗫𝗘𝗥 ▰▰"
AWAIT_BALANCE = False


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
STATE.setdefault("daily", {})     # user_id -> replies today
STATE.setdefault("targets", {})   # user_id -> random target chosen today
STATE.setdefault("history", {})   # date -> replies
STATE.setdefault("base", 0.0)     # balance you inserted
STATE.setdefault("since", 0)      # replies counted since that balance was set


def save_state():
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(STATE, f)
    except Exception as e:
        log.warning(f"state save failed: {e}")


def ensure_today():
    """Rolls the day at 00:00 IST. Returns True if a new day started."""
    t = today_str()
    if STATE["date"] != t:
        STATE["history"][STATE["date"]] = sum(STATE["daily"].values())
        STATE["history"] = dict(sorted(STATE["history"].items())[-30:])
        STATE["daily"], STATE["targets"], STATE["date"] = {}, {}, t
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


# ---------- QUESTIONS ----------
FALLBACK = [
    "hi", "hello", "how are you?", "what can you do?", "help me", "show menu",
    "what is your name?", "tell me something interesting", "how does this work?",
    "can you explain that again?", "what are your features?", "is this free?",
    "give me a quick tip", "what's new today?", "how do I get started?",
    "can you help me with something?", "tell me a fun fact", "what should I try first?",
    "how do I contact support?", "what are the commands?", "is there a guide?",
    "what is the best way to use you?", "can you recommend something?",
    "thanks, what next?", "explain in simple words", "what can I ask you?",
    "do you have any examples?", "how fast are you?", "are you online?",
    "give me one more idea",
]
pool: list = []
pool_lock = asyncio.Lock()


async def gemini_questions() -> list:
    if not GEMINI_KEY:
        return []
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_KEY}"
    prompt = (f"Generate 30 different short, casual questions a normal user might ask a chatbot about "
              f"{QUESTION_TOPIC}. Vary length and wording. Return only a JSON array of strings.")
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
        if not pool:
            qs = []
            try:
                qs = await gemini_questions()
            except Exception as e:
                log.warning(f"Gemini failed, using fallback: {e}")
            if not qs:
                qs = FALLBACK[:]
            random.shuffle(qs)
            pool.extend(qs)
        return pool.pop()


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
    role: str = "new"      # primary spare broken dead
    ready: bool = False
    error: str = ""
    target: int = 0        # replies to do in THIS run (remaining for today)
    done: int = 0          # replies done in THIS run
    fails: int = 0
    cycles: int = 0
    status: str = "idle"   # idle running cooldown done stopped error
    task: Optional[asyncio.Task] = None
    next_retry: float = 0.0

    def tag(self):
        u = self.username or self.last_user
        return f"{self.label} (@{u})" if u else self.label


accs = []
for i, (lab, raw) in enumerate(ENTRIES):
    label, sess = parse_entry(lab, raw)
    accs.append(Acc(i, label, sess, group=label.split("__")[0], last_user=NAMES.get(label, "")))

GROUPS: dict = {}   # telegram user_id -> [valid Acc sorted by idx]
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
        acc.client = TelegramClient(StringSession(acc.session), API_ID, API_HASH)
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


# ---------- GROUPING (one account = one active string) ----------
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
    """Find which working account a broken string belongs to (exact duplicate, name group, or last known @user)."""
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
    """Switch to a spare string of the same account."""
    for sp in GROUPS.get(dead.user_id, []):
        if sp is dead or sp.role != "spare":
            continue
        try:
            sp.client = TelegramClient(StringSession(sp.session), API_ID, API_HASH)
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


# ---------- WORKER ----------
async def run_account(acc: Acc):
    try:
        if acc.done == 0:
            await asyncio.sleep(acc.idx * random.uniform(5, 20))
        acc.status = "running"
        errors = 0
        while acc.done < acc.target:
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


# ---------- KEYBOARDS ----------
B_RUN, B_STOP, B_STATUS, B_PING = "▶️ Run", "🛑 Stop", "📊 Status", "🏓 Ping"
B_ACC, B_SET, B_HIDE = "👥 Accounts", "🎯 Set target", "⌨️ Hide"
B_DASH, B_BAL = "💰 Dashboard", "➕ Insert balance"
TEXT_MAP = {B_RUN: "run", B_STOP: "stop", B_STATUS: "status", B_PING: "ping",
            B_ACC: "accounts", B_SET: "set", B_HIDE: "hide", B_DASH: "dashboard", B_BAL: "balance"}


def reply_kb():
    return [
        [Button.text(B_RUN, resize=True), Button.text(B_STOP, resize=True)],
        [Button.text(B_STATUS, resize=True), Button.text(B_PING, resize=True)],
        [Button.text(B_DASH, resize=True), Button.text(B_BAL, resize=True)],
        [Button.text(B_ACC, resize=True), Button.text(B_SET, resize=True)],
        [Button.text(B_HIDE, resize=True)],
    ]


def inline_main():
    return [
        [Button.inline("▶️ Run", b"run"), Button.inline("🛑 Stop", b"stop")],
        [Button.inline("📊 Status", b"status"), Button.inline("🏓 Ping", b"ping")],
        [Button.inline("💰 Dashboard", b"dashboard"), Button.inline("➕ Insert balance", b"balance")],
        [Button.inline("👥 Accounts", b"accounts"), Button.inline("🎯 Set target", b"settarget")],
        [Button.inline("⌨️ Hide keyboard", b"kbhide"), Button.inline("⌨️ Show keyboard", b"kbshow")],
    ]


def inline_run():
    return [[Button.inline("🛑 Stop", b"stop"), Button.inline("🏓 Ping", b"ping")]]


def inline_targets():
    return [
        [Button.inline("10", b"set:10"), Button.inline("25", b"set:25"),
         Button.inline("50", b"set:50"), Button.inline("100", b"set:100")],
        [Button.inline("🎲 Random", b"set:off"), Button.inline("⬅️ Back", b"back")],
    ]


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
    lines = [f"📊 Progress ({mode})\n{bar(done, tot)}  {done}/{tot}\n"]
    for a in accs:
        if a.role in ("spare", "dead"):
            continue
        if a.role != "primary":
            lines.append(f"❌ {a.label} — {a.error or 'not connected'}")
            continue
        extra = ""
        if a.status == "cooldown":
            extra = f" ⏳ retry in {max(0, int((a.next_retry - time.time()) / 60))}m"
        elif a.status == "error":
            extra = f" ⚠️ {a.error}"
        elif a.fails:
            extra = f" ⚠️ fail {a.fails}/{MAX_RETRIES}"
        dn, dt = day_done(a.user_id), daily_target(a.user_id)
        lines.append(f"{ICON.get(a.status, '⚪')} {a.tag()} {bar(a.done, a.target, 10)} {a.done}/{a.target} · day {dn}/{dt}{extra}")
    spare = sum(1 for a in accs if a.role == "spare")
    if spare:
        lines.append(f"\n⚪ {spare} spare string(s) on standby")
    lines.append(f"\n🕒 {datetime.now(IST).strftime('%H:%M:%S')} IST")
    return "\n".join(lines)


def is_running():
    return any(a.task and not a.task.done() for a in accs)


async def say(text, buttons=None):
    return await bot.send_message(ADMIN_ID, f"{BRAND}\n{text}"[:4000], buttons=buttons)


async def progress_loop(msg):
    last = ""
    while True:
        running = is_running()
        text = render()
        if text != last:
            try:
                await msg.edit(f"{BRAND}\n{text}", buttons=inline_run() if running else inline_main())
                last = text
            except MessageNotModifiedError:
                pass
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds)
            except Exception as e:
                log.error(f"progress edit failed: {e!r}")
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


HELP = ("Commands:\n/run /stop /status /ping\n/dashboard - earnings today and lifetime\n"
        "/balance 12.50 - insert current balance\n/accounts - strings per account, corrupt ones\n"
        "/set 50 - replies per account\n/set off - random target\n/menu - show keyboard\n/hide - hide keyboard")


async def dispatch(cmd, arg=None):
    if cmd in ("start", "help", "menu"):
        await say("⌨️ Keyboard on." if cmd == "menu" else HELP, reply_kb())
        return await say("Controls:", inline_main())
    if cmd == "hide":
        return await say("⌨️ Keyboard hidden. Send /menu to show it again.", Button.clear())
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


# ---------- HANDLERS ----------
@bot.on(events.NewMessage(from_users=ADMIN_ID))
async def h_msg(ev):
    global AWAIT_BALANCE
    t = (ev.raw_text or "").strip()
    if AWAIT_BALANCE and t and not t.startswith("/") and t not in TEXT_MAP:
        return await act_balance(t)
    AWAIT_BALANCE = False
    m = re.match(r"^/(\w+)(?:@\w+)?(?:\s+(\S+))?$", t)
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
            await bot.send_message(ADMIN_ID, f"🚨 {text[:3800]}")
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
            await say(f"🌅 New day (IST). Yesterday: {y} replies → {money(y * REPLY_RATE)}\nDaily counters reset.", inline_main())


async def main():
    await bot.start(bot_token=BOT_TOKEN)
    asyncio.create_task(alert_sender())
    asyncio.create_task(midnight_loop())
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