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
    client: Optional[TelegramClient] = None
    username: str = ""
    last_user: str = ""
    ready: bool = False
    error: str = ""
    target: int = 0
    done: int = 0
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
    accs.append(Acc(i, label, sess, last_user=NAMES.get(label, "")))

bot = TelegramClient(StringSession(), API_ID, API_HASH)
progress_task: Optional[asyncio.Task] = None

FATAL = (UserDeactivatedError, UserDeactivatedBanError, AuthKeyUnregisteredError,
         AuthKeyDuplicatedError, SessionRevokedError)


def pick_target() -> int:
    return FIXED_TARGET if FIXED_TARGET else random.randint(DAILY_MIN, DAILY_MAX)


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
        err = f"same session string as {seen[acc.session]}"
    if err:
        acc.status = "error"
        acc.error = err
        log.error(f"BROKEN SESSION -> {acc.tag()}: {err}")
        return
    seen[acc.session] = acc.label
    try:
        acc.client = TelegramClient(StringSession(acc.session), API_ID, API_HASH)
        await acc.client.connect()
        if not await acc.client.is_user_authorized():
            acc.status = "error"
            acc.error = "session dead/revoked (not authorized)"
            log.error(f"BROKEN SESSION -> {acc.tag()}: {acc.error}")
            return
        me = await acc.client.get_me()
        acc.username = me.username or str(me.id)
        NAMES[acc.label] = acc.username
        save_names(NAMES)
        acc.ready = True
        acc.error = ""
        log.info(f"[{acc.tag()}] connected")
    except Exception as e:
        acc.status = "error"
        acc.error = repr(e)[:150]
        log.error(f"CONNECT FAILED -> {acc.tag()}: {acc.error}")


async def run_account(acc: Acc):
    try:
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
                acc.error = f"{type(e).__name__} (session dead/banned)"
                log.error(f"BROKEN SESSION -> {acc.tag()}: {acc.error}. Stopped.")
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
B_RUN, B_STOP, B_STATUS, B_PING, B_SET, B_HIDE = "▶️ Run", "🛑 Stop", "📊 Status", "🏓 Ping", "🎯 Set target", "⌨️ Hide"
TEXT_MAP = {B_RUN: "run", B_STOP: "stop", B_STATUS: "status", B_PING: "ping", B_SET: "set", B_HIDE: "hide"}


def reply_kb():
    return [
        [Button.text(B_RUN, resize=True), Button.text(B_STOP, resize=True)],
        [Button.text(B_STATUS, resize=True), Button.text(B_PING, resize=True)],
        [Button.text(B_SET, resize=True), Button.text(B_HIDE, resize=True)],
    ]


def inline_main():
    return [
        [Button.inline("▶️ Run", b"run"), Button.inline("🛑 Stop", b"stop")],
        [Button.inline("📊 Status", b"status"), Button.inline("🏓 Ping", b"ping")],
        [Button.inline("🎯 Set target", b"settarget")],
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
    active = [a for a in accs if a.ready]
    tot = sum(a.target for a in active)
    done = sum(a.done for a in active)
    mode = f"fixed {FIXED_TARGET}" if FIXED_TARGET else f"random {DAILY_MIN}-{DAILY_MAX}"
    lines = [f"📊 Progress ({mode})\n{bar(done, tot)}  {done}/{tot}\n"]
    for a in accs:
        if not a.ready:
            lines.append(f"❌ {a.tag()} — {a.error or 'not connected'}")
            continue
        extra = ""
        if a.status == "cooldown":
            extra = f" ⏳ retry in {max(0, int((a.next_retry - time.time()) / 60))}m"
        elif a.status == "error":
            extra = f" ⚠️ {a.error}"
        elif a.fails:
            extra = f" ⚠️ fail {a.fails}/{MAX_RETRIES}"
        lines.append(f"{ICON.get(a.status, '⚪')} {a.tag()} {bar(a.done, a.target, 10)} {a.done}/{a.target}{extra}")
    lines.append(f"\n🕒 {datetime.now(IST).strftime('%H:%M:%S')} IST")
    return "\n".join(lines)


def is_running():
    return any(a.task and not a.task.done() for a in accs)


async def say(text, buttons=None):
    return await bot.send_message(ADMIN_ID, text, buttons=buttons)


async def progress_loop(msg):
    last = ""
    while True:
        running = is_running()
        text = render()
        if text != last:
            try:
                await msg.edit(text, buttons=inline_run() if running else inline_main())
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
    if not a.ready:
        return False, f"❌ {a.tag()} — BROKEN: {a.error or 'not connected'}"
    try:
        if not a.client.is_connected():
            await a.client.connect()
        t = time.perf_counter()
        await asyncio.wait_for(a.client.get_me(), 10)
        ms = int((time.perf_counter() - t) * 1000)
        return True, f"✅ {a.tag()} — {ms}ms · {a.status} {a.done}/{a.target}"
    except FATAL as e:
        a.ready = False
        a.error = f"{type(e).__name__} (session dead/banned)"
        return False, f"❌ {a.tag()} — {a.error}"
    except Exception as e:
        return False, f"❌ {a.tag()} — no response ({type(e).__name__})"


async def act_ping():
    if not accs:
        return await say("No accounts configured.", inline_main())
    results = await asyncio.gather(*(ping_one(a) for a in accs))
    alive = sum(1 for ok, _ in results if ok)
    lines = [f"🏓 Ping: {alive}/{len(accs)} alive\n"] + [t for _, t in results]
    lines.append(f"\n🕒 {datetime.now(IST).strftime('%H:%M:%S')} IST")
    await say("\n".join(lines), inline_main())


# ---------- ACTIONS ----------
async def act_run():
    global progress_task
    if is_running():
        return await say("Already running.", inline_run())
    ready = [a for a in accs if a.ready]
    if not ready:
        return await say("No working accounts. Tap Ping to see which are broken.", inline_main())
    for a in ready:
        a.target = pick_target()
        a.done = a.fails = a.cycles = 0
        a.error = ""
        a.status = "idle"
        a.task = asyncio.create_task(run_account(a))
    msg = await say(render(), inline_run())
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
                a.target = FIXED_TARGET
                live += 1
        note = f" Updated {live} running account(s)." if live else " Applies from the next run."
        return await say(f"🎯 Target set: {FIXED_TARGET} replies per account.{note}", inline_main())
    await say("Usage: /set 50  or  /set off", inline_targets())


HELP = ("Commands:\n/run /stop /status /ping\n/set 50 - replies per account\n/set off - random target\n"
        "/menu - show keyboard\n/hide - hide keyboard")


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
    if cmd == "set":
        return await act_set(arg)


# ---------- HANDLERS ----------
@bot.on(events.NewMessage(from_users=ADMIN_ID))
async def h_msg(ev):
    t = (ev.raw_text or "").strip()
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


async def main():
    await bot.start(bot_token=BOT_TOKEN)
    asyncio.create_task(alert_sender())
    seen = {}
    for a in accs:
        await connect(a, seen)
    ok = sum(1 for a in accs if a.ready)
    broken = [a for a in accs if not a.ready]
    text = f"✅ Online. {ok}/{len(accs)} accounts ready."
    if broken:
        text += "\n\n❌ Broken sessions:\n" + "\n".join(f"• {a.tag()} — {a.error}" for a in broken)
    text += "\n\nTap Run, or send /set 50 then /run."
    await say(text, reply_kb())
    await say("Controls:", inline_main())
    await bot.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
