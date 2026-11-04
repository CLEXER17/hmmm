import os, asyncio, random, logging, json, time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiohttp
from telethon import TelegramClient, events
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

SESSIONS = [s.strip() for s in os.environ.get("SESSIONS", "").replace("\n", ",").split(",") if s.strip()]
SESSIONS += [v.strip() for k, v in sorted(os.environ.items(), key=lambda x: (len(x[0]), x[0]))
             if k.startswith("SESSION_") and v.strip()]

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

# Fixed target set by /set (None = random between DAILY_MIN and DAILY_MAX)
FIXED_TARGET: Optional[int] = int(os.getenv("FIXED_TARGET", "0")) or None

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
    session: str
    client: Optional[TelegramClient] = None
    name: str = "?"
    ready: bool = False
    target: int = 0
    done: int = 0
    fails: int = 0
    cycles: int = 0
    status: str = "idle"   # idle running cooldown done stopped error
    task: Optional[asyncio.Task] = None
    next_retry: float = 0.0


accs = [Acc(i, s) for i, s in enumerate(SESSIONS)]
bot = TelegramClient(StringSession(), API_ID, API_HASH)
progress_task: Optional[asyncio.Task] = None

FATAL = (UserDeactivatedError, UserDeactivatedBanError, AuthKeyUnregisteredError,
         AuthKeyDuplicatedError, SessionRevokedError)


def pick_target() -> int:
    return FIXED_TARGET if FIXED_TARGET else random.randint(DAILY_MIN, DAILY_MAX)


async def connect(acc: Acc):
    try:
        acc.client = TelegramClient(StringSession(acc.session), API_ID, API_HASH)
        await acc.client.connect()
        if not await acc.client.is_user_authorized():
            acc.status = "error"
            log.error(f"[acc{acc.idx}] session not authorized (dead/invalid). Regenerate it.")
            return
        me = await acc.client.get_me()
        acc.name = f"acc{acc.idx}:{me.username or me.id}"
        acc.ready = True
        log.info(f"[{acc.name}] connected")
    except Exception as e:
        acc.status = "error"
        log.error(f"[acc{acc.idx}] connect failed: {e!r}")


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
                log.info(f"[{acc.name}] #{acc.done}/{acc.target} q='{q[:40]}' reply='{(reply.text or '')[:40]}'")
            except asyncio.TimeoutError:
                acc.fails += 1
                log.info(f"[{acc.name}] no reply in {REPLY_TIMEOUT}s ({acc.fails}/{MAX_RETRIES})")
                if acc.fails >= MAX_RETRIES:
                    acc.cycles += 1
                    acc.fails = 0
                    if acc.cycles >= 2:
                        acc.status = "stopped"
                        log.error(f"[{acc.name}] still no reply after cooldown. Stopped. Send /run to retry.")
                        return
                    acc.status = "cooldown"
                    acc.next_retry = time.time() + COOLDOWN_HOURS * 3600
                    log.info(f"[{acc.name}] {MAX_RETRIES} fails, cooling down {COOLDOWN_HOURS}h")
                    await asyncio.sleep(COOLDOWN_HOURS * 3600)
                    acc.status = "running"
                continue
            except FloodWaitError as e:
                log.error(f"[{acc.name}] FloodWait {e.seconds}s")
                await asyncio.sleep(e.seconds + 5)
                continue
            except PeerFloodError:
                acc.status = "error"
                log.error(f"[{acc.name}] PeerFlood (Telegram limited this account). Stopped.")
                return
            except FATAL as e:
                acc.status = "error"
                log.error(f"[{acc.name}] fatal: {type(e).__name__}. Stopped.")
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                errors += 1
                log.error(f"[{acc.name}] error ({errors}/5): {e!r}")
                if errors >= 5:
                    acc.status = "error"
                    log.error(f"[{acc.name}] too many errors. Stopped.")
                    return
                await asyncio.sleep(30)
                continue

            await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

        acc.status = "done"
        log.info(f"[{acc.name}] finished {acc.done}/{acc.target}")
    except asyncio.CancelledError:
        acc.status = "idle"
        raise


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
        extra = ""
        if a.status == "cooldown":
            extra = f" ⏳ retry in {max(0, int((a.next_retry - time.time()) / 60))}m"
        elif a.fails:
            extra = f" ⚠️ fail {a.fails}/{MAX_RETRIES}"
        lines.append(f"{ICON.get(a.status, '⚪')} {a.name} {bar(a.done, a.target, 10)} {a.done}/{a.target}{extra}")
    lines.append(f"\n🕒 {datetime.now(IST).strftime('%H:%M:%S')} IST")
    return "\n".join(lines)


async def progress_loop(msg):
    last = ""
    while True:
        running = any(a.task and not a.task.done() for a in accs)
        text = render()
        if text != last:
            try:
                await msg.edit(text)
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
    await bot.send_message(ADMIN_ID, "🏁 Run finished. Send /run to start again.")


def is_running():
    return any(a.task and not a.task.done() for a in accs)


# ---------- ADMIN COMMANDS ----------
@bot.on(events.NewMessage(from_users=ADMIN_ID, pattern=r"(?i)^/(start|help)(@\w+)?$"))
async def h_help(ev):
    await ev.respond(
        "Commands:\n"
        "/set 50 - every account needs 50 bot replies\n"
        "/set off - back to random target\n"
        "/set - show current target\n"
        "/run - start all accounts\n"
        "/stop - stop all\n"
        "/status - progress now"
    )


@bot.on(events.NewMessage(from_users=ADMIN_ID, pattern=r"(?i)^/set(@\w+)?(?:\s+(\S+))?$"))
async def h_set(ev):
    global FIXED_TARGET
    arg = ev.pattern_match.group(2)
    if not arg:
        cur = f"{FIXED_TARGET} per account" if FIXED_TARGET else f"random {DAILY_MIN}-{DAILY_MAX}"
        return await ev.respond(f"🎯 Current target: {cur}\nUsage: /set 50  or  /set off")
    if arg.lower() in ("off", "random"):
        FIXED_TARGET = None
        return await ev.respond(f"🎲 Target is random ({DAILY_MIN}-{DAILY_MAX}) from the next /run.")
    if arg.isdigit() and int(arg) > 0:
        FIXED_TARGET = int(arg)
        live = 0
        for a in accs:
            if a.task and not a.task.done():
                a.target = FIXED_TARGET
                live += 1
        note = f" Updated {live} running account(s)." if live else " Applies from the next /run."
        return await ev.respond(f"🎯 Target set: {FIXED_TARGET} replies per account.{note}")
    await ev.respond("Usage: /set 50  or  /set off")


@bot.on(events.NewMessage(from_users=ADMIN_ID, pattern=r"(?i)^/run(@\w+)?$"))
async def h_run(ev):
    global progress_task
    if is_running():
        return await ev.respond("Already running. Use /status or /stop.")
    ready = [a for a in accs if a.ready]
    if not ready:
        return await ev.respond("No working accounts. Check the error alerts.")
    for a in ready:
        a.target = pick_target()
        a.done = a.fails = a.cycles = 0
        a.status = "idle"
        a.task = asyncio.create_task(run_account(a))
    msg = await ev.respond(render())
    progress_task = asyncio.create_task(progress_loop(msg))


@bot.on(events.NewMessage(from_users=ADMIN_ID, pattern=r"(?i)^/stop(@\w+)?$"))
async def h_stop(ev):
    for a in accs:
        if a.task and not a.task.done():
            a.task.cancel()
    await ev.respond("🛑 Stopping all accounts.")


@bot.on(events.NewMessage(from_users=ADMIN_ID, pattern=r"(?i)^/status(@\w+)?$"))
async def h_status(ev):
    await ev.respond(render())


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
    for a in accs:
        await connect(a)
    ok = sum(1 for a in accs if a.ready)
    await bot.send_message(ADMIN_ID, f"✅ Online. {ok}/{len(accs)} accounts ready.\nSend /set 50 then /run.")
    await bot.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())