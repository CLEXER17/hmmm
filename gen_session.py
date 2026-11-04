import os, re
from telethon.sync import TelegramClient
from telethon.sessions import StringSession


def load_env(path=".env"):
    if os.path.exists(path):
        for line in open(path, encoding="utf-8"):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


load_env()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")

if not API_ID or not API_HASH:
    API_ID = input("API_ID: ").strip()
    API_HASH = input("API_HASH: ").strip()
    with open(".env", "a", encoding="utf-8") as f:
        f.write(f"\nAPI_ID={API_ID}\nAPI_HASH={API_HASH}\n")
    print("Saved to .env, you won't be asked again.")

label = input("Account name (e.g. CLEXEER): ").strip().upper().replace(" ", "_")
label = re.sub(r"[^A-Z0-9_]", "", label) or "ACCOUNT"
base = f"SESSION_{label}"

existing = []
if os.path.exists("sessions.txt"):
    for line in open("sessions.txt", encoding="utf-8"):
        k = line.split("=", 1)[0].strip()
        if k == base or k.startswith(base + "__"):
            existing.append(k)

var = base if not existing else f"{base}__{len(existing) + 1}"
if existing:
    print(f"{len(existing)} string(s) already saved for {label}. Saving this one as {var}.")

with TelegramClient(StringSession(), int(API_ID), API_HASH) as client:
    s = client.session.save()

print(f"\nPaste this into Railway Variables:\n\n{var}={s}\n")
print(f"Length: {len(s)}")

with open("sessions.txt", "a", encoding="utf-8") as f:
    f.write(f"{var}={s}\n")
print("Also appended to sessions.txt")
