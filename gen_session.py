from telethon.sync import TelegramClient
from telethon.sessions import StringSession

API_ID = int(input("API_ID: "))
API_HASH = input("API_HASH: ")

with TelegramClient(StringSession(), API_ID, API_HASH) as client:
    print("\nSESSION STRING:\n")
    print(client.session.save())
