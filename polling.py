"""Polling entrypoint for hosts without a webhook (FadeHost free tier).

Reuses the webhook bot in app.py: importing app starts the download
worker thread, and each getUpdates result is fed to app.handle_update,
so /start, links, the admin panel, broadcasts and the inline share
button all behave exactly as in webhook mode.

Env vars (set in the hosting dashboard, never in code):
  BOT_TOKEN - Telegram bot token (required)
  ADMIN_ID  - Telegram user id allowed to open /admin

On start it deletes any webhook and drops the pending backlog: the
old webhook URL is dead, and stale queued jobs would only confuse
users. No HTTP server is started; there is nothing to keep awake.
"""

import os
import sys
import time

import requests

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
if not BOT_TOKEN:
    print("BOT_TOKEN is not set, exiting", flush=True)
    sys.exit(1)

# Import AFTER the token check: app.py builds TG_API from BOT_TOKEN
# at import time and starts the download worker thread.
import app  # noqa: E402

TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"


def main():
    try:
        resp = requests.post(
            f"{TG_API}/deleteWebhook",
            json={"drop_pending_updates": True},
            timeout=20,
        )
        print(f"deleteWebhook: {resp.status_code} {resp.text[:200]}", flush=True)
    except Exception as exc:  # noqa: BLE001 - poller must keep going
        print(f"deleteWebhook failed: {exc}", flush=True)

    offset = None
    backoff = 1
    while True:
        try:
            params = {
                "timeout": 30,
                "allowed_updates": ["message", "callback_query", "inline_query"],
            }
            if offset is not None:
                params["offset"] = offset
            resp = requests.get(f"{TG_API}/getUpdates", params=params, timeout=45)
            if resp.status_code == 409:
                # Another getUpdates loop or a webhook is still
                # holding the bot. Wait, do not crash-loop.
                print("getUpdates 409 (bot busy elsewhere), waiting 10s", flush=True)
                time.sleep(10)
                continue
            resp.raise_for_status()
            backoff = 1
            for update in resp.json().get("result", []):
                offset = update["update_id"] + 1
                try:
                    app.handle_update(update)
                except Exception as exc:  # noqa: BLE001 - one bad update must not kill the poller
                    print(f"handle_update failed: {exc}", flush=True)
        except requests.exceptions.RequestException as exc:
            print(f"getUpdates error: {exc}; retry in {backoff}s", flush=True)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


if __name__ == "__main__":
    main()
