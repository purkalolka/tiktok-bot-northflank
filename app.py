"""
TikTok downloader bot for Koyeb Hobby (free) — webhook mode.

Why this shape, and how it beats scale-to-zero:
  * Telegram sends an update to /webhook. We answer 200 OK *immediately*
    and put the job in a queue. Telegram never waits for the download,
    so there are no timeouts and no duplicate retries.
  * One background worker thread downloads and sends, strictly one
    video at a time. On 512 MB / 0.1 vCPU this is what keeps the
    process from being OOM-killed by parallel downloads.
  * The service also exposes /health. An external pinger (UptimeRobot,
    every 5 min) hitting /health counts as inbound traffic for Koyeb,
    so the free instance never reaches the 1-hour scale-to-zero idle
    window. Even without the pinger, a webhook wakes the instance in
    1-5 seconds — the queue then runs normally.
  * No database, no Docker, no ffmpeg merge. TikTok mp4 is normally a
    single file, so yt-dlp downloads it as-is. Files live only in a
    temp dir and are deleted right after sending.

Secrets: BOT_TOKEN and WEBHOOK_SECRET come ONLY from environment
variables set in the Koyeb dashboard. Never put the token in code,
in git, or in chat.
"""

import os
import re
import sys
import glob
import queue
import shutil
import subprocess
import tempfile
import threading

import requests
from flask import Flask, request, jsonify

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""

# 48 MB: Telegram Bot API will not accept a video over 50 MB anyway,
# so there is no point downloading a bigger file into 512 MB / 2 GB.
MAX_BYTES = 48 * 1024 * 1024

TIKTOK_RE = re.compile(
    r"https?://(?:www\.|m\.|vm\.|vt\.)?tiktok\.com/[^\s]+",
    re.IGNORECASE,
)

app = Flask(__name__)
job_queue: "queue.Queue[dict]" = queue.Queue()
_seen_updates = set()
_seen_lock = threading.Lock()


def tg_send_message(chat_id, text):
    if not TG_API:
        print("No BOT_TOKEN set, cannot send message", flush=True)
        return
    try:
        requests.post(
            f"{TG_API}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=15,
        )
    except Exception as exc:  # noqa: BLE001 - worker must never die
        print(f"sendMessage failed: {exc}", flush=True)


def tg_send_video_url(chat_id, video_url, caption):
    """Ask Telegram itself to fetch the video from a direct URL.

    This is the fast path: no download to Koyeb and no re-upload
    from a 0.1 vCPU instance, so a video arrives in seconds instead
    of minutes. Returns False if Telegram cannot fetch the URL,
    in which case the caller falls back to download + upload.
    """
    if not TG_API:
        return False
    try:
        resp = requests.post(
            f"{TG_API}/sendVideo",
            json={"chat_id": chat_id, "video": video_url, "caption": caption[:1024]},
            timeout=60,
        )
        if resp.status_code == 200:
            return True
        print(f"sendVideo-by-url failed {resp.status_code}: {resp.text[:300]}", flush=True)
        return False
    except Exception as exc:  # noqa: BLE001
        print(f"sendVideo-by-url failed: {exc}", flush=True)
        return False


def tg_send_video(chat_id, path, caption):
    if not TG_API:
        return False
    try:
        with open(path, "rb") as f:
            resp = requests.post(
                f"{TG_API}/sendVideo",
                data={"chat_id": chat_id, "caption": caption[:1024]},
                files={"video": (os.path.basename(path), f)},
                timeout=120,
            )
        if resp.status_code == 200:
            return True
        print(f"sendVideo failed {resp.status_code}: {resp.text[:300]}", flush=True)
        return False
    except Exception as exc:  # noqa: BLE001
        print(f"sendVideo failed: {exc}", flush=True)
        return False


def download_file(url, dest):
    """Stream-download a file, aborting over MAX_BYTES. Returns True on success."""
    try:
        with requests.get(url, stream=True, timeout=90,
                          headers={"User-Agent": "Mozilla/5.0"}) as resp:
            if resp.status_code != 200:
                print(f"download_file HTTP {resp.status_code} for {url[:80]}", flush=True)
                return False
            size = 0
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 256):
                    if not chunk:
                        continue
                    size += len(chunk)
                    if size > MAX_BYTES:
                        return False
                    f.write(chunk)
        return os.path.getsize(dest) > 0
    except Exception as exc:  # noqa: BLE001
        print(f"download_file failed: {exc}", flush=True)
        return False


CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)


def resolve_url(url):
    """Resolve vm.tiktok.com / vt.tiktok.com short links to a full URL.

    tikwm (and sometimes yt-dlp) chokes on short links, returning an
    empty / non-JSON body. Following the redirect first gives both
    tools the canonical https://www.tiktok.com/@user/video/ID form.
    """
    if "vm.tiktok.com" not in url and "vt.tiktok.com" not in url:
        return url
    try:
        resp = requests.get(url, headers={"User-Agent": CHROME_UA},
                            timeout=10, allow_redirects=True)
        if resp.url and "tiktok.com" in resp.url:
            print(f"resolved short link -> {resp.url[:120]}", flush=True)
            return resp.url
    except Exception as exc:  # noqa: BLE001
        print(f"short link resolve failed: {exc}", flush=True)
    return url


def get_tikwm_data(url):
    """Call tikwm, but never let a bad response look like a mystery.

    The old code did resp.json() blindly, so an HTML challenge page
    or an empty body from a blocked datacenter IP surfaced only as
    'Expecting value: line 1 column 1 (char 0)' with no clue why.
    Now status + first 200 chars of the body are logged, so the
    next log line actually tells whether it was 403, 429, or empty.
    """
    headers = {"User-Agent": CHROME_UA, "Accept": "application/json",
               "Referer": "https://www.tikwm.com/"}
    for attempt in range(2):
        try:
            resp = requests.get(
                "https://www.tikwm.com/api/",
                params={"url": url},
                headers=headers,
                timeout=10,
            )
            if resp.status_code != 200:
                print(f"tikwm HTTP {resp.status_code}: {resp.text[:200]!r}", flush=True)
                return {}
            try:
                return resp.json().get("data") or {}
            except Exception as exc:  # noqa: BLE001
                print(f"tikwm bad json: {exc} body={resp.text[:200]!r}", flush=True)
                return {}
        except Exception as exc:  # noqa: BLE001
            print(f"tikwm api failed (attempt {attempt + 1}): {exc}", flush=True)
            if attempt == 0:
                import time as _time
                _time.sleep(1)
    return {}


def try_tikwm(chat_id, url, tmpdir, data=None):
    """
    Fallback when yt-dlp is blocked by TikTok on a datacenter IP.
    tikwm resolves the video on its side and returns a direct
    no-watermark play URL from tiktokcdn, which the bot then
    downloads like any normal file. Returns True if a video
    (or slideshow) was sent.
    """
    if data is None:
        data = get_tikwm_data(url)
    if not data:
        return False

    play = data.get("play") or data.get("wmplay") or ""
    if play:
        path = os.path.join(tmpdir, "video.mp4")
        if download_file(play, path):
            title = (data.get("title") or "").strip()
            return tg_send_video(chat_id, path, title or "Готово ✅")
        print("tikwm play download failed or too large", flush=True)
        return False

    images = data.get("images") or []
    if images:
        # Photo slideshow: send as one media group, up to 10 photos.
        media = [{"type": "photo", "media": img} for img in images[:10]]
        try:
            r = requests.post(
                f"{TG_API}/sendMediaGroup",
                json={"chat_id": chat_id, "media": media},
                timeout=120,
            )
            return r.status_code == 200
        except Exception as exc:  # noqa: BLE001
            print(f"sendMediaGroup failed: {exc}", flush=True)
            return False
    return False


def process_job(job):
    chat_id = job["chat_id"]
    text = job.get("text", "") or ""

    if text.strip() in ("/start", "/help"):
        tg_send_message(
            chat_id,
            "Пришли ссылку на видео из TikTok — я скачаю его без водяного знака и пришлю сюда.",
        )
        return

    match = TIKTOK_RE.search(text)
    if not match:
        tg_send_message(chat_id, "Это не похоже на ссылку на TikTok. Пришли ссылку на видео.")
        return

    url = resolve_url(match.group(0))
    # NOTE: "Качаю…" is already sent by the webhook handler, so the
    # user sees it immediately even while a previous download is
    # still running in the worker. Do not send it again here.
    tmpdir = tempfile.mkdtemp(prefix="tiktok_")
    try:
        # Fast path first: let Telegram fetch the direct tiktokcdn
        # URL itself. This skips the slow download + re-upload
        # through a 0.1 vCPU instance entirely.
        tikwm_data = get_tikwm_data(url)
        play_url = (tikwm_data.get("play") or tikwm_data.get("wmplay") or "") if tikwm_data else ""
        if play_url:
            title = (tikwm_data.get("title") or "").strip()
            if tg_send_video_url(chat_id, play_url, title or "Готово ✅"):
                return
            print("sendVideo-by-url failed, falling back to download", flush=True)
        out_tpl = os.path.join(tmpdir, "%(id)s.%(ext)s")
        # Single-file mp4 first: avoids an ffmpeg merge step that
        # spikes RAM/CPU on a 0.1 vCPU free instance.
        # --impersonate chrome makes yt-dlp look like a real Chrome
        # browser to TikTok; without it datacenter IPs get a
        # 403 / "Unexpected response" page. Needs curl_cffi,
        # which is in requirements.txt; if it is missing, yt-dlp
        # only warns and still tries.
        cmd = [
            sys.executable, "-m", "yt_dlp",
            "--impersonate", "chrome",
            "-f", "best[ext=mp4]/best",
            "--no-playlist",
            "--max-filesize", "48M",
            "--no-warnings",
            "-o", out_tpl,
            url,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
        files = sorted(glob.glob(os.path.join(tmpdir, "*")))
        if result.returncode != 0 or not files:
            print(f"yt-dlp failed: {result.stderr[-500:]}", flush=True)
            # Fallback: download the tikwm URL through Koyeb
            # (reuses the API result from the fast path above).
            tg_send_message(chat_id, "yt-dlp не справился, пробую запасной способ…")
            if try_tikwm(chat_id, url, tmpdir, data=tikwm_data):
                return
            tg_send_message(chat_id, "Не получилось скачать это видео. Возможно, оно приватное, удалено или слишком большое (>50 МБ).")
            return

        path = files[0]
        if os.path.getsize(path) > MAX_BYTES:
            tg_send_message(chat_id, "Видео больше 50 МБ — Telegram не примет его через Bot API.")
            return

        if not tg_send_video(chat_id, path, "Готово ✅"):
            tg_send_message(chat_id, "Скачал, но не смог отправить видео в Telegram. Попробуй ещё раз.")
    except subprocess.TimeoutExpired:
        tg_send_message(chat_id, "Скачивание заняло слишком долго, попробуй ещё раз.")
    except Exception as exc:  # noqa: BLE001
        print(f"job failed: {exc}", flush=True)
        tg_send_message(chat_id, "Что-то пошло не так при скачивании. Попробуй ещё раз.")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _worker():
    while True:
        job = job_queue.get()
        try:
            process_job(job)
        except Exception as exc:  # noqa: BLE001 - worker must never die
            print(f"worker error: {exc}", flush=True)
        finally:
            job_queue.task_done()


threading.Thread(target=_worker, daemon=True).start()


@app.route("/", methods=["GET"])
def index():
    return jsonify({"ok": True, "service": "tiktok-bot"}), 200


@app.route("/health", methods=["GET"])
def health():
    # Koyeb health check + UptimeRobot pinger target.
    return jsonify({"ok": True}), 200


@app.route("/webhook", methods=["POST"])
def webhook():
    # Optional Telegram secret-token check (set WEBHOOK_SECRET in Koyeb
    # and pass the same value as secret_token when calling setWebhook).
    if WEBHOOK_SECRET:
        if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
            return jsonify({"ok": False}), 403

    update = request.get_json(silent=True) or {}
    update_id = update.get("update_id")
    if update_id is not None:
        with _seen_lock:
            if update_id in _seen_updates:
                return jsonify({"ok": True}), 200
            _seen_updates.add(update_id)
            if len(_seen_updates) > 1000:
                _seen_updates.clear()

    message = update.get("message") or {}
    chat = message.get("chat") or {}
    if chat.get("id") is not None:
        text = message.get("text", "") or ""
        # Instant replies bypass the download queue entirely.
        # Previously /start was queued behind a slow download, so
        # it only arrived after the download finished (or looked
        # lost) — that is why /start answered "every other time"
        # even though CPU/RAM were idle. The webhook runs on
        # gunicorn threads, so this send does not wait for the
        # single download worker.
        if text.strip() in ("/start", "/help"):
            tg_send_message(
                chat["id"],
                "Пришли ссылку на видео из TikTok — я скачаю его без водяного знака и пришлю сюда.",
            )
        elif not TIKTOK_RE.search(text):
            tg_send_message(chat["id"], "Это не похоже на ссылку на TikTok. Пришли ссылку на видео.")
        else:
            queued = job_queue.qsize()
            if queued:
                tg_send_message(chat["id"], f"Качаю… (в очереди перед тобой: {queued})")
            else:
                tg_send_message(chat["id"], "Качаю…")
            # Return 200 BEFORE any download: this is the whole point.
            job_queue.put({"chat_id": chat["id"], "text": text})
    return jsonify({"ok": True}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, threaded=True)
