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
variables set in the hosting dashboard. Never put the token in code,
in git, or in chat. ADMIN_ID is also an environment variable: only
that Telegram user id can open /admin, everyone else is denied.
"""

import os
import re
import secrets
import sys
import glob
import json
import queue
import shutil
import subprocess
import tempfile
import threading
import time
import requests
from flask import Flask, request, jsonify

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
try:
    ADMIN_ID = int(os.environ.get("ADMIN_ID", "0") or 0)
except ValueError:
    ADMIN_ID = 0
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

# --- Statistics: PostgreSQL if DATABASE_URL is set, else JSON -----
# DATABASE_URL comes from the Northflank secret group linked to the
# bot-stats addon; it is never in code or git. If the DB is missing
# or unreachable, stats fall back to the old in-memory + stats.json
# behaviour so a DB outage never breaks downloads. Chat ids are
# stored only to know who to broadcast to; they are never sent
# anywhere except back to the admin panel (as counts, not ids).
DATABASE_URL = os.environ.get("DATABASE_URL", "")
STATS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stats.json")
_stats_lock = threading.Lock()
_stats = {
    "users": [], "starts": 0, "requests": 0,
    "success": 0, "failed": 0, "fast_path": 0, "fallback": 0,
    "broadcasts": 0,
}
_COUNTER_KEYS = ("starts", "requests", "success", "failed", "fast_path", "fallback", "broadcasts")
_db_conn = None
_db_ok = False


def _db_connect():
    """Return a live connection, reconnecting once if it died."""
    global _db_conn, _db_ok
    if not DATABASE_URL:
        return None
    try:
        import psycopg
        if _db_conn is not None:
            try:
                with _db_conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return _db_conn
            except Exception:
                _db_conn = None
        _db_conn = psycopg.connect(DATABASE_URL, autocommit=True, connect_timeout=5)
        with _db_conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS users ("
                "chat_id BIGINT PRIMARY KEY, "
                "first_seen TIMESTAMPTZ DEFAULT now(), "
                "last_seen TIMESTAMPTZ DEFAULT now())"
            )
            cur.execute(
                "CREATE TABLE IF NOT EXISTS counters ("
                "key TEXT PRIMARY KEY, value BIGINT NOT NULL DEFAULT 0)"
            )
            cur.execute(
                "CREATE TABLE IF NOT EXISTS daily_events ("
                "day DATE NOT NULL, key TEXT NOT NULL, value BIGINT NOT NULL DEFAULT 0, "
                "PRIMARY KEY (day, key))"
            )
            cur.execute(
                "CREATE TABLE IF NOT EXISTS shared_videos ("
                "token TEXT PRIMARY KEY, file_id TEXT NOT NULL)"
            )
        _db_ok = True
        return _db_conn
    except Exception as exc:  # noqa: BLE001 - DB must never break the bot
        print(f"db unavailable, using JSON fallback: {exc}", flush=True)
        _db_conn = None
        _db_ok = False
        return None


def _load_stats():
    try:
        with open(STATS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            for key in _stats:
                if key in data:
                    _stats[key] = data[key]
    except Exception:
        pass


def _save_stats():
    try:
        with open(STATS_FILE, "w", encoding="utf-8") as f:
            json.dump(_stats, f, ensure_ascii=False)
    except Exception as exc:  # noqa: BLE001 - stats must never break the bot
        print(f"stats save failed: {exc}", flush=True)


def record_user(chat_id):
    with _stats_lock:
        conn = _db_connect()
        if conn is not None:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO users (chat_id) VALUES (%s) "
                        "ON CONFLICT (chat_id) DO UPDATE SET last_seen = now()",
                        (chat_id,),
                    )
                return
            except Exception as exc:  # noqa: BLE001
                print(f"db record_user failed, using JSON fallback: {exc}", flush=True)
        if chat_id not in _stats["users"]:
            _stats["users"].append(chat_id)
            _save_stats()


def record_stat(key, amount=1):
    if key not in _COUNTER_KEYS:
        return
    with _stats_lock:
        conn = _db_connect()
        if conn is not None:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO counters (key, value) VALUES (%s, %s) "
                        "ON CONFLICT (key) DO UPDATE SET value = counters.value + EXCLUDED.value",
                        (key, amount),
                    )
                    cur.execute(
                        "INSERT INTO daily_events (day, key, value) VALUES (CURRENT_DATE, %s, %s) "
                        "ON CONFLICT (day, key) DO UPDATE SET value = daily_events.value + EXCLUDED.value",
                        (key, amount),
                    )
                return
            except Exception as exc:  # noqa: BLE001
                print(f"db record_stat failed, using JSON fallback: {exc}", flush=True)
        _stats[key] = _stats.get(key, 0) + amount
        _save_stats()


def _db_stats_snapshot():
    """Return (users, counters dict, today dict, last7 dict) or None."""
    conn = _db_connect()
    if conn is None:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM users")
            users = cur.fetchone()[0]
            cur.execute("SELECT key, value FROM counters")
            counters = dict(cur.fetchall())
            cur.execute(
                "SELECT key, value FROM daily_events WHERE day = CURRENT_DATE"
            )
            today = dict(cur.fetchall())
            cur.execute(
                "SELECT key, COALESCE(SUM(value), 0) FROM daily_events "
                "WHERE day >= CURRENT_DATE - INTERVAL '6 days' GROUP BY key"
            )
            last7 = dict(cur.fetchall())
        return users, counters, today, last7
    except Exception as exc:  # noqa: BLE001
        print(f"db stats read failed: {exc}", flush=True)
        return None


def broadcast_user_ids():
    with _stats_lock:
        conn = _db_connect()
        if conn is not None:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT chat_id FROM users")
                    return [row[0] for row in cur.fetchall()]
            except Exception as exc:  # noqa: BLE001
                print(f"db broadcast list failed, using JSON fallback: {exc}", flush=True)
        return list(_stats["users"])


def stats_text():
    with _stats_lock:
        snap = _db_stats_snapshot()
        if snap is not None:
            users, counters, today, last7 = snap
            get = lambda d, k: d.get(k, 0)
            return (
                "📊 Статистика бота (PostgreSQL)\n\n"
                f"👥 Пользователей: {users}\n"
                f"▶️ /start: {get(counters, 'starts')}\n"
                f"🔗 Запросов на скачивание: {get(counters, 'requests')}\n"
                f"✅ Успешно: {get(counters, 'success')}\n"
                f"❌ Не удалось: {get(counters, 'failed')}\n"
                f"⚡ Быстрый путь (tikwm): {get(counters, 'fast_path')}\n"
                f"📥 Запасной путь (yt-dlp/tikwm): {get(counters, 'fallback')}\n"
                f"📢 Рассылок: {get(counters, 'broadcasts')}\n\n"
                f"📅 Сегодня: запросов {get(today, 'requests')}, успешно {get(today, 'success')}\n"
                f"🗓 За 7 дней: запросов {get(last7, 'requests')}, успешно {get(last7, 'success')}"
            )
        users = len(_stats["users"])
        return (
            "📊 Статистика бота (файл, БД недоступна)\n\n"
            f"👥 Пользователей: {users}\n"
            f"▶️ /start: {_stats['starts']}\n"
            f"🔗 Запросов на скачивание: {_stats['requests']}\n"
            f"✅ Успешно: {_stats['success']}\n"
            f"❌ Не удалось: {_stats['failed']}\n"
            f"⚡ Быстрый путь (tikwm): {_stats['fast_path']}\n"
            f"📥 Запасной путь (yt-dlp/tikwm): {_stats['fallback']}\n"
            f"📢 Рассылок: {_stats['broadcasts']}"
        )


_load_stats()

# Admin broadcast mode: when True, the next message from the admin
# (text, photo, video, document, ...) is copied to every known user.
_broadcast_waiting = set()
_broadcast_lock = threading.Lock()


def is_admin(user_id):
    return ADMIN_ID != 0 and user_id == ADMIN_ID


def share_keyboard(token):
    """One inline button under a video: tapping it opens Telegram's
    chat picker with the bot's inline query prefilled. The user then
    taps the video result and the video file itself (not a link, no
    caption) lands in the chosen chat.

    This is the only way an inline button can share media itself:
    a plain url button can only share a link (t.me/share/url), and
    a callback button cannot open Telegram's share sheet at all.
    Requires inline mode enabled for the bot in BotFather, otherwise
    tapping the button does nothing.
    """
    return {"inline_keyboard": [[{"text": "↗️ Поделиться", "switch_inline_query": token}]]}


# token -> Telegram file_id of a video this bot already sent.
# The token is what the share button puts in the inline query, so
# the inline handler can answer with that exact file (cached video).
# Persisted in PostgreSQL when available (so a restart does not
# break old share buttons), with an in-memory fallback.
_share_map = {}


def _register_share(file_id):
    """Store file_id under a short token, return the token."""
    with _stats_lock:
        token = secrets.token_hex(4)
        while token in _share_map:
            token = secrets.token_hex(4)
        _share_map[token] = file_id
        conn = _db_connect()
        if conn is not None:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO shared_videos (token, file_id) VALUES (%s, %s) "
                        "ON CONFLICT (token) DO NOTHING",
                        (token, file_id),
                    )
            except Exception as exc:  # noqa: BLE001
                print(f"db share store failed: {exc}", flush=True)
        return token


def _lookup_share(token):
    if not token:
        return None
    with _stats_lock:
        if token in _share_map:
            return _share_map[token]
        conn = _db_connect()
        if conn is not None:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT file_id FROM shared_videos WHERE token = %s", (token,))
                    row = cur.fetchone()
                if row:
                    _share_map[token] = row[0]
                    return row[0]
            except Exception as exc:  # noqa: BLE001
                print(f"db share lookup failed: {exc}", flush=True)
        return None


def admin_keyboard():
    return {"inline_keyboard": [
        [{"text": "📊 Статистика", "callback_data": "admin_stats"}],
        [{"text": "📢 Рассылка", "callback_data": "admin_broadcast"}],
        [{"text": "🖥 Нода", "callback_data": "admin_node"}],
    ]}


def _read_int_file(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            value = f.read().strip()
        return None if value in ("max", "") else int(value)
    except Exception:
        return None


def _fmt_bytes(n):
    if n is None:
        return "?"
    for unit in ("Б", "КБ", "МБ", "ГБ"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "Б" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} ТБ"


def _fmt_uptime(seconds):
    seconds = int(seconds)
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days:
        return f"{days} д {hours} ч {minutes} мин"
    if hours:
        return f"{hours} ч {minutes} мин"
    return f"{minutes} мин {seconds} сек"


_START_TIME = time.time()


def node_stats_text():
    """Stats of the container the bot actually runs in.

    Stdlib only, no psutil: cgroup v2 files give the real container
    limits (a plain /proc/meminfo would show the whole host, which
    would lie about a 256 MB service). Falls back gracefully when
    a file is missing (cgroup v1 paths, or no cgroup at all).
    """
    import platform
    import socket

    mem_used = _read_int_file("/sys/fs/cgroup/memory.current")
    if mem_used is None:
        mem_used = _read_int_file("/sys/fs/cgroup/memory/memory.usage_in_bytes")
    mem_limit = _read_int_file("/sys/fs/cgroup/memory.max")
    if mem_limit is None:
        mem_limit = _read_int_file("/sys/fs/cgroup/memory/memory.limit_in_bytes")
    if mem_limit is None or mem_used is None:
        # No cgroup (local run, not Northflank): fall back to host
        # meminfo. In the Northflank container the cgroup branch
        # above wins, so this never hides the real 256 MB limit.
        try:
            total = avail = None
            with open("/proc/meminfo", "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        total = int(line.split()[1]) * 1024
                    elif line.startswith("MemAvailable:"):
                        avail = int(line.split()[1]) * 1024
            if total is not None:
                mem_limit = mem_limit or total
                mem_used = mem_used or (total - avail if avail is not None else None)
        except Exception:
            pass
    proc_rss = None
    proc_cpu_sec = None
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    proc_rss = int(line.split()[1]) * 1024
                    break
    except Exception:
        pass
    try:
        with open("/proc/self/stat", "r", encoding="utf-8") as f:
            parts = f.read().rsplit(")", 1)[1].split()
            # fields after comm: utime is #14 overall, #12 after comm
            utime, stime = int(parts[11]), int(parts[12])
            proc_cpu_sec = (utime + stime) / os.sysconf("SC_CLK_TCK")
    except Exception:
        pass
    try:
        load1, load5, load15 = os.getloadavg()
        load_text = f"{load1:.2f} / {load5:.2f} / {load15:.2f}"
    except Exception:
        load_text = "?"
    try:
        disk = shutil.disk_usage("/")
        disk_text = f"{_fmt_bytes(disk.used)} / {_fmt_bytes(disk.total)} ({disk.used * 100 // disk.total}%)"
    except Exception:
        disk_text = "?"
    uptime = time.time() - _START_TIME
    mem_pct = f" ({mem_used * 100 // mem_limit}%)" if mem_used and mem_limit else ""
    lines = [
        "🖥 Нода (контейнер бота)",
        "",
        f"🆔 Под: {socket.gethostname()}",
        f"🐍 Python: {platform.python_version()}",
        f"⏱ Аптайм процесса: {_fmt_uptime(uptime)}",
        f"🧠 Память контейнера: {_fmt_bytes(mem_used)} / {_fmt_bytes(mem_limit)}{mem_pct}",
        f"🧠 Память процесса (RSS): {_fmt_bytes(proc_rss)}",
    ]
    if proc_cpu_sec is not None:
        if uptime >= 10:
            lines.append(
                f"⚙️ CPU процесса: {proc_cpu_sec:.1f} сек "
                f"(в среднем {proc_cpu_sec / uptime * 100:.1f}% за жизнь процесса)"
            )
        else:
            # Uptime is shorter than the CPU time already spent on
            # interpreter startup before _START_TIME was set, so an
            # average here would be absurd (thousands of percent).
            lines.append(f"⚙️ CPU процесса: {proc_cpu_sec:.1f} сек")
    lines += [
        f"📈 Load average (1/5/15 мин): {load_text}",
        f"💾 Диск /: {disk_text}",
        f"📥 Очередь скачиваний: {job_queue.qsize()}",
        f"🧵 Потоков: {threading.active_count()}",
        f"🗄 База: {'PostgreSQL' if DATABASE_URL else 'нет (JSON-режим)'}",
    ]
    return "\n".join(lines)


def admin_back_keyboard():
    return {"inline_keyboard": [
        [{"text": "⬅️ Назад в панель", "callback_data": "admin_back"}],
    ]}


def tg_send_message(chat_id, text, reply_markup=None):
    if not TG_API:
        print("No BOT_TOKEN set, cannot send message", flush=True)
        return
    try:
        payload = {"chat_id": chat_id, "text": text}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        requests.post(f"{TG_API}/sendMessage", json=payload, timeout=15)
    except Exception as exc:  # noqa: BLE001 - worker must never die
        print(f"sendMessage failed: {exc}", flush=True)


def tg_answer_callback(callback_id, text="", show_alert=False):
    if not TG_API:
        return
    try:
        requests.post(
            f"{TG_API}/answerCallbackQuery",
            json={"callback_query_id": callback_id, "text": text, "show_alert": show_alert},
            timeout=15,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"answerCallbackQuery failed: {exc}", flush=True)


def tg_edit_message(chat_id, message_id, text, reply_markup=None):
    if not TG_API:
        return
    try:
        payload = {"chat_id": chat_id, "message_id": message_id, "text": text}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        requests.post(f"{TG_API}/editMessageText", json=payload, timeout=15)
    except Exception as exc:  # noqa: BLE001
        print(f"editMessageText failed: {exc}", flush=True)


def tg_edit_reply_markup(chat_id, message_id, reply_markup):
    """Attach (or replace) the inline keyboard of a sent message."""
    if not TG_API:
        return
    try:
        requests.post(
            f"{TG_API}/editMessageReplyMarkup",
            json={"chat_id": chat_id, "message_id": message_id, "reply_markup": reply_markup},
            timeout=15,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"editMessageReplyMarkup failed: {exc}", flush=True)


def tg_answer_inline_query(inline_query_id, results):
    if not TG_API:
        return
    try:
        requests.post(
            f"{TG_API}/answerInlineQuery",
            json={"inline_query_id": inline_query_id, "results": results,
                  "cache_time": 0, "is_personal": True},
            timeout=15,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"answerInlineQuery failed: {exc}", flush=True)


def tg_copy_message(chat_id, from_chat_id, message_id):
    """Copy (not forward) a message to one user. Returns True on 200."""
    if not TG_API:
        return False
    try:
        resp = requests.post(
            f"{TG_API}/copyMessage",
            json={"chat_id": chat_id, "from_chat_id": from_chat_id, "message_id": message_id},
            timeout=30,
        )
        if resp.status_code == 429:
            try:
                wait = resp.json().get("parameters", {}).get("retry_after", 1)
            except Exception:  # noqa: BLE001
                wait = 1
            time.sleep(min(wait, 60))
            return tg_copy_message(chat_id, from_chat_id, message_id)
        return resp.status_code == 200
    except Exception as exc:  # noqa: BLE001
        print(f"copyMessage failed: {exc}", flush=True)
        return False


def _attach_share_button(chat_id, resp):
    """After a video is sent, attach the share button to it.

    The button needs the video's Telegram file_id, which only
    exists after sendVideo succeeds, so it cannot be attached in
    the send call itself: send first, then edit the markup. The
    button carries a short token, not the file_id (file_ids are
    long and must not be exposed in a keyboard).
    """
    try:
        result = resp.json().get("result") or {}
        file_id = (result.get("video") or {}).get("file_id")
        message_id = result.get("message_id")
        if file_id and message_id:
            tg_edit_reply_markup(chat_id, message_id, share_keyboard(_register_share(file_id)))
    except Exception as exc:  # noqa: BLE001
        print(f"attach share button failed: {exc}", flush=True)


def tg_send_video_url(chat_id, video_url, caption, source_url=None):
    """Ask Telegram itself to fetch the video from a direct URL.

    This is the fast path: no download to Koyeb and no re-upload
    from a 0.1 vCPU instance, so a video arrives in seconds instead
    of minutes. Returns False if Telegram cannot fetch the URL,
    in which case the caller falls back to download + upload.
    """
    if not TG_API:
        return False
    try:
        payload = {"chat_id": chat_id, "video": video_url, "caption": caption[:1024]}
        resp = requests.post(f"{TG_API}/sendVideo", json=payload, timeout=60)
        if resp.status_code == 200:
            _attach_share_button(chat_id, resp)
            return True
        print(f"sendVideo-by-url failed {resp.status_code}: {resp.text[:300]}", flush=True)
        return False
    except Exception as exc:  # noqa: BLE001
        print(f"sendVideo-by-url failed: {exc}", flush=True)
        return False


def tg_send_video(chat_id, path, caption, source_url=None):
    if not TG_API:
        return False
    try:
        with open(path, "rb") as f:
            data = {"chat_id": chat_id, "caption": caption[:1024]}
            files = {"video": (os.path.basename(path), f)}
            resp = requests.post(
                f"{TG_API}/sendVideo",
                data=data,
                files=files,
                timeout=120,
            )
        if resp.status_code == 200:
            _attach_share_button(chat_id, resp)
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
            return tg_send_video(chat_id, path, title or "Готово ✅", source_url=url)
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


def _do_broadcast(job):
    admin_id = job["admin_chat_id"]
    from_chat_id = job["from_chat_id"]
    message_id = job["message_id"]
    targets = [u for u in broadcast_user_ids() if u != admin_id]
    tg_send_message(admin_id, f"📢 Рассылка началась. Получателей: {len(targets)}")
    sent = failed = 0
    for target in targets:
        if tg_copy_message(target, from_chat_id, message_id):
            sent += 1
        else:
            failed += 1
        time.sleep(0.05)
    record_stat("broadcasts")
    tg_send_message(admin_id, f"📢 Рассылка завершена.\n✅ Отправлено: {sent}\n❌ Не доставлено: {failed}")


def process_job(job):
    if job.get("type") == "broadcast":
        _do_broadcast(job)
        return

    chat_id = job["chat_id"]
    text = job.get("text", "") or ""
    record_user(chat_id)

    if text.strip() in ("/start", "/help"):
        record_stat("starts")
        tg_send_message(
            chat_id,
            "Пришли ссылку на видео из TikTok — я скачаю его без водяного знака и пришлю сюда.",
        )
        return

    match = TIKTOK_RE.search(text)
    if not match:
        tg_send_message(chat_id, "Это не похоже на ссылку на TikTok. Пришли ссылку на видео.")
        return

    record_stat("requests")
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
            if tg_send_video_url(chat_id, play_url, title or "Готово ✅", source_url=url):
                record_stat("success")
                record_stat("fast_path")
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
                record_stat("success")
                record_stat("fallback")
                return
            record_stat("failed")
            tg_send_message(chat_id, "Не получилось скачать это видео. Возможно, оно приватное, удалено или слишком большое (>50 МБ).")
            return

        path = files[0]
        if os.path.getsize(path) > MAX_BYTES:
            record_stat("failed")
            tg_send_message(chat_id, "Видео больше 50 МБ — Telegram не примет его через Bot API.")
            return

        if not tg_send_video(chat_id, path, "Готово ✅", source_url=url):
            record_stat("failed")
            tg_send_message(chat_id, "Скачал, но не смог отправить видео в Telegram. Попробуй ещё раз.")
        else:
            record_stat("success")
            record_stat("fallback")
    except subprocess.TimeoutExpired:
        record_stat("failed")
        tg_send_message(chat_id, "Скачивание заняло слишком долго, попробуй ещё раз.")
    except Exception as exc:  # noqa: BLE001
        record_stat("failed")
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

    # --- Inline mode: share button -> video file itself ------------
    # Tapping "Поделиться" opens the chat picker with this query
    # (the share token). Answer with the cached video: the user
    # taps the result and the video, with no link and no caption,
    # lands in the chosen chat.
    inline = update.get("inline_query") or {}
    if inline:
        token = (inline.get("query") or "").strip()
        file_id = _lookup_share(token)
        results = []
        if file_id:
            results = [{"type": "video", "id": token, "video_file_id": file_id,
                        "title": "Видео"}]
        tg_answer_inline_query(inline.get("id", ""), results)
        return jsonify({"ok": True}), 200

    # --- Inline buttons (admin panel) -------------------------------
    callback = update.get("callback_query") or {}
    if callback:
        user = callback.get("from") or {}
        cb_chat = (callback.get("message") or {}).get("chat") or {}
        cb_message_id = (callback.get("message") or {}).get("message_id")
        data = callback.get("data", "") or ""
        if not is_admin(user.get("id")):
            tg_answer_callback(callback.get("id", ""), "Нет доступа.", show_alert=True)
            return jsonify({"ok": True}), 200
        tg_answer_callback(callback.get("id", ""))
        if data == "admin_stats":
            tg_edit_message(cb_chat.get("id"), cb_message_id, stats_text(), reply_markup=admin_back_keyboard())
        elif data == "admin_node":
            tg_edit_message(cb_chat.get("id"), cb_message_id, node_stats_text(), reply_markup=admin_back_keyboard())
        elif data == "admin_broadcast":
            with _broadcast_lock:
                _broadcast_waiting.add(cb_chat.get("id"))
            tg_edit_message(
                cb_chat.get("id"), cb_message_id,
                "📢 Рассылка\n\nПришли следующее сообщение (текст, фото, видео — что угодно), и я разошлю его копию всем пользователям бота.\n\n/cancel — отменить.",
                reply_markup={"inline_keyboard": [[{"text": "❌ Отменить рассылку", "callback_data": "admin_cancel"}]]},
            )
        elif data == "admin_back":
            with _broadcast_lock:
                _broadcast_waiting.discard(cb_chat.get("id"))
            tg_edit_message(cb_chat.get("id"), cb_message_id, "🛠 Админ-панель", reply_markup=admin_keyboard())
        elif data == "admin_cancel":
            with _broadcast_lock:
                _broadcast_waiting.discard(cb_chat.get("id"))
            tg_edit_message(cb_chat.get("id"), cb_message_id, "Рассылка отменена.", reply_markup=admin_back_keyboard())
        return jsonify({"ok": True}), 200

    message = update.get("message") or {}
    chat = message.get("chat") or {}
    if chat.get("id") is not None:
        text = message.get("text", "") or ""
        sender = (message.get("from") or {}).get("id")
        record_user(chat["id"])
        # Instant replies bypass the download queue entirely.
        if text.strip() in ("/start", "/help"):
            record_stat("starts")
            tg_send_message(
                chat["id"],
                "Пришли ссылку на видео из TikTok — я скачаю его без водяного знака и пришлю сюда.",
            )
        elif text.strip() == "/admin":
            if is_admin(sender):
                tg_send_message(chat["id"], "🛠 Админ-панель", reply_markup=admin_keyboard())
            else:
                tg_send_message(chat["id"], "Нет доступа.")
        elif text.strip() == "/cancel":
            with _broadcast_lock:
                waiting = chat["id"] in _broadcast_waiting
                _broadcast_waiting.discard(chat["id"])
            tg_send_message(chat["id"], "Рассылка отменена." if waiting else "Отменять нечего.")
        elif is_admin(sender) and chat["id"] in _broadcast_waiting:
            # Admin's next message becomes the broadcast. Queue it so
            # the webhook still returns 200 immediately.
            with _broadcast_lock:
                _broadcast_waiting.discard(chat["id"])
            job_queue.put({
                "type": "broadcast",
                "admin_chat_id": chat["id"],
                "from_chat_id": chat["id"],
                "message_id": message.get("message_id"),
            })
            tg_send_message(chat["id"], "Принято, запускаю рассылку…")
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
