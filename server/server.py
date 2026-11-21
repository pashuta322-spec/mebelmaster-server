#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
МебельМастер — сервер заявок + Telegram-бот.
Только стандартная библиотека Python 3.10+. Никакого pip, никаких зависимостей.

Запуск:      python server.py
Диагностика: python server.py --check
"""
from __future__ import annotations

import csv
import hashlib
import html
import io
import json
import logging
import mimetypes
import os
import re
import socket
import sqlite3
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

BASE = Path(__file__).resolve().parent
STOP = threading.Event()
DB_LOCK = threading.RLock()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("mm")

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


# ============================== НАСТРОЙКИ ==============================
def load_env(path: Path) -> dict[str, str]:
    data: dict[str, str] = {}
    if not path.exists():
        return data
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        data[key.strip()] = val.strip().strip('"').strip("'")
    return data


ENV = load_env(BASE / ".env")


def env(key: str, default: str = "") -> str:
    return (os.environ.get(key) or ENV.get(key) or default).strip()


def env_int(key: str, default: int) -> int:
    try:
        return int(env(key, str(default)) or default)
    except ValueError:
        return default


def env_ids(key: str) -> list[int]:
    out: list[int] = []
    for chunk in env(key).replace(";", ",").split(","):
        chunk = chunk.strip()
        if chunk.lstrip("-").isdigit():
            out.append(int(chunk))
    return out


class S:
    bot_token = env("BOT_TOKEN")
    admin_ids = env_ids("ADMIN_IDS")
    tg_proxy = env("TG_PROXY")
    tg_api = env("TG_API_BASE", "https://api.telegram.org").rstrip("/")
    tg_verify = env("TG_VERIFY_SSL", "1") != "0"

    host = env("HOST", "127.0.0.1") or "127.0.0.1"
    port = env_int("PORT", 8000)
    admin_key = env("ADMIN_KEY")

    site_dir = (BASE / env("SITE_DIR", "../site")).resolve()
    db_path = (BASE / env("DB_PATH", "leads.db")).resolve()

    tz = env_int("TZ_OFFSET", 3)
    digest_hour = env_int("DIGEST_HOUR", 20)
    rate_limit = env_int("RATE_LIMIT", 5)
    min_fill = env_int("MIN_FILL_SECONDS", 3)
    cors = [x.strip() for x in env(
        "CORS_ORIGINS",
        "http://127.0.0.1:8000,http://localhost:8000,http://127.0.0.1:5500,http://localhost:5500"
    ).split(",") if x.strip()]

    @classmethod
    def telegram_ready(cls) -> bool:
        return bool(cls.bot_token and cls.admin_ids)


# ============================== УТИЛИТЫ ==============================
STATUSES = ("new", "work", "done")
STATUS_RU = {"new": "Новая", "work": "В работе", "done": "Закрыта"}
STATUS_BADGE = {"new": "🆕 НОВАЯ", "work": "⚙️ В РАБОТЕ", "done": "✅ ЗАКРЫТА"}

TYPES_RU = {"kitchen": "Кухня", "wardrobe": "Шкаф-купе", "dressing": "Гардеробная", "tv": "ТВ-зона"}
MATERIALS_RU = {"ldsp": "ЛДСП Egger", "enamel": "МДФ эмаль", "veneer": "Шпон дуба", "acryl": "Акрил глянец"}
TONES_RU = {"oak": "Дуб натуральный", "walnut": "Орех тёмный", "graphite": "Графит",
            "cream": "Кремовая эмаль", "sage": "Шалфей", "terra": "Терракота"}
HANDLES_RU = {"gola": "Gola-профиль", "bar": "Ручка-скоба", "push": "Push-to-open",
              "integrated": "Интегрированные"}
TOPS_RU = {"ldsp": "ЛДСП", "stone": "Акриловый камень", "solid": "Массив дуба"}
HW_RU = {"eco": "Эконом (Boyard)", "std": "Стандарт (Hettich)", "pro": "Премиум (Blum)"}


def money(n) -> str:
    try:
        return f"{int(n):,}".replace(",", " ") + " ₽"
    except (TypeError, ValueError):
        return "0 ₽"


def e(text) -> str:
    return html.escape(str(text if text is not None else ""))


def tzinfo():
    return timezone(timedelta(hours=S.tz))


def when(ts: int) -> str:
    return datetime.fromtimestamp(ts, tzinfo()).strftime("%d.%m %H:%M")


def normalize_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw or "")
    if len(digits) == 11 and digits[0] in ("7", "8"):
        digits = "7" + digits[1:]
    elif len(digits) == 10:
        digits = "7" + digits
    if not 10 <= len(digits) <= 15:
        raise ValueError("phone")
    return "+" + digits


# ============================== БАЗА ==============================
SCHEMA = """
CREATE TABLE IF NOT EXISTS leads (
    id       TEXT PRIMARY KEY,
    created  INTEGER NOT NULL,
    name     TEXT NOT NULL,
    phone    TEXT NOT NULL,
    kind     TEXT NOT NULL DEFAULT 'Замер',
    comment  TEXT NOT NULL DEFAULT '',
    amount   INTEGER NOT NULL DEFAULT 0,
    config   TEXT NOT NULL DEFAULT '',
    status   TEXT NOT NULL DEFAULT 'new',
    source   TEXT NOT NULL DEFAULT 'site',
    page     TEXT NOT NULL DEFAULT '',
    msg_id   INTEGER NOT NULL DEFAULT 0,
    iph      TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_leads_created ON leads(created DESC);
CREATE INDEX IF NOT EXISTS idx_leads_status  ON leads(status);
"""


def conn() -> sqlite3.Connection:
    S.db_path.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(S.db_path, timeout=15)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    return c


def db_init() -> None:
    with DB_LOCK, conn() as c:
        c.executescript(SCHEMA)


def row_to_dict(r) -> dict | None:
    if r is None:
        return None
    d = dict(r)
    try:
        d["config_obj"] = json.loads(d.get("config") or "null")
    except (ValueError, TypeError):
        d["config_obj"] = None
    d["code"] = str(d["id"])[:4].upper()
    return d


def db_create(data: dict) -> dict:
    lead_id = uuid.uuid4().hex[:10]
    cfg = data.get("config")
    cfg_text = json.dumps(cfg, ensure_ascii=False)[:4000] if cfg else ""
    with DB_LOCK, conn() as c:
        c.execute(
            """INSERT INTO leads
               (id,created,name,phone,kind,comment,amount,config,status,source,page,msg_id,iph)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (lead_id, int(time.time()), data["name"], data["phone"],
             data.get("kind") or "Замер", data.get("comment") or "",
             int(data.get("amount") or 0), cfg_text, "new",
             data.get("source") or "site", data.get("page") or "", 0, data.get("iph") or ""),
        )
    return db_get(lead_id)


def db_get(lead_id: str) -> dict | None:
    with DB_LOCK, conn() as c:
        return row_to_dict(c.execute("SELECT * FROM leads WHERE id=?", (lead_id,)).fetchone())


def db_status(lead_id: str, status: str) -> dict | None:
    if status not in STATUSES:
        return db_get(lead_id)
    with DB_LOCK, conn() as c:
        c.execute("UPDATE leads SET status=? WHERE id=?", (status, lead_id))
    return db_get(lead_id)


def db_msg_id(lead_id: str, msg_id: int) -> None:
    with DB_LOCK, conn() as c:
        c.execute("UPDATE leads SET msg_id=? WHERE id=?", (int(msg_id), lead_id))


def db_delete(lead_id: str) -> bool:
    with DB_LOCK, conn() as c:
        return c.execute("DELETE FROM leads WHERE id=?", (lead_id,)).rowcount > 0


def db_list(status: str | None = None, query: str = "", limit: int = 30) -> list[dict]:
    sql, where, args = "SELECT * FROM leads", [], []
    if status in STATUSES:
        where.append("status=?")
        args.append(status)
    if query:
        where.append("(name LIKE ? OR phone LIKE ? OR comment LIKE ? OR kind LIKE ?)")
        like = f"%{query}%"
        args += [like, like, like, like]
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created DESC LIMIT ?"
    args.append(max(1, min(int(limit), 200)))
    with DB_LOCK, conn() as c:
        return [row_to_dict(r) for r in c.execute(sql, args).fetchall()]


def db_stats() -> dict:
    now = int(time.time())
    with DB_LOCK, conn() as c:
        total = c.execute("SELECT COUNT(*) n FROM leads").fetchone()["n"]
        by = {s: c.execute("SELECT COUNT(*) n FROM leads WHERE status=?", (s,)).fetchone()["n"]
              for s in STATUSES}
        pipe = c.execute("SELECT COALESCE(SUM(amount),0) s FROM leads WHERE status IN ('new','work')").fetchone()["s"]
        won = c.execute("SELECT COALESCE(SUM(amount),0) s FROM leads WHERE status='done'").fetchone()["s"]
        today = c.execute("SELECT COUNT(*) n FROM leads WHERE created>=?", (now - 86400,)).fetchone()["n"]
        week = c.execute("SELECT COUNT(*) n FROM leads WHERE created>=?", (now - 604800,)).fetchone()["n"]
    return {"total": total, "new": by["new"], "work": by["work"], "done": by["done"],
            "pipeline": int(pipe), "won": int(won), "today": today, "week": week}


def db_csv() -> list[list[str]]:
    rows = [["Код", "Дата", "Имя", "Телефон", "Изделие", "Комментарий", "Сумма", "Статус"]]
    for lead in db_list(limit=200):
        rows.append([lead["code"], when(lead["created"]), lead["name"], lead["phone"],
                     lead["kind"], (lead["comment"] or "").replace("\n", " ").replace(";", ","),
                     str(lead["amount"]), STATUS_RU.get(lead["status"], lead["status"])])
    return rows


# ============================== TELEGRAM ==============================
def build_opener():
    ctx = ssl.create_default_context()
    if not S.tg_verify:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        log.warning("TG_VERIFY_SSL=0 — проверка сертификата отключена (небезопасно)")
    proxies = {"http": S.tg_proxy, "https": S.tg_proxy} if S.tg_proxy else {}
    return urllib.request.build_opener(
        urllib.request.ProxyHandler(proxies),
        urllib.request.HTTPSHandler(context=ctx),
    )


OPENER = build_opener()


def encode_multipart(fields: dict, files: dict) -> tuple[bytes, str]:
    boundary = "----MM" + uuid.uuid4().hex
    buf = io.BytesIO()
    for key, val in fields.items():
        buf.write(f"--{boundary}\r\n".encode())
        buf.write(f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode())
        buf.write(str(val).encode("utf-8"))
        buf.write(b"\r\n")
    for key, (filename, content, mime) in files.items():
        buf.write(f"--{boundary}\r\n".encode())
        buf.write(f'Content-Disposition: form-data; name="{key}"; filename="{filename}"\r\n'.encode())
        buf.write(f"Content-Type: {mime}\r\n\r\n".encode())
        buf.write(content)
        buf.write(b"\r\n")
    buf.write(f"--{boundary}--\r\n".encode())
    return buf.getvalue(), f"multipart/form-data; boundary={boundary}"


def tg(method: str, payload: dict | None = None, files: dict | None = None, timeout: int = 50):
    if not S.bot_token:
        return None
    url = f"{S.tg_api}/bot{S.bot_token}/{method}"
    for attempt in range(3):
        if STOP.is_set():
            return None
        try:
            if files:
                body, ctype = encode_multipart(payload or {}, files)
            else:
                body = json.dumps(payload or {}, ensure_ascii=False).encode("utf-8")
                ctype = "application/json"
            req = urllib.request.Request(url, data=body, method="POST", headers={
                "Content-Type": ctype, "User-Agent": "MebelMaster/1.0",
            })
            with OPENER.open(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            if data.get("ok"):
                return data.get("result")
            if data.get("error_code") == 429:
                wait = int(data.get("parameters", {}).get("retry_after", 3))
                log.warning("Telegram flood limit, ждём %s c", wait)
                STOP.wait(wait + 1)
                continue
            log.error("Telegram %s: %s %s", method, data.get("error_code"), data.get("description"))
            return None
        except urllib.error.HTTPError as exc:
            try:
                data = json.loads(exc.read().decode("utf-8"))
            except Exception:
                data = {}
            if data.get("error_code") == 429:
                STOP.wait(int(data.get("parameters", {}).get("retry_after", 3)) + 1)
                continue
            log.error("Telegram %s HTTP %s: %s", method, exc.code, data.get("description", exc))
            return None
        except (urllib.error.URLError, socket.timeout, ssl.SSLError, OSError) as exc:
            log.warning("Сеть %s (попытка %s/3): %s", method, attempt + 1, exc)
            STOP.wait(1.5 * (attempt + 1))
        except Exception as exc:
            log.error("Ошибка %s: %s", method, exc)
            return None
    return None


def tg_send(chat_id: int, text: str, markup: dict | None = None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML",
               "disable_web_page_preview": True}
    if markup:
        payload["reply_markup"] = markup
    return tg("sendMessage", payload)


def tg_edit(chat_id: int, msg_id: int, text: str, markup: dict | None = None):
    payload = {"chat_id": chat_id, "message_id": msg_id, "text": text,
               "parse_mode": "HTML", "disable_web_page_preview": True}
    if markup is not None:
        payload["reply_markup"] = markup
    return tg("editMessageText", payload)


def tg_markup(chat_id: int, msg_id: int, markup: dict):
    return tg("editMessageReplyMarkup",
              {"chat_id": chat_id, "message_id": msg_id, "reply_markup": markup})


def tg_answer(cb_id: str, text: str = "", alert: bool = False):
    return tg("answerCallbackQuery",
              {"callback_query_id": cb_id, "text": text[:190], "show_alert": alert})


# ============================== ФОРМАТИРОВАНИЕ ==============================
def config_lines(cfg) -> list[str]:
    if not isinstance(cfg, dict):
        return []
    out = []
    kind = TYPES_RU.get(cfg.get("type"), cfg.get("type"))
    if kind:
        out.append(f"🪑 {e(kind)}")
    out.append(f"📐 {e(cfg.get('width','?'))}×{e(cfg.get('height','?'))}×{e(cfg.get('depth','?'))} мм"
               f" · секций: {e(cfg.get('sections','?'))}")
    mat = MATERIALS_RU.get(cfg.get("material"), cfg.get("material"))
    tone = TONES_RU.get(cfg.get("tone"), cfg.get("tone"))
    if mat or tone:
        out.append(f"🎨 {e(mat)} · {e(tone)}")
    handle = HANDLES_RU.get(cfg.get("handle"))
    if handle:
        out.append(f"🚪 {e(handle)}")
    if cfg.get("top") and cfg.get("type") in ("kitchen", "tv"):
        out.append(f"🧱 Столешница: {e(TOPS_RU.get(cfg.get('topMat'), cfg.get('topMat')))}")
    out.append(f"💡 Подсветка: {'да' if cfg.get('led') else 'нет'}")
    hw = HW_RU.get(cfg.get("hw"))
    if hw:
        out.append(f"🔧 {e(hw)}")
    return out


def lead_text(lead: dict) -> str:
    lines = [f"<b>{STATUS_BADGE.get(lead['status'], lead['status'])} · заявка #{e(lead['code'])}</b>", ""]
    lines.append(f"👤 <b>{e(lead['name'])}</b>")
    digits = re.sub(r"\D", "", lead["phone"])
    lines.append(f'📞 <a href="tel:+{digits}">{e(lead["phone"])}</a>')
    lines.append(f"📦 {e(lead['kind'])}")
    if lead["amount"]:
        lines.append(f"💰 <b>{money(lead['amount'])}</b>")
    if lead["comment"]:
        lines.append(f"📝 {e(lead['comment'])}")
    cfg = config_lines(lead.get("config_obj"))
    if cfg:
        lines.append("")
        lines.extend(cfg)
    lines.append("")
    lines.append(f"🕒 {when(lead['created'])} · {e(lead['source'])}")
    return "\n".join(lines)


def lead_markup(lead: dict) -> dict:
    lid = lead["id"]
    digits = re.sub(r"\D", "", lead["phone"])
    rows, actions = [], []
    if lead["status"] != "work":
        actions.append({"text": "⚙️ В работу", "callback_data": f"st:{lid}:work"})
    if lead["status"] != "done":
        actions.append({"text": "✅ Закрыть", "callback_data": f"st:{lid}:done"})
    if lead["status"] != "new":
        actions.append({"text": "🆕 Вернуть", "callback_data": f"st:{lid}:new"})
    if actions:
        rows.append(actions)
    rows.append([{"text": "💬 WhatsApp", "url": f"https://wa.me/{digits}"},
                 {"text": "📋 Смета", "callback_data": f"spec:{lid}"}])
    rows.append([{"text": "🗑 Удалить", "callback_data": f"del:{lid}"}])
    return {"inline_keyboard": rows}


def spec_text(lead: dict) -> str:
    lines = [f"<b>📋 Смета #{e(lead['code'])}</b>", "",
             f"👤 {e(lead['name'])}", f"📞 {e(lead['phone'])}", f"📦 {e(lead['kind'])}"]
    lines += config_lines(lead.get("config_obj"))
    if lead["amount"]:
        lines += ["", f"💰 Итого: <b>{money(lead['amount'])}</b>",
                  f"Предоплата 50%: {money(lead['amount'] / 2)}"]
    if lead["comment"]:
        lines += ["", f"📝 {e(lead['comment'])}"]
    lines += ["", f"🕒 {when(lead['created'])}"]
    return "\n".join(lines)


def notify_lead(lead: dict) -> bool:
    if not S.admin_ids:
        log.warning("ADMIN_IDS пуст — заявка #%s сохранена, слать некому", lead["code"])
        return False
    ok = False
    for chat_id in S.admin_ids:
        res = tg_send(chat_id, lead_text(lead), lead_markup(lead))
        if res:
            ok = True
            if not lead.get("msg_id"):
                db_msg_id(lead["id"], int(res["message_id"]))
    return ok


# ============================== БОТ ==============================
KEYBOARD = {"keyboard": [[{"text": "🆕 Новые"}, {"text": "⚙️ В работе"}],
                         [{"text": "✅ Закрытые"}, {"text": "📊 Статистика"}],
                                                  [{"text": "📁 CSV"}, {"text": "❓ Помощь"}]],
            "resize_keyboard": True, "is_persistent": True}

MENU = {"🆕 Новые": "/new", "⚙️ В работе": "/work", "✅ Закрытые": "/done",
        "📊 Статистика": "/stats", "📁 CSV": "/csv", "❓ Помощь": "/help"}

COMMANDS = [
    {"command": "start", "description": "Меню мастерской"},
    {"command": "new", "description": "Новые заявки"},
    {"command": "work", "description": "В работе"},
    {"command": "done", "description": "Закрытые"},
    {"command": "all", "description": "Все заявки"},
    {"command": "stats", "description": "Статистика и суммы"},
    {"command": "find", "description": "Поиск: /find Марина"},
    {"command": "csv", "description": "Выгрузить CSV"},
    {"command": "id", "description": "Мой Telegram ID"},
    {"command": "ping", "description": "Проверка связи"},
    {"command": "help", "description": "Помощь"},
]

HELP_TEXT = (
    "<b>❓ Как пользоваться</b>\n\n"
    "Заявки с сайта приходят сюда сами. Под каждой кнопки:\n"
    "⚙️ <b>В работу</b> — взял в обработку\n"
    "✅ <b>Закрыть</b> — сделка состоялась\n"
    "🆕 <b>Вернуть</b> — вернуть в новые\n"
    "💬 <b>WhatsApp</b> — написать клиенту\n"
    "📋 <b>Смета</b> — полная конфигурация\n"
    "🗑 <b>Удалить</b> — с подтверждением\n\n"
    "<b>Команды:</b>\n"
    "/new /work /done /all — списки\n"
    "/stats — цифры и суммы\n"
    "/find Марина — поиск\n"
    "/csv — выгрузка в Excel\n"
    "/id — твой Telegram ID\n"
    "/ping — жив ли сервер"
)


def is_admin(user_id) -> bool:
    return bool(user_id) and int(user_id) in S.admin_ids


def bot_list(chat_id: int, status: str | None, query: str = "", title: str = "Заявки") -> None:
    items = db_list(status=status, query=query, limit=15)
    if not items:
        tg_send(chat_id, f"<b>{e(title)}</b>\n\nПусто. Как придёт заявка — будет здесь.", KEYBOARD)
        return
    tg_send(chat_id, f"<b>{e(title)}</b> · {len(items)} шт.", KEYBOARD)
    for lead in items:
        tg_send(chat_id, lead_text(lead), lead_markup(lead))


def bot_stats(chat_id: int) -> None:
    st = db_stats()
    tg_send(chat_id,
            "<b>📊 Статистика мастерской</b>\n\n"
            f"Всего заявок: <b>{st['total']}</b>\n"
            f"🆕 Новых: <b>{st['new']}</b>\n"
            f"⚙️ В работе: <b>{st['work']}</b>\n"
            f"✅ Закрыто: <b>{st['done']}</b>\n\n"
            f"💰 В трубе: <b>{money(st['pipeline'])}</b>\n"
            f"🏁 Закрытые суммы: <b>{money(st['won'])}</b>\n\n"
            f"За сутки: {st['today']} · за неделю: {st['week']}", KEYBOARD)


def bot_csv(chat_id: int) -> None:
    rows = db_csv()
    if len(rows) <= 1:
        tg_send(chat_id, "Выгружать нечего — заявок пока нет.", KEYBOARD)
        return
    buf = io.StringIO(newline="")
    csv.writer(buf, delimiter=";").writerows(rows)
    content = ("\ufeff" + buf.getvalue()).encode("utf-8")
    stamp = datetime.now(tzinfo()).strftime("%Y-%m-%d")
    tg("sendDocument",
       {"chat_id": chat_id, "caption": f"📁 Выгрузка заявок · {len(rows) - 1} шт.", "parse_mode": "HTML"},
       files={"document": (f"zayavki-{stamp}.csv", content, "text/csv")})


def bot_message(msg: dict) -> None:
    chat_id = msg["chat"]["id"]
    user_id = (msg.get("from") or {}).get("id")
    raw = (msg.get("text") or "").strip()
    text = MENU.get(raw, raw)
    if not text:
        return
    cmd = text.split()[0].split("@")[0].lower()
    arg = text[len(cmd):].strip()

    if cmd == "/id":
        tg_send(chat_id, f"Твой Telegram ID: <code>{user_id}</code>\n\n"
                         "Впиши в .env → <code>ADMIN_IDS=</code> и перезапусти сервер.")
        return

    if not is_admin(user_id):
        tg_send(chat_id, "Доступ закрыт. Это бот мастерской МебельМастер.\n"
                         "Если ты мастер — впиши свой ID в настройки сервера.")
        return

    if cmd == "/start":
        tg_send(chat_id, "<b>🪑 МебельМастер на связи.</b>\nЗаявки с сайта будут приходить сюда.", KEYBOARD)
        tg_send(chat_id, HELP_TEXT, KEYBOARD)
    elif cmd == "/help":
        tg_send(chat_id, HELP_TEXT, KEYBOARD)
    elif cmd == "/new":
        bot_list(chat_id, "new", title="🆕 Новые заявки")
    elif cmd == "/work":
        bot_list(chat_id, "work", title="⚙️ В работе")
    elif cmd == "/done":
        bot_list(chat_id, "done", title="✅ Закрытые")
    elif cmd == "/all":
        bot_list(chat_id, None, title="📦 Все заявки")
    elif cmd == "/stats":
        bot_stats(chat_id)
    elif cmd == "/csv":
        bot_csv(chat_id)
    elif cmd == "/ping":
        tg_send(chat_id, f"Живой ✅\nЗаявок в базе: {db_stats()['total']}\nВремя: {when(int(time.time()))}", KEYBOARD)
    elif cmd == "/find":
        if not arg:
            tg_send(chat_id, "Напиши так: <code>/find Марина</code> или <code>/find 900</code>", KEYBOARD)
        else:
            bot_list(chat_id, None, query=arg, title=f"🔎 Поиск: {arg}")
    else:
        tg_send(chat_id, "Не понял. Жми кнопки внизу или /help", KEYBOARD)


def bot_callback(cb: dict) -> None:
    cb_id = cb["id"]
    user_id = (cb.get("from") or {}).get("id")
    data = cb.get("data") or ""
    msg = cb.get("message") or {}
    chat_id = (msg.get("chat") or {}).get("id")
    msg_id = msg.get("message_id")

    if not is_admin(user_id):
        tg_answer(cb_id, "Доступ закрыт", True)
        return

    parts = data.split(":")
    action = parts[0]

    if action == "st" and len(parts) == 3:
        lead = db_status(parts[1], parts[2])
        if not lead:
            tg_answer(cb_id, "Заявка не найдена", True)
            return
        tg_answer(cb_id, "Статус: " + STATUS_RU[lead["status"]])
        if chat_id and msg_id:
            tg_edit(chat_id, msg_id, lead_text(lead), lead_markup(lead))
        return

    if action == "spec" and len(parts) == 2:
        lead = db_get(parts[1])
        tg_answer(cb_id, "Отправляю смету")
        if lead and chat_id:
            tg_send(chat_id, spec_text(lead))
        return

    if action == "del" and len(parts) == 2:
        lead = db_get(parts[1])
        if not lead:
            tg_answer(cb_id, "Уже удалена", True)
            return
        tg_answer(cb_id, "Подтверди удаление")
        if chat_id and msg_id:
            tg_markup(chat_id, msg_id, {"inline_keyboard": [[
                {"text": "🗑 Да, удалить", "callback_data": f"delok:{lead['id']}"},
                {"text": "↩️ Отмена", "callback_data": f"cancel:{lead['id']}"},
            ]]})
        return

    if action == "delok" and len(parts) == 2:
        ok = db_delete(parts[1])
        tg_answer(cb_id, "Удалена" if ok else "Не найдена")
        if chat_id and msg_id:
            tg_edit(chat_id, msg_id, "🗑 <b>Заявка удалена</b>", {"inline_keyboard": []})
        return

    if action == "cancel" and len(parts) == 2:
        lead = db_get(parts[1])
        tg_answer(cb_id, "Отменено")
        if lead and chat_id and msg_id:
            tg_edit(chat_id, msg_id, lead_text(lead), lead_markup(lead))
        return

    tg_answer(cb_id)


def polling_loop() -> None:
    if not S.bot_token:
        log.warning("BOT_TOKEN не задан — бот не запущен. Заявки всё равно копятся в базу.")
        return
    tg("deleteWebhook", {"drop_pending_updates": False})
    tg("setMyCommands", {"commands": COMMANDS})
    me = tg("getMe")
    if me:
        log.info("Бот на связи: @%s", me.get("username"))
        if not S.admin_ids:
            log.warning("ADMIN_IDS пуст! Напиши боту /id, впиши число в .env, перезапусти.")
    else:
        log.error("Telegram недоступен. Проверь токен, VPN и TG_PROXY в .env.")

    offset = 0
    while not STOP.is_set():
        updates = tg("getUpdates", {"offset": offset, "timeout": 25,
                                    "allowed_updates": ["message", "callback_query"]}, timeout=45)
        if updates is None:
            STOP.wait(4)
            continue
        for upd in updates:
            offset = max(offset, int(upd["update_id"]) + 1)
            try:
                if "message" in upd:
                    bot_message(upd["message"])
                elif "callback_query" in upd:
                    bot_callback(upd["callback_query"])
            except Exception:
                log.exception("Ошибка обработки апдейта")


def digest_loop() -> None:
    if S.digest_hour < 0 or not S.telegram_ready():
        return
    while not STOP.is_set():
        now = datetime.now(tzinfo())
        target = now.replace(hour=S.digest_hour, minute=0, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        if STOP.wait(max((target - now).total_seconds(), 30)):
            return
        st = db_stats()
        if st["today"] or st["new"]:
            text = ("<b>🌙 Сводка за день</b>\n\n"
                    f"Новых сегодня: <b>{st['today']}</b>\n"
                    f"Ждут ответа: <b>{st['new']}</b>\n"
                    f"В работе: <b>{st['work']}</b>\n"
                    f"💰 В трубе: <b>{money(st['pipeline'])}</b>")
            for chat_id in S.admin_ids:
                tg_send(chat_id, text, KEYBOARD)


# ============================== АНТИ-СПАМ ==============================
HITS: dict[str, list[float]] = {}
HITS_LOCK = threading.Lock()


def rate_ok(iph: str) -> bool:
    now = time.time()
    with HITS_LOCK:
        window = [t for t in HITS.get(iph, []) if now - t < 600]
        if len(window) >= S.rate_limit:
            HITS[iph] = window
            return False
        window.append(now)
        HITS[iph] = window
        if len(HITS) > 5000:
            for key in list(HITS):
                if not HITS[key] or now - HITS[key][-1] > 600:
                    HITS.pop(key, None)
    return True


def client_ip(handler) -> str:
    fwd = handler.headers.get("X-Forwarded-For", "")
    ip = fwd.split(",")[0].strip() if fwd else (handler.client_address[0] if handler.client_address else "0.0.0.0")
    return hashlib.sha256((ip + "|mebelmaster").encode()).hexdigest()[:16]


# ============================== HTTP ==============================
BRIDGE_TAG = '<script src="/js/telegram-bridge.js" defer></script>'
MIME = {".html": "text/html; charset=utf-8", ".css": "text/css; charset=utf-8",
        ".js": "application/javascript; charset=utf-8", ".svg": "image/svg+xml",
        ".json": "application/json; charset=utf-8", ".png": "image/png",
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".webp": "image/webp",
        ".ico": "image/x-icon", ".woff2": "font/woff2", ".txt": "text/plain; charset=utf-8"}


class Handler(BaseHTTPRequestHandler):
    server_version = "MebelMaster/1.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        if "/api/health" not in (self.path or ""):
            log.info("%s %s", self.command, self.path)

    # ---------- отправка ----------
    def _send(self, code: int, body: bytes, ctype: str, extra: dict | None = None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        origin = self.headers.get("Origin", "")
        if origin in S.cors:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    def _json(self, code: int, obj: dict):
        self._send(code, json.dumps(obj, ensure_ascii=False).encode("utf-8"),
                   "application/json; charset=utf-8", {"Cache-Control": "no-store"})

    def _error(self, code: int, detail: str):
        self._json(code, {"ok": False, "detail": detail})

    def _body(self) -> dict:
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        if length <= 0:
            return {}
        if length > 65536:
            raise ValueError("too big")
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise ValueError("bad json")
        return data if isinstance(data, dict) else {}

    def _admin_ok(self) -> bool:
        return bool(S.admin_key) and self.headers.get("X-Admin-Key", "") == S.admin_key

    # ---------- статика ----------
    def _static(self, rel: str):
        rel = urllib.parse.unquote(rel).lstrip("/")
        target = (S.site_dir / rel).resolve()
        try:
            target.relative_to(S.site_dir)
        except ValueError:
            return self._error(403, "Запрещено")
        if not target.is_file():
            return self._error(404, "Файл не найден: " + rel)
        ctype = MIME.get(target.suffix.lower()) or mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        self._send(200, target.read_bytes(), ctype, {"Cache-Control": "no-cache"})

    def _index(self):
        index = S.site_dir / "index.html"
        if not index.is_file():
            return self._send(500,
                              f"<h1>index.html не найден</h1><p>SITE_DIR указывает на <code>{e(S.site_dir)}</code></p>"
                              .encode("utf-8"), "text/html; charset=utf-8")
        text = index.read_text(encoding="utf-8")
        if "telegram-bridge.js" not in text:
            text = text.replace("</body>", BRIDGE_TAG + "\n</body>") if "</body>" in text else text + BRIDGE_TAG
        self._send(200, text.encode("utf-8"), "text/html; charset=utf-8", {"Cache-Control": "no-cache"})

    # ---------- роуты ----------
    def do_OPTIONS(self):
        origin = self.headers.get("Origin", "")
        headers = {"Access-Control-Allow-Methods": "GET, POST, PATCH, OPTIONS",
                   "Access-Control-Allow-Headers": "Content-Type, X-Admin-Key",
                   "Access-Control-Max-Age": "86400"}
        self._send(204, b"", "text/plain", headers if origin in S.cors else {})

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        parsed = urllib.parse.urlsplit(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        if path in ("/", "/index.html"):
            return self._index()
        if path == "/favicon.ico":
            return self._send(204, b"", "image/x-icon")
        if path == "/api/health":
            return self._json(200, {"ok": True, "service": "mebelmaster", "time": int(time.time()),
                                    "telegram_configured": S.telegram_ready(),
                                    "leads_total": db_stats()["total"]})
        if path == "/api/leads":
            if not self._admin_ok():
                return self._error(403, "Неверный админ-ключ")
            items = db_list(status=(query.get("status") or [None])[0],
                            query=(query.get("q") or [""])[0],
                            limit=int((query.get("limit") or ["50"])[0]))
            for item in items:
                item.pop("iph", None)
            return self._json(200, {"ok": True, "count": len(items), "items": items})
        if path == "/api/stats":
            if not self._admin_ok():
                return self._error(403, "Неверный админ-ключ")
            return self._json(200, {"ok": True, "stats": db_stats()})
        if path.startswith(("/css/", "/js/", "/img/", "/assets/")):
            return self._static(path)
        return self._error(404, "Нет такого маршрута")

    def do_PATCH(self):
        parsed = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        match = re.fullmatch(r"/api/leads/([a-f0-9]{6,20})", parsed.path)
        if not match:
            return self._error(404, "Нет такого маршрута")
        if not self._admin_ok():
            return self._error(403, "Неверный админ-ключ")
        status = (query.get("status") or [""])[0]
        if status not in STATUSES:
            return self._error(422, "Статус: new / work / done")
        lead = db_status(match.group(1), status)
        if not lead:
            return self._error(404, "Заявка не найдена")
        return self._json(200, {"ok": True, "lead": lead})

    def do_POST(self):
        path = urllib.parse.urlsplit(self.path).path
        if path != "/api/lead":
            return self._error(404, "Нет такого маршрута")
        try:
            data = self._body()
        except ValueError as exc:
            return self._error(413 if "big" in str(exc) else 400, "Некорректный запрос")

        if str(data.get("company") or "").strip():
            log.info("Honeypot: бот отсечён")
            return self._json(200, {"ok": True, "code": "0000", "delivered": False})

        try:
            elapsed = int(data.get("elapsed") or 0)
        except (TypeError, ValueError):
            elapsed = 0
        if elapsed < S.min_fill:
            return self._error(429, "Слишком быстро. Заполни форму спокойно.")

        iph = client_ip(self)
        if not rate_ok(iph):
            return self._error(429, "Слишком много заявок. Подожди 10 минут.")

        name = str(data.get("name") or "").strip()[:60]
        if len(name) < 2:
            return self._error(422, "Имя слишком короткое")
        try:
            phone = normalize_phone(str(data.get("phone") or ""))
        except ValueError:
            return self._error(422, "Телефон должен содержать 10–15 цифр")

        try:
            amount = max(0, min(int(data.get("amount") or 0), 100_000_000))
        except (TypeError, ValueError):
            amount = 0

        cfg = data.get("config")
        if not isinstance(cfg, dict):
            cfg = None

        lead = db_create({
            "name": name, "phone": phone,
            "kind": (str(data.get("kind") or "Замер").strip() or "Замер")[:60],
            "comment": str(data.get("comment") or "").strip()[:1200],
            "amount": amount, "config": cfg,
            "source": str(data.get("source") or "site")[:40],
            "page": str(data.get("page") or "")[:300], "iph": iph,
        })

        delivered = False
        try:
            delivered = notify_lead(lead)
        except Exception:
            log.exception("Telegram не ответил, заявка #%s сохранена", lead["code"])

        log.info("Заявка #%s · %s · %s · telegram=%s", lead["code"], name, phone, delivered)
        return self._json(200, {"ok": True, "code": lead["code"], "delivered": bool(delivered)})


# ============================== ДИАГНОСТИКА ==============================
def check() -> int:
    print("=" * 64)
    print("  ДИАГНОСТИКА МЕБЕЛЬМАСТЕР")
    print("=" * 64)
    ok = True

    print(f"[i] Python: {sys.version.split()[0]}")
    if sys.version_info < (3, 10):
        print("[X] Нужен Python 3.10 или новее")
        ok = False
    else:
        print("[+] Версия подходит")

    index = S.site_dir / "index.html"
    print(f"[i] Сайт: {S.site_dir}")
    if index.is_file():
        print("[+] index.html найден")
        bridge = S.site_dir / "js" / "telegram-bridge.js"
        print("[+] telegram-bridge.js найден" if bridge.is_file()
              else "[X] Нет site/js/telegram-bridge.js — положи файл из прошлого сообщения")
        if not bridge.is_file():
            ok = False
    else:
        print("[X] index.html не найден — поправь SITE_DIR в .env")
        ok = False

    print(f"[i] База: {S.db_path}")
    try:
        db_init()
        print(f"[+] База работает, заявок: {db_stats()['total']}")
    except Exception as exc:
        print(f"[X] База не открылась: {exc}")
        ok = False

    print(f"[i] Прокси: {S.tg_proxy or 'нет (прямое соединение / TUN-режим)'}")
    if S.tg_proxy.startswith("socks"):
        print("[!] Стандартный Python не умеет SOCKS. Укажи HTTP-порт v2rayN, например http://127.0.0.1:10809")
        ok = False

    if not (BASE / ".env").exists():
        print("[X] Нет файла .env — скопируй .env.example в .env")
        ok = False
    if not S.bot_token:
        print("[X] BOT_TOKEN пуст — возьми у @BotFather")
        ok = False
    else:
        masked = S.bot_token[:8] + "…" + S.bot_token[-4:]
        print(f"[+] Токен задан: {masked}")
        print("[*] Проверяю связь с Telegram…")
        me = tg("getMe", timeout=25)
        if me:
            print(f"[+] Telegram доступен, бот: @{me.get('username')}")
        else:
            print("[X] Telegram недоступен. Включи VPN (TUN) или пропиши TG_PROXY=http://127.0.0.1:10809")
            ok = False

    if not S.admin_ids:
        print("[!] ADMIN_IDS пуст — запусти сервер, напиши боту /id, впиши число в .env")
    else:
        print(f"[+] Админы: {S.admin_ids}")

    if not S.admin_key:
        print("[!] ADMIN_KEY пуст — админские HTTP-ручки будут закрыты (для бота не критично)")

    print("-" * 64)
    print(f"  Сайт будет тут: http://{S.host}:{S.port}")
    print("  Итог: " + ("всё готово, запускай python server.py" if ok else "есть проблемы, исправь пункты [X]"))
    print("=" * 64)
    return 0 if ok else 1


# ============================== СТАРТ ==============================
def main() -> int:
    db_init()

    if "--check" in sys.argv:
        return check()

    print("=" * 64)
    print("  МЕБЕЛЬМАСТЕР · сервер заявок (без зависимостей)")
    print(f"  Сайт:    http://{S.host}:{S.port}")
    print(f"  База:    {S.db_path.name}")
    print(f"  Токен:   {'задан' if S.bot_token else 'НЕ ЗАДАН'}")
    print(f"  Админы:  {S.admin_ids or 'не заданы — напиши боту /id'}")
    print(f"  Прокси:  {S.tg_proxy or 'нет'}")
    print("  Стоп:    Ctrl+C")
    print("=" * 64)

    if not (S.site_dir / "index.html").is_file():
        log.error("Не найден index.html в %s — поправь SITE_DIR в .env", S.site_dir)

    threads = [threading.Thread(target=polling_loop, name="polling", daemon=True),
               threading.Thread(target=digest_loop, name="digest", daemon=True)]
    for t in threads:
        t.start()

    try:
        httpd = ThreadingHTTPServer((S.host, S.port), Handler)
    except OSError as exc:
        log.error("Не могу занять порт %s: %s", S.port, exc)
        print(f"[X] Порт {S.port} занят. Закрой старую копию сервера или поставь PORT=8010 в .env")
        STOP.set()
        return 1

    httpd.daemon_threads = True
    log.info("Сервер поднят. Открой в браузере: http://%s:%s", S.host, S.port)
    try:
        httpd.serve_forever(poll_interval=0.4)
    except KeyboardInterrupt:
        print("\n[*] Останавливаюсь…")
    finally:
        STOP.set()
        httpd.shutdown()
        httpd.server_close()
        for t in threads:
            t.join(timeout=2)
        print("[+] Сервер остановлен. Заявки сохранены в базе.")
    return 0


if __name__ == "__main__":
    sys.exit(main())