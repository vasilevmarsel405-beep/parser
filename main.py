import argparse
import html
import os
import random
import re
import sqlite3
import time
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

import gspread
import instaloader
import requests
import yaml
from dotenv import load_dotenv
from gspread.exceptions import APIError
from itertools import islice
from urllib.parse import unquote


DB_PATH = "state.db"
DEFAULT_CONFIG = "accounts.yaml"


HEADER = [
    "Дата проверки",
    "Сотрудник",
    "Платформа",
    "Аккаунт",
    "Дата публикации",
    "Текст поста",
    "Ссылка",
    "ID поста",
]
SUMMARY_HEADER = [
    "Сотрудник",
    "Всего постов",
    "VK",
    "Instagram",
    "Последняя активность",
    "Статус сегодня",
]

# Colors for platform rows (RGB 0–1)
PLATFORM_COLORS = {
    "instagram": {"red": 0.933, "green": 0.894, "blue": 0.980},  # фиолетовый
    "vk":        {"red": 0.706, "green": 0.784, "blue": 0.918},  # темно-синий
}
# Column widths in pixels
COL_WIDTHS = [160, 200, 110, 160, 160, 420, 320, 130]
PERSON_HEADER = [
    "Дата проверки",
    "Платформа",
    "Аккаунт",
    "Дата публикации",
    "Текст поста",
    "Ссылка",
    "Просмотры",
    "ID поста",
]


@dataclass
class PostRecord:
    person_name: str
    platform: str
    account: str
    post_id: str
    text: str
    url: str
    published_at: str
    fetched_at: str
    views: str = ""


@dataclass
class ErrorRecord:
    fetched_at: str
    person_name: str
    platform: str
    account: str
    error_type: str
    details: str


# ─── Database ────────────────────────────────────────────────────────────────

def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS seen_posts (
            platform TEXT NOT NULL,
            account  TEXT NOT NULL,
            post_id  TEXT NOT NULL,
            seen_at  TEXT NOT NULL,
            PRIMARY KEY (platform, account, post_id)
        )
        """
    )
    conn.commit()
    return conn


def is_seen(conn: sqlite3.Connection, platform: str, account: str, post_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM seen_posts WHERE platform=? AND account=? AND post_id=?",
        (platform, account, post_id),
    ).fetchone()
    return row is not None


def mark_seen(conn: sqlite3.Connection, post: PostRecord) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO seen_posts(platform,account,post_id,seen_at) VALUES(?,?,?,?)",
        (post.platform, post.account, post.post_id, post.fetched_at),
    )


# ─── Helpers ─────────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_accounts(path: str) -> List[Dict]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    people = data.get("people", [])
    if not isinstance(people, list):
        raise ValueError("`people` must be a list in config.")
    for person in people:
        raw_name = str(person.get("person_name", "")).strip()
        if raw_name:
            person["person_name_full"] = raw_name
            person["person_name"] = to_short_name(raw_name)
    return people


def strip_html(raw_html: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", raw_html, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    return html.unescape(text).strip()


def to_short_name(full_name: str) -> str:
    parts = [p for p in re.split(r"\s+", full_name.strip()) if p]
    if len(parts) < 2:
        return full_name.strip()
    surname = parts[0]
    name_initial = parts[1][0].upper() if parts[1] else ""
    patronymic_initial = parts[2][0].upper() if len(parts) > 2 and parts[2] else ""
    if patronymic_initial:
        return f"{surname} {name_initial}.{patronymic_initial}."
    return f"{surname} {name_initial}."


def _append_error(
    errors: List[ErrorRecord],
    person_name: str,
    platform: str,
    account: str,
    error_type: str,
    details: str,
) -> None:
    errors.append(
        ErrorRecord(
            fetched_at=now_iso(),
            person_name=person_name,
            platform=platform,
            account=account,
            error_type=error_type,
            details=details[:500],
        )
    )


def _is_quota_error(exc: Exception) -> bool:
    return "429" in str(exc) or "Quota exceeded" in str(exc)


def _call_with_retry(action, label: str, max_attempts: int = 8, base_sleep: int = 5):
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return action()
        except APIError as exc:
            last_exc = exc
            if not _is_quota_error(exc):
                raise
            wait = base_sleep * attempt
            print(f"[sheets] quota on {label}, retry in {wait}s ({attempt}/{max_attempts})")
            time.sleep(wait)
        except Exception as exc:
            last_exc = exc
            raise
    if last_exc:
        raise last_exc


def _append_rows_safe(ws, rows: List[List[str]], chunk_size: int = 12) -> None:
    if not rows:
        return
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        _call_with_retry(
            lambda c=chunk: ws.append_rows(c, value_input_option="RAW"),
            label=f"{ws.title} append_rows",
            max_attempts=10,
            base_sleep=6,
        )
        time.sleep(1)


def sort_posts_newest_first(posts: List[PostRecord]) -> List[PostRecord]:
    """Сортировка: свежие посты первыми (для листов и вывода)."""
    return sorted(posts, key=lambda p: p.published_at, reverse=True)


def keep_only_new(conn: sqlite3.Connection, posts: List[PostRecord]) -> List[PostRecord]:
    new_posts = [p for p in posts if not is_seen(conn, p.platform, p.account, p.post_id)]
    return sort_posts_newest_first(new_posts)


def dedupe_posts_in_memory(posts: List[PostRecord]) -> List[PostRecord]:
    unique = {}
    for p in posts:
        key = (p.person_name, p.platform, p.post_id)
        current = unique.get(key)
        if current is None:
            unique[key] = p
            continue
        # Keep the richer version if fields differ.
        if len((p.text or "")) > len((current.text or "")):
            unique[key] = p
    return sort_posts_newest_first(list(unique.values()))


def mark_all_seen(conn: sqlite3.Connection, posts: List[PostRecord]) -> None:
    for p in posts:
        mark_seen(conn, p)
    conn.commit()


def clear_seen_posts(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM seen_posts")
    conn.commit()
    print("[db] таблица seen_posts очищена")


def apply_per_person_platform_cap(posts: List[PostRecord], cap: int) -> List[PostRecord]:
    """Не больше `cap` постов на сотрудника на одну платформу (по дате: свежие первые)."""
    if cap <= 0:
        return posts
    buckets: Dict[tuple, List[PostRecord]] = defaultdict(list)
    for p in posts:
        buckets[(p.person_name, p.platform)].append(p)
    out: List[PostRecord] = []
    for _key, items in buckets.items():
        items_sorted = sorted(items, key=lambda x: x.published_at, reverse=True)
        out.extend(items_sorted[:cap])
    return sort_posts_newest_first(out)


def clear_person_sheets_data(spreadsheet, people: List[Dict]) -> None:
    """Очищает данные на листах сотрудников, оставляет только шапку PERSON_HEADER."""
    ws_map = _ensure_person_sheets(spreadsheet, people)
    for name, ws in ws_map.items():
        _call_with_retry(lambda w=ws: w.clear(), f"clear person sheet {name}", max_attempts=8, base_sleep=5)
        _call_with_retry(
            lambda w=ws: w.update(range_name="A1:H1", values=[PERSON_HEADER]),
            f"{ws.title} header after reset",
            max_attempts=8,
            base_sleep=5,
        )
        try:
            _format_person_sheet(ws)
        except Exception as exc:
            print(f"[sheets] стиль листа '{ws.title}' после сброса: {exc}")
    print("[sheets] листы сотрудников очищены (шапка восстановлена)")


def clear_auxiliary_sheets(spreadsheet) -> None:
    """Полностью очищает служебные листы (без шапки — пустая таблица)."""
    for title in ("Сводка", "Bitbanker", "Ошибки"):
        try:
            ws = _call_with_retry(
                lambda t=title: spreadsheet.worksheet(t),
                f"worksheet get {title}",
                max_attempts=10,
                base_sleep=6,
            )
            _call_with_retry(lambda w=ws: w.clear(), f"clear {title}", max_attempts=8, base_sleep=5)
            print(f"[sheets] лист «{title}» очищен полностью")
        except gspread.WorksheetNotFound:
            pass


def delete_orphan_sheets(spreadsheet, people: List[Dict]) -> None:
    """Удаляет листы-призраки: старые имена типа 'Копарушкина А.' когда актуальный 'Копарушкина А.И.'"""
    valid_titles = set()
    for person in people:
        name = person.get("person_name", "")
        valid_titles.add(_sanitize_sheet_title(name))
        full_key = str(person.get("person_name_full", "") or "").strip()
        if full_key:
            valid_titles.add(_sanitize_sheet_title(full_key))
    # Также защищаем служебные листы
    valid_titles.update({"Сводка", "Bitbanker", "Ошибки"})

    all_ws = spreadsheet.worksheets()
    for ws in all_ws:
        if ws.title in valid_titles:
            continue
        # Проверяем: похоже ли название на персональный лист-призрак (Фамилия И. или Фамилия И.О.)
        if re.match(r"^[А-ЯЁ][а-яёА-ЯЁ]+\s+[А-ЯЁ]\.([А-ЯЁ]\.)?\s*$", ws.title):
            row_count = _sheet_data_row_count(ws)
            if row_count <= 1:
                try:
                    spreadsheet.del_worksheet(ws)
                    print(f"[sheets] удалён пустой лист-призрак «{ws.title}»")
                except Exception as exc:
                    print(f"[sheets] не удалось удалить «{ws.title}»: {exc}")


def run_clear_only(config_path: str) -> None:
    """Очистка state.db и всех целевых листов Google Sheets без сбора постов."""
    people = load_accounts(config_path)
    conn = init_db()
    try:
        spreadsheet = _open_spreadsheet()
        clear_seen_posts(conn)
        delete_orphan_sheets(spreadsheet, people)
        clear_person_sheets_data(spreadsheet, people)
        clear_auxiliary_sheets(spreadsheet)
        print("[sheets] таблица очищена (сотрудники: только шапка; Сводка / Bitbanker / Ошибки: пусто).")
    finally:
        conn.close()


# ─── VK ──────────────────────────────────────────────────────────────────────

def _vk_post_text(item: dict) -> str:
    """Текст поста VK; репосты часто только в copy_history."""
    text = (item.get("text") or "").strip()
    if text:
        return text
    for sub in item.get("copy_history") or []:
        if isinstance(sub, dict):
            t = (sub.get("text") or "").strip()
            if t:
                return t
    return ""


def resolve_vk_owner_id(raw: str, token: str, version: str) -> Optional[int]:
    raw = raw.strip()

    # club<id> → group, id<id> → user
    m_club = re.match(r"^club(\d+)$", raw, flags=re.IGNORECASE)
    if m_club:
        return -int(m_club.group(1))
    m_id = re.match(r"^id(\d+)$", raw, flags=re.IGNORECASE)
    if m_id:
        return int(m_id.group(1))
    if re.match(r"^-?\d+$", raw):
        return int(raw)

    # Otherwise try resolveScreenName
    try:
        resp = requests.get(
            "https://api.vk.com/method/utils.resolveScreenName",
            params={"access_token": token, "v": version, "screen_name": raw},
            timeout=30,
        ).json()
        result = resp.get("response")
        if not result:
            print(f"[vk] resolveScreenName returned nothing for: {raw}")
            return None
        oid = result.get("object_id")
        otype = result.get("type")
        if otype in ("group", "public"):
            return -int(oid)
        return int(oid)
    except Exception as exc:
        print(f"[vk] resolve failed for {raw}: {exc}")
        return None


def fetch_vk_posts(
    person_name: str,
    account: str,
    limit: int = 20,
    full_history: bool = False,
    errors: Optional[List[ErrorRecord]] = None,
    history_cap: Optional[int] = None,
) -> List[PostRecord]:
    token = os.getenv("VK_ACCESS_TOKEN")
    version = os.getenv("VK_API_VERSION", "5.199")
    if not token:
        raise RuntimeError("Missing VK_ACCESS_TOKEN in .env")

    owner_id = resolve_vk_owner_id(account, token, version)
    if owner_id is None:
        print(f"[vk] could not resolve: {account}")
        if errors is not None:
            _append_error(errors, person_name, "vk", account, "resolve_failed", "Не удалось определить owner_id")
        return []

    fetched_at = now_iso()
    posts: List[PostRecord] = []
    offset = 0
    page_size = 100 if full_history else max(limit, 20)
    while True:
        try:
            resp = requests.get(
                "https://api.vk.com/method/wall.get",
                params={
                    "access_token": token,
                    "v": version,
                    "owner_id": owner_id,
                    "count": page_size,
                    "offset": offset,
                },
                timeout=30,
            ).json()
        except Exception as exc:
            print(f"[vk] request failed for {account}: {exc}")
            if errors is not None:
                _append_error(errors, person_name, "vk", account, "request_failed", str(exc))
            break

        if "error" in resp:
            print(f"[vk] api error for {account}: {resp['error']}")
            if errors is not None:
                _append_error(errors, person_name, "vk", account, "api_error", str(resp["error"]))
            break
        items = resp.get("response", {}).get("items", [])
        if not items:
            break
        for item in items:
            text = _vk_post_text(item)
            if not text:
                text = "(без текста)"
            post_id = str(item["id"])
            owner = item["owner_id"]
            published_at = datetime.fromtimestamp(item.get("date", 0), tz=timezone.utc).isoformat()
            posts.append(PostRecord(
                person_name=person_name,
                platform="vk",
                account=account,
                post_id=post_id,
                text=text,
                url=f"https://vk.com/wall{owner}_{post_id}",
                published_at=published_at,
                fetched_at=fetched_at,
                views=str((item.get("views") or {}).get("count", "")),
            ))
            if not full_history and len(posts) >= limit:
                return posts
            if full_history and history_cap is not None and len(posts) >= history_cap:
                return posts
        if not full_history:
            break
        if len(items) < page_size:
            break
        offset += len(items)

    return sort_posts_newest_first(posts)


# ─── Instagram (instaloader) ─────────────────────────────────────────────────

_INSTALOADER: Optional[instaloader.Instaloader] = None


def _get_instaloader() -> Optional[instaloader.Instaloader]:
    """Единый экземпляр Instaloader. Авторизация через sessionid из браузера — пароль не нужен."""
    global _INSTALOADER
    if _INSTALOADER is not None:
        return _INSTALOADER

    username = os.getenv("INSTAGRAM_USERNAME", "").strip()
    raw_sid = os.getenv("INSTAGRAM_SESSION_ID", "").strip()

    if not username:
        print("[instagram] Укажи INSTAGRAM_USERNAME в .env (логин аккаунта, от которого взят sessionid).")
        return None
    if not raw_sid:
        print("[instagram] Укажи INSTAGRAM_SESSION_ID в .env — Instagram пропущен.")
        return None

    session_id = unquote(raw_sid)

    L = instaloader.Instaloader(
        download_pictures=False,
        download_videos=False,
        download_video_thumbnails=False,
        download_geotags=False,
        download_comments=False,
        save_metadata=False,
        compress_json=False,
        quiet=True,
        max_connection_attempts=5,
        iphone_support=True,
    )

    # Вставляем куки прямо в сессию instaloader — без пароля и 2FA
    L.context._session.cookies.update({
        "sessionid": session_id,
        "ds_user_id": session_id.split(":")[0] if ":" in session_id else "",
    })
    # Сообщаем instaloader какой логин у сессии
    L.context._username = username

    print(f"[instagram] сессия загружена для @{username} (через sessionid-куку)")
    _INSTALOADER = L
    return L


def fetch_instagram_posts(
    person_name: str,
    account: str,
    limit: int = 20,
    full_history: bool = False,
    errors: Optional[List[ErrorRecord]] = None,
    history_cap: Optional[int] = None,
) -> List[PostRecord]:
    account = (account or "").strip().lstrip("@").lower()
    if not account:
        return []

    L = _get_instaloader()
    if L is None:
        if errors is not None:
            _append_error(errors, person_name, "instagram", account, "no_auth",
                          "Нет INSTAGRAM_USERNAME/PASSWORD в .env")
        return []

    fetched_at = now_iso()
    max_items = (history_cap if history_cap else 500) if full_history else limit

    # Получаем user_id через iphone API (без GraphQL)
    try:
        data = L.context.get_iphone_json(
            "api/v1/users/web_profile_info/",
            params={"username": account}
        )
        user_info = data.get("data", {}).get("user") or {}
        user_id = user_info.get("id")
        if not user_id:
            raise ValueError("user_id не найден")
    except Exception as exc:
        print(f"[instagram] не удалось получить профиль @{account}: {exc}")
        if errors is not None:
            _append_error(errors, person_name, "instagram", account, "profile_error", str(exc)[:300])
        return []

    # Получаем посты через iphone feed API — без GraphQL
    posts: List[PostRecord] = []
    max_id = None
    max_pages = 150 if full_history else 3

    for page in range(max_pages):
        params: dict = {"count": min(50, max_items)}
        if max_id:
            params["max_id"] = max_id
        try:
            feed = L.context.get_iphone_json(
                f"api/v1/feed/user/{user_id}/",
                params=params
            )
        except Exception as exc:
            print(f"[instagram] feed error @{account} стр.{page+1}: {exc}")
            if errors is not None:
                _append_error(errors, person_name, "instagram", account, "feed_error", str(exc)[:300])
            break

        items = feed.get("items", [])
        if not items:
            break

        for item in items:
            cap_obj = item.get("caption") or {}
            caption = (cap_obj.get("text", "") if isinstance(cap_obj, dict) else "").strip()
            code = item.get("code", "") or item.get("shortcode", "")
            if not code:
                continue
            caption = caption or "(медиа без подписи)"
            post_id = str(item.get("pk") or item.get("id") or code)
            ts = item.get("taken_at", 0)
            views = (
                item.get("play_count") or item.get("video_view_count") or
                item.get("view_count") or ""
            )
            published_at = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else fetched_at
            posts.append(PostRecord(
                person_name=person_name,
                platform="instagram",
                account=account,
                post_id=post_id,
                text=caption,
                url=f"https://www.instagram.com/p/{code}/",
                published_at=published_at,
                fetched_at=fetched_at,
                views=str(views),
            ))
            if len(posts) >= max_items:
                print(f"[instagram] @{account}: достигнут лимит {max_items} постов")
                return sort_posts_newest_first(posts)

        if not full_history:
            break
        max_id = feed.get("next_max_id")
        if not max_id:
            break

    print(f"[instagram] @{account}: собрано {len(posts)} постов")
    return sort_posts_newest_first(posts)


# ─── Google Sheets ────────────────────────────────────────────────────────────

def _open_worksheet() -> tuple:
    sheet_name = os.getenv("GOOGLE_SHEET_NAME")
    worksheet_name = os.getenv("GOOGLE_WORKSHEET_NAME", "Posts")
    if not sheet_name:
        raise RuntimeError("Missing GOOGLE_SHEET_NAME in .env")
    client = gspread.service_account(
        filename=os.getenv("GOOGLE_SHEETS_CREDENTIALS_PATH", "service_account.json")
    )
    spreadsheet = client.open(sheet_name)
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=worksheet_name, rows=5000, cols=len(HEADER)
        )
    return spreadsheet, worksheet


def _open_spreadsheet():
    sheet_name = os.getenv("GOOGLE_SHEET_NAME")
    if not sheet_name:
        raise RuntimeError("Missing GOOGLE_SHEET_NAME in .env")
    client = gspread.service_account(
        filename=os.getenv("GOOGLE_SHEETS_CREDENTIALS_PATH", "service_account.json")
    )
    return client.open(sheet_name)


def _sanitize_sheet_title(name: str) -> str:
    title = unicodedata.normalize("NFKC", name).strip()
    title = re.sub(r"[\[\]\*\?/\\:]", " ", title)
    title = re.sub(r"\s+", " ", title)
    return title[:95] if len(title) > 95 else title


def setup_sheet_format(spreadsheet, worksheet) -> None:
    ws_id = worksheet.id
    n_cols = len(HEADER)

    # Batch: freeze row, column widths, row height for header
    col_width_reqs = [
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": ws_id,
                    "dimension": "COLUMNS",
                    "startIndex": i,
                    "endIndex": i + 1,
                },
                "properties": {"pixelSize": COL_WIDTHS[i]},
                "fields": "pixelSize",
            }
        }
        for i in range(n_cols)
    ]
    spreadsheet.batch_update({
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": ws_id,
                        "gridProperties": {"frozenRowCount": 1},
                    },
                    "fields": "gridProperties.frozenRowCount",
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": ws_id, "dimension": "ROWS", "startIndex": 0, "endIndex": 1},
                    "properties": {"pixelSize": 36},
                    "fields": "pixelSize",
                }
            },
        ] + col_width_reqs
    })

    # Header row style — тёмно-синий, белый жирный текст
    worksheet.format(f"A1:{chr(64 + n_cols)}1", {
        "backgroundColor": {"red": 0.118, "green": 0.278, "blue": 0.569},
        "textFormat": {
            "bold": True,
            "fontSize": 11,
            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
        },
        "horizontalAlignment": "CENTER",
        "verticalAlignment": "MIDDLE",
    })

    # Wrap text for "Текст поста" (col F = index 5)
    worksheet.format("F2:F5000", {"wrapStrategy": "WRAP"})

    # Conditional formatting — цвет всей строки по платформе (col C = index 2)
    cond_requests = []
    for idx, (platform, color) in enumerate(PLATFORM_COLORS.items()):
        cond_requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{
                        "sheetId": ws_id,
                        "startRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": n_cols,
                    }],
                    "booleanRule": {
                        "condition": {
                            "type": "CUSTOM_FORMULA",
                            "values": [{"userEnteredValue": f'=$C2="{platform}"'}],
                        },
                        "format": {"backgroundColor": color},
                    },
                },
                "index": idx,
            }
        })
    if cond_requests:
        spreadsheet.batch_update({"requests": cond_requests})

    print("[sheets] форматирование таблицы применено")


def _to_display_text(text: str, url: str, max_len: int = 180) -> str:
    cleaned = " ".join((text or "").split())
    if len(cleaned) > max_len:
        cleaned = cleaned[: max_len - 1] + "…"
    return f"{cleaned}\n{url}".strip()


def _open_or_create_worksheet(spreadsheet, title: str, rows: int, cols: int):
    try:
        return _call_with_retry(
            lambda: spreadsheet.worksheet(title),
            f"worksheet get {title}",
            max_attempts=15,
            base_sleep=10,
        )
    except gspread.WorksheetNotFound:
        return _call_with_retry(
            lambda: spreadsheet.add_worksheet(title=title, rows=rows, cols=cols),
            f"worksheet add {title}",
            max_attempts=15,
            base_sleep=10,
        )


def _delete_legacy_sheets(spreadsheet) -> None:
    for title in ("Sheet1", "Posts", "Лист1", "лист1"):
        try:
            ws = _call_with_retry(
                lambda t=title: spreadsheet.worksheet(t),
                f"worksheet get legacy {title}",
                max_attempts=6,
                base_sleep=5,
            )
            _call_with_retry(
                lambda w=ws: spreadsheet.del_worksheet(w),
                f"del legacy sheet {title}",
                max_attempts=6,
                base_sleep=5,
            )
            print(f"[sheets] удален лист '{title}'")
        except gspread.WorksheetNotFound:
            pass
        except Exception as exc:
            print(f"[sheets] не удалось удалить '{title}': {exc}")


def _format_person_sheet(ws) -> None:
    # Remove existing conditional rules to avoid duplicates.
    try:
        ss = ws.spreadsheet
        meta = ss.fetch_sheet_metadata()
        rules_count = 0
        for s in meta.get("sheets", []):
            if s.get("properties", {}).get("sheetId") == ws.id:
                rules_count = len(s.get("conditionalFormats", []))
                break
        if rules_count:
            ss.batch_update(
                {
                    "requests": [
                        {"deleteConditionalFormatRule": {"sheetId": ws.id, "index": 0}}
                        for _ in range(rules_count)
                    ]
                }
            )
    except Exception:
        pass

    ws.freeze(rows=1)
    ws.format(
        "A1:H1",
        {
            "backgroundColor": {"red": 0.118, "green": 0.278, "blue": 0.569},
            "textFormat": {
                "bold": True,
                "fontSize": 11,
                "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
            },
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
        },
    )
    # Large row span so formatting applies after long history imports.
    _fmt_end = 100000
    ws.format(f"E2:E{_fmt_end}", {"wrapStrategy": "WRAP"})
    ws.format(f"A2:H{_fmt_end}", {"verticalAlignment": "TOP"})
    ws.format(f"A2:D{_fmt_end}", {"horizontalAlignment": "CENTER"})
    # Wider columns for readability
    ws.spreadsheet.batch_update(
        {
            "requests": [
                {
                    "updateDimensionProperties": {
                        "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
                        "properties": {"pixelSize": 185},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 4},
                        "properties": {"pixelSize": 170},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 4, "endIndex": 5},
                        "properties": {"pixelSize": 520},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 5, "endIndex": 6},
                        "properties": {"pixelSize": 360},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 6, "endIndex": 8},
                        "properties": {"pixelSize": 180},
                        "fields": "pixelSize",
                    }
                },
            ]
        }
    )
    # Color rows by platform (column B)
    color_rules = []
    for i, (platform, color) in enumerate(PLATFORM_COLORS.items()):
        color_rules.append(
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [
                            {
                                "sheetId": ws.id,
                                "startRowIndex": 1,
                                "startColumnIndex": 0,
                                "endColumnIndex": 8,
                            }
                        ],
                        "booleanRule": {
                            "condition": {
                                "type": "CUSTOM_FORMULA",
                                "values": [{"userEnteredValue": f'=$B2="{platform}"'}],
                            },
                            "format": {"backgroundColor": color},
                        },
                    },
                    "index": i,
                }
            }
        )
    ws.spreadsheet.batch_update({"requests": color_rules})


def _style_summary_sheet(ws) -> None:
    ws.freeze(rows=1)
    ws.format(
        "A1:F1",
        {
            "backgroundColor": {"red": 0.118, "green": 0.278, "blue": 0.569},
            "textFormat": {
                "bold": True,
                "fontSize": 11,
                "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
            },
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
        },
    )


def _rename_worksheet(ws, new_title: str) -> None:
    if ws.title == new_title:
        return
    ws.spreadsheet.batch_update(
        {
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {"sheetId": ws.id, "title": new_title},
                        "fields": "title",
                    }
                }
            ]
        }
    )


def _sheet_data_row_count(ws) -> int:
    try:
        vals = ws.get_all_values()
        return max(0, len(vals) - 1)
    except Exception:
        return 0


def _ensure_person_sheets(spreadsheet, people: List[Dict]) -> Dict[str, object]:
    result = {}
    existing = {ws.title: ws for ws in spreadsheet.worksheets()}
    for person in people:
        name = person.get("person_name", "Unknown")
        title_primary = _sanitize_sheet_title(name)
        full_key = str(person.get("person_name_full", "") or "").strip()
        title_legacy = _sanitize_sheet_title(full_key) if full_key else ""
        if title_legacy == title_primary:
            title_legacy = ""

        ws_leg = existing.get(title_legacy) if title_legacy else None
        ws_pri = existing.get(title_primary)
        if ws_leg and ws_pri and ws_leg is not ws_pri:
            rc_leg = _sheet_data_row_count(ws_leg)
            rc_pri = _sheet_data_row_count(ws_pri)
            # Раньше создавался второй лист по короткому ФИО, данные остались на полном названии.
            if rc_pri <= 1 and rc_leg > rc_pri:
                try:
                    spreadsheet.del_worksheet(ws_pri)
                    print(f"[sheets] удалён пустой дубликат листа «{title_primary}», используется «{title_legacy}»")
                except Exception as exc:
                    print(f"[sheets] не удалось удалить дубликат листа «{title_primary}»: {exc}")
                existing.pop(title_primary, None)
                ws_pri = None
            elif rc_leg <= 1 and rc_pri > rc_leg:
                try:
                    spreadsheet.del_worksheet(ws_leg)
                    print(f"[sheets] удалён пустой лист «{title_legacy}», остаётся «{title_primary}»")
                except Exception as exc:
                    print(f"[sheets] не удалось удалить лист «{title_legacy}»: {exc}")
                else:
                    existing.pop(title_legacy, None)
                    ws_leg = None
            else:
                # Оба листа с именем — каноническое короткое имя уже занято листом ws_pri;
                # переименовать ws_leg нельзя. Оставляем ws_pri, дубль по полному ФИО удаляем.
                try:
                    spreadsheet.del_worksheet(ws_leg)
                    print(
                        f"[sheets] удалён дублирующий лист «{title_legacy}», "
                        f"данные ведём на «{title_primary}»"
                    )
                except Exception as exc:
                    print(f"[sheets] не удалось удалить дубль «{title_legacy}»: {exc}")
                else:
                    existing.pop(title_legacy, None)
                    ws_leg = None

        ws = ws_leg if ws_leg is not None else ws_pri

        # ── Поиск устаревшего листа по старому короткому имени ────────────────
        # Например: имя изменили с «Копарушкина Анна» (→ «Копарушкина А.»)
        # на «Копарушкина Анна Ильгисовна» (→ «Копарушкина А.И.»).
        # Такой лист не попадёт ни в ws_leg, ни в ws_pri и будет пустым orphan-ом.
        if ws is None:
            surname = title_primary.split()[0] if title_primary else ""
            if surname:
                # Ищем лист вида «Фамилия И.» или «Фамилия И.О.» (одна или две инициали)
                _old_short_re = re.compile(
                    r"^" + re.escape(surname) + r"\s+[А-ЯЁA-Z]\.([А-ЯЁA-Z]\.)?\s*$"
                )
                for ex_title in list(existing.keys()):
                    if ex_title in (title_primary, title_legacy):
                        continue
                    if _old_short_re.match(ex_title):
                        old_ws = existing[ex_title]
                        print(
                            f"[sheets] найден устаревший лист «{ex_title}» "
                            f"→ переименовываем в «{title_primary}»"
                        )
                        try:
                            _call_with_retry(
                                lambda w=old_ws, t=title_primary: _rename_worksheet(w, t),
                                f"rename orphan {ex_title!r} -> {title_primary!r}",
                                max_attempts=6,
                                base_sleep=4,
                            )
                            existing.pop(ex_title, None)
                            existing[title_primary] = old_ws
                            ws = old_ws
                        except Exception as exc:
                            print(f"[sheets] не удалось переименовать устаревший лист: {exc}")
                        break

        if ws is None:
            ws = spreadsheet.add_worksheet(title=title_primary, rows=100000, cols=8)
            existing[title_primary] = ws
        elif ws.title != title_primary:
            old_t = ws.title
            _call_with_retry(
                lambda: _rename_worksheet(ws, title_primary),
                f"rename sheet {old_t!r} -> {title_primary!r}",
                max_attempts=6,
                base_sleep=4,
            )
            existing.pop(old_t, None)
            existing[title_primary] = ws
        if ws.row_values(1) != PERSON_HEADER:
            # Migrate header without losing existing history.
            existing_rows = ws.get_all_values()[1:]
            _call_with_retry(lambda: ws.clear(), f"{ws.title} clear", max_attempts=6, base_sleep=4)
            _call_with_retry(
                lambda: ws.update(range_name="A1:H1", values=[PERSON_HEADER]),
                f"{ws.title} header update",
                max_attempts=8,
                base_sleep=4,
            )
            if existing_rows:
                migrated = []
                for row in existing_rows:
                    row8 = (row + ["", ""])[:8]
                    # If old format had 7 columns, insert empty views before post_id.
                    if len(row) == 7:
                        row8 = row[:6] + [""] + row[6:7]
                    migrated.append(row8)
                _call_with_retry(
                    lambda: ws.update(range_name=f"A2:H{len(migrated)+1}", values=migrated),
                    f"{ws.title} migrate rows",
                    max_attempts=8,
                    base_sleep=4,
                )
            try:
                _format_person_sheet(ws)
            except Exception:
                # If quota is tight, keep the sheet usable and skip styling.
                pass
        result[name] = ws
    return result


def restyle_all_person_sheets(spreadsheet, people: List[Dict]) -> None:
    ws_map = _ensure_person_sheets(spreadsheet, people)
    for ws in ws_map.values():
        try:
            _format_person_sheet(ws)
        except Exception as exc:
            print(f"[sheets] не удалось обновить стиль листа '{ws.title}': {exc}")


def _read_person_data_batch(spreadsheet, ws_map: Dict[str, object]) -> Dict[str, List[List[str]]]:
    """Read all data rows (A:H from row 2) per person sheet. No row cap — A2:H5000
    caused missing rows for Bitbanker/Сводка and wrong append row after 4999 posts."""
    items = list(ws_map.items())
    if not items:
        return {}
    result: Dict[str, List[List[str]]] = {}
    # Sheets API allows at most 100 ranges per values.batchGet.
    for start in range(0, len(items), 100):
        chunk = items[start : start + 100]
        ranges = []
        for _, ws in chunk:
            safe_title = ws.title.replace("'", "''")
            ranges.append(f"'{safe_title}'!A2:H")
        resp = spreadsheet.values_batch_get(ranges)
        value_ranges = resp.get("valueRanges", [])
        for idx, (person_name, _) in enumerate(chunk):
            block = value_ranges[idx].get("values", []) if idx < len(value_ranges) else []
            result[person_name] = block
    return result


def _row_post_key(row: List[str]) -> Optional[tuple]:
    """Ключ строки листа: платформа, аккаунт, id поста (id глобально не уникален между стенами VK)."""
    if len(row) >= 8:
        return (row[1], row[2], row[7])
    if len(row) >= 7:
        return (row[1], row[2], row[6])
    return None


def _existing_sheet_post_keys(all_existing: Dict[str, List[List[str]]]) -> set:
    keys: set = set()
    for rows in all_existing.values():
        for row in rows:
            k = _row_post_key(row)
            if k:
                keys.add(k)
    return keys


def append_to_person_sheets(spreadsheet, people: List[Dict], posts: List[PostRecord]) -> None:
    if not posts:
        return

    ws_map = _ensure_person_sheets(spreadsheet, people)

    # ── Step 1: read ALL existing post IDs in one batch request ──────────────
    all_existing = _call_with_retry(
        lambda: _read_person_data_batch(spreadsheet, ws_map),
        "batch read existing rows",
        max_attempts=10,
        base_sleep=6,
    )

    # ── Step 2: group new posts by person, filter duplicates in memory ────────
    grouped: Dict[str, List[PostRecord]] = {}
    for p in posts:
        grouped.setdefault(p.person_name, []).append(p)

    # ── Step 3: build one batch-update payload for ALL sheets at once ─────────
    # Google allows writing to multiple ranges in a single API call.
    CHUNK = 500          # max rows per values_batch_update data entry
    BATCH_SIZE = 7       # sheets per single API call (conservative)
    person_names = list(grouped.keys())

    def _build_row(p: PostRecord) -> List[str]:
        return [
            p.fetched_at, p.platform, p.account,
            p.published_at, p.text, p.url, p.views, p.post_id,
        ]

    # Collect (ws, new_rows) per person
    write_plan: List[tuple] = []
    for person_name, items in grouped.items():
        ws = ws_map.get(person_name)
        if ws is None:
            continue
        existing_data = all_existing.get(person_name, [])
        existing_keys = set()
        for row in existing_data:
            k = _row_post_key(row)
            if k:
                existing_keys.add(k)
        fresh_posts = [
            p for p in items
            if (p.platform, p.account, p.post_id) not in existing_keys
        ]
        fresh_posts = sort_posts_newest_first(fresh_posts)
        new_rows = [_build_row(p) for p in fresh_posts]
        if new_rows:
            next_row = len(existing_data) + 2  # +1 header, +1 1-based
            write_plan.append((ws, next_row, new_rows))

    if not write_plan:
        return

    # ── Step 4: send in batches of BATCH_SIZE sheets per API call ─────────────
    def _flush_batch(batch_data: List[dict]) -> None:
        if not batch_data:
            return
        _call_with_retry(
            lambda: spreadsheet.values_batch_update({
                "valueInputOption": "RAW",
                "data": batch_data,
            }),
            f"values_batch_update ({len(batch_data)} ranges)",
            max_attempts=12,
            base_sleep=6,
        )
        time.sleep(1.5)

    batch_data: List[dict] = []
    total_written = 0

    for ws, next_row, new_rows in write_plan:
        safe_title = ws.title.replace("'", "''")
        # Split into CHUNK-size slices to stay within cell limits
        for offset in range(0, len(new_rows), CHUNK):
            chunk = new_rows[offset: offset + CHUNK]
            end_row = next_row + offset + len(chunk) - 1
            range_str = f"'{safe_title}'!A{next_row + offset}:H{end_row}"
            batch_data.append({"range": range_str, "values": chunk})
            total_written += len(chunk)

            if len(batch_data) >= BATCH_SIZE:
                _flush_batch(batch_data)
                batch_data = []

    # Flush remaining
    _flush_batch(batch_data)
    print(f"[sheets] записано {total_written} строк в листы сотрудников")


def update_summary_sheet(spreadsheet, people: List[Dict]) -> None:
    ws = _open_or_create_worksheet(
        spreadsheet, title="Сводка", rows=max(200, len(people) + 20), cols=5
    )
    # Remove existing conditional formatting rules to avoid duplicates.
    try:
        meta = spreadsheet.fetch_sheet_metadata()
        rules_count = 0
        for s in meta.get("sheets", []):
            props = s.get("properties", {})
            if props.get("sheetId") == ws.id:
                rules_count = len(s.get("conditionalFormats", []))
                break
        if rules_count:
            spreadsheet.batch_update(
                {
                    "requests": [
                        {"deleteConditionalFormatRule": {"sheetId": ws.id, "index": 0}}
                        for _ in range(rules_count)
                    ]
                }
            )
    except Exception:
        pass

    ws_map = _ensure_person_sheets(spreadsheet, people)
    all_person_rows = _read_person_data_batch(spreadsheet, ws_map)
    today_utc = datetime.now(timezone.utc).date()
    rows = []
    for person in people:
        name = person.get("person_name", "Unknown")
        data = all_person_rows.get(name, [])
        vk_count = sum(1 for r in data if len(r) > 1 and r[1] == "vk")
        ig_count = sum(1 for r in data if len(r) > 1 and r[1] == "instagram")
        total = vk_count + ig_count
        latest_ts = max((r[3] for r in data if len(r) > 3 and r[3]), default="-")
        posted_today = False
        for r in data:
            if len(r) > 3 and r[3]:
                try:
                    dt = datetime.fromisoformat(r[3].replace("Z", "+00:00"))
                    if dt.date() == today_utc:
                        posted_today = True
                        break
                except Exception:
                    pass
        status_today = "Есть пост сегодня" if posted_today else "Нет поста сегодня"
        rows.append([name, total, vk_count, ig_count, latest_ts, status_today])

    # Most active first
    rows.sort(key=lambda r: int(r[1]), reverse=True)

    _call_with_retry(lambda: ws.clear(), "Сводка clear", max_attempts=8, base_sleep=5)
    _call_with_retry(
        lambda: ws.update(range_name="A1:F1", values=[SUMMARY_HEADER]),
        "Сводка header",
        max_attempts=8,
        base_sleep=5,
    )
    if rows:
        _call_with_retry(
            lambda: ws.update(range_name=f"A2:F{len(rows)+1}", values=rows),
            "Сводка rows",
            max_attempts=8,
            base_sleep=5,
        )

    ws_id = ws.id
    _call_with_retry(
        lambda: spreadsheet.batch_update(
        {
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": ws_id,
                            "gridProperties": {"frozenRowCount": 1},
                        },
                        "fields": "gridProperties.frozenRowCount",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": ws_id,
                            "dimension": "COLUMNS",
                            "startIndex": 0,
                            "endIndex": 1,
                        },
                        "properties": {"pixelSize": 280},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": ws_id,
                            "dimension": "COLUMNS",
                            "startIndex": 1,
                            "endIndex": 4,
                        },
                        "properties": {"pixelSize": 120},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": ws_id,
                            "dimension": "COLUMNS",
                            "startIndex": 4,
                            "endIndex": 5,
                        },
                        "properties": {"pixelSize": 180},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": ws_id,
                            "dimension": "COLUMNS",
                            "startIndex": 5,
                            "endIndex": 6,
                        },
                        "properties": {"pixelSize": 170},
                        "fields": "pixelSize",
                    }
                },
                {
                    "setBasicFilter": {
                        "filter": {
                            "range": {
                                "sheetId": ws_id,
                                "startRowIndex": 0,
                                "startColumnIndex": 0,
                                "endColumnIndex": 6,
                            }
                        }
                    }
                },
            ]
        }
        ),
        "Сводка layout",
        max_attempts=8,
        base_sleep=5,
    )
    _style_summary_sheet(ws)
    ws.format(
        f"A2:F{max(len(rows)+1, 2)}",
        {"wrapStrategy": "WRAP", "verticalAlignment": "TOP"},
    )
    ws.format(
        f"B2:D{max(len(rows)+1, 2)}",
        {"horizontalAlignment": "CENTER"},
    )
    ws.format(
        f"F2:F{max(len(rows)+1, 2)}",
        {"textFormat": {"bold": True}},
    )
    # Status highlighting
    ws.format(
        f"F2:F{max(len(rows)+1, 2)}",
        {
            "backgroundColorStyle": {"rgbColor": {"red": 1, "green": 1, "blue": 1}}
        },
    )
    _call_with_retry(
        lambda: spreadsheet.batch_update(
        {
            "requests": [
                {
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [
                                {
                                    "sheetId": ws_id,
                                    "startRowIndex": 1,
                                    "startColumnIndex": 5,
                                    "endColumnIndex": 6,
                                }
                            ],
                            "booleanRule": {
                                "condition": {
                                    "type": "TEXT_EQ",
                                    "values": [{"userEnteredValue": "Есть пост сегодня"}],
                                },
                                "format": {
                                    "backgroundColor": {"red": 0.843, "green": 0.941, "blue": 0.847}
                                },
                            },
                        },
                        "index": 0,
                    }
                },
                {
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [
                                {
                                    "sheetId": ws_id,
                                    "startRowIndex": 1,
                                    "startColumnIndex": 5,
                                    "endColumnIndex": 6,
                                }
                            ],
                            "booleanRule": {
                                "condition": {
                                    "type": "TEXT_EQ",
                                    "values": [{"userEnteredValue": "Нет поста сегодня"}],
                                },
                                "format": {
                                    "backgroundColor": {"red": 1.0, "green": 0.878, "blue": 0.878}
                                },
                            },
                        },
                        "index": 1,
                    }
                },
            ]
        }
        ),
        "Сводка rules",
        max_attempts=8,
        base_sleep=5,
    )
    print("[sheets] лист 'Сводка' обновлен")


def update_bitbanker_sheet(spreadsheet, people: List[Dict]) -> None:
    ws = _open_or_create_worksheet(
        spreadsheet, title="Bitbanker", rows=max(200, len(people) + 50), cols=7
    )
    header = [
        "Сотрудник",
        "Платформа",
        "Дата публикации",
        "Аккаунт",
        "Текст поста",
        "Просмотры",
        "Ссылка",
    ]

    ws_map = _ensure_person_sheets(spreadsheet, people)
    all_person_rows = _read_person_data_batch(spreadsheet, ws_map)
    rows = []
    for person in people:
        name = person.get("person_name", "Unknown")
        data = all_person_rows.get(name, [])
        for r in data:
            if len(r) >= 8:
                plat, acct, pub, text = r[1], r[2], r[3], r[4] or ""
                url, views = r[5], r[6]
            elif len(r) >= 7:
                plat, acct, pub, text = r[1], r[2], r[3], r[4] or ""
                url, views = r[5], ""
            else:
                continue
            if "bitbanker" in text.lower():
                rows.append([name, plat, pub, acct, text, views, url])

    rows.sort(key=lambda x: x[2], reverse=True)
    _call_with_retry(lambda: ws.clear(), "Bitbanker clear", max_attempts=8, base_sleep=5)
    _call_with_retry(
        lambda: ws.update(range_name="A1:G1", values=[header]),
        "Bitbanker header",
        max_attempts=8,
        base_sleep=5,
    )
    if rows:
        _call_with_retry(
            lambda: ws.update(range_name=f"A2:G{len(rows)+1}", values=rows),
            "Bitbanker rows",
            max_attempts=8,
            base_sleep=5,
        )

    # Remove existing conditional rules on this sheet.
    try:
        meta = spreadsheet.fetch_sheet_metadata()
        rules_count = 0
        for s in meta.get("sheets", []):
            if s.get("properties", {}).get("sheetId") == ws.id:
                rules_count = len(s.get("conditionalFormats", []))
                break
        if rules_count:
            spreadsheet.batch_update(
                {
                    "requests": [
                        {"deleteConditionalFormatRule": {"sheetId": ws.id, "index": 0}}
                        for _ in range(rules_count)
                    ]
                }
            )
    except Exception:
        pass

    ws.freeze(rows=1)
    ws.format(
        "A1:G1",
        {
            "backgroundColor": {"red": 0.196, "green": 0.447, "blue": 0.196},
            "textFormat": {
                "bold": True,
                "fontSize": 11,
                "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
            },
            "horizontalAlignment": "CENTER",
        },
    )
    ws.format(
        f"A2:G{max(len(rows)+1, 2)}",
        {"wrapStrategy": "WRAP", "verticalAlignment": "TOP"},
    )
    ws.format(f"E2:E{max(len(rows)+1, 2)}", {"wrapStrategy": "WRAP"})
    _call_with_retry(
        lambda: spreadsheet.batch_update(
        {
            "requests": [
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": ws.id,
                            "dimension": "COLUMNS",
                            "startIndex": 0,
                            "endIndex": 1,
                        },
                        "properties": {"pixelSize": 240},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": ws.id,
                            "dimension": "COLUMNS",
                            "startIndex": 1,
                            "endIndex": 4,
                        },
                        "properties": {"pixelSize": 170},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": ws.id,
                            "dimension": "COLUMNS",
                            "startIndex": 4,
                            "endIndex": 5,
                        },
                        "properties": {"pixelSize": 540},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": ws.id,
                            "dimension": "COLUMNS",
                            "startIndex": 5,
                            "endIndex": 6,
                        },
                        "properties": {"pixelSize": 150},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": ws.id,
                            "dimension": "COLUMNS",
                            "startIndex": 6,
                            "endIndex": 7,
                        },
                        "properties": {"pixelSize": 360},
                        "fields": "pixelSize",
                    }
                },
            ]
        }
        ),
        "Bitbanker layout",
        max_attempts=8,
        base_sleep=5,
    )
    # Color rows by platform (column B)
    color_rules = []
    for i, (platform, color) in enumerate(PLATFORM_COLORS.items()):
        color_rules.append(
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [
                            {
                                "sheetId": ws.id,
                                "startRowIndex": 1,
                                "startColumnIndex": 0,
                                "endColumnIndex": 7,
                            }
                        ],
                        "booleanRule": {
                            "condition": {
                                "type": "CUSTOM_FORMULA",
                                "values": [{"userEnteredValue": f'=$B2="{platform}"'}],
                            },
                            "format": {"backgroundColor": color},
                        },
                    },
                    "index": i,
                }
            }
        )
    _call_with_retry(
        lambda: spreadsheet.batch_update({"requests": color_rules}),
        "Bitbanker rules",
        max_attempts=8,
        base_sleep=5,
    )
    print(f"[sheets] лист 'Bitbanker' обновлен ({len(rows)} совпадений)")


def update_errors_sheet(spreadsheet, errors: List[ErrorRecord]) -> None:
    ws = _open_or_create_worksheet(spreadsheet, title="Ошибки", rows=1000, cols=6)
    header = ["Дата проверки", "Сотрудник", "Платформа", "Аккаунт", "Тип ошибки", "Детали"]
    _call_with_retry(lambda: ws.clear(), "Ошибки clear", max_attempts=8, base_sleep=5)
    _call_with_retry(
        lambda: ws.update(range_name="A1:F1", values=[header]),
        "Ошибки header",
        max_attempts=8,
        base_sleep=5,
    )

    if errors:
        rows = [
            [e.fetched_at, e.person_name, e.platform, e.account, e.error_type, e.details]
            for e in errors
        ]
        _call_with_retry(
            lambda: ws.update(range_name=f"A2:F{len(rows)+1}", values=rows),
            "Ошибки rows",
            max_attempts=8,
            base_sleep=5,
        )
    else:
        _call_with_retry(
            lambda: ws.update(
                range_name="A2:F2",
                values=[[now_iso(), "-", "-", "-", "ok", "Ошибок в последнем запуске нет"]],
            ),
            "Ошибки empty row",
            max_attempts=8,
            base_sleep=5,
        )

    ws.freeze(rows=1)
    ws.format(
        "A1:F1",
        {
            "backgroundColor": {"red": 0.580, "green": 0.114, "blue": 0.114},
            "textFormat": {
                "bold": True,
                "fontSize": 11,
                "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
            },
            "horizontalAlignment": "CENTER",
        },
    )
    ws.format("A2:F1000", {"wrapStrategy": "WRAP", "verticalAlignment": "TOP"})
    ws.format("A2:A1000", {"horizontalAlignment": "CENTER"})
    ws.format("C2:E1000", {"horizontalAlignment": "CENTER"})
    _call_with_retry(
        lambda: spreadsheet.batch_update(
        {
            "requests": [
                {
                    "updateDimensionProperties": {
                        "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
                        "properties": {"pixelSize": 170},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 2},
                        "properties": {"pixelSize": 220},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 2, "endIndex": 5},
                        "properties": {"pixelSize": 140},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 5, "endIndex": 6},
                        "properties": {"pixelSize": 450},
                        "fields": "pixelSize",
                    }
                },
            ]
        }
        ),
        "Ошибки layout",
        max_attempts=8,
        base_sleep=5,
    )
    print(f"[sheets] лист 'Ошибки' обновлен ({len(errors)} записей)")


def append_to_sheet(posts: List[PostRecord], spreadsheet=None, worksheet=None) -> None:
    if not posts:
        return

    if spreadsheet is None or worksheet is None:
        spreadsheet, worksheet = _open_worksheet()

    # Write header if missing or outdated
    current_header = worksheet.row_values(1)
    is_new = current_header != HEADER
    if is_new:
        _call_with_retry(
            lambda: worksheet.update(range_name="A1", values=[HEADER]),
            "Posts header",
            max_attempts=8,
            base_sleep=5,
        )
        setup_sheet_format(spreadsheet, worksheet)

    rows = [
        [
            p.fetched_at, p.person_name, p.platform, p.account,
            p.published_at, p.text, p.url, p.post_id,
        ]
        for p in posts
    ]
    _append_rows_safe(worksheet, rows, chunk_size=12)


# ─── Orchestration ────────────────────────────────────────────────────────────

def collect_posts(
    people: List[Dict],
    limit: int,
    full_history: bool = False,
    errors: Optional[List[ErrorRecord]] = None,
    per_platform_max: int = 500,
    sample_one_post_per_platform: bool = False,
) -> List[PostRecord]:
    cap = per_platform_max if full_history else None
    collected: List[PostRecord] = []
    for person in people:
        name = person.get("person_name", "Unknown")
        if sample_one_post_per_platform and not full_history:
            vk_got = 0
            for acc in person.get("vk", []) or []:
                if vk_got >= 1:
                    break
                chunk = fetch_vk_posts(
                    name,
                    acc,
                    limit=1,
                    full_history=False,
                    errors=errors,
                    history_cap=None,
                )
                collected.extend(chunk)
                if chunk:
                    vk_got += len(chunk)
            ig_got = 0
            for acc in person.get("instagram", []) or []:
                if ig_got >= 1:
                    break
                chunk = fetch_instagram_posts(
                    name,
                    acc,
                    limit=1,
                    full_history=False,
                    errors=errors,
                    history_cap=None,
                )
                collected.extend(chunk)
                if chunk:
                    ig_got += len(chunk)
        else:
            for acc in person.get("vk", []) or []:
                collected.extend(
                    fetch_vk_posts(
                        name,
                        acc,
                        limit=limit,
                        full_history=full_history,
                        errors=errors,
                        history_cap=cap,
                    )
                )
            for acc in person.get("instagram", []) or []:
                collected.extend(
                    fetch_instagram_posts(
                        name,
                        acc,
                        limit=limit,
                        full_history=full_history,
                        errors=errors,
                        history_cap=cap,
                    )
                )
    return collected


def run(
    config_path: str,
    limit: int,
    full_history: bool,
    dry_run: bool,
    reset: bool = False,
    per_platform_max: int = 500,
) -> None:
    people = load_accounts(config_path)
    conn = init_db()
    errors: List[ErrorRecord] = []
    try:
        spreadsheet = _open_spreadsheet()
        _delete_legacy_sheets(spreadsheet)
        delete_orphan_sheets(spreadsheet, people)
        if reset:
            clear_seen_posts(conn)
            clear_person_sheets_data(spreadsheet, people)
        if dry_run and not full_history:
            print(
                "[dry-run] Сбор постов: не более 1 поста VK и 1 Instagram на каждого "
                "(следующий аккаунт той же сети не опрашивается, если уже есть пост)."
            )
        collected = collect_posts(
            people,
            limit=limit,
            full_history=full_history,
            errors=errors,
            per_platform_max=per_platform_max,
            sample_one_post_per_platform=(dry_run and not full_history),
        )
        collected = dedupe_posts_in_memory(collected)
        if full_history:
            collected = apply_per_person_platform_cap(collected, per_platform_max)

        ws_map = _ensure_person_sheets(spreadsheet, people)
        all_existing = _call_with_retry(
            lambda: _read_person_data_batch(spreadsheet, ws_map),
            "batch read sheets for append",
            max_attempts=10,
            base_sleep=6,
        )
        sheet_keys = _existing_sheet_post_keys(all_existing)
        new_posts = [
            p for p in collected if (p.platform, p.account, p.post_id) not in sheet_keys
        ]
        new_posts = dedupe_posts_in_memory(new_posts)

        if not new_posts:
            print("Нет строк для дописывания — все посты из этого прогона уже есть на листах сотрудников.")
        else:
            print(f"К дописыванию на листы: {len(new_posts)} пост(ов):")
            for p in new_posts[:30]:
                print(f"  [{p.platform}] {p.person_name}: {p.url}")
            if len(new_posts) > 30:
                print(f"  … и ещё {len(new_posts) - 30}")

        if dry_run:
            print("Dry run — Google Sheets not updated.")
        else:
            if new_posts:
                append_to_person_sheets(spreadsheet, people, new_posts)
                print(f"Записано в Google Sheets строк: {len(new_posts)}.")
                time.sleep(4)
            update_summary_sheet(spreadsheet, people)
            time.sleep(4)
            update_bitbanker_sheet(spreadsheet, people)
            if collected:
                mark_all_seen(conn, collected)
            time.sleep(6)
            try:
                update_errors_sheet(spreadsheet, errors)
            except APIError as exc:
                if _is_quota_error(exc):
                    print(f"[sheets] лист «Ошибки» не обновлён (квота Google Sheets): {exc}")
                else:
                    raise
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Collect latest posts from VK/Instagram and write to Google Sheets."
    )
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument(
        "--limit", type=int, default=1,
        help="How many latest posts to fetch per account (default: 1).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Пробный прогон: не пишет в Sheets/state; собирает не более 1 поста VK и 1 Instagram на каждого.",
    )
    parser.add_argument(
        "--full-history",
        action="store_true",
        help="Один раз собрать максимальную доступную историю постов по аккаунтам.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Очистить state.db (seen_posts) и данные на листах сотрудников перед прогоном.",
    )
    parser.add_argument(
        "--per-platform-max",
        type=int,
        default=500,
        help="При --full-history: не больше N постов на сотрудника на одну соцсеть (по умолчанию 500).",
    )
    parser.add_argument(
        "--reformat", action="store_true",
        help="Переформатировать таблицу Google Sheets (запустить один раз при настройке).",
    )
    parser.add_argument(
        "--clear-only",
        action="store_true",
        help="Только очистить Google Sheets (листы сотрудников + Сводка + Bitbanker + Ошибки) и seen_posts в state.db.",
    )
    args = parser.parse_args()

    load_dotenv()

    if args.clear_only:
        run_clear_only(args.config)
    elif args.reformat:
        spreadsheet = _open_spreadsheet()
        _delete_legacy_sheets(spreadsheet)
        people = load_accounts(args.config)
        restyle_all_person_sheets(spreadsheet, people)
        update_summary_sheet(spreadsheet, people)
        update_bitbanker_sheet(spreadsheet, people)
        update_errors_sheet(spreadsheet, [])
    else:
        run(
            args.config,
            limit=args.limit,
            full_history=args.full_history,
            dry_run=args.dry_run,
            reset=args.reset,
            per_platform_max=args.per_platform_max,
        )
