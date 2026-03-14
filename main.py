import telebot
import requests
from bs4 import BeautifulSoup, NavigableString
from telebot import types
import threading
import time
import re
import io
import json
import os
from PIL import Image

# ─────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────

CONFIG_PATH     = os.path.join(os.path.dirname(__file__), 'config.json')
PROCESSED_PATH  = os.path.join(os.path.dirname(__file__), 'processed_ids.json')

def load_config() -> dict:
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

cfg = load_config()

TOKEN               = cfg['token']
MODERATION_GROUP_ID = cfg['moderation_group_id']
MY_CHANNEL_LINK     = cfg['my_channel_link']
MY_CHANNEL_NAME     = cfg.get('my_channel_name', 'Наш канал')
MY_CHANNEL_EMOJI    = cfg.get('my_channel_emoji', '📢')
SOURCE_CHANNELS     = cfg['source_channels']
MAX_POSTS           = cfg.get('max_posts_per_channel', 5)
CHECK_INTERVAL      = cfg.get('check_interval_seconds', 300)
CLEANUP_RULES       = cfg.get('cleanup_rules', {})
BLOCKED_KEYWORDS    = cfg.get('blocked_keywords', [])

# Поддержка нескольких каналов для публикации
# Формат в config.json:
# "target_channels": [
#   {"id": -100123, "name": "Канал 1"},
#   {"id": -100456, "name": "Канал 2"}
# ]
# Для обратной совместимости работает и старый target_channel_id
def load_target_channels() -> list:
    channels = cfg.get('target_channels', [])
    if channels:
        return channels
    # Фолбэк на старый формат
    old_id = cfg.get('target_channel_id')
    if old_id:
        return [{'id': old_id, 'name': cfg.get('my_channel_name', 'Канал')}]
    return []

TARGET_CHANNELS = load_target_channels()

bot = telebot.TeleBot(TOKEN, parse_mode='HTML')

# ─────────────────────────────────────────────────────────
#  PERSISTENT PROCESSED IDs
# ─────────────────────────────────────────────────────────

def load_processed_ids() -> set:
    if os.path.exists(PROCESSED_PATH):
        try:
            with open(PROCESSED_PATH, 'r', encoding='utf-8') as f:
                return set(json.load(f))
        except Exception:
            pass
    return set()

def save_processed_ids():
    try:
        with open(PROCESSED_PATH, 'w', encoding='utf-8') as f:
            # Храним только последние 10000 ID чтобы файл не рос бесконечно
            ids_list = list(processed_news_ids)[-10000:]
            json.dump(ids_list, f)
    except Exception as e:
        print(f"[ids] ошибка сохранения: {e}")

processed_news_ids: set = load_processed_ids()
print(f"[ids] Загружено {len(processed_news_ids)} обработанных постов")

news_cache: dict = {}

# Состояния ожидания ввода от пользователя
# pending_edit[user_id] = news_id  — ждём новый текст
# pending_photo[user_id] = news_id — ждём новое фото
pending_edit:  dict = {}
pending_photo: dict = {}

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/122.0.0.0 Safari/537.36'
    )
}

# Флаг для ручного запуска через /post
manual_check_event = threading.Event()


# ─────────────────────────────────────────────────────────
#  RELOAD CONFIG
# ─────────────────────────────────────────────────────────

def reload_config():
    global cfg, CLEANUP_RULES, MY_CHANNEL_LINK, MY_CHANNEL_EMOJI
    global MY_CHANNEL_NAME, BLOCKED_KEYWORDS, TARGET_CHANNELS
    try:
        cfg              = load_config()
        CLEANUP_RULES    = cfg.get('cleanup_rules', {})
        MY_CHANNEL_LINK  = cfg['my_channel_link']
        MY_CHANNEL_NAME  = cfg.get('my_channel_name', 'Наш канал')
        MY_CHANNEL_EMOJI = cfg.get('my_channel_emoji', '📢')
        BLOCKED_KEYWORDS = cfg.get('blocked_keywords', [])
        TARGET_CHANNELS  = load_target_channels()
    except Exception as e:
        print(f"[config] ошибка: {e}")


# ─────────────────────────────────────────────────────────
#  HTML TEXT EXTRACTION
# ─────────────────────────────────────────────────────────

def node_to_html(node) -> str:
    if isinstance(node, NavigableString):
        return str(node)

    tag = node.name.lower() if node.name else ''

    if tag == 'br':
        return '\n'
    if tag == 'img':
        return node.get('alt', '')

    inner = ''.join(node_to_html(c) for c in node.children)

    if tag == 'a':
        href = node.get('href', '')
        if re.search(r't\.me/', href, re.IGNORECASE):
            return ''   # убираем авторские ссылки вместе с текстом
        return f'<a href="{href}">{inner}</a>'
    if tag in ('b', 'strong'):
        return f'<b>{inner}</b>'
    if tag in ('i', 'em'):
        return f'<i>{inner}</i>'
    if tag in ('u', 'ins'):
        return f'<u>{inner}</u>'
    if tag in ('s', 'strike', 'del'):
        return f'<s>{inner}</s>'
    if tag == 'code':
        return f'<code>{inner}</code>'
    if tag == 'pre':
        return f'<pre>{inner}</pre>'
    if tag == 'p':
        return inner + '\n\n'   # абзац = двойной перенос
    if tag in ('div', 'section'):
        return inner + '\n'

    return inner


def extract_html_text(text_el) -> str:
    if not text_el:
        return ''
    result = ''.join(node_to_html(c) for c in text_el.children)
    result = re.sub(r'\n{3,}', '\n\n', result)
    return result.strip()


# ─────────────────────────────────────────────────────────
#  CLEANING
# ─────────────────────────────────────────────────────────

# Теги которые Telegram принимает в HTML parse_mode
TG_ALLOWED_TAGS = {'b', 'strong', 'i', 'em', 'u', 'ins', 's', 'strike',
                   'del', 'code', 'pre', 'a', 'tg-spoiler', 'blockquote'}

def sanitize_tg_html(html: str) -> str:
    """
    Убирает теги которые Telegram не поддерживает (tg-emoji и др.)
    и чинит незакрытые теги.
    Оставляет только безопасный набор тегов.
    """
    # Убираем tg-emoji — оставляем только текст внутри
    html = re.sub(r'<tg-emoji[^>]*>(.*?)</tg-emoji>', r'\1', html, flags=re.DOTALL)
    # Убираем span и другие неподдерживаемые теги, сохраняя содержимое
    html = re.sub(r'<(?!/?(?:b|strong|i|em|u|ins|s|strike|del|code|pre|a|tg-spoiler|blockquote)\b)[^>]+>', '', html)
    # Убираем пустые теги
    html = re.sub(r'<(b|i|u|s|code)>\s*</\1>', '', html)
    return html


WATERMARK_RE = re.compile(
    r'(?:https?://)?t\.me/[^\s,;)\]"\'<>\n]+',
    re.IGNORECASE
)
MENTION_RE = re.compile(r'@[A-Za-z0-9_]{3,}')


def build_channel_patterns(channel_name: str) -> list:
    phrases = (
        list(CLEANUP_RULES.get('_global', [])) +
        list(CLEANUP_RULES.get(channel_name, []))
    )
    patterns = []
    for phrase in phrases:
        if not phrase or phrase.startswith('_'):
            continue
        try:
            patterns.append(re.compile(re.escape(phrase.strip()), re.IGNORECASE))
        except re.error:
            pass
    return patterns


def channel_footer() -> str:
    return f'{MY_CHANNEL_EMOJI} <a href="{MY_CHANNEL_LINK}">{MY_CHANNEL_NAME}</a>'


def clean_html(html: str, channel_name: str = '') -> str:
    """
    Очищает HTML от авторских подписей и невалидных тегов.
    Сохраняет абзацы (двойные переносы строк).
    НЕ разрезает <pre>/<code> блоки.
    """
    channel_patterns = build_channel_patterns(channel_name)

    # Сначала санитизируем теги
    html = sanitize_tg_html(html)

    lines   = html.split('\n')
    cleaned = []
    in_pre  = False
    prev_empty = False  # для сохранения одного пустого разделителя между абзацами

    for line in lines:
        # Отслеживаем вход/выход из <pre>
        if '<pre>' in line.lower():
            in_pre = True
        if '</pre>' in line.lower():
            in_pre = False
            cleaned.append(line)
            prev_empty = False
            continue

        if in_pre:
            cleaned.append(line)
            continue

        # Строка содержит t.me → убираем всю строку
        if WATERMARK_RE.search(line):
            continue

        # Убираем @упоминания
        line = MENTION_RE.sub('', line)

        # Фразы из конфига
        for pat in channel_patterns:
            line = pat.sub('', line)

        line = line.strip()

        # Строка пустая → сохраняем как разделитель абзаца (только один раз подряд)
        if not line:
            if not prev_empty and cleaned:
                cleaned.append('')
            prev_empty = True
            continue

        # Строка только из эмодзи/пунктуации без текста — пропускаем
        if not re.search(r'[А-Яа-яA-Za-z0-9]', re.sub(r'<[^>]+>', '', line)):
            continue

        cleaned.append(line)
        prev_empty = False

    # Убираем пустые строки в конце
    while cleaned and cleaned[-1] == '':
        cleaned.pop()

    body = '\n'.join(cleaned).strip()
    # Максимум одна пустая строка между абзацами
    body = re.sub(r'\n{3,}', '\n\n', body)
    return f"{body}\n\n{channel_footer()}"
    return f"{body}\n\n{channel_footer()}"


def is_blocked(text: str) -> bool:
    """Возвращает True если пост содержит заблокированные слова."""
    text_lower = text.lower()
    for kw in BLOCKED_KEYWORDS:
        if kw.lower() in text_lower:
            print(f"[filter] заблокировано по слову: '{kw}'")
            return True
    return False


# ─────────────────────────────────────────────────────────
#  DOWNLOAD IMAGE
# ─────────────────────────────────────────────────────────

def download_image(url: str) -> bytes | None:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        img = Image.open(io.BytesIO(resp.content))
        if img.mode != 'RGB':
            img = img.convert('RGB')
        out = io.BytesIO()
        img.save(out, format='JPEG', quality=90)
        return out.getvalue()
    except Exception as e:
        print(f"[img] {url}: {e}")
        return None


def make_buf(raw: bytes, name='photo.jpg') -> io.BytesIO:
    buf = io.BytesIO(raw)
    buf.name = name
    return buf


# ─────────────────────────────────────────────────────────
#  DOWNLOAD VIDEO
# ─────────────────────────────────────────────────────────

def fetch_video_url_from_post(post_url: str) -> str | None:
    """
    Ищет прямой URL видео для поста.
    Стратегии:
    1. t.me/channel/id?embed=1 — embed-страница, там <video src>
    2. og:video в мета-тегах embed страницы
    3. Прямая страница поста, og:video
    """
    # Стратегия 1 и 2: embed-версия страницы (даёт video src без JS)
    embed_url = post_url + '?embed=1&mode=tme'
    print(f"[video] пробую embed: {embed_url}")
    try:
        resp = requests.get(embed_url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        # <video src="..."> или data-src
        for video_el in soup.find_all('video'):
            src = video_el.get('src') or video_el.get('data-src')
            if src:
                if not src.startswith('http'):
                    src = 'https:' + src if src.startswith('//') else None
                if src:
                    print(f"[video] найден <video src> в embed: {src[:80]}")
                    return src

        # <source src="...">
        for source in soup.find_all('source'):
            src = source.get('src', '')
            if src.startswith('http') and any(ext in src for ext in ('.mp4', '.webm', 'video')):
                print(f"[video] найден <source> в embed: {src[:80]}")
                return src

        # og:video в embed
        og = soup.find('meta', property='og:video')
        if og and og.get('content', '').startswith('http'):
            print(f"[video] найден og:video в embed: {og['content'][:80]}")
            return og['content']

        print(f"[video] embed не содержит видео, пробую прямую страницу")
    except Exception as e:
        print(f"[video] embed ошибка: {e}")

    # Стратегия 3: прямая страница поста
    print(f"[video] пробую прямую страницу: {post_url}")
    try:
        resp = requests.get(post_url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        for video_el in soup.find_all('video'):
            src = video_el.get('src') or video_el.get('data-src')
            if src and src.startswith('http'):
                print(f"[video] найден <video src> в прямой странице: {src[:80]}")
                return src

        og = soup.find('meta', property='og:video')
        if og and og.get('content', '').startswith('http'):
            print(f"[video] найден og:video в прямой странице: {og['content'][:80]}")
            return og['content']

        # Ищем любую ссылку на .mp4 в HTML
        mp4_match = re.search(r'(https?://[^\s"\']+\.mp4[^\s"\']*)', resp.text)
        if mp4_match:
            print(f"[video] найден .mp4 в HTML: {mp4_match.group(1)[:80]}")
            return mp4_match.group(1)

        print(f"[video] видео не найдено ни в одной стратегии для {post_url}")
    except Exception as e:
        print(f"[video] прямая страница ошибка: {e}")

    return None


def download_video(url: str) -> bytes | None:
    print(f"[video] скачиваю: {url[:80]}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=60, stream=True)
        resp.raise_for_status()
        ctype = resp.headers.get('Content-Type', '')
        size_header = resp.headers.get('Content-Length', '?')
        print(f"[video] Content-Type: {ctype}, Content-Length: {size_header}")

        if 'video' not in ctype and not any(url.split('?')[0].endswith(e) for e in ('.mp4', '.mov', '.webm')):
            print(f"[video] ❌ не видео по Content-Type: {ctype}")
            return None

        data = b''
        for chunk in resp.iter_content(512 * 1024):
            data += chunk
            if len(data) > 50 * 1024 * 1024:
                print(f"[video] ❌ файл >50MB, пропускаю")
                return None

        print(f"[video] ✅ скачано {len(data) // 1024}KB")
        return data
    except Exception as e:
        print(f"[video] ❌ ошибка скачивания: {e}")
        return None


# ─────────────────────────────────────────────────────────
#  RATE-LIMIT WRAPPER
# ─────────────────────────────────────────────────────────

def tg_call(func, *args, **kwargs):
    for attempt in range(5):
        try:
            return func(*args, **kwargs)
        except telebot.apihelper.ApiTelegramException as e:
            if e.error_code == 429:
                m = re.search(r'retry after (\d+)', str(e))
                wait = int(m.group(1)) + 2 if m else 30
                print(f"[429] жду {wait}с…")
                time.sleep(wait)
            else:
                raise
    return None


# ─────────────────────────────────────────────────────────
#  SCRAPING
# ─────────────────────────────────────────────────────────

def scrape_channel(channel_name: str) -> list:
    url = f'https://t.me/s/{channel_name}'
    print(f"[scrape] 🔍 GET {url}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        print(f"[scrape] ✅ {channel_name}: HTTP {resp.status_code}, {len(resp.content)//1024}KB")
    except Exception as e:
        print(f"[scrape] ❌ {channel_name}: {e}")
        return []

    soup    = BeautifulSoup(resp.text, 'html.parser')
    results = []

    for wrap in soup.find_all('div', class_='tgme_widget_message_wrap'):
        try:
            msg_div = wrap.find('div', class_='tgme_widget_message')
            if not msg_div:
                continue
            msg_id = msg_div.get('data-post', '')
            if not msg_id or msg_id in processed_news_ids:
                continue

            # Текст
            text_el  = wrap.find('div', class_='tgme_widget_message_text')
            raw_html = extract_html_text(text_el) if text_el else ''

            # Фото
            photo_urls = []
            for pw in wrap.find_all('a', class_='tgme_widget_message_photo_wrap'):
                m = re.search(r"background-image:url\('([^']+)'\)", pw.get('style', ''))
                if m and m.group(1).startswith('http') and m.group(1) not in photo_urls:
                    photo_urls.append(m.group(1))

            # Видео — проверяем наличие видео-блока
            has_video = bool(
                wrap.find('video') or
                wrap.find(class_=re.compile(r'tgme_widget_message_video(?!_thumb)')) or
                wrap.find('i', class_=re.compile(r'tgme_widget_message_video_thumb'))
            )

            original_url = f"https://t.me/{msg_id}"

            if not raw_html and not photo_urls and not has_video:
                continue

            # Фильтр по ключевым словам
            plain_text = re.sub(r'<[^>]+>', '', raw_html)
            if is_blocked(plain_text):
                processed_news_ids.add(msg_id)
                continue

            results.append({
                'id':            msg_id,
                'html':          raw_html,
                'photos':        photo_urls,
                'has_video':     has_video,
                'video_url':     None,
                'channel':       channel_name,
                'original_url':  original_url,
                '_photos_bytes': [],
                '_video_bytes':  None,
            })

        except Exception as e:
            print(f"[parse] {e}")
            continue

    return results[-MAX_POSTS:]


# ─────────────────────────────────────────────────────────
#  FETCH MEDIA
# ─────────────────────────────────────────────────────────

def fetch_media(item: dict):
    post_id = item['id']

    # Видео
    if item['has_video'] and not item['_video_bytes']:
        print(f"[media] 🎬 пост {post_id} содержит видео, ищу URL...")
        video_url = fetch_video_url_from_post(item['original_url'])
        if video_url:
            item['video_url']    = video_url
            item['_video_bytes'] = download_video(video_url)
            if item['_video_bytes']:
                print(f"[media] ✅ видео успешно скачано для {post_id}")
            else:
                print(f"[media] ❌ видео найдено но не скачалось для {post_id}")
        else:
            print(f"[media] ❌ URL видео не найден для {post_id} ({item['original_url']})")

    # Фото
    if item['photos'] and not item['_photos_bytes']:
        print(f"[media] 🖼 скачиваю {len(item['photos'])} фото для {post_id}...")
        downloaded = []
        for i, url in enumerate(item['photos']):
            raw = download_image(url)
            if raw:
                downloaded.append(raw)
                print(f"[media] ✅ фото {i+1}/{len(item['photos'])} скачано ({len(raw)//1024}KB)")
            else:
                print(f"[media] ❌ фото {i+1}/{len(item['photos'])} не скачалось: {url[:60]}")
        item['_photos_bytes'] = downloaded
        print(f"[media] итого скачано фото: {len(downloaded)}/{len(item['photos'])}")


# ─────────────────────────────────────────────────────────
#  MARKUP
# ─────────────────────────────────────────────────────────

def build_markup(news_id: str, original_url: str) -> types.InlineKeyboardMarkup:
    mk = types.InlineKeyboardMarkup(row_width=2)

    # Кнопка публикации для каждого канала
    if len(TARGET_CHANNELS) == 1:
        mk.add(types.InlineKeyboardButton(
            "✅ Опубликовать",
            callback_data=f"now0_{news_id}"
        ))
    else:
        for i, ch in enumerate(TARGET_CHANNELS):
            mk.add(types.InlineKeyboardButton(
                f"✅ → {ch['name']}",
                callback_data=f"now{i}_{news_id}"
            ))

    mk.add(
        types.InlineKeyboardButton("⏰ 15м", callback_data=f"s15_{news_id}"),
        types.InlineKeyboardButton("⏰ 1ч",  callback_data=f"s60_{news_id}"),
        types.InlineKeyboardButton("⏰ 4ч",  callback_data=f"s240_{news_id}"),
    )
    mk.add(
        types.InlineKeyboardButton("✏️ Изменить текст", callback_data=f"edit_{news_id}"),
        types.InlineKeyboardButton("🖼 Заменить фото",  callback_data=f"photo_{news_id}"),
    )
    mk.add(types.InlineKeyboardButton("🔗 Оригинальный пост", url=original_url))
    mk.add(types.InlineKeyboardButton("🗑 Удалить", callback_data=f"del_{news_id}"))
    return mk


# ─────────────────────────────────────────────────────────
#  SEND CONTENT
# ─────────────────────────────────────────────────────────

def send_content_to(chat_id: int, item: dict, caption: str, reply_markup=None) -> bool:
    photos_bytes = item.get('_photos_bytes', [])
    video_bytes  = item.get('_video_bytes')

    # Видео
    if video_bytes:
        try:
            result = tg_call(bot.send_video, chat_id,
                             make_buf(video_bytes, 'video.mp4'),
                             caption=caption,
                             reply_markup=reply_markup,
                             supports_streaming=True)
            if result:
                return (True, None)
        except Exception as e:
            print(f"[send video] {e}")

    # Одно фото
    if len(photos_bytes) == 1:
        try:
            result = tg_call(bot.send_photo, chat_id,
                             make_buf(photos_bytes[0]),
                             caption=caption,
                             reply_markup=reply_markup)
            return (result is not None, None)
        except Exception as e:
            print(f"[send photo] {e}")
            return (False, None)

    # Альбом
    if len(photos_bytes) > 1:
        media = []
        bufs  = []
        for i, raw in enumerate(photos_bytes):
            buf = make_buf(raw, f'photo{i}.jpg')
            bufs.append(buf)
            mi = types.InputMediaPhoto(media=buf)
            if i == 0:
                mi.caption    = caption
                mi.parse_mode = 'HTML'
            media.append(mi)
        try:
            sent = tg_call(bot.send_media_group, chat_id, media)
            if sent:
                if reply_markup:
                    btn_msg = tg_call(bot.send_message, chat_id, "☝️ Выбери действие:",
                                      reply_markup=reply_markup,
                                      reply_to_message_id=sent[0].message_id)
                    # Возвращаем (True, btn_msg) чтобы сохранить ID кнопочного сообщения
                    return (True, btn_msg)
                return (True, None)
        except Exception as e:
            print(f"[send album] {e}")
            try:
                result = tg_call(bot.send_photo, chat_id,
                                 make_buf(photos_bytes[0]),
                                 caption=caption, reply_markup=reply_markup)
                return (result is not None, None)
            except Exception as e2:
                print(f"[send album fallback] {e2}")

    return (False, None)


# ─────────────────────────────────────────────────────────
#  MODERATION
# ─────────────────────────────────────────────────────────

def send_to_moderation(item: dict):
    reload_config()
    news_cache[item['id']] = item
    fetch_media(item)

    caption = clean_html(item['html'], item['channel'])
    mk      = build_markup(item['id'], item['original_url'])

    has_photo = bool(item['_photos_bytes'])
    has_video = bool(item['_video_bytes'])
    print(f"[mod] 📤 отправляю в мод группу: {item['id']} | фото:{len(item['_photos_bytes'])} видео:{'✅' if has_video else '❌'}")

    if has_photo or has_video:
        ok, btn_msg = send_content_to(MODERATION_GROUP_ID, item, caption, reply_markup=mk)
        if ok:
            item['_mod_chat_id'] = MODERATION_GROUP_ID
            if btn_msg:
                item['_btn_msg_id']  = btn_msg.message_id
                item['_btn_is_text'] = True
                print(f"[mod] ✅ альбом отправлен, кнопки в msg_id={btn_msg.message_id}")
            else:
                item['_btn_msg_id']  = None
                item['_btn_is_text'] = False
                print(f"[mod] ✅ медиа отправлено")
            return
        print(f"[mod] ❌ медиа не удалось, отправляю текстом")

    result = tg_call(bot.send_message, MODERATION_GROUP_ID, caption, reply_markup=mk)
    if result:
        item['_mod_chat_id'] = MODERATION_GROUP_ID
        item['_btn_msg_id']  = None
        item['_btn_is_text'] = True
        print(f"[mod] ✅ текст отправлен")


# ─────────────────────────────────────────────────────────
#  POST TO CHANNEL
# ─────────────────────────────────────────────────────────

def post_to_channel(item: dict, channel_idx: int = 0):
    if not TARGET_CHANNELS:
        print("[post] ❌ нет каналов для публикации!")
        return
    channel_id   = TARGET_CHANNELS[channel_idx]['id']
    channel_name = TARGET_CHANNELS[channel_idx]['name']
    print(f"[post] 📢 публикую {item['id']} в «{channel_name}» (id={channel_id})")
    caption = clean_html(item['html'], item['channel'])

    if item['has_video'] and not item['_video_bytes']:
        print(f"[post] видео не было скачано, пробую повторно...")
        video_url = fetch_video_url_from_post(item['original_url'])
        if video_url:
            item['_video_bytes'] = download_video(video_url)

    if item['photos'] and not item['_photos_bytes']:
        print(f"[post] фото не были скачаны, скачиваю...")
        item['_photos_bytes'] = [
            raw for url in item['photos']
            if (raw := download_image(url))
        ]

    if item['_photos_bytes'] or item['_video_bytes']:
        ok, _ = send_content_to(channel_id, item, caption)
        if ok:
            print(f"[post] ✅ опубликовано с медиа в «{channel_name}»")
            return

    tg_call(bot.send_message, channel_id, caption)
    print(f"[post] ✅ опубликовано текстом в «{channel_name}»")


# ─────────────────────────────────────────────────────────
#  CALLBACKS
# ─────────────────────────────────────────────────────────

def _safe_edit(call, item: dict, text: str, markup=None):
    """
    Редактирует сообщение с кнопками.
    Для альбомов кнопки в отдельном текстовом сообщении (_btn_msg_id).
    Для фото/видео/текста — само сообщение.
    """
    chat_id = call.message.chat.id
    btn_msg_id  = item.get('_btn_msg_id')
    btn_is_text = item.get('_btn_is_text', False)

    # Если есть отдельное кнопочное сообщение (альбом) — редактируем его
    if btn_msg_id:
        try:
            bot.edit_message_text(text,
                chat_id=chat_id,
                message_id=btn_msg_id,
                reply_markup=markup,
                parse_mode='HTML')
            return
        except Exception as e:
            print(f"[edit btn_msg] {e}")

    # Иначе редактируем само сообщение
    try:
        if call.message.content_type == 'photo':
            bot.edit_message_caption(text,
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=markup,
                parse_mode='HTML')
        else:
            bot.edit_message_text(text,
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=markup,
                parse_mode='HTML')
    except Exception as e:
        print(f"[edit] {e}")


def caption_with_status(item: dict, status_line: str) -> str:
    """Добавляет строку статуса в конец caption поста."""
    base = clean_html(item['html'], item['channel'])
    return f"{base}\n\n<i>{status_line}</i>"


@bot.callback_query_handler(func=lambda c: True)
def handle_callback(call):
    parts   = call.data.split('_', 1)
    action  = parts[0]
    news_id = parts[1] if len(parts) > 1 else ''

    item = news_cache.get(news_id)
    if not item:
        bot.answer_callback_query(call.id, "⚠️ Пост не найден в кэше.")
        return

    # ── Опубликовать в канал (now0_, now1_, ...) ──────────
    if action.startswith('now'):
        try:
            ch_idx = int(action[3:]) if action[3:].isdigit() else 0
            ch_name = TARGET_CHANNELS[ch_idx]['name'] if ch_idx < len(TARGET_CHANNELS) else 'Канал'
            post_to_channel(item, ch_idx)
            now_str = time.strftime('%H:%M %d.%m.%Y')
            by_name = call.from_user.first_name or 'Админ'
            status  = f"✅ Опубликовано в «{ch_name}» в {now_str} • {by_name}"
            mk      = build_markup(item['id'], item['original_url'])
            _safe_edit(call, item, caption_with_status(item, status), markup=mk)
            bot.answer_callback_query(call.id, f"✅ Опубликовано в {ch_name}!")
        except Exception as e:
            bot.answer_callback_query(call.id, f"Ошибка: {e}")
            print(f"[now] {e}")

    # ── Запланировать ─────────────────────────────────────
    elif action.startswith('s'):
        try:
            minutes = int(action[1:])
        except ValueError:
            bot.answer_callback_query(call.id, "Ошибка таймера.")
            return

        publish_at = time.strftime('%H:%M', time.localtime(time.time() + minutes * 60))

        def scheduled_post(it, pub_at, caller_name, msg_id, chat_id, ch_idx=0):
            post_to_channel(it, ch_idx)
            ch_name    = TARGET_CHANNELS[ch_idx]['name'] if ch_idx < len(TARGET_CHANNELS) else 'Канал'
            status     = f"⏰ Опубликовано в «{ch_name}» по расписанию в {pub_at} • {caller_name}"
            mk         = build_markup(it['id'], it['original_url'])
            btn_msg_id = it.get('_btn_msg_id')
            try:
                if btn_msg_id:
                    bot.edit_message_text(
                        caption_with_status(it, status),
                        chat_id=chat_id, message_id=btn_msg_id,
                        reply_markup=mk, parse_mode='HTML')
                elif it.get('_btn_is_text'):
                    bot.edit_message_text(
                        caption_with_status(it, status),
                        chat_id=chat_id, message_id=msg_id,
                        reply_markup=mk, parse_mode='HTML')
                else:
                    bot.edit_message_caption(
                        caption_with_status(it, status),
                        chat_id=chat_id, message_id=msg_id,
                        reply_markup=mk, parse_mode='HTML')
            except Exception as e:
                print(f"[scheduled edit] {e}")

        by_name = call.from_user.first_name or 'Админ'
        threading.Timer(
            minutes * 60, scheduled_post,
            args=[item, publish_at, by_name,
                  call.message.message_id, call.message.chat.id]
        ).start()

        status = f"⏰ Запланировано на {publish_at} • {by_name}"
        mk     = build_markup(item['id'], item['original_url'])
        _safe_edit(call, item, caption_with_status(item, status), markup=mk)
        bot.answer_callback_query(call.id, f"⏰ Запланировано на {publish_at}")

    # ── Редактировать текст ───────────────────────────────
    elif action == 'edit':
        pending_edit[call.from_user.id] = news_id
        bot.answer_callback_query(call.id)
        bot.send_message(
            call.message.chat.id,
            "✏️ Отправь новый текст для поста.\n\n"
            "<i>Поддерживается HTML: &lt;b&gt;жирный&lt;/b&gt;, &lt;i&gt;курсив&lt;/i&gt;</i>\n"
            "Для отмены отправь /cancel",
            parse_mode='HTML'
        )

    # ── Заменить фото ─────────────────────────────────────
    elif action == 'photo':
        pending_photo[call.from_user.id] = news_id
        bot.answer_callback_query(call.id)
        bot.send_message(
            call.message.chat.id,
            "🖼 Отправь новое фото для поста.\n"
            "Для отмены отправь /cancel"
        )

    # ── Удалить ───────────────────────────────────────────
    elif action == 'del':
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception as e:
            print(f"[del] {e}")
        bot.answer_callback_query(call.id, "🗑 Удалено.")
        news_cache.pop(news_id, None)


# ─────────────────────────────────────────────────────────
#  MESSAGE HANDLERS — редактирование текста и фото
# ─────────────────────────────────────────────────────────

@bot.message_handler(commands=['cancel'])
def handle_cancel(message):
    uid = message.from_user.id
    if uid in pending_edit:
        del pending_edit[uid]
        bot.reply_to(message, "✅ Редактирование отменено.")
    elif uid in pending_photo:
        del pending_photo[uid]
        bot.reply_to(message, "✅ Замена фото отменена.")
    else:
        bot.reply_to(message, "Нечего отменять.")


@bot.message_handler(
    func=lambda m: m.from_user.id in pending_edit,
    content_types=['text']
)
def handle_new_text(message):
    uid     = message.from_user.id
    news_id = pending_edit.pop(uid, None)
    if not news_id:
        return

    item = news_cache.get(news_id)
    if not item:
        bot.reply_to(message, "⚠️ Пост не найден в кэше.")
        return

    # Сохраняем новый текст
    item['html']    = message.html_text or message.text
    item['_edited'] = True

    caption = clean_html(item['html'], item['channel'])
    mk      = build_markup(news_id, item['original_url'])

    print(f"[edit] новый текст для {news_id}: {len(item['html'])} симв")

    try:
        if item['_photos_bytes'] or item['_video_bytes']:
            ok, btn_msg = send_content_to(message.chat.id, item, caption, reply_markup=mk)
            if ok:
                # Обновляем _btn_msg_id если это альбом
                if btn_msg:
                    item['_btn_msg_id']  = btn_msg.message_id
                    item['_btn_is_text'] = True
                bot.reply_to(message, "✅ Текст обновлён! Предпросмотр выше ☝️")
            else:
                tg_call(bot.send_message, message.chat.id, caption, reply_markup=mk)
                bot.reply_to(message, "✅ Текст обновлён! (медиа не удалось, показан текст)")
        else:
            tg_call(bot.send_message, message.chat.id, caption, reply_markup=mk)
            bot.reply_to(message, "✅ Текст обновлён! Предпросмотр выше ☝️")
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка предпросмотра: {e}")
        print(f"[edit preview] {e}")


@bot.message_handler(
    func=lambda m: m.from_user.id in pending_photo,
    content_types=['photo']
)
def handle_new_photo(message):
    uid     = message.from_user.id
    news_id = pending_photo.pop(uid, None)
    if not news_id:
        return

    item = news_cache.get(news_id)
    if not item:
        bot.reply_to(message, "⚠️ Пост не найден в кэше.")
        return

    # Скачиваем фото из сообщения пользователя
    photo    = message.photo[-1]   # самое большое
    file_info = bot.get_file(photo.file_id)
    file_url  = f"https://api.telegram.org/file/bot{TOKEN}/{file_info.file_path}"

    try:
        resp = requests.get(file_url, timeout=15)
        img  = Image.open(io.BytesIO(resp.content))
        if img.mode != 'RGB':
            img = img.convert('RGB')
        out = io.BytesIO()
        img.save(out, format='JPEG', quality=90)
        item['_photos_bytes'] = [out.getvalue()]
        item['photos']        = []   # больше не нужны оригинальные URL
        item['_video_bytes']  = None
        item['has_video']     = False

        caption = clean_html(item['html'], item['channel'])
        mk      = build_markup(news_id, item['original_url'])

        bot.reply_to(message, "✅ Фото заменено! Предпросмотр выше ☝️")
        ok, btn_msg = send_content_to(message.chat.id, item, caption, reply_markup=mk)
        if btn_msg:
            item['_btn_msg_id']  = btn_msg.message_id
            item['_btn_is_text'] = True

    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка загрузки фото: {e}")


# ─────────────────────────────────────────────────────────
#  КОМАНДЫ
# ─────────────────────────────────────────────────────────

@bot.message_handler(commands=['chatid'])
def handle_chatid(message):
    """Показывает ID текущего чата — отправь в канале или группе чтобы узнать ID."""
    chat = message.chat
    text = (
        f"🆔 <b>ID этого чата:</b> <code>{chat.id}</code>\n"
        f"📝 Тип: {chat.type}\n"
        f"📛 Название: {chat.title or chat.first_name or '—'}"
    )
    bot.reply_to(message, text)


@bot.message_handler(commands=['start', 'help'])
def handle_help(message):
    text = (
        "🤖 <b>News Bot</b>\n\n"
        "Команды:\n"
        "/post — запустить парсинг прямо сейчас\n"
        "/status — статистика бота\n"
        "/cancel — отменить редактирование\n"
        "/help — это сообщение\n\n"
        "Кнопки в постах:\n"
        "✅ Опубликовать — сразу в канал\n"
        "⏰ 15м/1ч/4ч — отложить\n"
        "✏️ Изменить текст — редактировать\n"
        "🖼 Заменить фото — новое фото\n"
        "🔗 Оригинальный пост — открыть источник\n"
        "🗑 Удалить — убрать из очереди"
    )
    bot.reply_to(message, text)


@bot.message_handler(commands=['post'])
def handle_post(message):
    bot.reply_to(message, "🔄 Запускаю парсинг всех каналов...")
    manual_check_event.set()


@bot.message_handler(commands=['status'])
def handle_status(message):
    text = (
        f"📊 <b>Статус бота</b>\n\n"
        f"📌 Обработано постов: {len(processed_news_ids)}\n"
        f"📦 В кэше: {len(news_cache)} постов\n"
        f"📡 Каналов: {len(SOURCE_CHANNELS)}\n"
        f"⏱ Интервал: {CHECK_INTERVAL} сек\n"
    )
    bot.reply_to(message, text)


# ─────────────────────────────────────────────────────────
#  MONITORING LOOP
# ─────────────────────────────────────────────────────────

def run_check():
    """Один обход всех каналов."""
    print(f"\n{'='*50}")
    print(f"[monitor] 🔄 Начинаю обход каналов: {time.strftime('%H:%M:%S')}")
    print(f"{'='*50}")
    for channel in SOURCE_CHANNELS:
        print(f"\n[monitor] 📡 Проверяю @{channel}…")
        try:
            posts = scrape_channel(channel)
            new_count = len(posts)
            print(f"[monitor] 📬 @{channel}: новых постов {new_count}")
            for i, item in enumerate(posts):
                print(f"[monitor] ── пост {i+1}/{new_count}: {item['id']} | фото:{len(item['photos'])} видео:{'да' if item['has_video'] else 'нет'} | текст:{len(item['html'])} симв")
                send_to_moderation(item)
                processed_news_ids.add(item['id'])
                save_processed_ids()
                time.sleep(3)
        except Exception as e:
            print(f"[monitor] ❌ Ошибка @{channel}: {e}")
        time.sleep(5)
    print(f"\n[monitor] ✅ Обход завершён. Следующий через {CHECK_INTERVAL}с")


def monitoring_loop():
    print("[monitor] Запущен.")
    while True:
        run_check()
        # Ждём либо таймер либо ручной запуск через /post
        triggered = manual_check_event.wait(timeout=CHECK_INTERVAL)
        if triggered:
            manual_check_event.clear()
            print("[monitor] Ручной запуск через /post")


# ─────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────

if __name__ == '__main__':
    threading.Thread(target=monitoring_loop, daemon=True).start()
    print("[bot] Запущен…")
    bot.infinity_polling(timeout=30, long_polling_timeout=20)