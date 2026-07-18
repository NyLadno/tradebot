import os
import logging
import asyncio
import xml.etree.ElementTree as ET
import re
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, Query
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import httpx

from supabase_handler import save_newsapi_batch, save_google_batch, save_russian_rss_batch




# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("tradebot.parser")

load_dotenv()

# Инициализация планировщика
scheduler = AsyncIOScheduler()
auth = os.getenv('NEWS_API')

GOOGLE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Единый глобальный HTTP-клиент
_http_client: Optional[httpx.AsyncClient] = None


def get_http_client() -> httpx.AsyncClient:
    """
    Возвращает переиспользуемый экземпляр асинхронного HTTP-клиента.
    Инициализирует его, если он закрыт или не создан.
    """
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
    return _http_client


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Управление жизненным циклом приложения: инициализация HTTP-клиента и планировщика,
    а также закрытие ресурсов.
    """
    # Инициализация клиента
    get_http_client()

    # Google News: каждые 5 минут с 9:00 до 22:00
    scheduler.add_job(scheduler_google_fetch, CronTrigger(minute='*/5', hour='9-23'), args=['Татнефть', 'ru', 'RU', 'RU:ru'])
    scheduler.add_job(scheduler_google_fetch, CronTrigger(minute='*/5', hour='9-23'), args=['Tatneft', 'en-US', 'US', 'US:en'])
    scheduler.add_job(scheduler_google_fetch, CronTrigger(minute='*/5', hour='9-23'), args=['Tatneft', 'de', 'DE', 'DE:de'])

    # NewsAPI: 3 раза в час (9:00–22:00)
    scheduler.add_job(scheduler_newsapi_fetch, CronTrigger(minute='0,24,48', hour='9-23'), args=['Татнефть', 'ru'])
    scheduler.add_job(scheduler_newsapi_fetch, CronTrigger(minute='0,24,48', hour='9-23'), args=['Tatneft', 'en'])
    scheduler.add_job(scheduler_newsapi_fetch, CronTrigger(minute='0,24,48', hour='9-23'), args=['Tatneft', 'de'])

    # Российские RSS-источники: каждые 5 минут
    scheduler.add_job(scheduler_russian_rss_fetch, CronTrigger(minute='*/5'))

    # Запуск планировщика
    scheduler.start()
    logger.info("Планировщик запущен. Google: каждые 5 мин (9-22). NewsAPI: по расписанию (9-22). Российские RSS: каждые 5 мин.")

    yield

    # Корректное завершение работы
    scheduler.shutdown()
    logger.info("Планировщик остановлен.")
    if _http_client:
        await _http_client.aclose()
        logger.info("HTTP-клиент закрыт.")


app = FastAPI(lifespan=lifespan)


def parse_google_rss(xml_text: str, week_ago: datetime) -> List[Dict[str, Any]]:
    """
    Парсит XML-ленту Google RSS и фильтрует новости по дате.
    """
    items = []
    try:
        root = ET.fromstring(xml_text)
        channel = root.find('channel')
        if channel is None:
            return items
        for item in channel.findall('item'):
            pub_str = item.findtext('pubDate', default='')
            try:
                if parsedate_to_datetime(pub_str) < week_ago:
                    continue
            except Exception:
                pass
            items.append({
                'title': item.findtext('title', default=''),
                'link': item.findtext('link', default=''),
                'pubDate': pub_str,
                'description': item.findtext('description', default=''),
                'source': item.findtext('source', default=''),
            })
    except ET.ParseError:
        logger.warning("Ошибка парсинга XML Google RSS.")
    return items


# --- Вспомогательные функции запросов новостей ---

async def run_google_news_fetch(query: str, hl: str, gl: str, ceid: str, client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """
    Скачивает и парсит Google News RSS, затем сохраняет в БД.
    """
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    try:
        resp = await client.get(
            'https://news.google.com/rss/search',
            params={'q': query, 'hl': hl, 'gl': gl, 'ceid': ceid},
            headers=GOOGLE_HEADERS
        )
        resp.raise_for_status()
        items = parse_google_rss(resp.text, week_ago)

        # Сохранение в Supabase пакетом
        saved_count = await save_google_batch(items, query, client)
        logger.info(f"Google {gl}: найдено {len(items)}, сохранено {saved_count} новых")
        return items
    except Exception as e:
        logger.error(f"Ошибка получения новостей Google {gl}: {e}")
        return []


async def run_newsapi_fetch(query: str, lang: str, client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """
    Скачивает новости из NewsAPI, затем сохраняет в БД.
    """
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    week_ago_str = week_ago.strftime('%Y-%m-%d')
    try:
        resp = await client.get(
            'https://newsapi.org/v2/everything',
            params={
                'q': query,
                'language': lang,
                'from': week_ago_str,
                'sortBy': 'publishedAt',
                'pageSize': 20,
                'apiKey': auth
            }
        )
        resp.raise_for_status()
        data = resp.json()
        articles = data.get("articles", [])

        # Сохранение в Supabase пакетом
        saved_count = await save_newsapi_batch(articles, query, client)
        logger.info(f"NewsAPI {lang}: получено {len(articles)} из {data.get('totalResults', 0)}, сохранено {saved_count} новых")
        return articles
    except Exception as e:
        logger.error(f"Ошибка получения новостей NewsAPI {lang}: {e}")
        return []



# --- Фоновые задачи планировщика ---

async def scheduler_google_fetch(query: str, hl: str, gl: str, ceid: str):
    client = get_http_client()
    await run_google_news_fetch(query, hl, gl, ceid, client)


async def scheduler_newsapi_fetch(query: str, lang: str):
    client = get_http_client()
    await run_newsapi_fetch(query, lang, client)




# --- Общая логика для API эндпоинтов ---

async def get_and_save_news(query: str, lang: str, hl: str, gl: str, ceid: str) -> Dict[str, Any]:
    """
    Загружает новости параллельно из всех источников и сохраняет их в базу.
    """
    client = get_http_client()
    
    google_task = run_google_news_fetch(query, hl, gl, ceid, client)
    newsapi_task = run_newsapi_fetch(query, lang, client)

    results = await asyncio.gather(
        google_task,
        newsapi_task
    )
    
    google_items = results[0]
    newsapi_articles = results[1]
        
    return {
        'google': google_items,
        'newsapi': {
            'status': 'ok',
            'totalResults': len(newsapi_articles),
            'articles': newsapi_articles
        }
    }



# --- API Эндпоинты ---

@app.get('/politics_government_ru')
async def get_news_ru():
    return await get_and_save_news('Татнефть', 'ru', 'ru', 'RU', 'RU:ru')


@app.get('/politics_government_en')
async def get_news_en():
    return await get_and_save_news('Tatneft', 'en', 'en-US', 'US', 'US:en')


@app.get('/business_tatneft_de')
async def get_news_de():
    return await get_and_save_news('Tatneft', 'de', 'de', 'DE', 'DE:de')


@app.get('/news')
async def get_news_dynamic(
    query: str = Query(..., description="Ключевое слово для поиска"),
    lang: str = Query("ru", description="Язык новостей (ru, en, de, etc.)"),
    country: str = Query("RU", description="Двухсимвольный код страны (RU, US, DE, etc.)")
):
    """
    Динамический эндпоинт для поиска и сохранения любых новостей.
    """
    # Сопоставляем параметры hl, gl, ceid
    hl = f"{lang}-{country}" if lang == "en" and country == "US" else lang
    gl = country
    ceid = f"{country}:{lang}"
    return await get_and_save_news(query, lang, hl, gl, ceid)


# --- Российские RSS-источники ---

RUSSIAN_RSS_SOURCES = {
    "ТАСС": "https://tass.ru/rss/v2.xml",
    "РИА Новости": "https://ria.ru/export/rss2/index.xml",
    "Интерфакс": "https://www.interfax.ru/rss.asp",
    "Ведомости": "https://www.vedomosti.ru/rss/news",
    "Лента.ру": "https://lenta.ru/rss/",
    "Новости Mail.ru": "https://news.mail.ru/rss/90/",
}

RSS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Регулярное выражение для фильтрации новостей, которые могут повлиять на котировки акций Татнефти
RELEVANT_KEYWORDS = [
    r'татнефть', r'tatneft',
    r'танэко', r'taneco',
    r'маганов',
    r'нефть', r'нефтян', r'нефтепродукт', r'нефтеперераб', r'нефтедобыч', r'нпз', r'нефтепровод',
    r'\bndpi\b', r'\bндпи\b', r'\bндд\b', r'демпфер',
    r'\bопек', r'\bopec',
    r'\bbrent\b', r'брент', r'\burals\b', r'юралс',
    r'баррел',
    r'бензин', r'дизел', r'топлив',
    r'лукойл', r'lukoil',
    r'роснефть', r'rosneft',
    r'сургутнефтег', r'surgutneft',
    r'газпром\s*нефть', r'транснефть', r'transneft'
]
FILTER_PATTERN = re.compile('|'.join(RELEVANT_KEYWORDS), re.IGNORECASE)

def is_relevant_to_tatneft(title: str, description: str) -> bool:
    """
    Проверяет, содержит ли заголовок или описание новости упоминания компании Татнефть,
    ее конкурентов или ключевых факторов нефтяного рынка (цены, налоги, ОПЕК+, санкции),
    которые могут существенно повлиять на котировки акций компании.
    """
    text_to_check = f"{title or ''} {description or ''}"
    return bool(FILTER_PATTERN.search(text_to_check))

def clean_html(raw_html: str) -> str:
    if not raw_html:
        return ""
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    cleantext = re.sub(r'\s+', ' ', cleantext).strip()
    return cleantext

def extract_image_url_from_html(html: str) -> Optional[str]:
    if not html:
        return None
    match = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', html)
    if match:
        return match.group(1)
    return None

def find_ns_text(item: ET.Element, tag_name: str, namespaces: Optional[Dict[str, str]] = None) -> Optional[str]:
    val = item.findtext(tag_name)
    if val is not None:
        return val.strip()
    if namespaces:
        for ns_uri in namespaces.values():
            val = item.findtext(f"{{{ns_uri}}}{tag_name}")
            if val is not None:
                return val.strip()
    for child in item:
        local_name = child.tag.split('}')[-1]
        if local_name == tag_name:
            return child.text.strip() if child.text else None
    return None

def normalize_categories(raw_categories: List[str]) -> List[str]:
    normalized = []
    for cat in raw_categories:
        if not cat:
            continue
        parts = re.split(r'\s*/\s*|\s*:\s*', cat)
        for part in parts:
            part_cleaned = part.strip()
            if part_cleaned and part_cleaned not in normalized:
                normalized.append(part_cleaned)
    return normalized

def parse_rss_to_schema(xml_text: str, source_name: str, source_url: str) -> List[Dict[str, Any]]:
    items = []
    try:
        root = ET.fromstring(xml_text.encode('utf-8'))
        channel = root.find('channel')
        if channel is None:
            return items
        
        ria_ns = {'ria': 'http://rian.ru/ns'}
        
        for item in channel.findall('item'):
            title = item.findtext('title', default='').strip()
            link = item.findtext('link', default='').strip()
            guid = item.findtext('guid', default='').strip()
            pub_date_str = item.findtext('pubDate', default='').strip()
            
            # Categories
            categories = [cat.text.strip() for cat in item.findall('category') if cat.text]
            categories = normalize_categories(categories)
            
            # Author
            author = item.findtext('author')
            if author:
                author = author.strip()
            
            # Description (can contain HTML/CDATA)
            description_raw = item.findtext('description', default='')
            description = clean_html(description_raw)
            
            # Фильтрация по релевантности к Татнефти и нефтяному сектору
            if not is_relevant_to_tatneft(title, description):
                continue
            
            # Enclosure
            enclosure = item.find('enclosure')
            image_url = None
            if enclosure is not None:
                enc_url = enclosure.get('url')
                if enc_url:
                    image_url = enc_url

            # Fallback to image in HTML description if no enclosure image
            if not image_url and description_raw:
                image_url = extract_image_url_from_html(description_raw)

            # Date parsing
            published_at = None
            if pub_date_str:
                try:
                    dt = parsedate_to_datetime(pub_date_str)
                    published_at = dt.isoformat()
                except Exception as e:
                    logger.warning(f"[{source_name}] Error parsing date '{pub_date_str}': {e}")
                    published_at = datetime.now(timezone.utc).isoformat()
            else:
                published_at = datetime.now(timezone.utc).isoformat()

            # Source specific custom tags (using namespace-aware helper)
            priority_val = find_ns_text(item, 'priority', ria_ns)
            priority = None
            if priority_val is not None:
                try:
                    priority = int(priority_val)
                except ValueError:
                    priority = priority_val

            type_val = find_ns_text(item, 'type', ria_ns)
            pdalink = item.findtext('pdalink')
            if pdalink:
                pdalink = pdalink.strip()

            mapped_item = {
                "source": source_name,
                "source_url": source_url,
                "title": title,
                "link": link,
                "guid": guid,
                "published_at": published_at,
                "description": description,
                "categories": categories,
                "image_url": image_url,
                "author": author,
                "priority": priority,
                "type": type_val,
                "pdalink": pdalink
            }
            items.append(mapped_item)
    except Exception as e:
        logger.error(f"Error parsing RSS for source {source_name}: {e}")
    return items

async def run_russian_rss_fetch(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """
    Скачивает и парсит RSS-ленты российских СМИ, сохраняет новые в Supabase.
    """
    all_parsed_items = []
    
    for name, url in RUSSIAN_RSS_SOURCES.items():
        try:
            logger.info(f"Начало запроса RSS-ленты {name}...")
            resp = await client.get(url, headers=RSS_HEADERS)
            resp.raise_for_status()
            
            # Парсинг XML
            items = parse_rss_to_schema(resp.text, name, url)
            
            # Сохранение в Supabase пакетом
            saved_count = await save_russian_rss_batch(items, client)
            logger.info(f"RSS {name}: найдено {len(items)}, сохранено {saved_count} новых")
            
            all_parsed_items.extend(items)
        except Exception as e:
            logger.error(f"Ошибка при обработке источника RSS {name}: {e}")
            
    return all_parsed_items

async def scheduler_russian_rss_fetch():
    client = get_http_client()
    await run_russian_rss_fetch(client)

@app.get('/fetch_russian_rss')
async def get_russian_rss():
    client = get_http_client()
    items = await run_russian_rss_fetch(client)
    return {
        "status": "ok",
        "total_parsed": len(items),
        "articles": items
    }