# TradeBot — News Analysis Pipeline for TATNEFT Trading

Асинхронный Python-pipeline для сбора новостей об эмитенте **Татнефть (TATN / TATNEFT)**, оценки рисков через LLM и оповещения о запрете торговли в Telegram.

---

## Возможности

- **Сбор новостей** из нескольких источников:
  - Google News RSS (`ru`, `en-US`, `de`)
  - NewsAPI (`ru`, `en`, `de`)
  - Российские RSS-ленты: ТАСС, РИА Новости, Интерфакс, Ведомости, Лента.ру, Новости Mail.ru
- **LLM-оценка риска** через Google Gemini (`gemini-3.1-flash-lite` по умолчанию) с математической верификацией формулы на стороне бота.
- **Хранение алертов** в Supabase (`public.news_alerts`).
- **Telegram-оповещения** только при `is_blocked = True`, с защитой от дублей через флаг `telegram_sent`.
- **Retry с экспоненциальным backoff** для всех внешних HTTP-вызовов и LLM.
- **Рабочее окно 09:00–22:00 по московскому времени** для scheduled-задач.

---

## Архитектура

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Google RSS     │     │  NewsAPI        │     │  Russian RSS    │
│  app/sources/   │     │  app/sources/   │     │  app/sources/   │
│  google.py      │     │  newsapi.py     │     │  russian_rss.py │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 ▼
                    ┌─────────────────────────┐
                    │  app/storage/pipeline.py│
                    │  map → dedupe → LLM     │
                    │  → insert → notify      │
                    └───────────┬─────────────┘
                                │
              ┌─────────────────┼─────────────────┐
              ▼                 ▼                 ▼
      ┌──────────────┐ ┌───────────────┐ ┌──────────────┐
      │  Supabase    │ │  Telegram Bot │ │  LLM Gemini  │
      │  app/storage/│ │ app/telegram │ │ app/llm/     │
      │  supabase.py │ │ _bot.py       │ │ evaluator.py │
      └──────────────┘ └───────────────┘ └──────────────┘
```

---

## Структура проекта

```
.
├── app/
│   ├── config.py              # Настройки из env
│   ├── http_client.py         # Общий httpx.AsyncClient
│   ├── logging_setup.py       # Единая конфигурация логирования
│   ├── retry.py               # Retry с экспоненциальным backoff
│   ├── main.py                # FastAPI + APScheduler
│   ├── llm/
│   │   ├── evaluator.py       # LLMRiskEvaluator
│   │   ├── models.py          # Pydantic-схемы ответа LLM
│   │   └── prompts.py         # Системный промпт
│   ├── sources/
│   │   ├── google.py          # Google News RSS
│   │   ├── newsapi.py         # NewsAPI
│   │   ├── russian_rss.py     # Российские RSS-ленты
│   │   ├── filters.py         # Фильтр релевантности Татнефти
│   │   └── rss_utils.py       # Утилиты парсинга RSS
│   ├── storage/
│   │   ├── supabase.py        # Низкоуровневые запросы к Supabase
│   │   └── pipeline.py        # Общий pipeline сохранения
│   └── telegram_bot.py        # Telegram-уведомления
├── main.py                    # Точка входа uvicorn
├── requirements.txt
└── README.md
```

---

## Установка

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# .venv\Scripts\activate   # Windows

pip install -r requirements.txt
```

---

## Настройка окружения

Создайте файл `.env` в корне проекта:

```env
# NewsAPI
NEWS_API=your_newsapi_key

# Supabase
NEXT_PUBLIC_SUPABASE_URL=https://your-project.supabase.co
NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY=your-anon-key
SUPABASE_TABLE=news_alerts

# Google Gemini
GEMINI_API_KEY=your_gemini_key
# GEMINI_MODEL_NAME=gemini-3.1-flash-lite

# Telegram
TELEGRAM_BOT_TOKEN=your_bot_token
# Один или несколько чатов через запятую
TELEGRAM_CHAT_ID=your_chat_id
# Для тем (топиков): одно значение на все чаты или через запятую для каждого чата
# TELEGRAM_THREAD_ID=optional_thread_id

# Pipeline
ENABLE_LLM_EVALUATION=true
```

> **Важно:** не коммитьте `.env` в репозиторий. Файл уже добавлен в `.gitignore`.

---

## Запуск

```bash
python main.py
```

Или напрямую через uvicorn:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

После старта автоматически запускается планировщик:

- **Google News RSS**: каждые 5 минут с 09:00 до 22:00 МСК
- **NewsAPI**: в 00, 24, 48 минут каждого часа с 09:00 до 22:00 МСК
- **Russian RSS**: каждые 5 минут с 09:00 до 22:00 МСК

---

## API Endpoints

| Метод | Endpoint | Описание |
|-------|----------|----------|
| GET | `/politics_government_ru` | Собрать русскоязычные новости по Татнефти |
| GET | `/politics_government_en` | Собрать англоязычные новости по Tatneft |
| GET | `/business_tatneft_de` | Собрать немецкоязычные новости по Tatneft |
| GET | `/fetch_russian_rss` | Запустить парсинг российских RSS-лент |
| GET | `/news?query=...&lang=...&country=...` | Динамический поиск и сохранение |

---

## Логика риск-оценки

Каждая новость проходит через LLM, который возвращает структурированный JSON:

- `decision: true` — торговля разрешена (`is_blocked = false`)
- `decision: false` или `tier0: true` — торговля запрещена (`is_blocked = true`)

Бот независимо пересчитывает итоговый скор:

```text
RiskScore_base = (R / 10) × C × (0.35×S + 0.40×M + 0.25×U)
λ(M) = 0.347 − 0.277 × (M / 10)
T_decay = exp(−λ(M) × τ)
RiskScore_final = RiskScore_base × T_decay
```

Порог: `T = 6.0`.

---

## Idempotency Telegram

Telegram-уведомление отправляется только если:

1. Новость реально вставлена в `news_alerts` (получен `id`).
2. `is_blocked = True`.
3. `telegram_sent = False`.

После успешной отправки флаг `telegram_sent` обновляется в Supabase.

---

## Требования

- Python 3.11+
- Зависимости из `requirements.txt`:
  - `fastapi`, `uvicorn`
  - `httpx`
  - `pydantic`
  - `apscheduler`
  - `google-genai`
  - `beautifulsoup4`
  - `tenacity`
  - `pytz`

---

## Безопасность

- API-ключи и токены загружаются исключительно из переменных окружения.
- `.env` исключён из git.
- Секреты не логируются.
- Запросы к Supabase идут через PostgREST с параметризованными фильтрами.

---

## Примечания

- Модель по умолчанию — `gemini-3.1-flash-lite`. Переопределите через `GEMINI_MODEL_NAME`, если нужна другая модель.
- Если `TELEGRAM_CHAT_ID` не задан, бот попытается определить его автоматически из `getUpdates` (требуется, чтобы боту хотя бы раз отправили сообщение).

---

## Лицензия

Проект разрабатывается в закрытом режиме. Распространение и использование регулируется внутренними соглашениями.
