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
      │ news_alerts  │ │ _bot.py       │ │ evaluator.py │
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
│   │   ├── supabase.py        # Клиент supabase-py (синглтон, жизненный цикл)
│   │   ├── news_alerts.py     # Новостная таблица: вставка, дедуп, блокировки
│   │   ├── bot_state.py       # Остальные таблицы — по модулю на таблицу
│   │   └── pipeline.py        # Общий pipeline сохранения
│   └── telegram_bot.py        # Telegram-уведомления
├── migrations/                # Инкрементальные правки схемы (SQL)
├── main.py                    # Точка входа uvicorn
├── requirements.txt           # Боевой контур
├── requirements-research.txt  # pandas/numpy/statsmodels для app/research
├── .env.example
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

Пакеты для оффлайн-исследования (`app/research`) держатся отдельно — боевой
контур их не импортирует:

```bash
pip install -r requirements-research.txt
```

### Миграции БД

Схема заводится вручную в Supabase; инкрементальные правки лежат в `migrations/`
и применяются через SQL Editor или `supabase db execute --file <файл>`.

---

## Настройка окружения

Скопируйте `.env.example` в `.env` и подставьте значения:

```bash
cp .env.example .env
```

```env
# NewsAPI
NEWS_API=your_newsapi_key

# Supabase — ключ СЕКРЕТНЫЙ, живёт только на сервере.
# Все таблицы закрыты RLS-политикой "Deny all for anon", поэтому
# publishable/anon-ключ здесь не работает: SELECT'ы вернут пустой список,
# а UPDATE'ы молча не изменят ни одной строки.
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SECRET_KEY=your_secret_key
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

# --- БКС Trade API ---
# Refresh-токены выпускаются в веб-версии БКС Мир инвестиций:
# Профиль → Управление счетами → <счёт> → Токены API. Живут 90 суток.
# READ хватает для PAPER; WRITE нужен только для LIVE.
BCS_REFRESH_TOKEN_READ=your_read_token
# BCS_REFRESH_TOKEN_WRITE=your_write_token
BCS_TOKEN_STORE=.bcs_tokens.json
BCS_API_BASE=https://be.broker.ru
BCS_WS_BASE=wss://ws.broker.ru
BCS_CLASS_CODE=TQBR

# --- Торговый движок ---
ENABLE_TRADING_ENGINE=true
BCS_TRADING_MODE=PAPER      # PAPER | LIVE
PAIR_LEG1=TATN
PAIR_LEG2=TATNP

# Секрет для /strategy/start и /strategy/stop (заголовок X-Bot-Token).
# Пустое значение = управляющие эндпоинты отключены.
BOT_ADMIN_TOKEN=
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
| GET | `/health` | Состояние приложения, WebSocket БКС и движка |
| GET | `/strategy/state` | `bot_state`, активные параметры, текущий z-score |
| GET | `/bcs/selftest` | Проверка связи с БКС (только чтение, заявок не шлёт) |
| POST | `/strategy/stop?emergency=true` | Остановить торговлю (нужен `X-Bot-Token`) |
| POST | `/strategy/start` | Возобновить торговлю (нужен `X-Bot-Token`) |

---

## Торговый контур (парный арбитраж TATN/TATNP)

Вторая половина системы: подключение к **БКС Trade API**, поток минутных
баров и котировок, расчёт z-score спреда и исполнение сделок.

### Как это работает

```
БКС WebSocket (свечи M1 + котировки)   БКС REST (/candles-chart, догрузка)
                │                                    │
                └────────────────┬───────────────────┘
                                 ▼
                  app/strategy/engine.py — PairsEngine
                  ln(TATN/TATNP) → скользящие mean/std → z-score
                                 │
        ┌────────────────────────┼────────────────────────┐
        ▼                        ▼                        ▼
  вход |z| ≥ entry_z      выход TP/STOP/           запись в candles,
  (если сессия открыта,   TIMEOUT/NEWS             trades, bot_state
   нет новостной                                   + Telegram ENTRY/EXIT
   блокировки)
```

Параметры (`spread_window`, `entry_zscore`, `stop_zscore`, `min_hold_min`, …)
читаются из таблицы `strategy_params` **на каждой итерации** с кэшем 60 секунд —
их можно менять без рестарта.

### Режимы

- **`PAPER`** (по умолчанию) — котировки настоящие, заявки не отправляются.
  Исполнение моделируется по противоположной стороне стакана: покупка по
  `offer`, продажа по `bid`, то есть спред мы платим как в реальности.
- **`LIVE`** — реальные заявки. Включается только после предстартовых проверок
  (доступность шорта, маржа, незаблокированные инструменты); при неудаче
  движок остаётся в `PAPER` и шлёт алерт. **У БКС нет sandbox.**

### Защита

- **Legging risk**: если исполнилась только одна нога, вторая немедленно
  раскрывается рыночной заявкой, пишется `CRITICAL` и поднимается
  `emergency_stop_flag`. Попыток «доисполнить» не делается.
- **Новостная блокировка**: активные записи в `news_alerts` синхронизируются
  в `bot_state.is_news_blocked`; при блокировке позиция закрывается с
  `exit_reason='NEWS'`, новые входы запрещены.
- **Разрывы данных**: молчание потока дольше `data_gap_alert_min` открывает
  запись в `quote_gaps` и шлёт Telegram-алерт; после восстановления
  пропущенные бары догружаются через REST.
- **Аварийная остановка**: `emergency_stop_flag` в `bot_state` или
  `POST /strategy/stop?emergency=true`.

---

## Исследование стратегии (`app/research`)

Оффлайн-модули для честной проверки параметров. Боевой цикл их **не импортирует** —
pandas/numpy/statsmodels нужны только здесь.

```bash
# Walk-forward: параметры подбираются на обучающем окне,
# метрики снимаются на следующем, которого оптимизатор не видел
python -m app.research.walkforward --from 2024-06-01 --to 2026-06-01

# С учётом проскальзывания
python -m app.research.walkforward --slippage-bps 3 --pair TATN TATNP
```

Отчёт включает оценку коинтеграции (Engle-Granger), тест Дики-Фуллера на
стационарность спреда, half-life и вытекающий из него разумный размер окна,
плюс сравнение in-sample и out-of-sample результатов по каждому окну.

Данные для исследования тянутся с MOEX ISS (`app/market/moex_client.py`) и
кэшируются в CSV (`candles_cache/`).

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

- Python 3.11+ (проверено на 3.14)
- Зависимости из `requirements.txt`:
  - `fastapi`, `uvicorn`
  - `supabase` — весь `app/storage` работает через него
  - `httpx` — БКС, Telegram, RSS и скрейпинг статей
  - `pydantic`
  - `apscheduler`
  - `google-genai`
  - `beautifulsoup4`
  - `tenacity`
  - `pytz`
  - `websockets`

---

## Безопасность

- API-ключи и токены загружаются исключительно из переменных окружения.
- `.env` исключён из git.
- Секреты не логируются.
- Запросы к Supabase идут через официальный клиент `supabase-py` — фильтры
  параметризованы, строковая сборка URL исключена.
- Все таблицы под RLS с политикой «Deny all for anon»: публичного доступа к
  торговому состоянию нет. Бот ходит секретным серверным ключом.

---

## Примечания

- Модель по умолчанию — `gemini-3.1-flash-lite`. Переопределите через `GEMINI_MODEL_NAME`, если нужна другая модель.
- Если `TELEGRAM_CHAT_ID` не задан, бот попытается определить его автоматически из `getUpdates` (требуется, чтобы боту хотя бы раз отправили сообщение).

---

## Лицензия

Проект разрабатывается в закрытом режиме. Распространение и использование регулируется внутренними соглашениями.
