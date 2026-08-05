-- Миграция 001. Применить один раз к боевой базе.
--
--   Supabase Dashboard → SQL Editor → вставить и выполнить,
--   либо: supabase db execute --file migrations/001_news_alerts_unique_url.sql
--
-- Проверено на текущих данных: в news_alerts 108 строк, все article_url
-- различны — индекс встанет без конфликтов.

-- Идемпотентная вставка новостей. Google- и RSS-фетчеры ходят каждые 5 минут
-- и пересекаются по ссылкам, а проверка дублей на стороне приложения
-- подвержена гонке между ними. Уникальный индекс закрывает её на стороне БД
-- и даёт опору для ON CONFLICT (article_url) в app/storage/news_alerts.py.
--
-- ВАЖНО: до применения этой миграции insert_news_batch() будет получать от
-- PostgREST ошибку 42P10 на каждый батч и молча деградировать в поштучную
-- вставку (_insert_one_by_one) — работает, но медленнее и с ERROR в логах.
CREATE UNIQUE INDEX IF NOT EXISTS news_alerts_article_url_key
    ON public.news_alerts (article_url);

-- idx_candles_symbol_time полностью дублирует candles_symbol_timestamp_key:
-- оба — UNIQUE btree по (symbol, "timestamp"). Двойная работа на каждой
-- вставке бара. Уникальность сохраняет оставшийся индекс.
DROP INDEX IF EXISTS public.idx_candles_symbol_time;
