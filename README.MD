# 📚 RAG Knowledge Base: Regulatory Documents AI-Assistant

> Построение ETL-пайплайна для подготовки базы знаний из нормативных PDF-документов с гибридным хранилищем (Supabase + Qdrant) и специализированным чанкингом для RAG-ассистента инженеров-проектировщиков.

---

## 🛠 Технологический стек
- **Реляционное хранилище:** Supabase
- **Векторное хранилище:** Qdrant
- **Источник данных:** Локальная файловая система (PDF)
- **Обработка документов:** Docling, pypdf, langchain-text-splitters
- **Векторизация:** sentence-transformers (ai-forever/ru-en-RoSBERTa)
- **Архитектура:** Гибридная (реляционная + векторная), RLS, версионность, мультитенантность
- **Языки:** Python, SQL
- **Тестирование:** pytest
- **Контейнеризация:** Docker, Docker Compose

## Cсылка: https://github.com/daria-rim/AI-Assistant-with-RAG
---

# 📦 DWH: Courier Settlement Report. Расчёт выплат курьерам

> Расширение существующего многослойного хранилища данных (DWH) третьим источником — REST API курьерской службы — и создание новой витрины `cdm.dm_courier_ledger` для автоматизированного расчёта вознаграждений курьерам.

---

## 🛠 Технологический стек
- **Источники**: MongoDB, PostgreSQL, REST API
- **Хранилище**: PostgreSQL
- **Оркестрация**: Apache Airflow
- **Язык**: Python 3.9+
- **Инструменты**: SQL, REST, JSON, пагинация (limit/offset)

## Cсылка: https://github.com/daria-rim/Couriers-multi-dwh
---

# 📊 FinTech DWH: Global Transaction Analytics Platform

> Построение инкрементального ETL-пайплайна из PostgreSQL в Vertica с автоматическим формированием витрины данных `global_metrics` для ежедневного мониторинга транзакций, мультивалютной аналитики и расчёта оборота компании в долларовом эквиваленте.

---

## 🛠️ Технологический стек
- **Хранилище:** Vertica (колоночная СУБД)
- **Источник данных:** PostgreSQL (операционная БД)
- **Оркестрация:** Apache Airflow
- **Языки:** Python, SQL
- **Библиотеки:** psycopg2, vertica-python
- **Архитектура:** Инкрементальная загрузка, снежинка (STG → DWH)

## Cсылка: https://github.com/daria-rim/FinTech-DWH
---