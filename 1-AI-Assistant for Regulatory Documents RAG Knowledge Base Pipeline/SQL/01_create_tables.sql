-- ============================================================================
-- РАСШИРЕНИЯ
-- ============================================================================

-- Включаем расширение для криптографических функций (генерация UUID v4, хеширование)
-- UUID v4 используется для всех первичных ключей в системе
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Включаем расширение для триграмм (нечеткий поиск, GIN индексы с триграммами)
-- НЕОБХОДИМО для работы операторного класса gin_trgm_ops
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ============================================================================
-- ТАБЛИЦА: users
-- Назначение: Хранение учетных записей пользователей системы
-- ============================================================================

CREATE TABLE users (
    -- Первичный ключ. UUID v4 генерируется автоматически при вставке.
    -- Обеспечивает глобальную уникальность и невозможность перебора ID (безопасность URL).
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Email пользователя. Уникальный идентификатор для входа в систему.
    -- TEXT выбран для гибкости (нет жесткого ограничения длины как у VARCHAR).
    email TEXT UNIQUE,
    
    -- Тип личной подписки пользователя. По умолчанию 'free'.
    -- Допустимые значения: 'free', 'pro', 'enterprise'.
    -- Используется для определения доступных пользователю функций.
    subscription_type TEXT NOT NULL DEFAULT 'free',

    -- Уникальный идентификатор пользователя в Telegram.
    id_telegram BIGINT CHECK (id_telegram > 0) UNIQUE,
    
    -- Время создания записи. TIMESTAMPTZ хранит время с часовым поясом.
    -- Критично для распределенной аудитории (разные часовые пояса).
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Проверка допустимых значений подписки на уровне БД для целостности данных.
    -- Гарантирует, что в поле subscription_type не попадут некорректные значения.
    CONSTRAINT valid_subscription CHECK (subscription_type IN ('free', 'pro', 'enterprise'))
);

-- Индекс для быстрого поиска пользователя по email.
-- Используется при каждом входе в систему (высокочастотная операция).
CREATE INDEX idx_users_email ON users(email);

-- ============================================================================
-- ТАБЛИЦА: subscription_plans
-- Назначение: Справочник тарифных планов с их характеристиками и лимитами
-- ============================================================================

CREATE TABLE subscription_plans (
    -- Первичный ключ. UUID v4.
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Человеко-читаемое название тарифа (например, "Профессиональный").
    -- Уникальное, так как названия тарифов не должны повторяться.
    name TEXT NOT NULL UNIQUE,
    
    -- Тип тарифа: 'personal' (для физлиц/B2C) или 'corporate' (для компаний/B2B).
    -- Влияет на логику биллинга и доступные функции.
    type TEXT NOT NULL,
    
    -- Количество запросов к LLM/RAG, включенное в тариф в месяц.
    -- Используется для проверки лимитов при обработке запросов.
    monthly_queries INT NOT NULL,
    
    -- Максимальное количество пользователей в workspace для этого тарифа.
    -- NULL означает отсутствие ограничения (для personal тарифов).
    max_users INT,
    
    -- Максимальное количество документов, которые можно загрузить в workspace.
    -- NULL означает отсутствие ограничения.
    max_documents INT,
    
    -- Базовая стоимость тарифа в рублях (ежемесячная плата).
    -- Используется в биллинге для расчета суммы к оплате.
    price_rub INT NOT NULL,
        
    -- Флаг, разрешает ли тариф подключение собственной базы знаний.
    -- TRUE только для enterprise-тарифов.
    can_custom_kb BOOLEAN DEFAULT FALSE,
    
    -- Дополнительная плата за активацию собственной базы знаний (единоразово).
    -- NULL для тарифов, где эта опция недоступна.
    custom_kb_price_rub INT,
    
    -- Время создания записи о тарифе.
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Проверка, что тип тарифа может быть только 'personal' или 'corporate'.
    CONSTRAINT valid_plan_type CHECK (type IN ('personal', 'corporate'))
);

-- Индекс для быстрой фильтрации тарифов по типу (personal vs corporate).
CREATE INDEX idx_plans_type ON subscription_plans(type);

-- ============================================================================
-- ТАБЛИЦА: workspaces
-- Назначение: Изолированные рабочие пространства для B2B клиентов и personal аккаунтов
-- ============================================================================

CREATE TABLE workspaces (
    -- Первичный ключ. UUID v4.
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Название рабочего пространства (например, "ООО Проектный Институт №5").
    name TEXT NOT NULL,
    
    -- Уровень биллинга workspace. Определяет лимиты и доступные функции.
    -- По умолчанию 'free'. Значения: 'free', 'pro', 'business', 'enterprise', 'enterprise_plus'.
    billing_tier TEXT NOT NULL DEFAULT 'free',
    
    -- Денормализованный счетчик пользователей в workspace.
    -- Обновляется триггерами. Используется для быстрой проверки лимита max_users.
    user_count INT NOT NULL DEFAULT 0,
    
    -- Денормализованный счетчик загруженных документов в workspace.
    -- Обновляется триггерами. Используется для быстрой проверки лимита max_documents.
    document_count INT NOT NULL DEFAULT 0,
    
    -- Количество запросов к LLM/RAG, использованных в текущем расчетном периоде.
    -- Инкрементируется при каждом вызове. Сбрасывается в 0 по cron-задаче.
    queries_used_this_month INT NOT NULL DEFAULT 0,
    
    -- Статус подписки: 'active' (активна), 'past_due' (просрочена), 'canceled' (отменена), 'trial' (пробный период).
    subscription_status TEXT NOT NULL DEFAULT 'active',
    
    -- Дата окончания пробного периода. По умолчанию +14 дней от создания.
    -- NULL, если пробный период не применяется или уже завершен.
    trial_ends_at TIMESTAMPTZ DEFAULT (NOW() + INTERVAL '14 days'),
    
    -- Дата следующего сброса счетчиков квот (обычно первое число следующего месяца).
    quota_reset_at TIMESTAMPTZ NOT NULL DEFAULT (DATE_TRUNC('month', NOW()) + INTERVAL '1 month'),
    
    -- Флаг, активирована ли собственная база знаний для этого workspace.
    -- TRUE, если клиент оплатил опцию custom KB.
    custom_kb_enabled BOOLEAN DEFAULT FALSE,
    
    -- Дата активации собственной базы знаний.
    -- Используется для биллинга (единоразовый платеж взимается при первой активации).
    custom_kb_activated_at TIMESTAMPTZ,
    
    -- Ссылка на пользователя, создавшего workspace.
    -- ON DELETE SET NULL: при удалении пользователя workspace сохраняется.
    created_by UUID REFERENCES users(id) ON DELETE SET NULL,
    
    -- Время создания workspace.
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Время последнего обновления workspace. Обновляется триггером.
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Проверка допустимых значений billing_tier.
    CONSTRAINT valid_billing_tier CHECK (billing_tier IN ('free', 'pro', 'business', 'enterprise', 'enterprise_plus')),
    
    -- Проверка допустимых значений статуса подписки.
    CONSTRAINT valid_subscription_status CHECK (subscription_status IN ('active', 'past_due', 'canceled', 'trial')),
    
    -- Проверка, что счетчики не могут быть отрицательными.
    CONSTRAINT non_negative_counts CHECK (user_count >= 0 AND document_count >= 0 AND queries_used_this_month >= 0)
);

-- Индекс для быстрой фильтрации workspaces по уровню биллинга.
CREATE INDEX idx_workspaces_billing ON workspaces(billing_tier);

-- Индекс для поиска всех workspaces, созданных конкретным пользователем.
CREATE INDEX idx_workspaces_created_by ON workspaces(created_by);

-- Индекс для фильтрации workspaces по статусу подписки (например, найти все просроченные).
CREATE INDEX idx_workspaces_status ON workspaces(subscription_status);

-- Составной индекс для мониторинга использования квот.
-- Применяется только к активным подпискам для оптимизации.
CREATE INDEX idx_workspaces_quota_usage ON workspaces(queries_used_this_month, quota_reset_at) 
    WHERE subscription_status = 'active';

-- ============================================================================
-- ТАБЛИЦА: user_workspaces
-- Назначение: Связь многие-ко-многим между пользователями и workspaces с указанием роли
-- ============================================================================

CREATE TABLE user_workspaces (
    -- Внешний ключ на пользователя.
    -- ON DELETE CASCADE: при удалении пользователя удаляются все его связи с workspaces.
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    
    -- Внешний ключ на workspace.
    -- ON DELETE CASCADE: при удалении workspace удаляются все связи с пользователями.
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    
    -- Роль пользователя в workspace. По умолчанию 'engineer' (обычный инженер).
    -- Возможные значения: 'owner', 'admin', 'engineer', 'viewer', 'compliance', 'support'.
    role TEXT NOT NULL DEFAULT 'engineer',
    
    -- Дата и время добавления пользователя в workspace.
    joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Кто пригласил пользователя (для аудита действий).
    -- ON DELETE SET NULL: при удалении пригласившего связь не теряется.
    invited_by UUID REFERENCES users(id) ON DELETE SET NULL,
    
    -- Составной первичный ключ (user_id, workspace_id).
    -- Гарантирует, что пользователь не может быть добавлен в один workspace дважды.
    PRIMARY KEY (user_id, workspace_id),
    
    -- Проверка допустимых значений роли.
    CONSTRAINT valid_role CHECK (role IN ('owner', 'admin', 'engineer', 'viewer', 'compliance', 'support'))
);

-- Индекс для быстрого поиска всех пользователей конкретного workspace.
CREATE INDEX idx_user_workspaces_workspace ON user_workspaces(workspace_id);

-- Индекс для быстрой фильтрации пользователей по роли (например, найти всех админов).
CREATE INDEX idx_user_workspaces_role ON user_workspaces(role);

-- ============================================================================
-- ТАБЛИЦА: projects
-- Назначение: Группировка чат-сессий по проектам (Feature #1 из ТЗ)
-- ============================================================================

CREATE TABLE projects (
    -- Первичный ключ. UUID v4.
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Связь с workspace. NULL для личных проектов пользователей (вне workspace).
    -- ON DELETE CASCADE: при удалении workspace удаляются все его проекты.
    workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE,
    
    -- Пользователь, создавший проект.
    -- ON DELETE CASCADE: при удалении пользователя удаляются его проекты.
    created_by UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    
    -- Название проекта (например, "ЖК Солнечный", "Школа №45").
    name TEXT NOT NULL,
    
    -- Описание проекта (опционально).
    description TEXT,
    
    -- Метаданные проекта в формате JSONB.
    -- Хранит: адрес объекта, тип здания, этажность, дату начала проектирования.
    -- Используется для выбора корректной версии нормативов (п.8.4 ТЗ).
    project_metadata JSONB NOT NULL DEFAULT '{}',
    
    -- Флаг активности проекта. FALSE = архивный проект.
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    
    -- Время создания проекта.
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Время последнего обновления. Обновляется триггером.
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Уникальность имени проекта в рамках: workspace + пользователь.
    -- Позволяет разным пользователям иметь проекты с одинаковыми названиями.
    CONSTRAINT unique_project_name_per_user UNIQUE (workspace_id, created_by, name)
);

-- Индекс для поиска всех проектов, созданных конкретным пользователем.
CREATE INDEX idx_projects_created_by ON projects(created_by);

-- Индекс для поиска всех проектов в конкретном workspace.
CREATE INDEX idx_projects_workspace ON projects(workspace_id);

-- Частичный индекс для быстрого получения только активных проектов.
CREATE INDEX idx_projects_active ON projects(is_active) WHERE is_active = true;

-- ============================================================================
-- ТАБЛИЦА: chat_sessions
-- Назначение: Сессии общения с AI-ассистентом, сгруппированные по проектам
-- ============================================================================

CREATE TABLE chat_sessions (
    -- Первичный ключ. UUID v4.
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Пользователь, ведущий диалог.
    -- ON DELETE CASCADE: при удалении пользователя удаляются его сессии.
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    
    -- Связь с проектом (опционально).
    -- ON DELETE SET NULL: при удалении проекта сессия сохраняется, но теряет привязку.
    project_id UUID REFERENCES projects(id) ON DELETE SET NULL,
    
    -- Автоматически генерируемый заголовок сессии (первый вопрос пользователя).
    title TEXT,
    
    -- Контекстное окно диалога в формате JSONB.
    -- Структура: {"window_size": 5, "messages": [{role, content, timestamp}]}.
    -- Используется для многошагового диалога (Feature #2).
    context_window JSONB NOT NULL DEFAULT '{"window_size": 5, "messages": []}',
    
    -- Денормализованный счетчик сообщений для быстрого отображения в UI.
    message_count INT NOT NULL DEFAULT 0,
    
    -- Флаг активности сессии. FALSE = сессия завершена/архивирована.
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    
    -- Время создания сессии.
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Время последней активности в сессии. Обновляется триггером.
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Проверка, что счетчик сообщений не может быть отрицательным.
    CONSTRAINT non_negative_messages CHECK (message_count >= 0)
);

-- Индекс для поиска всех сессий конкретного пользователя.
CREATE INDEX idx_sessions_user ON chat_sessions(user_id);

-- Индекс для поиска всех сессий в конкретном проекте.
CREATE INDEX idx_sessions_project ON chat_sessions(project_id);

-- Частичный индекс для быстрого получения только активных сессий.
CREATE INDEX idx_sessions_active ON chat_sessions(is_active) WHERE is_active = true;

-- Индекс для сортировки сессий по дате обновления (частый запрос в UI).
CREATE INDEX idx_sessions_updated ON chat_sessions(updated_at DESC);

-- ============================================================================
-- ТАБЛИЦА: messages
-- Назначение: Отдельные сообщения в рамках чат-сессии
-- ============================================================================

CREATE TABLE messages (
    -- Первичный ключ. UUID v4.
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Связь с сессией.
    -- ON DELETE CASCADE: при удалении сессии удаляются все её сообщения.
    session_id UUID NOT NULL REFERENCES chat_sessions(id) ON DELETE CASCADE,
    
    -- Роль отправителя: 'system' (системные), 'user' (пользователь), 'assistant' (AI).
    role TEXT NOT NULL,
    
    -- Текст сообщения.
    content TEXT NOT NULL,
    
    -- Цитаты из документов в формате JSONB.
    -- Структура: [{doc_id, designation, clause_number, content, similarity_score}].
    citations JSONB NOT NULL DEFAULT '[]',
    
    -- Метаданные сообщения: latency_ms, model_used, tokens_used и т.д.
    metadata JSONB NOT NULL DEFAULT '{}',
    
    -- Количество токенов в сообщении (для аналитики использования и расчета стоимости).
    token_count INT,
    
    -- Ссылка на предыдущее сообщение для построения цепочки диалога.
    -- ON DELETE SET NULL: при удалении сообщения цепочка не рвется.
    in_response_to UUID REFERENCES messages(id) ON DELETE SET NULL,
    
    -- Время создания сообщения.
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Проверка допустимых значений роли.
    CONSTRAINT valid_role CHECK (role IN ('system', 'user', 'assistant'))
);

-- Индекс для быстрого получения всех сообщений конкретной сессии.
CREATE INDEX idx_messages_session ON messages(session_id);

-- Индекс для сортировки сообщений по времени создания.
CREATE INDEX idx_messages_created ON messages(created_at);

-- Индекс для поиска ответов на конкретное сообщение.
CREATE INDEX idx_messages_in_response_to ON messages(in_response_to);

-- ============================================================================
-- ТАБЛИЦА: documents
-- Назначение: Метаданные нормативных документов в базе знаний
-- ВАЖНО: Реализована версионность согласно п.8.4 ТЗ
-- ============================================================================

CREATE TABLE documents (
    -- Первичный ключ. UUID v4.
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Привязка к workspace. NULL = публичный документ, доступный всем.
    -- ON DELETE CASCADE: при удалении workspace удаляются его приватные документы.
    workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE,
    
    -- UUID семейства документов. Группирует разные версии одного документа.
    -- Например, СП 113.13330.2016 и СП 113.13330.2023 имеют одинаковый document_family_id.
    document_family_id UUID,
    
    -- Тип документа. TEXT без ограничений, так как типов может быть много.
    -- Основные: 'СП', 'ГОСТ', 'СанПиН', 'ТР ТС', 'РМД', 'Пособие', 'Договор', 'Политика'.
    type TEXT NOT NULL,
    
    -- Тематика документа (например, "Пожарная безопасность", "Вентиляция").
    topic TEXT,
    
    -- Обозначение документа (например, "113.13330.2023").
    designation TEXT NOT NULL,
    
    -- Полное официальное название документа.
    official_title TEXT NOT NULL,
    
    -- Год утверждения/издания.
    year INT NOT NULL,
    
    -- Теги для фильтрации и поиска. Массив строк.
    tags TEXT[] DEFAULT '{}',
    
    -- Дата вступления документа в силу..
    valid_from DATE,
    
    -- Дата окончания действия документа. NULL = действующий.
    valid_to DATE,
    
    -- Флаг обязательности применения (может быть рекомендательным).
    is_mandatory BOOLEAN DEFAULT FALSE,
    
    -- Видимость документа: 'public' (всем), 'private' (только workspace), 'shared' (несколько workspaces).
    visibility TEXT NOT NULL DEFAULT 'public',
    
    -- Источник документа: 'official' (из реестра), 'uploaded' (загружен), 'parsed' (спарсен).
    source_type TEXT NOT NULL DEFAULT 'official',
    
    -- Кто загрузил документ (для приватных документов workspace).
    -- ON DELETE SET NULL: при удалении пользователя документ сохраняется.
    uploaded_by UUID REFERENCES users(id) ON DELETE SET NULL,
    
    -- Статус версии: 'active' (действующая), 'superseded' (заменена).
    version_status TEXT DEFAULT 'active',
    
    -- Ссылка на версию, которую заменяет этот документ.
    supersedes_id UUID REFERENCES documents(id),
    
    -- Ссылка на версию, которой заменен этот документ.
    superseded_by_id UUID REFERENCES documents(id),
    
    -- Метаданные обработки: статус парсинга, ошибки, количество чанков и т.д.
    processing_metadata JSONB DEFAULT '{}',
    
    -- Время создания записи.
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Время последнего обновления. Обновляется триггером.
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Проверка допустимых значений видимости.
    CONSTRAINT valid_visibility CHECK (visibility IN ('public', 'private', 'shared')),
    
    -- Проверка допустимых значений статуса версии.
    CONSTRAINT valid_version_status CHECK (version_status IN ('active', 'superseded')),
    
    -- Проверка допустимых значений источника.
    CONSTRAINT valid_source CHECK (source_type IN ('official', 'uploaded', 'parsed'))
);

-- Частичный уникальный индекс: гарантирует, что в публичной базе только ОДНА активная версия документа.
-- Предотвращает ситуацию, когда одновременно активны СП 113.13330.2016 и СП 113.13330.2023.
CREATE UNIQUE INDEX idx_unique_active_version_per_family 
    ON documents (document_family_id) 
    WHERE visibility = 'public' AND version_status = 'active';

-- Индекс для поиска всех версий одного семейства документов.
CREATE INDEX idx_documents_family ON documents(document_family_id);

-- Индекс для фильтрации документов по диапазону дат действия.
-- Используется для проверки актуальности документа на дату проекта.
CREATE INDEX idx_documents_valid_range ON documents(valid_from, valid_to) WHERE valid_from IS NOT NULL;

-- Индекс для поиска документов в конкретном workspace.
CREATE INDEX idx_documents_workspace ON documents(workspace_id);

-- Индекс для фильтрации документов по типу.
CREATE INDEX idx_documents_type ON documents(type);

-- Индекс для поиска документов по обозначению (частый запрос).
CREATE INDEX idx_documents_designation ON documents(designation);

-- Индекс для фильтрации документов по видимости.
CREATE INDEX idx_documents_visibility ON documents(visibility);

-- Индекс для фильтрации документов по статусу версии.
CREATE INDEX idx_documents_version_status ON documents(version_status);

-- GIN индекс для быстрого поиска по массиву тегов.
CREATE INDEX idx_documents_tags ON documents USING GIN (tags);

-- Составной индекс для быстрого поиска публичных активных документов по обозначению и году.
CREATE INDEX idx_documents_public_active ON documents(designation, year) 
    WHERE visibility = 'public' AND version_status = 'active';

-- ============================================================================
-- ТАБЛИЦА: glossary_terms (п.5 "Ответов на вопросы")
-- Назначение: Централизованное хранилище терминов для нормализации и query rewriting
-- ============================================================================
CREATE TABLE glossary_terms (
    -- Первичный ключ. UUID v4.
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Термин (например, "пандус", "рампа").
    term TEXT NOT NULL,
    
    -- Определение термина из нормативного документа.
    definition TEXT NOT NULL,
    
    -- Ссылка на документ-источник (глава "Термины и определения").
    -- ON DELETE CASCADE: при удалении документа удаляются его термины.
    source_doc_id UUID REFERENCES documents(id) ON DELETE CASCADE,
    
    -- Синонимы и альтернативные написания термина (например, ["рампа", "наклонная поверхность"]).
    -- Используется для расширения поискового запроса (query expansion).
    aliases TEXT[] DEFAULT '{}',
    
    -- Время создания записи.
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- GIN индекс с триграммами для нечеткого поиска по термину.
-- Позволяет находить термины даже с опечатками (например, "пондус" -> "пандус").
CREATE INDEX idx_glossary_term ON glossary_terms USING GIN (term gin_trgm_ops);

-- Индекс для поиска всех терминов из конкретного документа.
CREATE INDEX idx_glossary_source ON glossary_terms(source_doc_id);

-- ============================================================================
-- ТАБЛИЦА: document_references
-- Назначение: Связи между документами и библиографическими ссылками
-- ============================================================================
CREATE TABLE document_references (
    -- Первичный ключ. UUID v4.
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Документ, в котором встречается ссылка.
    -- ON DELETE CASCADE: при удалении документа удаляются его ссылки.
    source_doc_id UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    
    -- Маркер ссылки в тексте (например, "[1]", "[6]", "[15]").
    ref_marker TEXT NOT NULL,
    
    -- Расшифрованное название документа-источника.
    -- Например: "Федеральный закон №190-ФЗ 'Градостроительный кодекс РФ'".
    resolved_title TEXT,
    
    -- Ссылка на документ в системе, если он загружен в базу.
    -- ON DELETE SET NULL: при удалении целевого документа связь обнуляется.
    target_doc_id UUID REFERENCES documents(id) ON DELETE SET NULL,
    
    -- Время создания записи.
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Индекс для поиска всех ссылок в конкретном документе.
CREATE INDEX idx_ref_source_doc ON document_references(source_doc_id);

-- Индекс для поиска документов, которые ссылаются на конкретный документ.
CREATE INDEX idx_ref_target_doc ON document_references(target_doc_id);

-- ============================================================================
-- ТАБЛИЦА: document_sections
-- Назначение: Иерархическая структура разделов документа
-- ============================================================================

CREATE TABLE document_sections (
    -- Первичный ключ. UUID v4.
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Связь с документом.
    -- ON DELETE CASCADE: при удалении документа удаляются все его разделы.
    doc_id UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    
    -- Номер/код раздела (например, "5", "5.1", "Приложение А").
    section_code TEXT,
    
    -- Название раздела (например, "Требования к зданиям и сооружениям").
    section_title TEXT NOT NULL,
    
    -- Полный путь в иерархии для быстрой навигации.
    -- Например: "6. Требования > 6.5 Жилые здания > 6.5.6 Одноквартирные жилые дома".
    hierarchy_path TEXT,
    
    -- Уровень вложенности (0 = корневой, 1 = раздел первого уровня и т.д.).
    level INT NOT NULL DEFAULT 0,
    
    -- Ссылка на родительский раздел для построения дерева.
    -- ON DELETE CASCADE: при удалении родителя удаляются все дочерние разделы.
    parent_section_id UUID REFERENCES document_sections(id) ON DELETE CASCADE,
    
    -- Время создания записи.
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Индекс для поиска всех разделов конкретного документа.
CREATE INDEX idx_sections_doc ON document_sections(doc_id);

-- Индекс для поиска дочерних разделов по родителю.
CREATE INDEX idx_sections_parent ON document_sections(parent_section_id);

-- Индекс для поиска разделов по пути в иерархии.
CREATE INDEX idx_sections_path ON document_sections(hierarchy_path);

-- Индекс для поиска разделов по коду/номеру.
CREATE INDEX idx_sections_code ON document_sections(section_code);

-- ============================================================================
-- ТАБЛИЦА: chunks
-- Назначение: Фрагменты документов для векторного поиска (RAG)
-- ВАЖНО: Один чанк может содержать НЕСКОЛЬКО пунктов, если они короткие и идут подряд
-- Вектор хранится во внешней векторной БД (Qdrant), связь через qdrant_point_id
-- ============================================================================

CREATE TABLE chunks (
    -- Первичный ключ. UUID v4 генерируется автоматически.
    -- Этот же ID используется как Point ID в Qdrant для синхронизации.
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- ID точки в Qdrant. Дублирует id для удобства внешних запросов и дебага.
    -- Уникальное, так как каждый чанк имеет ровно один вектор в Qdrant.
    qdrant_point_id UUID UNIQUE,
    
    -- Связь с документом. Обязательное поле.
    -- ON DELETE CASCADE: при удалении документа удаляются все его чанки.
    doc_id UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    
    -- Связь с разделом документа. Может быть NULL, если чанк не привязан к разделу.
    -- ON DELETE SET NULL: при удалении раздела чанк сохраняется.
    section_id UUID REFERENCES document_sections(id) ON DELETE SET NULL,
    
    -- Денормализованный workspace_id. КРИТИЧЕСКИ ВАЖНО для RLS и фильтрации поиска.
    -- Передается в Qdrant как payload для фильтрации результатов по workspace.
    -- ON DELETE CASCADE: при удалении workspace удаляются все его чанки.
    workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE,
    
    -- Путь в иерархии разделов (копия из document_sections.hierarchy_path).
    -- Используется для отображения контекста в UI и навигации.
    section_path TEXT,
    
    -- Начальный пункт в чанке (например, "5.26.1")
    -- Может быть NULL, если чанк не привязан к конкретному пункту
    clause_start TEXT,
    
    -- Конечный пункт в чанке (например, "5.26.3")
    -- Если чанк содержит только один пункт, clause_start = clause_end
    -- NULL, если чанк не привязан к пунктам
    clause_end TEXT,
    
    -- Массив ВСЕХ номеров пунктов, включенных в чанк
    -- Пример: ['5.26.1', '5.26.2', '5.26.3']
    -- Используется для точного поиска по номеру пункта (UC1) и валидации ответов LLM
    -- GIN индекс позволяет быстро искать: WHERE '5.26.1' = ANY(clause_numbers)
    clause_numbers TEXT[] DEFAULT '{}',
    
    -- Человеко-читаемое отображение пунктов для UI
    -- Например: "п. 5.26.1" (один пункт) или "пп. 5.26.1-5.26.3" (диапазон)
    -- Генерируется в ETL пайплайне, чтобы не вычислять на лету
    clause_display TEXT,
    
    -- Количество пунктов, объединенных в этот чанк
    -- 0 = чанк без пунктов
    -- 1 = один пункт
    -- >1 = несколько пунктов объединены
    merged_clauses_count INT DEFAULT 1,
    
    -- Порядковый номер чанка в документе.
    -- Используется для навигации ("следующий чанк", "предыдущий чанк") и сортировки.
    chunk_index INT,
    
    -- Тип контента: 'text', 'table', 'formula', 'image'
    -- Определяет, как отображать и обрабатывать чанк.
    content_type TEXT NOT NULL DEFAULT 'text',
    
    -- Ссылка на родительский чанк для иерархической навигации.
    -- Например, чанк с таблицей может быть дочерним для чанка с текстом.
    -- ON DELETE SET NULL: при удалении родителя связь обнуляется.
    parent_chunk_id UUID REFERENCES chunks(id) ON DELETE SET NULL,
    
    -- URL для изображений
    -- NULL для текстовых чанков.
    content_url TEXT,
    
    -- Текстовое содержимое чанка. ОБЯЗАТЕЛЬНОЕ ПОЛЕ.
    -- Для объединенных пунктов содержит текст всех пунктов подряд с сохранением нумерации.
    -- Для чанков типа 'image'/'table' содержит OCR-текст или текстовое описание.
    -- Используется для полнотекстового поиска и как контекст для LLM.
    text_content TEXT NOT NULL,
    
    -- Количество токенов в text_content.
    -- Используется для контроля размера чанка при объединении пунктов (лимит ~500 токенов).
    token_count INT,
    
    -- Время создания чанка.
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Проверка допустимых значений типа контента.
    CONSTRAINT valid_content_type CHECK (content_type IN ('text', 'table', 'formula', 'image')),
    
    -- Проверка, что если указан clause_start, то и clause_end должен быть указан.
    CONSTRAINT clause_range_valid CHECK (
        (clause_start IS NULL AND clause_end IS NULL) OR 
        (clause_start IS NOT NULL AND clause_end IS NOT NULL)
    ),
    
    -- Проверка, что merged_clauses_count соответствует clause_numbers
    CONSTRAINT merged_count_valid CHECK (
        merged_clauses_count >= 0 AND
        (clause_numbers IS NULL OR array_length(clause_numbers, 1) = merged_clauses_count)
    )
);

-- Индекс для поиска всех чанков конкретного документа.
-- Высокочастотный запрос при навигации по документу.
CREATE INDEX idx_chunks_doc ON chunks(doc_id);

-- Индекс для поиска чанков в конкретном разделе.
CREATE INDEX idx_chunks_section ON chunks(section_id);

-- Индекс для фильтрации чанков по workspace.
-- КРИТИЧЕСКИ ВАЖНО для RLS и быстрой фильтрации при поиске.
CREATE INDEX idx_chunks_workspace ON chunks(workspace_id);

-- Индекс для фильтрации чанков по типу контента.
-- Используется, когда нужно искать только текст или только таблицы.
CREATE INDEX idx_chunks_type ON chunks(content_type);

-- Индекс для поиска дочерних чанков по родителю.
CREATE INDEX idx_chunks_parent ON chunks(parent_chunk_id);

-- Составной индекс для сортировки чанков по порядку в документе.
-- Используется для пагинации и навигации "следующий/предыдущий".
CREATE INDEX idx_chunks_doc_index ON chunks(doc_id, chunk_index);

-- Индекс для поиска по начальному пункту.
-- Используется при запросах вида "найти все чанки, начиная с пункта 5.26".
CREATE INDEX idx_chunks_clause_start ON chunks(clause_start) WHERE clause_start IS NOT NULL;

-- GIN индекс для поиска по массиву номеров пунктов.
-- КЛЮЧЕВОЙ ИНДЕКС для UC1: "В каком пункте описываются требования к уклонам?"
-- Позволяет искать: WHERE '5.26.1' = ANY(clause_numbers)
CREATE INDEX idx_chunks_clause_numbers ON chunks USING GIN (clause_numbers);

-- GIN индекс с триграммами для полнотекстового поиска по содержимому.
-- Используется в гибридном поиске (векторный + полнотекстовый).
-- pg_trgm поддерживает нечеткий поиск и поиск по подстроке.
CREATE INDEX idx_chunks_text_content ON chunks USING GIN (text_content gin_trgm_ops);

-- Индекс для поиска по qdrant_point_id (используется при синхронизации и удалении).
CREATE INDEX idx_chunks_qdrant_point ON chunks(qdrant_point_id) WHERE qdrant_point_id IS NOT NULL;

-- ============================================================================
-- ТАБЛИЦА: document_uploads
-- Назначение: Отслеживание процесса загрузки документов (Post-MVP Feature #3)
-- ============================================================================

CREATE TABLE document_uploads (
    -- Первичный ключ. UUID v4.
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Workspace, в который загружается документ.
    -- ON DELETE CASCADE: при удалении workspace удаляется история загрузок.
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    
    -- Пользователь, загрузивший документ.
    -- ON DELETE CASCADE: при удалении пользователя история загрузок сохраняется? 
    -- В DDL указано CASCADE, но логичнее SET NULL. Проверить!
    uploaded_by UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    
    -- Оригинальное имя загруженного файла (например, "СП_113_13330_2023.pdf").
    original_filename TEXT NOT NULL,
    
    -- Путь к файлу в объектном хранилище (S3/MinIO).
    storage_path TEXT NOT NULL,
    
    -- MIME-тип файла (например, "application/pdf").
    mime_type TEXT,
    
    -- Размер файла в байтах.
    file_size_bytes BIGINT,
    
    -- Статус обработки: 'pending', 'processing', 'completed', 'failed'.
    processing_status TEXT NOT NULL DEFAULT 'pending',
    
    -- JSONB массив ошибок валидации/обработки.
    -- Пример: [{"field": "pages", "error": "Unable to parse page 5"}].
    validation_errors JSONB DEFAULT '[]',
    
    -- Ссылка на созданный документ после успешной обработки.
    -- ON DELETE SET NULL: при удалении документа запись о загрузке сохраняется.
    resulting_doc_id UUID REFERENCES documents(id) ON DELETE SET NULL,
    
    -- Время создания записи о загрузке.
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Время завершения обработки (успешной или с ошибкой).
    completed_at TIMESTAMPTZ,
    
    -- Проверка допустимых значений статуса обработки.
    CONSTRAINT valid_upload_status CHECK (processing_status IN ('pending', 'processing', 'completed', 'failed')),
    
    -- Проверка, что размер файла положительный (если указан).
    CONSTRAINT positive_file_size CHECK (file_size_bytes IS NULL OR file_size_bytes > 0)
);

-- Индекс для поиска всех загрузок в конкретном workspace.
CREATE INDEX idx_uploads_workspace ON document_uploads(workspace_id);

-- Индекс для фильтрации загрузок по статусу (например, найти все pending).
CREATE INDEX idx_uploads_status ON document_uploads(processing_status);

-- Индекс для сортировки загрузок по дате создания (частый запрос в админке).
CREATE INDEX idx_uploads_created ON document_uploads(created_at DESC);

-- Индекс для поиска всех загрузок конкретного пользователя.
CREATE INDEX idx_uploads_uploaded_by ON document_uploads(uploaded_by);

-- ============================================================================
-- ТАБЛИЦА: query_cache
-- Назначение: Кеширование ответов на частые запросы для экономии токенов
-- ============================================================================

CREATE TABLE query_cache (
    -- Первичный ключ. UUID v4.
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Хеш нормализованного запроса (MD5 или SHA256).
    -- Используется для быстрого поиска в кеше.
    query_hash TEXT UNIQUE NOT NULL,
    
    -- Оригинальный запрос после нормализации.
    -- Нормализация: приведение к нижнему регистру, удаление пунктуации.
    normalized_query TEXT,
    
    -- Закешированный ответ (текст ответа AI).
    response_text TEXT,
    
    -- Цитаты из документов, использованные в ответе.
    citations JSONB DEFAULT '[]',
    
    -- Счетчик обращений к кешу (для аналитики популярности запросов).
    hits INT NOT NULL DEFAULT 1,
    
    -- Время последнего обращения к кешу.
    last_used TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Время создания записи в кеше.
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Индекс для быстрого поиска по хешу (основной сценарий использования кеша).
CREATE INDEX idx_cache_hash ON query_cache(query_hash);

-- Индекс для получения самых популярных запросов (по количеству hits).
CREATE INDEX idx_cache_hits ON query_cache(hits DESC);

-- Индекс для поиска устаревших записей (для очистки кеша).
CREATE INDEX idx_cache_last_used ON query_cache(last_used DESC);

-- ============================================================================
-- ТРИГГЕРЫ ДЛЯ АВТОМАТИЧЕСКОГО ОБНОВЛЕНИЯ updated_at
-- ============================================================================

-- Функция, обновляющая поле updated_at при любом изменении строки.
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    -- Устанавливаем текущее время в поле updated_at.
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггер для таблицы workspaces.
CREATE TRIGGER update_workspaces_updated_at 
    BEFORE UPDATE ON workspaces 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Триггер для таблицы projects.
CREATE TRIGGER update_projects_updated_at 
    BEFORE UPDATE ON projects 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Триггер для таблицы chat_sessions.
CREATE TRIGGER update_chat_sessions_updated_at 
    BEFORE UPDATE ON chat_sessions 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Триггер для таблицы documents.
CREATE TRIGGER update_documents_updated_at 
    BEFORE UPDATE ON documents 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- ФУНКЦИЯ ДЛЯ УВЕЛИЧЕНИЯ СЧЕТЧИКА СООБЩЕНИЙ В СЕССИИ
-- ============================================================================

CREATE OR REPLACE FUNCTION increment_message_count(p_session_id UUID)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    -- Увеличиваем счетчик сообщений и обновляем updated_at.
    UPDATE chat_sessions 
    SET message_count = message_count + 1,
        updated_at = NOW()
    WHERE id = p_session_id;
END;
$$;

-- ============================================================================
-- ФУНКЦИЯ ДЛЯ ИНКРЕМЕНТА СЧЕТЧИКА ЗАПРОСОВ WORKSPACE
-- ============================================================================

CREATE OR REPLACE FUNCTION increment_workspace_query_counter(p_workspace_id UUID)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    -- Увеличиваем счетчик использованных запросов в текущем месяце.
    UPDATE workspaces 
    SET queries_used_this_month = queries_used_this_month + 1,
        updated_at = NOW()
    WHERE id = p_workspace_id;
END;
$$;

-- ============================================================================
-- ФУНКЦИЯ ДЛЯ СБРОСА СЧЕТЧИКОВ ЗАПРОСОВ ПО РАСПИСАНИЮ
-- ============================================================================

CREATE OR REPLACE FUNCTION reset_expired_query_counters()
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    -- Сбрасываем счетчики для всех workspaces, у которых наступила дата сброса.
    UPDATE workspaces 
    SET queries_used_this_month = 0,
        -- Устанавливаем следующую дату сброса (первое число следующего месяца).
        quota_reset_at = DATE_TRUNC('month', NOW()) + INTERVAL '1 month',
        updated_at = NOW()
    WHERE quota_reset_at <= NOW() 
      AND queries_used_this_month > 0;
END;
$$;

-- ============================================================================
-- ROW LEVEL SECURITY (RLS) ПОЛИТИКИ
-- Назначение: Изоляция приватных документов на уровне БД
-- ============================================================================

-- Включаем RLS для таблиц, содержащих приватные данные workspace.
ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE chunks ENABLE ROW LEVEL SECURITY;
ALTER TABLE document_sections ENABLE ROW LEVEL SECURITY;
ALTER TABLE document_uploads ENABLE ROW LEVEL SECURITY;
ALTER TABLE chat_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE projects ENABLE ROW LEVEL SECURITY;

-- ----------------------------------------------------------------------------
-- Вспомогательная функция: получение ID текущего пользователя из сессии
-- Примечание: Бэкенд должен устанавливать переменную app.current_user_id при старте соединения.
-- Пример: SELECT set_config('app.current_user_id', user_id::TEXT, false);
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_current_user_id()
RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
    user_id_str TEXT;
BEGIN
    -- Пытаемся прочитать переменную сессии app.current_user_id.
    user_id_str := current_setting('app.current_user_id', TRUE);
    
    -- Если переменная не установлена или пустая, возвращаем NULL.
    IF user_id_str IS NULL OR user_id_str = '' THEN
        RETURN NULL;
    END IF;
    
    -- Преобразуем строку в UUID и возвращаем.
    RETURN user_id_str::UUID;
EXCEPTION
    -- При любой ошибке (например, неверный формат) возвращаем NULL.
    WHEN OTHERS THEN
        RETURN NULL;
END;
$$;

-- ----------------------------------------------------------------------------
-- Вспомогательная функция: проверка доступа пользователя к workspace
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION user_has_workspace_access(p_workspace_id UUID, p_user_id UUID DEFAULT NULL)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER  -- Выполняется с правами владельца функции (обход RLS)
AS $$
DECLARE
    v_user_id UUID;
BEGIN
    -- Определяем ID пользователя: переданный параметр или текущий из сессии.
    v_user_id := COALESCE(p_user_id, get_current_user_id());
    
    -- Если пользователь не определен, доступ запрещен.
    IF v_user_id IS NULL THEN
        RETURN FALSE;
    END IF;
    
    -- Проверяем наличие связи пользователя с workspace в таблице user_workspaces.
    RETURN EXISTS (
        SELECT 1 FROM user_workspaces 
        WHERE user_id = v_user_id AND workspace_id = p_workspace_id
    );
END;
$$;

-- ----------------------------------------------------------------------------
-- Вспомогательная функция: получение списка workspaces пользователя
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_user_workspaces(p_user_id UUID DEFAULT NULL)
RETURNS UUID[]
LANGUAGE plpgsql
SECURITY DEFINER  -- Выполняется с правами владельца функции
AS $$
DECLARE
    v_user_id UUID;
BEGIN
    -- Определяем ID пользователя.
    v_user_id := COALESCE(p_user_id, get_current_user_id());
    
    -- Если пользователь не определен, возвращаем пустой массив.
    IF v_user_id IS NULL THEN
        RETURN ARRAY[]::UUID[];
    END IF;
    
    -- Возвращаем массив ID всех workspaces, к которым имеет доступ пользователь.
    RETURN ARRAY(
        SELECT workspace_id FROM user_workspaces WHERE user_id = v_user_id
    );
END;
$$;

-- ============================================================================
-- ПОЛИТИКИ RLS ДЛЯ ТАБЛИЦЫ documents
-- ============================================================================

-- Политика для SELECT:
-- 1. Видны все публичные документы (visibility = 'public')
-- 2. Видны приватные документы только для пользователей из того же workspace
CREATE POLICY documents_select_policy ON documents
    FOR SELECT
    USING (
        -- Публичные документы доступны всем.
        visibility = 'public' 
        OR (
            -- Приватные документы доступны только членам workspace.
            visibility = 'private' 
            AND workspace_id IS NOT NULL 
            AND user_has_workspace_access(workspace_id)
        )
    );

-- Политика для INSERT: только пользователи workspace могут добавлять документы.
CREATE POLICY documents_insert_policy ON documents
    FOR INSERT
    WITH CHECK (
        -- Публичные документы добавляются системой (workspace_id = NULL).
        workspace_id IS NULL
        -- Приватные документы может добавлять только член workspace.
        OR user_has_workspace_access(workspace_id)
    );

-- Политика для UPDATE: только пользователи workspace с ролью admin/owner.
CREATE POLICY documents_update_policy ON documents
    FOR UPDATE
    USING (
        workspace_id IS NOT NULL 
        AND EXISTS (
            SELECT 1 FROM user_workspaces 
            WHERE user_id = get_current_user_id() 
                AND workspace_id = documents.workspace_id 
                AND role IN ('owner', 'admin')
        )
    );

-- Политика для DELETE: только владелец workspace.
CREATE POLICY documents_delete_policy ON documents
    FOR DELETE
    USING (
        workspace_id IS NOT NULL 
        AND EXISTS (
            SELECT 1 FROM user_workspaces 
            WHERE user_id = get_current_user_id() 
                AND workspace_id = documents.workspace_id 
                AND role = 'owner'
        )
    );

-- ============================================================================
-- ROW LEVEL SECURITY (RLS) ПОЛИТИКИ
-- Назначение: Изоляция приватных данных на уровне БД
-- Доступ:
--   - postgres / supabase_admin / DE команда: полные права (DDL + DML)
--   - authenticated (команда DS): CRUD в указанных таблицах + SELECT в остальных
--   - anon: полный запрет доступа
-- ============================================================================

-- ============================================================================
-- ВКЛЮЧЕНИЕ RLS ДЛЯ ВСЕХ ТАБЛИЦ
-- ============================================================================

-- Таблицы с полным CRUD для DS (SELECT, INSERT, UPDATE, DELETE)
ALTER TABLE chat_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE query_cache ENABLE ROW LEVEL SECURITY;

-- Таблицы только для чтения (SELECT)
ALTER TABLE chunks ENABLE ROW LEVEL SECURITY;
ALTER TABLE document_references ENABLE ROW LEVEL SECURITY;
ALTER TABLE document_sections ENABLE ROW LEVEL SECURITY;
ALTER TABLE document_uploads ENABLE ROW LEVEL SECURITY;
ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE glossary_terms ENABLE ROW LEVEL SECURITY;
ALTER TABLE projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscription_plans ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_workspaces ENABLE ROW LEVEL SECURITY;
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE workspaces ENABLE ROW LEVEL SECURITY;

-- ============================================================================
-- ОТКЛЮЧЕНИЕ ДОСТУПА ДЛЯ РОЛИ anon КО ВСЕМ ТАБЛИЦАМ
-- ============================================================================

-- Отзываем все права у роли anon на все таблицы
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') 
    LOOP
        EXECUTE format('REVOKE ALL ON TABLE public.%I FROM anon', r.tablename);
    END LOOP;
END $$;

-- Отзываем USAGE на схему public для anon
REVOKE USAGE ON SCHEMA public FROM anon;

-- ============================================================================
-- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ (для будущих B2B сценариев)
-- ============================================================================

-- Функция: получение ID текущего пользователя из сессии
CREATE OR REPLACE FUNCTION get_current_user_id()
RETURNS UUID
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    user_id_str TEXT;
BEGIN
    user_id_str := current_setting('app.current_user_id', TRUE);
    IF user_id_str IS NULL OR user_id_str = '' THEN
        RETURN NULL;
    END IF;
    RETURN user_id_str::UUID;
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$$;

-- Функция: проверка доступа пользователя к workspace
CREATE OR REPLACE FUNCTION user_has_workspace_access(p_workspace_id UUID, p_user_id UUID DEFAULT NULL)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_user_id UUID;
BEGIN
    v_user_id := COALESCE(p_user_id, get_current_user_id());
    IF v_user_id IS NULL THEN
        RETURN FALSE;
    END IF;
    RETURN EXISTS (
        SELECT 1 FROM user_workspaces 
        WHERE user_id = v_user_id AND workspace_id = p_workspace_id
    );
END;
$$;

-- Функция: получение списка workspaces пользователя
CREATE OR REPLACE FUNCTION get_user_workspaces(p_user_id UUID DEFAULT NULL)
RETURNS UUID[]
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_user_id UUID;
BEGIN
    v_user_id := COALESCE(p_user_id, get_current_user_id());
    IF v_user_id IS NULL THEN
        RETURN ARRAY[]::UUID[];
    END IF;
    RETURN ARRAY(
        SELECT workspace_id FROM user_workspaces WHERE user_id = v_user_id
    );
END;
$$;

-- ============================================================================
-- ПОЛИТИКИ ДЛЯ ТАБЛИЦ С ПОЛНЫМ CRUD (ТОЛЬКО ДЛЯ authenticated)
-- chat_sessions, messages, query_cache
-- ============================================================================

-- ----------------------------------------------------------------------------
-- chat_sessions: полный доступ для DS команды
-- ----------------------------------------------------------------------------
CREATE POLICY chat_sessions_ds_policy ON chat_sessions
    FOR ALL
    TO authenticated
    USING (true)
    WITH CHECK (true);

-- ----------------------------------------------------------------------------
-- messages: полный доступ для DS команды
-- ----------------------------------------------------------------------------
CREATE POLICY messages_ds_policy ON messages
    FOR ALL
    TO authenticated
    USING (true)
    WITH CHECK (true);

-- ----------------------------------------------------------------------------
-- query_cache: полный доступ для DS команды
-- ----------------------------------------------------------------------------
CREATE POLICY query_cache_ds_policy ON query_cache
    FOR ALL
    TO authenticated
    USING (true)
    WITH CHECK (true);

-- ============================================================================
-- ПОЛИТИКИ ДЛЯ ТАБЛИЦ ТОЛЬКО ДЛЯ ЧТЕНИЯ (ТОЛЬКО ДЛЯ authenticated)
-- chunks, document_references, document_sections, document_uploads, documents,
-- glossary_terms, projects, subscription_plans, user_workspaces, users, workspaces
-- ============================================================================

-- ----------------------------------------------------------------------------
-- documents: публичные видны всем DS, приватные - только своим workspace
-- ----------------------------------------------------------------------------
CREATE POLICY documents_select_policy ON documents
    FOR SELECT
    TO authenticated
    USING (
        visibility = 'public' 
        OR (
            visibility = 'private' 
            AND workspace_id IS NOT NULL 
            AND user_has_workspace_access(workspace_id)
        )
    );

-- ----------------------------------------------------------------------------
-- chunks: наследует видимость от documents
-- ----------------------------------------------------------------------------
CREATE POLICY chunks_select_policy ON chunks
    FOR SELECT
    TO authenticated
    USING (
        EXISTS (
            SELECT 1 FROM documents d 
            WHERE d.id = chunks.doc_id AND d.visibility = 'public'
        )
        OR (
            chunks.workspace_id IS NOT NULL 
            AND user_has_workspace_access(chunks.workspace_id)
        )
    );

-- ----------------------------------------------------------------------------
-- document_sections: доступ через родительский документ
-- ----------------------------------------------------------------------------
CREATE POLICY document_sections_select_policy ON document_sections
    FOR SELECT
    TO authenticated
    USING (
        EXISTS (
            SELECT 1 FROM documents d 
            WHERE d.id = document_sections.doc_id 
              AND (d.visibility = 'public' 
                   OR (d.visibility = 'private' AND user_has_workspace_access(d.workspace_id)))
        )
    );

-- ----------------------------------------------------------------------------
-- document_uploads: только загрузки из доступных workspace
-- ----------------------------------------------------------------------------
CREATE POLICY document_uploads_select_policy ON document_uploads
    FOR SELECT
    TO authenticated
    USING (user_has_workspace_access(workspace_id));

-- ----------------------------------------------------------------------------
-- document_references: все DS видят все ссылки
-- ----------------------------------------------------------------------------
CREATE POLICY document_references_select_policy ON document_references
    FOR SELECT
    TO authenticated
    USING (true);

-- ----------------------------------------------------------------------------
-- glossary_terms: справочник терминов виден всем DS
-- ----------------------------------------------------------------------------
CREATE POLICY glossary_terms_select_policy ON glossary_terms
    FOR SELECT
    TO authenticated
    USING (true);

-- ----------------------------------------------------------------------------
-- projects: только свои или из доступного workspace
-- ----------------------------------------------------------------------------
CREATE POLICY projects_select_policy ON projects
    FOR SELECT
    TO authenticated
    USING (
        created_by = get_current_user_id()
        OR (
            workspace_id IS NOT NULL 
            AND user_has_workspace_access(workspace_id)
        )
    );

-- ----------------------------------------------------------------------------
-- subscription_plans: справочник тарифов виден всем DS
-- ----------------------------------------------------------------------------
CREATE POLICY subscription_plans_select_policy ON subscription_plans
    FOR SELECT
    TO authenticated
    USING (true);

-- ----------------------------------------------------------------------------
-- user_workspaces: только свои связи
-- ----------------------------------------------------------------------------
CREATE POLICY user_workspaces_select_policy ON user_workspaces
    FOR SELECT
    TO authenticated
    USING (user_id = get_current_user_id());

-- ----------------------------------------------------------------------------
-- users: Доступ к таблице users для DS команды (SELECT, INSERT, UPDATE, DELETE)
-- ----------------------------------------------------------------------------
CREATE POLICY users_ds_policy ON public.users
    FOR ALL
    TO authenticated
    USING (true)
    WITH CHECK (true);

-- ----------------------------------------------------------------------------
-- workspaces: только те, куда пользователь имеет доступ
-- ----------------------------------------------------------------------------
CREATE POLICY workspaces_select_policy ON workspaces
    FOR SELECT
    TO authenticated
    USING (user_has_workspace_access(id));

-- ============================================================================
-- ВЫДАЧА ПРАВ ДЛЯ РОЛИ authenticated
-- ============================================================================

-- Даем USAGE на схему public
GRANT USAGE ON SCHEMA public TO authenticated;

-- Таблицы с полным CRUD
GRANT SELECT, INSERT, UPDATE, DELETE ON chat_sessions TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON messages TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON query_cache TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON users TO authenticated;

-- Таблицы только для чтения
GRANT SELECT ON chunks TO authenticated;
GRANT SELECT ON document_references TO authenticated;
GRANT SELECT ON document_sections TO authenticated;
GRANT SELECT ON document_uploads TO authenticated;
GRANT SELECT ON documents TO authenticated;
GRANT SELECT ON glossary_terms TO authenticated;
GRANT SELECT ON projects TO authenticated;
GRANT SELECT ON subscription_plans TO authenticated;
GRANT SELECT ON user_workspaces TO authenticated;
GRANT SELECT ON workspaces TO authenticated;

-- ============================================================================
-- ЗАПРЕТ DDL (структурных изменений) для authenticated
-- ============================================================================

REVOKE CREATE ON SCHEMA public FROM authenticated;
REVOKE TEMP ON DATABASE postgres FROM authenticated;

-- ============================================================================
-- СВОДКА ПРАВ ДОСТУПА
-- ============================================================================
-- 
-- ┌─────────────────────────┬────────────┬──────────────────┬─────────────┐
-- │ Роль                    │ BYPASSRLS  │ chat_sessions    │ documents   │
-- │                         │            │ messages         │ chunks      │
-- │                         │            │ query_cache      │ projects    │
-- ├─────────────────────────┼────────────┼──────────────────┼─────────────┤
-- │ postgres (DE)           │    ✅ Да   │ ✅ ПОЛНЫЙ ДОСТУП │ ✅ ПОЛНЫЙ   │
-- │ supabase_admin          │    ✅ Да   │ ✅ ПОЛНЫЙ ДОСТУП │ ✅ ПОЛНЫЙ   │
-- ├─────────────────────────┼────────────┼──────────────────┼─────────────┤
-- │ authenticated (DS)      │    ❌ Нет  │ ✅ CRUD (через RLS)│ ✅ SELECT  │
-- ├─────────────────────────┼────────────┼──────────────────┼─────────────┤
-- │ anon                    │    ❌ Нет  │ ❌ НЕТ ДОСТУПА   │ ❌ НЕТ      │
-- └─────────────────────────┴────────────┴──────────────────┴─────────────┘
-- 
-- ============================================================================
-- ПРИМЕЧАНИЕ ДЛЯ DE КОМАНДЫ
-- ============================================================================
-- 
-- 1. DE должны подключаться к БД используя роль postgres
-- 
-- 2. При добавлении новых таблиц НЕ ЗАБЫВАТЬ:
--    - ALTER TABLE new_table ENABLE ROW LEVEL SECURITY;
--    - REVOKE ALL ON new_table FROM anon;
--    - GRANT нужные права TO authenticated;
--    - Создать политику для authenticated;
-- 
-- ============================================================================
-- ПРИМЕЧАНИЕ ДЛЯ DS КОМАНДЫ
-- ============================================================================
-- 
-- 1. DS должны входить через Supabase Dashboard ИЛИ через код:
--    - const { data } = await supabase.auth.signInWithPassword({...})
-- 
-- 2. Доступные таблицы для CRUD:
--    ✅ chat_sessions
--    ✅ messages  
--    ✅ query_cache
-- 
-- 3. Доступные таблицы только для чтения:
--    ✅ все остальные таблицы схемы public
-- 
-- 4. Запрещено:
--    ❌ CREATE, ALTER, DROP, TRUNCATE
--    ❌ INSERT/UPDATE/DELETE в таблицы только для чтения
-- 
-- ============================================================================