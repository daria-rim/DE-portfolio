"""
Скрипт 02_clean_markdown.py
Полная обработка документов: очистка + разбивка на секции + JSON
Вход: output/markdown/*.md
Выход:
- output/cleaned/СП_XX.XXX.XXXX.md (очищенный текст)
- output/json/СП_XX.XXX.XXXX.json (метаданные)
"""
import re
import json
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple

PROJECT_ROOT = Path(__file__).parent.parent
INPUT_DIR = PROJECT_ROOT / "output" / "markdown"
CLEANED_DIR = PROJECT_ROOT / "output" / "cleaned"
OUTPUT_DIR = PROJECT_ROOT / "output" / "json"


def _strip_json_recursive(obj):
    """Рекурсивно убирает пробелы в строках JSON."""
    if isinstance(obj, dict):
        return {k: _strip_json_recursive(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_strip_json_recursive(v) for v in obj]
    elif isinstance(obj, str):
        return obj.strip()
    return obj


def stitch_detached_headers(text: str) -> str:
    """
    Склеивает 'оторванные' заголовки с их содержимым.
    Пример:
      3.11
      инвалид: Лицо, которое...
    ->
      3.11 инвалид: Лицо, которое...
    """
    lines = text.split('\n')
    new_lines = []
    skip_next = False

    for i, line in enumerate(lines):
        if skip_next:
            skip_next = False
            continue

        stripped = line.strip()

        # Ищем строку, которая выглядит как "голый" номер пункта (3.11, 3.20 и т.д.)
        if re.match(r'^\s*\d+(\.\d+)+\s*$', stripped):
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                # Проверяем, что следующая строка не пустая и не похожа на новый заголовок
                if next_line and not re.match(r'^\s*\d+(\.\d+)*\s+[А-ЯA-Z]', next_line):
                    # Склеиваем: "3.11" + " " + "инвалид: Лицо..."
                    new_lines.append(stripped + " " + next_line)
                    skip_next = True
                    continue

        new_lines.append(line)

    return '\n'.join(new_lines)


def strip_markdown_formatting(text: str) -> str:
    """
    Удаляет базовую Markdown-разметку, чтобы не мешать
    последующим текстовым regex-парсерам.
    """
    # Удаляем заголовки Markdown (#, ##, ### и т.д.), оставляя текст заголовка
    text = re.sub(r'^#{1,6}\s+', '', text, flags=re.MULTILINE)
    return text


def fix_hyphenation(text: str) -> str:
    """
    Исправляет переносы слов с дефисом в конце строки.
    Примеры:
    - "норматив-\nнога" -> "нормативного"
    - "характери-\nстик" -> "характеристик"
    - "оповеща-\nтелей" -> "оповещателей"
    """
    text = re.sub(r'([а-яёa-z])-\s*\n\s*([а-яёa-z])', r'\1\2', text, flags=re.IGNORECASE)
    return text


def fix_word_spacing(text: str) -> str:
    """
    Исправляет пробелы в середине русских слов.
    Пример: "н а з в а н и е" -> "название"
    """
    pattern = re.compile(r'\b([а-яё])\s+([а-яё])\s+([а-яё])', re.IGNORECASE)

    prev_text = ""
    current_text = text
    iterations = 0

    while prev_text != current_text and iterations < 10:
        prev_text = current_text
        current_text = pattern.sub(r'\1\2\3', current_text)
        iterations += 1

    if iterations > 1:
        print(f"Исправлены пробелы в словах (проходов: {iterations})")

    return current_text


def remove_table_of_contents(text: str) -> Tuple[str, bool]:
    """
    Обнаруживает и удаляет содержание (оглавление) из текста.
    Поддерживает обычное и табличное содержание.
    """
    lines = text.split('\n')

    # Маркеры начала содержания
    toc_start_markers = [
        r'^\s*СОДЕРЖАНИЕ\s*$',
        r'^\s*Содержание\s*$',
        r'^\s*Оглавление\s*$',
    ]

    # Маркеры строк, похожих на содержание
    toc_content_markers = [
        r'^\s*\d+(\.\d+)*\s+[А-ЯЁ].+\.{3,}',       # "1.1 Название....... 5"
        r'^\s*\d+(\.\d+)*\s+[А-ЯЁ].+\d{1,3}$',      # "1.1 Название 5"
        r'^\s*\d+(\.\d+)*\s+[А-ЯЁ]',                 # "1.1 Название"
        r'^\|\s*\d+',                                 # Табличная строка (markdown)
    ]

    # Маркеры конца содержания
    toc_end_markers = [
        r'^\s*1\.\s+Область\s+применения',
        r'^\s*1\s+Область\s+применения',
        r'^\s*ВВЕДЕНИЕ\s*$',
        r'^\s*Введение\s*$',
        r'^\s*ПРЕДИСЛОВИЕ\s*$',
    ]

    # Ищем начало содержания
    toc_start = None
    for i, line in enumerate(lines[:100]):
        for marker in toc_start_markers:
            if re.match(marker, line.strip(), re.IGNORECASE):
                toc_start = i
                break
        if toc_start is not None:
            break

    if toc_start is None:
        return text, False

    # Ищем конец содержания
    toc_end = None
    toc_content_count = 0

    for i in range(toc_start + 1, min(toc_start + 200, len(lines))):
        line = lines[i].strip()

        for marker in toc_end_markers:
            if re.match(marker, line, re.IGNORECASE):
                toc_end = i
                break
        if toc_end is not None:
            break

        for marker in toc_content_markers:
            if re.match(marker, line, re.IGNORECASE):
                toc_content_count += 1
                break

        if i > toc_start + 3 and toc_content_count == 0:
            return text, False

    if toc_end is None and toc_content_count > 3:
        for i in range(toc_start + 3, min(toc_start + 200, len(lines))):
            if not lines[i].strip():
                for j in range(i + 1, min(i + 5, len(lines))):
                    if lines[j].strip() and re.match(r'^\s*\d+[\s\.]+[А-ЯЁ]', lines[j]):
                        toc_end = i
                        break
                if toc_end:
                    break

    if toc_end:
        cleaned_lines = lines[:toc_start] + lines[toc_end:]
        print(f"Удалено содержание (строки {toc_start}-{toc_end})")
        return '\n'.join(cleaned_lines), True

    return text, False


def extract_metadata_from_text(text: str, filename: str) -> Dict:
    """Извлекает метаданные из текста документа."""
    metadata = {
        "type": "СП",
        "designation": "",
        "year": 0,
        "official_title": "",
        "valid_from": None,
        "is_mandatory": False,
        "topic": ""
    }
    search_text = text[:20000]

    # 1. Обозначение СП и год
    sp_patterns = [
        r'СП\s+(\d{1,3}\.\d{5}\.\d{4})',
        r'СП(\d{1,3}\.\d{5}\.\d{4})',
        r'СП\s*\n+\s*(\d{1,3}\.\d{5}\.\d{4})',
        r'Свод\s+правил\s+(\d{1,3}\.\d{5}\.\d{4})',
        r'СП.*?(\d{1,3}\.\d{5}\.\d{4})',
    ]
    for pattern in sp_patterns:
        sp_match = re.search(pattern, search_text, re.IGNORECASE | re.DOTALL | re.MULTILINE)
        if sp_match:
            designation = sp_match.group(1)
            metadata["designation"] = designation
            metadata["year"] = int(designation.split('.')[-1])
            print(f"Найден номер СП: {designation}")
            break

    if not metadata["designation"]:
        metadata_from_filename = parse_filename_metadata(filename)
        metadata.update(metadata_from_filename)
        if metadata.get("designation"):
            print(f"Номер из имени файла: {metadata['designation']}")

    # 2. Полное название
    title_patterns = [
        r'СВОД\s+ПРАВИЛ\s*\n+\s*([А-ЯЁA-Z][А-ЯЁA-Z\s\-]+?)(?:\n|$)',
        r'СП\s*\d+\.\d+\.\d+\s*\n+\s*СВОД\s+ПРАВИЛ\s*\n+\s*([А-ЯЁA-Z][А-ЯЁA-Z\s\-]+?)(?:\n|$)',
        r'СП\s*\d+\.\d+\.\d+\s*\n+\s*([А-ЯЁA-Z][А-ЯЁA-Z\s\-]{5,80}?)(?:\n|$)',
        r'СВОД\s+ПРАВИЛ\s*\n+\s*([А-ЯЁA-Z][А-ЯЁA-Z\s\-]{5,80}?)\s*\n',
    ]
    for pattern in title_patterns:
        match = re.search(pattern, search_text, re.IGNORECASE | re.MULTILINE | re.DOTALL)
        if match:
            title = match.group(1).strip()
            title = re.sub(r'\s+', ' ', title)
            exclude_words = ['ПРЕДИСЛОВИЕ', 'ВВЕДЕНИЕ', 'СОДЕРЖАНИЕ', 'ОБЛАСТЬ', 'ИЗДАНИЕ', 'СВЕДЕНИЯ']
            if not any(word in title.upper() for word in exclude_words):
                metadata["official_title"] = title
                print(f"Найдено название: {title[:50]}...")
                break

    if not metadata["official_title"]:
        metadata["official_title"] = parse_filename_metadata(filename).get("official_title", "")

    # 3. Дата введения
    date_patterns = [
        r'Дата\s+введения\s+(\d{4}\s*-\s*\d{2}\s*-\s*\d{2})',
        r'Дата\s+введения\s+(\d{2}\.\d{2}\.\d{4})',
        r'введен\s+в\s+действие\s+с\s+(\d{1,2}\s+[а-я]+\s+\d{4})',
    ]
    for pattern in date_patterns:
        match = re.search(pattern, search_text, re.IGNORECASE)
        if match:
            date_str = match.group(1)
            if '-' in date_str:
                metadata["valid_from"] = re.sub(r'\s+', '', date_str)
            elif '.' in date_str:
                parts = date_str.split('.')
                if len(parts) == 3:
                    metadata["valid_from"] = f"{parts[2]}-{parts[1]}-{parts[0]}"
            print(f"Найдена дата введения: {metadata['valid_from']}")
            break

    # 4. Обязательность (Для СП все обязательные. Переделать для других документов.)
    if re.search(r'обязательный|обязательного|носит\s+обязательный', search_text, re.IGNORECASE):
        metadata["is_mandatory"] = True
        print(f"Документ имеет обязательный характер")

    return metadata


def parse_filename_metadata(filename: str) -> Dict:
    """Извлечение метаданных из имени файла."""
    name = filename.replace('.md', '')
    patterns = [
        r'(СП|ГОСТ|СанПиН|СНиП|ТР\s+TS)\s+(\d+(?:\.\d+)*)\.(\d{4})_(.+)$',
        r'(СП|ГОСТ|СанПиН|СНиП|ТР_TS)_(\d+(?:\.\d+)*)\.(\d{4})_(.+)$',
        r'(SP|GOST)\s+(\d+(?:\.\d+)*)\.(\d{4})_(.+)$',
        r'.*?(\d{1,3}\.\d{5}\.\d{4}).*',
    ]
    for pattern in patterns:
        match = re.match(pattern, name, re.IGNORECASE)
        if match:
            if len(match.groups()) == 4:
                doc_type = match.group(1).replace('_', ' ').upper()
                if doc_type == 'SP': doc_type = 'СП'
                return {
                    "type": doc_type,
                    "designation": f"{match.group(2)}.{match.group(3)}",
                    "year": int(match.group(3)),
                    "official_title": match.group(4).replace('_', ' ')
                }
            elif len(match.groups()) == 1:
                designation = match.group(1)
                return {
                    "type": "СП",
                    "designation": designation,
                    "year": int(designation.split('.')[-1]),
                    "official_title": name.replace('_', ' ')
                }
    return {"type": "СП", "designation": "", "year": 0, "official_title": name.replace('_', ' ')}


def clean_text(text: str) -> str:
    """Базовая очистка текста + исправление пробелов + удаление содержания."""
    # 1. Исправляем пробелы в словах
    text = fix_word_spacing(text)

    # 2. Склеиваем слова, разорванные переносом
    text = re.sub(r'(\w+)-\n(\w+)', r'\1\2', text)
    text = re.sub(r'(\w+)-\n\s+(\w+)', r'\1\2', text)

    # 3. Удаляем содержание
    text, _ = remove_table_of_contents(text)

    # 4. Построчная очистка (оригинальная)
    lines = text.split('\n')
    cleaned_lines = []
    trash_patterns = [
        re.compile(p, re.IGNORECASE) for p in [
            r'^\s*\d+\s*$', r'СП\s+\d+\.\d+\.\d+\s*$',
            r'^\s*Страница\s+\d+\s*$', r'^\s*Издание официальное\s*$',
            r'^\s*Титульный лист\s*$', r'^\s*Предисловие\s*$',
        ]
    ]
    for line in lines:
        if any(p.search(line) for p in trash_patterns):
            continue
        stripped = line.strip()
        if len(stripped) <= 3 and not re.match(r'^[а-я]\)$|^\d+\.?$', stripped):
            continue
        cleaned_lines.append(line)
    text = '\n'.join(cleaned_lines)
    text = re.sub(r'\n\s*\n\s*\n+', '\n', text)
    return text.strip()


def extract_main_content(text: str) -> str:
    """Вырезает основной нормативный текст."""
    start_patterns = [r"^\s*1\.\s+Область\s+применения", r"^\s*1\s+Область\s+применения"]
    start_pos = None
    for pattern in start_patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
        if match:
            start_pos = match.start()
            break
    if start_pos is None:
        match = re.search(r"^\s*1\s+[А-Я]", text, re.IGNORECASE | re.MULTILINE)
        if match: start_pos = match.start()
        else: return text
    text = text[start_pos:]

    end_patterns = [r"^\s*Приложение\s+[А-ЯA-Z]", r"^\s*Библиография"]
    end_pos = None
    for pattern in end_patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
        if match:
            end_pos = match.start()
            break
    if end_pos: text = text[:end_pos]
    return text.strip()


def normalize_section_headers(text: str) -> str:
    """
    Нормализует заголовки разделов для корректного парсинга.

    Проблемы, которые решает:
    1. '1. Область применения' -> '1 Область применения'
    2. '1.1. Текст' -> '1.1 Текст'
    3. '4.2.1. Текст' -> '4.2.1 Текст'
    4. '1.1.1. Текст' -> '1.1.1 Текст'

    Правило: убираем последнюю точку в номере пункта,
    если она не является частью нумерации (т.е. после неё пробел и заглавная буква)
    """

    # Паттерн 1: Одноуровневый номер с точкой в начале строки
    # "1. Область" -> "1 Область"
    text = re.sub(
        r'^(\d+)\.\s+([А-ЯA-Z])',
        r'\1 \2',
        text,
        flags=re.MULTILINE
    )

    # Паттерн 2: Многоуровневый номер с точкой на конце
    # "1.1. Текст" -> "1.1 Текст"
    # "4.2.1. Текст" -> "4.2.1 Текст"
    # "4.1.1. Требования" -> "4.1.1 Требования"
    text = re.sub(
        r'^(\d+(?:\.\d+)+)\.\s+([А-ЯA-Z])',
        r'\1 \2',
        text,
        flags=re.MULTILINE
    )

    # Паттерн 3: Запасной вариант - любое количество цифр с точками и финальной точкой
    # "3.1. Высота здания" -> "3.1 Высота здания"
    text = re.sub(
        r'^(\d+(?:\.\d+)*)\.(\s+)([А-ЯA-Z])',
        r'\1\2\3',
        text,
        flags=re.MULTILINE
    )

    return text


def extract_sections(text: str) -> List[Dict]:
    """
    Разбивает текст на секции по заголовкам с защитой от ложных срабатываний.
    """
    sections = []
    lines = text.split('\n')

    # Заголовки верхнего уровня: "3 Термины и определения"
    # Первое слово минимум 4 буквы (отсекаем "При", "Для", "На", "По", "В", "С" и т.п.)
    top_header = r'^\s*(\d+)\.?\s+([А-ЯA-Z][а-яёa-z]{3,}.+)$'

    # Подпункты: "3.1", "3.2", "4.2.1" — минимум одна точка
    sub_header = r'^\s*(\d+(?:\.\d+)+)\.?\s+([А-ЯA-Z].+)$'

    # Паттерны для ложных срабатываний
    # Строки типа "5 человек - в многоэтажных...", "8 мест и дополнительно...", "1 Прочерк в таблице..."
    list_item_pattern = re.compile(
        r'^\s*\d+\s+(?:человек|мест|м|мм|см|км|кг|г|°|%|Прочерк)\s*-?\s*[а-яёa-z]',
        re.IGNORECASE
    )

    # Строки типа "м;", "м2;", "м/с;" и т.п. (продолжение таблиц)
    table_continuation_pattern = re.compile(
        r'^\s*\d+\s+(?:м|мм|см|км|кг|г|л|°|%|мест|человек)[\s;]*$',
        re.IGNORECASE
    )

    current_section = None
    section_lines = []
    hierarchy = []

    for i, line in enumerate(lines):
        # Сначала пробуем подпункт, потом верхний уровень
        match = re.match(sub_header, line, re.IGNORECASE) or re.match(top_header, line, re.IGNORECASE)

        is_new_section = False

        if match:
            section_code = match.group(1)
            section_title = match.group(2).strip()

            # --- БЛОК ПРОВЕРОК НА ЛОЖНЫЕ СРАБАТЫВАНИЯ ---

            # Проверка 1: не является ли это пунктом перечисления или примечанием к таблице
            if list_item_pattern.match(line):
                is_new_section = False
            # Проверка 2: не является ли это продолжением таблицы (просто число и единица измерения)
            elif table_continuation_pattern.match(line):
                is_new_section = False
            else:
                # Проверка 3: контекст таблицы (строки с | ├ └ │)
                is_table_context = False
                for j in range(max(0, i - 5), i):
                    prev_line = lines[j].strip()
                    if (prev_line.startswith('|') or 
                        prev_line.startswith('│') or 
                        prev_line.startswith('├') or 
                        prev_line.startswith('└') or
                        prev_line.startswith('┌') or
                        prev_line.startswith('┐') or
                        prev_line.startswith('┘') or
                        prev_line.startswith('┤')):
                        is_table_context = True
                        break

                if is_table_context:
                    # Дополнительная проверка: если это заголовок следующей крупной секции
                    # (например, "6 Требования..."), то это не часть таблицы
                    if re.match(r'^\s*\d+\.?\s+[А-ЯA-Z][а-яёa-z]{3,}', line):
                        is_new_section = True
                    else:
                        is_new_section = False
                else:
                    # Проверка 4: не является ли это "голым" номером без текста
                    # (характерно для остатков таблиц)
                    if re.match(r'^\s*\d+\s*$', line):
                        is_new_section = False
                    # Проверка 5: если "заголовок" идет сразу после двоеточия в предыдущей строке
                    # (характерно для продолжения определений)
                    elif i > 0 and re.search(r':\s*$', lines[i - 1].strip()):
                        # Но пропускаем, если это действительно отдельный пункт
                        if not re.match(r'^\s*\d+\.\s+[А-ЯA-Z]', line):
                            is_new_section = False
                        else:
                            is_new_section = True
                    else:
                        is_new_section = True
            # --- КОНЕЦ БЛОКА ПРОВЕРОК ---

        if is_new_section:
            if current_section:
                current_section["content"] = '\n'.join(section_lines).strip()
                if current_section["content"]:
                    sections.append(current_section)
                section_lines = []

            level = section_code.count('.') + 1

            while hierarchy and hierarchy[-1]["level"] >= level:
                hierarchy.pop()
            hierarchy.append({"level": level, "code": section_code, "title": section_title})

            path_parts = [f"{h['code']} {h['title']}" for h in hierarchy]
            current_section = {
                "section_code": section_code,
                "section_title": section_title,
                "level": level,
                "hierarchy_path": "/".join(path_parts),
                "content": ""
            }
            section_lines.append(line)
        else:
            if current_section:
                section_lines.append(line)
            elif line.strip():
                current_section = {
                    "section_code": None, "section_title": "Введение",
                    "level": 0, "hierarchy_path": "Введение", "content": ""
                }
                section_lines.append(line)

    if current_section:
        current_section["content"] = '\n'.join(section_lines).strip()
        if current_section["content"]:
            sections.append(current_section)
    return sections


def get_clean_filename(metadata: Dict, original_filename: str) -> str:
    if metadata.get("designation"):
        return f"СП_{metadata['designation'].replace('.', '_')}.md"
    name = original_filename.replace('.md', '')
    name = re.sub(r'[<>:"/\\|?*]', '', name)
    clean_name = re.sub(r'\s+', '_', name)
    return f"{clean_name}_clean.md"


def remove_list_markers(text: str) -> str:
    """
    Удаляет маркеры списков в начале строк, которые являются частью
    форматирования документа, а не значимым текстом.

    Обрабатывает:
    - Одинарные дефисы: "- 1.1 Текст..." -> "1.1 Текст..."
    - Длинные тире: "— 1.1 Текст..." -> "1.1 Текст..."
    - Маркеры списков: "• 1.1 Текст..." -> "1.1 Текст..."
    - Звёздочки: "* 1.1 Текст..." -> "1.1 Текст..."
    """

    # Паттерн 1: Убираем одиночный дефис/тире в начале строки
    # Пример: "- 1.1 Текст..." -> "1.1 Текст..."
    text = re.sub(
        r'^[\-\–\—]\s+(?=\d+\.?\d*)',
        '',
        text,
        flags=re.MULTILINE
    )

    # Паттерн 2: Убираем маркер списка + заголовок секции
    # Пример: "- 4.2.1 Текст..." -> "4.2.1 Текст..."
    text = re.sub(
        r'^[\-\–\—]\s+(?=[А-ЯA-Z])',
        '',
        text,
        flags=re.MULTILINE
    )

    # Паттерн 3: Убираем остальные маркеры списков
    # Пример: "• Текст..." -> "Текст..."
    text = re.sub(
        r'^[•\*\-\–\—]\s+',
        '',
        text,
        flags=re.MULTILINE
    )

    # Паттерн 4: Убираем множественные дефисы в начале (если есть)
    # Пример: "-- Текст..." -> "Текст..."
    text = re.sub(
        r'^[\-\–\—]{2,}\s*',
        '',
        text,
        flags=re.MULTILINE
    )

    return text


def process_document(md_file: Path) -> Optional[Dict]:
    print(f"Обработка: {md_file.name}")
    try:
        with open(md_file, 'r', encoding='utf-8') as f:
            raw_text = f.read()
    except UnicodeDecodeError:
        with open(md_file, 'r', encoding='cp1251') as f:
            raw_text = f.read()
    except Exception as e:
        print(f"Ошибка чтения: {e}")
        return None

    # Шаг 0: Склейка оторванных заголовков
    raw_text = stitch_detached_headers(raw_text)
    print(f"Заголовки проверены и склеены при необходимости")

    # Шаг 1: Убираем Markdown-разметку
    preprocessed_text = strip_markdown_formatting(raw_text)
    print(f"Базовая разметка удалена")

    # Шаг 2: Убираем маркеры списков
    preprocessed_text = remove_list_markers(preprocessed_text)
    print(f"Маркеры списков удалены")

    # Шаг 3: Извлекаем метаданные
    metadata = extract_metadata_from_text(preprocessed_text, md_file.name)

    # Шаг 4: Очищаем текст
    cleaned_text = clean_text(preprocessed_text)

    # Шаг 5: Вырезаем основной контент
    main_text = extract_main_content(cleaned_text)
    if not main_text:
        main_text = cleaned_text

    # Шаг 6: Нормализуем заголовки секций
    main_text = normalize_section_headers(main_text)
    print(f"Заголовки секций нормализованы")

    # Шаг 7: Разбиваем на секции
    sections = extract_sections(main_text)

    # Шаг 8: Сохраняем очищенный текст
    clean_filename = get_clean_filename(metadata, md_file.name)
    clean_file = CLEANED_DIR / clean_filename
    with open(clean_file, 'w', encoding='utf-8') as f:
        f.write(main_text)
    print(f"Очищенный текст: {clean_filename}")

    return {
        "document": {
            "filename": md_file.name,
            "metadata": metadata,
            "sections": sections
        }
    }


def process_all_documents(input_dir: Path = INPUT_DIR, cleaned_dir: Path = CLEANED_DIR, output_dir: Path = OUTPUT_DIR):
    cleaned_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        print(f"Входная папка '{input_dir}' не существует!")
        return

    md_files = list(input_dir.glob("*.md"))
    if not md_files:
        print(f"Файлы .md не найдены в '{input_dir}'")
        return

    print(f"Найдено файлов: {len(md_files)}")
    results = []

    for md_file in md_files:
        doc_data = process_document(md_file)
        if not doc_data:
            continue

        metadata = doc_data["document"]["metadata"]
        sections = doc_data["document"]["sections"]

        json_filename = f"СП_{metadata['designation'].replace('.', '_')}.json" if metadata.get("designation") else md_file.stem + ".json"
        with open(output_dir / json_filename, 'w', encoding='utf-8') as f:
            json.dump(doc_data, f, ensure_ascii=False, indent=2)

        sections_count = len(sections)
        total_chars = sum(len(s["content"]) for s in sections)

        print(f"Метаданные: {metadata['type']} | {metadata['designation']} | {metadata['year']} | Тема: {metadata['topic']}")
        print(f"Секций: {sections_count} | Символов: {total_chars:,} | {json_filename}")

        results.append({
            "file": md_file.name,
            "designation": metadata.get("designation", ""),
            "topic": metadata.get("topic", ""),
            "sections": sections_count,
            "chars": total_chars
        })

    print("\n" + "=" * 60)
    print(f"ИТОГО: Обработано {len(results)} документов")
    print(f"Результаты сохранены в: {cleaned_dir} и {output_dir}")


if __name__ == "__main__":
    print("=" * 60)
    print("ПОЛНАЯ ОБРАБОТКА ДОКУМЕНТОВ")
    print("=" * 60)
    process_all_documents()
    print("ГОТОВО!")
