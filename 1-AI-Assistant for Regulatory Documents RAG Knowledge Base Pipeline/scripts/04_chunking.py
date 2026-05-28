import re
import uuid
import logging as lg
import importlib
import json

from pathlib import Path
from langchain_text_splitters import RecursiveCharacterTextSplitter
Embedder = importlib.import_module("scripts.05_embedding").Embedder


DEFAULT_CHUNK_SIZE_TOKENS = 412
DEFAULT_CHUNK_OVERLAP_TOKENS = 100
MIN_BLOCK_TOKENS = 200
MAX_BLOCK_TOKENS = 500
SEPARATORS = ["\n\n", "\n", ". ", " "]
PROJECT_ROOT = Path(__file__).resolve().parent.parent
JSON_DIR = PROJECT_ROOT / "output" / "json"

# Два случая:
# 1) Номер с точками (подпункты): 3.24, 4.9, 1.1, 5.3.6 — иногда после номера ещё точка: «5.3.6. В…».
# 2) Только цифры без точки (разделы): 4 Общие, 12 Сведения — только ЗАГЛАВНАЯ после номера,
#    иначе ловятся обрывы вроде «7 следующих…», «8 такие…» с середины абзаца.
_CLAUSE = re.compile(
    r"(?:^\s*(?P<sub>\d{1,3}(?:\.\d{1,3})+)\.?\s+(?P<sub_title>[А-ЯЁа-яA-Za-z«].+)$)"
    r"|(?:^\s*(?P<sec>\d{1,2})\s+(?P<sec_title>[А-ЯЁA-Z«].+)$)",
    re.MULTILINE,
)

# Только первая строка блока: «5.3.6 …» или «12 Сведения…» — для поля clause_ref в metadata
# ^ - начало строки
# \s* - ноль или более пробелов
# ?: - группировка без сохранения в результате
# d{1,3} - одна, две или три цифры
# \. - точка
# + - одна или более точек
# s+ - одна или более пробелов
_FIRST_LINE_NUM = re.compile(
    r"^\s*((?:\d{1,3}(?:\.\d{1,3})+)|(?:\d{1,2}))\s*\.?\s+",
)


# Старт табличного блока (в т.ч. OCR-варианты: "аблица", "Т а б л и ц а").
_TABLE_START = re.compile(
    r"^\s*(?:#+\s*)?(?:(?:Окончание|Продолжение)\s+)?(?:[ТT]\s*а\s*б\s*л\s*и\s*ц\s*а|[ТT]?аблица)\s*[A-Za-zА-Яа-я0-9\.\-]*\s*(?:\([^)]*\)|[^\n]*)?$",
    re.IGNORECASE | re.MULTILINE,
)

# Заголовки/границы, после которых таблица обычно заканчивается.
_TABLE_HARD_BOUNDARY = re.compile(
    r"^\s*(?:Приложение\s+[А-ЯA-Z]|СП\s+\d|#{1,6}\s+|(?:\d{1,3}(?:\.\d{1,3})+|\d{1,2})\s+)",
    re.IGNORECASE,
)

# Настройки логирования для скрипта
chunk_logger = lg.getLogger(__name__)
chunk_logger.setLevel(lg.INFO)


class SPDocumentChunker:
    def __init__(
        self,
        *,
        chunk_size: int = DEFAULT_CHUNK_SIZE_TOKENS,
        chunk_overlap: int = DEFAULT_CHUNK_OVERLAP_TOKENS,
        min_block_tokens: int = MIN_BLOCK_TOKENS,
        max_block_tokens: int = MAX_BLOCK_TOKENS,
        embedder: Embedder,
        separators: list[str] | None = None
    ) -> None:
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.min_block_tokens = min_block_tokens
        self.max_block_tokens = max_block_tokens
        self.separators = list(separators) if separators is not None else list(SEPARATORS)
        self.embedder = embedder

    def split_text_into_chunks(self, text: str) -> list[str]:
        """
        Стандартная функция RecursiveCharacterTextSplitter для разбиения
        текста на чанки по размеру chunk_size с перекрытием chunk_overlap.

        Args:
            text: текст для разбиения
        """
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap,
            separators=self.separators,
            length_function=self.embedder.count_tokens,
        )
        chunks = splitter.split_text(text)
        chunk_logger.debug(f"Разбили текст на чанки: {len(chunks)}")
        return chunks

    def is_same_level_and_parent(self, first_number_text: str, second_number_text: str) -> bool:
        """
        Проверяет, находятся ли два пункта на одном уровне
        и имеют ли одного родителя.

        Args:
            first_number_text: первый номер пункта в виде текста
            second_number_text: второй номер пункта в виде текста
        """
        if not first_number_text:
            chunk_logger.info("Нет first_number_text в функции is_same_level_and_parent")
            return False
        elif not second_number_text:
            chunk_logger.info("Нет second_number_text в функции is_same_level_and_parent")
            return False

        # Убираем точки и разделяем на части
        first_parts = first_number_text.rstrip(".").split(".")
        second_parts = second_number_text.rstrip(".").split(".")

        # Проверяем, находятся ли два пункта на одном уровне
        # и не являются ли они корневыми элементами
        return (
            len(first_parts) == len(second_parts) and
            len(first_parts) > 1 and
            first_parts[:-1] == second_parts[:-1]
        )

    def filter_false_boundaries(self, matches: list[re.Match[str]]) -> list[re.Match[str]]:
        """
        Убираем ложные границы sec после sub.

        Args:
            matches: список совпадений для склеивания
        """
        if not matches:
            chunk_logger.info("Список совпадений пустой")
            return []

        result: list[re.Match[str]] = []
        in_list_after_sub = False
        last_sub_head: int | None = None

        for m in matches:
            sub = m.group("sub")
            sec = m.group("sec")

            # sub всегда настоящая граница
            if sub is not None:
                result.append(m)
                in_list_after_sub = False
                last_sub_head = int(sub.split(".")[0])
                continue
            elif sec is not None and last_sub_head is not None:
                sec_int = int(sec)

                # Если мы в списке после подпункта, то пропускаем этот пункт
                if in_list_after_sub:
                    continue
                # Если список только начался
                if sec_int <= last_sub_head:
                    in_list_after_sub = True
                    continue
            result.append(m)

        return result

    def split_plain_sp_into_blocks(self, text: str) -> list[str]:
        """
        Разбивает текст на чанки по пунктам и возвращает список чанков.
        Если пункты не найдены, возвращает список чанков, разбитых по размеру.

        Args:
            text: текст для разбиения
        """
        # Ищем пункты в тексте
        matches = list(_CLAUSE.finditer(text))
        matches = self.filter_false_boundaries(matches)
        if not matches:
            if not text:
                chunk_logger.info("Текст пустой")
                return []
            chunk_logger.debug("Текст не содержит пунктов, разбиваем на чанки стандартной функцией RecursiveCharacterTextSplitter")
            return self.split_text_into_chunks(text)

        blocks: list[str] = []
        # Если первый пункт не в начале текста, добавляем текст до первого пункта в отдельный блок
        if matches[0].start() > 0:
            preamble = text[: matches[0].start()].strip()
            if preamble:
                blocks.append(preamble)

        for i, m in enumerate(matches):
            start = m.start()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            block = text[start:end].strip()
            if block:
                table_starts = list(_TABLE_START.finditer(block))

                # Если табличные блоки не найдены, то добавляем блок в список
                if not table_starts:
                    blocks.append(block)
                    continue

                #
                table_ranges: list[tuple[int, int]] = []
                for j, t in enumerate(table_starts):
                    start_t = t.start()
                    next_t = table_starts[j + 1].start() if j + 1 < len(table_starts) else len(block)

                    hard_boundary = _TABLE_HARD_BOUNDARY.search(block, pos=start_t + 1)

                    # Если жесткая граница найдена, то устанавливаем конец табличного блока на её координату
                    # Иначе устанавливаем конец табличного блока на координату следующего табличного блокаs
                    if hard_boundary:
                        end_t = min(hard_boundary.start(), next_t)
                    else:
                        end_t = next_t
                    # Подстраховка на случай некорректных границ
                    end_t = max(start_t, end_t)
                    table_ranges.append((start_t, end_t))

                # Текстовые части block без таблиц
                cursor = 0
                for start_t, end_t in table_ranges:
                    text_part = block[cursor:start_t].strip()
                    if text_part:
                        blocks.append(text_part)
                    cursor = end_t
                tail = block[cursor:].strip()
                if tail:
                    blocks.append(tail)

                # Добавляем табличные блоки в список
                for start_t, end_t in table_ranges:
                    table_part = block[start_t:end_t].strip()
                    if table_part:
                        blocks.append(table_part)

        chunk_logger.debug(f"Блоки после разбиения на чанки: {len(blocks)}")
        return self.merge_minimal_blocks(blocks)
        # return blocks

    def merge_minimal_blocks(self, blocks: list[str]) -> list[str]:
        """
        Склеивает блоки, если их длина меньше MIN_BLOCK_TOKENS токенов и они находятся на одном уровне нумерации.
        Если блок меньше минимума, пытается склеить его с последующими блоками того же уровня,
        пока не наберётся максимум токенов.

        Args:
            blocks: список чанков для склеивания
        """
        if not blocks:
            chunk_logger.info("Список блоков пустой")
            return []

        merged_blocks: list[str] = []
        i = 0

        while i < len(blocks):
            # Текущий блок и его размер в токенах
            current_block = blocks[i]
            current_block_tokens = self.embedder.count_tokens(current_block)

            # Если блок меньше минимума, пытаемся склеить его с последующими блоками того же уровня
            if current_block_tokens < self.min_block_tokens:
                # Извлекаем номер пункта из первой строки блока в виде текста
                current_block_first_line = current_block.strip().split("\n", 1)[0]
                current_match = _FIRST_LINE_NUM.match(current_block_first_line)
                # Если номер пункта найден, добавляем его в словарь иначе пустая строка
                current_number_text = current_match.group(1) if current_match else ""

                # Индекс следующего блока
                j = i + 1

                while j < len(blocks):
                    # Следующий блок и его размер в токенах
                    next_block = blocks[j]
                    next_block_tokens = self.embedder.count_tokens(next_block)

                    # Извлекаем номер пункта из первой строки следующего блока в виде текста
                    next_block_first_line = next_block.strip().split("\n", 1)[0]
                    next_match = _FIRST_LINE_NUM.match(next_block_first_line)
                    next_number_text = next_match.group(1) if next_match else ""

                    # Проверяем, находятся ли два пункта на одном уровне и имеют ли одного родителя
                    check_level = self.is_same_level_and_parent(current_number_text, next_number_text)

                    # Если пункты не на одном уровне или сумма токенов превышает максимум, прекращаем склеивание
                    if not check_level or current_block_tokens + next_block_tokens > self.max_block_tokens:
                        break

                    # Склеиваем текущий блок с следующим и переходим к следующему блоку
                    current_block += "\n\n" + next_block
                    current_block_tokens += next_block_tokens
                    j += 1

                # Добавляем склеенный блок в список и переходим к следующему блоку
                merged_blocks.append(current_block)
                i = j
                continue

            else:
                # Добавляем текущий блок в список и переходим к следующему блоку
                merged_blocks.append(current_block)
                i += 1

        chunk_logger.debug(f"Блоков после склеивания: {len(merged_blocks)}")
        return merged_blocks

    def get_section_path(self, section_number: str) -> str:
        """
        Путь в иерархии номеров: для 5.3.6 — «5 > 5.3 > 5.3.6», для 12 — «12».

        Args:
            section_number: номер раздела
        """
        if not section_number:
            chunk_logger.debug("Номер раздела пустой")
            return ""
        result = []
        # Навсякий убираем точку справа и сплитим по точкам
        section_number = section_number.rstrip(".").split(".")

        for i, _ in enumerate(section_number):
            result.append(".".join(section_number[:i+1]))
        chunk_logger.debug(f"Результат функции get_section_path: {result}")
        return " > ".join(result)

    def clause_display(self, nums: list[str]) -> str | None:
        """
        Человеко-читаемая подпись пунктов для UI (поле clause_display в БД).
        Один пункт: «п. 5.26.1»; несколько: «пп. 5.26.1-5.26.3».

        Args:
            nums: список номеров пунктов
        """
        chunk_logger.debug(f"Вызов функции clause_display для списка номеров пунктов: {nums}")
        if not nums:
            chunk_logger.debug("Список номеров пунктов пустой")
            return None
        if len(nums) == 1:
            chunk_logger.debug(f"Результат функции clause_display для одного пункта: {f'п. {nums[0]}'}")
            return f"п. {nums[0]}"
        result = f"пп. {nums[0]}-{nums[-1]}"
        chunk_logger.debug(f"Результат функции clause_display для нескольких пунктов: {result}")
        return result

    def get_data_from_json(self, json_path: Path) -> dict | None:
        """
        Читаем JSON-файл и возвращаем часть метаданных.

        Args:
            json_path: путь к JSON-файлу
        """
        chunk_logger.debug(f"Вызов функции get_data_from_json для пути к JSON-файлу: {json_path}")
        with open(json_path, "r", encoding="utf-8") as f:
            json_data = json.load(f)

        if json_data:
            chunk_logger.debug(f"JSON-файл найден: {json_data}")
            return {
                "designation": json_data["document"]["metadata"]["designation"],
                "year": json_data["document"]["metadata"]["year"],
                "type": json_data["document"]["metadata"]["type"],
            }
        chunk_logger.debug("JSON-файл не найден")
        return None

    def add_metadata(
        self,
        *,
        blocks: list[str],
        json_path: Path,
    ) -> list[dict]:
        """
        Оборачивает разбитые на чанки тексты в словари с метаданными.

        token_count для каждого чанка — через count_tokens_for_embedding_model
        (токенайзер EMBEDDING_MODEL из embedder).

        Args:
            blocks: список чанков для добавления метаданных
            embedder: экземпляр Embedder для подсчёта токенов
        """
        result: list[dict] = []
        # Получаем год и код из JSON-файла один раз на весь документ
        data_json = self.get_data_from_json(json_path)
        designation = data_json["designation"]
        year = data_json["year"]
        type = data_json["type"]

        # Оборачиваем чанки в словари с метаданными
        last_item_number: str | None = None
        for text in blocks:
            # Получаем первую строку блока и извлекаем номер пункта
            first = text.strip().split("\n", 1)[0] if text.strip() else ""
            m = _FIRST_LINE_NUM.match(first)
            first_item_number = m.group(1) if m else ""
            content_type = "text" if first_item_number else "table"

            nums: list[str] = []
            # Для таблиц не извлекаем номера по _CLAUSE
            if content_type == "text":
                clause_matches = list(_CLAUSE.finditer(text))
                filtered_clause_matches = self.filter_false_boundaries(clause_matches)
                for cm in filtered_clause_matches:
                    d = cm.groupdict()
                    n = d.get("sub") or d.get("sec")
                    if n:
                        nums.append(n)

            # Если тип контента таблица, но номера не найдены, то наследуем номера от предыдущего текстового блока
            if content_type == "table" and not nums and last_item_number:
                nums = [last_item_number]
                first_item_number = last_item_number

            clause_start: str | None = nums[0] if nums else None
            clause_end: str | None = nums[-1] if nums else None
            if nums:
                last_item_number = nums[-1]
            elif first_item_number:
                last_item_number = first_item_number
            section_path = (
                self.get_section_path(clause_start) if clause_start else None
            )
            clause_display_str = self.clause_display(nums)

            # Проверяем, если токены в блоке больше максимума, то разбиваем на чанки
            cur_token_count = self.embedder.count_tokens(text)
            pieces = [text] if cur_token_count <= self.max_block_tokens else self.split_text_into_chunks(text)

            for piece in pieces:
                result.append(
                    {
                        "metadata": {
                            "qdrant_point_id": str(uuid.uuid4()),
                            "designation": designation,
                            "year": year,
                            "type": type,
                            "chunk_index": len(result),
                            "section_id": None,
                            "first_item_number": first_item_number,
                            "clause_start": clause_start,
                            "clause_end": clause_end,
                            "section_path": section_path,
                            "clause_numbers": nums,
                            "clause_display": clause_display_str,
                            "merged_clauses_count": len(nums),
                            "content_type": content_type,
                            "parent_chunk_id": None,
                            "content_url": None,
                            "text_content": piece,
                            "token_count": self.embedder.count_tokens(piece),
                        },
                    }
                )
        chunk_logger.debug(f"Результат функции add_metadata: {len(result)}")
        return result
