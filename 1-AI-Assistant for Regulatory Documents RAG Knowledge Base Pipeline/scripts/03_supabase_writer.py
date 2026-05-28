# ============================================================================
# scripts/03_supabase_writer.py
# SupabaseMetadataWriter - запись документов и секций в Supabase
# ============================================================================

import json
import os
import uuid
from pathlib import Path
from typing import Dict, List, Optional
from datetime import date

from dotenv import load_dotenv
from supbase_helper import SupabaseHelper

PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / ".env")
OUTPUT_DIR = PROJECT_ROOT / "output"
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_PRIVATE_KEY_LONG")


class SupabaseMetadataWriter:
    """
    Записывает метаданные документов и секции в Supabase.
    Использует UPSERT логику: если документ существует - обновляет.
    """

    def __init__(self):
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise RuntimeError(
                "Задайте SUPABASE_URL и SUPABASE_PRIVATE_KEY_LONG "
                f"(файл {PROJECT_ROOT / '.env'} или переменные окружения)."
            )
        self.supabase = SupabaseHelper(supabase_url=SUPABASE_URL, supabase_key=SUPABASE_KEY).get_supabase_client()
        self.json_dir = OUTPUT_DIR / "json"

    def upsert_document(self, doc_data: Dict) -> Dict:
        """
        UPSERT документа по designation + year + type.

        Args:
            doc_data: словарь с метаданными документа

        Returns:
            Dict с id документа и флагом created
        """

        # Проверяем существование документа
        existing = self.supabase.table("documents") \
            .select("id") \
            .eq("designation", doc_data["designation"]) \
            .eq("year", doc_data["year"]) \
            .eq("type", doc_data["type"]) \
            .eq("visibility", "public") \
            .execute()


        # Генерируем document_family_id (если не передан)
        family_id = doc_data.get("document_family_id", str(uuid.uuid4()))

        if existing.data:
            # Документ существует - обновляем
            doc_id = existing.data[0]["id"]

            update_data = {
                "document_family_id": family_id,
                "official_title": doc_data["official_title"],
                "topic": doc_data.get("topic"),
                "valid_from": doc_data.get("valid_from"),
                "valid_to": doc_data.get("valid_to"),
                "is_mandatory": doc_data.get("is_mandatory", False),
                "version_status": doc_data.get("version_status", "active"),
                "source_type": doc_data.get("source_type", "parsed"),
                "processing_metadata": doc_data.get("processing_metadata", {}),
            }

            # Убираем None значения
            update_data = {k: v for k, v in update_data.items() if v is not None}

            self.supabase.table("documents") \
                .update(update_data) \
                .eq("id", doc_id) \
                .execute()

            print(f"Обновлен: {doc_data['type']} {doc_data['designation']}")
            return {"id": doc_id, "created": False}

        else:
            # Документ не существует - создаем новый
            insert_data = {
                "document_family_id": family_id,
                "type": doc_data["type"],
                "designation": doc_data["designation"],
                "official_title": doc_data["official_title"],
                "year": doc_data["year"],
                "topic": doc_data.get("topic"),
                "valid_from": doc_data.get("valid_from"),
                "valid_to": doc_data.get("valid_to"),
                "is_mandatory": doc_data.get("is_mandatory", False),
                "visibility": "public",
                "source_type": doc_data.get("source_type", "parsed"),
                "version_status": doc_data.get("version_status", "active"),
                "tags": doc_data.get("tags", []),
                "processing_metadata": doc_data.get("processing_metadata", {}),
            }

            # Убираем None значения
            insert_data = {k: v for k, v in insert_data.items() if v is not None}

            result = self.supabase.table("documents").insert(insert_data).execute()
            doc_id = result.data[0]["id"]

            print(f"Создан: {doc_data['type']} {doc_data['designation']}")
            return {"id": doc_id, "created": True}

    def upsert_sections(self, doc_id: str, sections: List[Dict]) -> int:
        """
        Вставляет секции документа с установлением правильных родительских связей.

        Алгоритм:
        1. Удаляем старые секции документа (если есть)
        2. Сортируем секции по уровню вложенности (родители перед детьми)
        3. Вставляем каждую секцию по очереди
        4. Запоминаем ID каждой вставленной секции в словаре section_code → id
        5. Для каждой новой секции пытаемся найти ID её родителя в словаре
        6. Если родитель найден — устанавливаем parent_section_id

        Args:
            doc_id: UUID документа в Supabase
            sections: список секций из JSON-файла

        Returns:
            int: количество успешно вставленных секций
        """

        # ============================================================
        # ШАГ 1: Удаляем старые секции этого документа
        # ============================================================
        print(f"Удаление старых секций для документа {doc_id}...")

        delete_result = self.supabase.table("document_sections") \
            .delete() \
            .eq("doc_id", doc_id) \
            .execute()

        print(f"Удалено старых секций: {len(delete_result.data) if delete_result.data else 0}")

        if not sections:
            print("Нет секций для вставки")
            return 0

        # ============================================================
        # ШАГ 2: Сортируем секции для правильного порядка вставки
        # ============================================================

        sorted_sections = sorted(
            sections, 
            key=lambda x: (x.get("level", 1), x.get("section_code", ""))
        )

        print(f"Всего секций для вставки: {len(sorted_sections)}")

        # ============================================================
        # ШАГ 3: Подготавливаем словарь для хранения связей
        # ============================================================
        section_map = {}

        # ============================================================
        # ШАГ 4: Вставляем секции по одной
        # ============================================================
        inserted_count = 0
        errors = []

        for idx, section in enumerate(sorted_sections, 1):
            section_code = section.get("section_code")
            section_title = section.get("section_title", "")
            level = section.get("level", 1)
            hierarchy_path = section.get("hierarchy_path")

            print(f"    [{idx}/{len(sorted_sections)}] Обработка: {section_code} - {section_title[:50]}...")

            # ========================================================
            # ШАГ 4.1: Базовые данные секции
            # ========================================================
            section_data = {
                "doc_id": doc_id,
                "section_code": section_code,
                "section_title": section_title,
                "level": level,
                "hierarchy_path": hierarchy_path,
            }


            section_data = {k: v for k, v in section_data.items() if v is not None}

            # ========================================================
            # ШАГ 4.2: Пытаемся найти родительскую секцию
            # ========================================================

            if level > 1 and section_code:
                # Разбиваем код на части
                parts = section_code.split(".")

                # Если частей больше 1, значит у этого раздела есть родитель
                if len(parts) > 1:
                    # Формируем код родителя (отбрасываем последнюю часть)
                    parent_code = ".".join(parts[:-1])

                    # Ищем родителя в словаре уже вставленных секций
                    if parent_code in section_map:
                        section_data["parent_section_id"] = section_map[parent_code]
                        print(f"Установлена связь: {section_code} → {parent_code} (ID: {section_map[parent_code]})")
                    else:
                        warning_msg = f"Родитель '{parent_code}' для '{section_code}' не найден в section_map"
                        print(f"      {warning_msg}")
                        errors.append(warning_msg)

            # ========================================================
            # ШАГ 4.3: Вставляем секцию в базу данных
            # ========================================================
            try:
                result = self.supabase.table("document_sections") \
                    .insert(section_data) \
                    .execute()

                # Получаем ID вставленной секции
                if result.data and len(result.data) > 0:
                    new_id = result.data[0]["id"]
                    inserted_count += 1

                    # ====================================================
                    # ШАГ 4.4: Сохраняем ID в словарь для будущих потомков
                    # ====================================================
                    if section_code:
                        section_map[section_code] = new_id
                        print(f"Вставлена секция {section_code} с ID: {new_id}")
                    else:
                        print(f"Вставлена секция без кода с ID: {new_id}")
                else:
                    print(f"Ошибка: не получен ID после вставки")
                    errors.append(f"Не удалось вставить секцию {section_code}")

            except Exception as e:
                print(f"Ошибка вставки: {str(e)}")
                errors.append(f"Ошибка вставки {section_code}: {str(e)}")

        # ============================================================
        # ШАГ 5: Выводим статистику и предупреждения
        # ============================================================
        print(f"\nРезультаты вставки секций:")
        print(f"     - Успешно вставлено: {inserted_count} из {len(sorted_sections)}")
        print(f"     - Ошибок/предупреждений: {len(errors)}")

        if errors:
            print(f"\n Предупреждения и ошибки:")
            for error in errors[:10]:  
                print(f"     - {error}")
            if len(errors) > 10:
                print(f"     ... и ещё {len(errors) - 10} ошибок")

        return inserted_count

    def process_json_file(self, json_path: Path) -> Dict:
        """
        Обрабатывает один JSON файл: записывает документ и секции.

        Args:
            json_path: путь к JSON файлу

        Returns:
            Dict с результатами обработки
        """

        print(f"Обработка: {json_path.name}")

        # Читаем JSON
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        doc_metadata = data["document"]["metadata"]
        sections = data["document"]["sections"]

        # Добавляем source_type = 'parsed'
        doc_metadata["source_type"] = "parsed"

        # UPSERT документа
        doc_result = self.upsert_document(doc_metadata)

        # UPSERT секций
        sections_count = self.upsert_sections(doc_result["id"], sections)

        print(f"Секций: {sections_count}")

        return {
            "file": json_path.name,
            "document_id": doc_result["id"],
            "created": doc_result["created"],
            "sections_count": sections_count
        }

    def process_all(self) -> List[Dict]:
        """
        Обрабатывает все JSON файлы в output/json/

        Returns:
            List[Dict]: результаты обработки всех файлов
        """

        json_files = list(self.json_dir.glob("*.json"))


        if not json_files:
            print("Нет JSON файлов для обработки")
            return []

        print(f"Запись в Supabase ({len(json_files)} файлов)")
        print("=" * 50)

        results = []
        for json_path in json_files:
            try:
                result = self.process_json_file(json_path)
                results.append(result)
            except Exception as e:
                print(f"Ошибка: {e}")
                results.append({
                    "file": json_path.name,
                    "error": str(e)
                })

        # Итоги
        print("\n" + "=" * 50)
        print("ИТОГИ:")

        created = sum(1 for r in results if r.get("created"))
        updated = sum(1 for r in results if r.get("created") is False)
        errors = sum(1 for r in results if "error" in r)
        total_sections = sum(r.get("sections_count", 0) for r in results)

        print(f"Создано документов: {created}")
        print(f"Обновлено документов: {updated}")
        print(f"Ошибок: {errors}")
        print(f"Всего секций: {total_sections}")

        return results


# ============================================================================
# Точка входа
# ============================================================================

if __name__ == "__main__":
    writer = SupabaseMetadataWriter()
    results = writer.process_all()
