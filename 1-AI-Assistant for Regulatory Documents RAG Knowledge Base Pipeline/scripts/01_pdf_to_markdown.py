import os
import time
from pathlib import Path
from pypdf import PdfReader, PdfWriter
from docling.document_converter import DocumentConverter

def convert_chunk(temp_pdf_path):
    """Вспомогательная функция для конвертации одного куска PDF."""
    converter = DocumentConverter()
    result = converter.convert(temp_pdf_path)
    return result.document.export_to_markdown()

def process_pdf(input_pdf_path, output_md_path, chunk_size=30):
    """
    Основная логика: маленькие файлы конвертирует целиком, 
    большие (больше 50 стр.) — режет на части.
    """
    reader = PdfReader(input_pdf_path)
    total_pages = len(reader.pages)
    
    # Если файл небольшой — обрабатываем стандартно (быстрее)
    if total_pages <= 50:
        print(f"📄 {input_pdf_path.name}: {total_pages} стр. Обработка целиком...")
        converter = DocumentConverter()
        result = converter.convert(input_pdf_path)
        with open(output_md_path, "w", encoding="utf-8") as f:
            f.write(result.document.export_to_markdown())
        return

    # Если файл тяжелый — включаем "режим Honor" (нарезку)
    print(f"📦 ТЯЖЕЛЫЙ ФАЙЛ: {input_pdf_path.name} ({total_pages} стр.)")
    print(f"🚀 Включен режим нарезки по {chunk_size} страниц...")
    
    full_markdown = ""
    
    for start_page in range(0, total_pages, chunk_size):
        end_page = min(start_page + chunk_size, total_pages)
        temp_pdf = f"temp_part_{start_page}.pdf"
        
        try:
            # 1. Вырезаем кусок
            writer = PdfWriter()
            for page in range(start_page, end_page):
                writer.add_page(reader.pages[page])
            
            with open(temp_pdf, "wb") as f:
                writer.write(f)

            # 2. Конвертируем кусок
            print(f"⚙️ Обработка страниц {start_page + 1} - {end_page}...")
            chunk_md = convert_chunk(temp_pdf)
            full_markdown += chunk_md + "\n\n"
            
        except Exception as e:
            print(f"⚠️ Ошибка на страницах {start_page}-{end_page}: {e}")
        finally:
            if os.path.exists(temp_pdf):
                os.remove(temp_pdf)

    # 3. Сохраняем финальный результат
    with open(output_md_path, "w", encoding="utf-8") as f:
        f.write(full_markdown)
    print(f"✅ Готово! Склеенный файл: {output_md_path.name}")

if __name__ == "__main__":
    BASE_DIR = Path(__file__).parent.parent  # Исправлено: file -> __file__
    INPUT_DIR = BASE_DIR / "data" / "pdfs"
    OUTPUT_DIR = BASE_DIR / "output" / "markdown"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    pdf_files = list(INPUT_DIR.glob("*.pdf"))
    
    for pdf_file in pdf_files:
        output_file = OUTPUT_DIR / f"{pdf_file.stem}.md"
        
        if output_file.exists():
            print(f"⏩ Пропуск: {output_file.name} уже есть.")
            continue
            
        start_time = time.time()
        # Для коллеги на Honor ставим chunk_size=30
        # Для себя на M3 можешь поменять на 100 или 150
        process_pdf(pdf_file, output_file, chunk_size=30)
        
        duration = time.time() - start_time
        print(f"⏱️ Время обработки: {duration:.1f} сек.\n")