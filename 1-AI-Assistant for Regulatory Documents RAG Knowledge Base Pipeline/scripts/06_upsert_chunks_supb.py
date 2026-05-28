from supabase import Client
import logging as lg


supabase_logger = lg.getLogger(__name__)
supabase_logger.setLevel(lg.INFO)


class SupabaseChunksUpserter:
    def __init__(
        self,
        *,
        supabase_client: Client,
    ):
        self.supabase_client = supabase_client

    def get_document(
        self,
        *,
        designation: str,
        year: int,
        doc_type: str,
    ) -> dict:
        """
        Ищет документ по designation, year и type и возвращает его id и workspace_id.
        Если документ не найден, выбрасывается ValueError.

        Args:
            supabase_client: клиент Supabase
            designation: код документа
            year: год документа
            doc_type: тип документа
        """
        response = (
            self.supabase_client.table("documents")
            .select("id, workspace_id")
            .eq("designation", designation)
            .eq("year", year)
            .eq("type", doc_type)
            .single()
            .execute()
        )
        document = response.data
        if not document:
            supabase_logger.error(f"Документ не найден: designation={designation}, year={year}, type={doc_type}")
            raise ValueError(
                f"Документ не найден: designation={designation}, year={year}, "
                f"type={doc_type}"
            )
        if not document.get("id"):
            supabase_logger.error("В documents отсутствуют id")
            raise ValueError("В documents отсутствуют id")
        return document

    def get_section_maps(
        self,
        *,
        doc_id: str,
    ) -> dict[str, str]:
        """
        Возвращает маппинги секций документа.

        Args:
            doc_id: id документа
        """
        response = (
            self.supabase_client.table("document_sections")
            .select("id, section_code")
            .eq("doc_id", doc_id)
            .execute()
        )
        rows = response.data or []

        by_section_code: dict[str, str] = {}
        for row in rows:
            section_id = row.get("id")
            if not section_id:
                continue

            section_code = row.get("section_code")
            if section_code:
                by_section_code[section_code] = section_id

        return by_section_code

    def delete_chunks_by_document(
        self,
        *,
        doc_id: str,
    ) -> None:
        """
        Удаляет все чанки, связанные с документом.

        Args:
            doc_id: id документа
        """
        (
            self.supabase_client.table("chunks")
            .delete()
            .eq("doc_id", doc_id)
            .execute()
        )

    def insert_chunks(
        self,
        *,
        chunks: list[dict],
        batch_size: int = 100,
    ) -> list[dict]:
        """
        Вставляет список чанков в таблицу chunks батчами и возвращает
        данные, необходимые для загрузки в векторное хранилище.

        Args:
            chunks: список словарей с данными чанков
            batch_size: размер одного батча для insert
        """
        if not chunks or batch_size <= 0:
            return []

        # Получаем type, designation, year из чанка и ищем документ в Supabase
        doc_type = chunks[0]["metadata"]["type"]
        designation = chunks[0]["metadata"]["designation"]
        year = chunks[0]["metadata"]["year"]

        # Ищем документ в Supabase
        document = self.get_document(
            designation=designation,
            year=year,
            doc_type=doc_type,
        )

        doc_id = document["id"]
        workspace_id = document.get("workspace_id")
        section_ids_by_code = self.get_section_maps(doc_id=doc_id)

        # Перед вставкой очищаем старые чанки документа.
        self.delete_chunks_by_document(doc_id=doc_id)

        records: list[dict] = []
        for chunk in chunks:
            metadata = chunk["metadata"]
            section_id = (
                section_ids_by_code.get(metadata.get("clause_start"))
                or metadata.get("section_id")
                or section_ids_by_code.get(metadata.get("section_code"))
            )

            records.append(
                {
                    "qdrant_point_id": metadata.get("qdrant_point_id"),
                    "doc_id": doc_id,
                    "section_id": section_id,
                    "workspace_id": workspace_id,
                    "section_path": metadata.get("section_path"),
                    "clause_start": metadata.get("clause_start"),
                    "clause_end": metadata.get("clause_end"),
                    "clause_numbers": metadata.get("clause_numbers", []),
                    "clause_display": metadata.get("clause_display"),
                    "merged_clauses_count": metadata.get("merged_clauses_count", 0),
                    "chunk_index": metadata.get("chunk_index"),
                    "content_type": metadata.get("content_type", "text"),
                    "parent_chunk_id": metadata.get("parent_chunk_id"),
                    "content_url": metadata.get("content_url"),
                    "text_content": metadata["text_content"],
                    "token_count": metadata.get("token_count"),
                }
            )

        vector_rows: list[dict] = []
        for start in range(0, len(records), batch_size):
            batch = records[start : start + batch_size]
            response = self.supabase_client.table("chunks").upsert(batch).execute()
            rows = response.data or []
            vector_rows.extend(
                {
                    "text": row.get("text_content"),
                    "point_id": row.get("id"),
                    "payload": {
                        "workspace_id": row.get("workspace_id"),
                        "doc_id": row.get("doc_id"),
                        "clause_number": row.get("clause_start"),
                        "content_type": row.get("content_type"),
                        "chunk_id": row.get("id"),
                    },
                }
                for row in rows
                if row.get("id")
            )

        if not vector_rows:
            supabase_logger.error("Не удалось вставить чанки в таблицу chunks.")
            raise ValueError("Не удалось вставить чанки в таблицу chunks.")

        return vector_rows

    def read_chunks(
        self,
        *,
        doc_id: str,
    ) -> list[dict]:
        """
        Читает чанки из таблицы chunks.

        Args:
            doc_id: id документа, который нужно прочитать
        """
        vector_rows: list[dict] = []
        response = self.supabase_client.table("chunks").select("*").eq("doc_id", doc_id).execute()
        if not response.data:
            supabase_logger.info(f"Не найдено чанков для документа {doc_id}")
            return []
        rows = response.data or []
        vector_rows.extend(
            {
                "text": row.get("text_content"),
                "point_id": row.get("id"),
                "payload": {
                    "workspace_id": row.get("workspace_id"),
                    "doc_id": row.get("doc_id"),
                    "clause_number": row.get("clause_start"),
                    "content_type": row.get("content_type"),
                    "chunk_id": row.get("id"),
                },
            }
            for row in rows
            if row.get("id")
        )
        return vector_rows
