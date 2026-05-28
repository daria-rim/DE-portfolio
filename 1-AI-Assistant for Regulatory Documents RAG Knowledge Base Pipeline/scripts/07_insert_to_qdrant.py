import importlib
import logging as lg

from qdrant_client import QdrantClient, models

Embedder = importlib.import_module("scripts.05_embedding").Embedder


qdrant_logger = lg.getLogger(__name__)
qdrant_logger.setLevel(lg.INFO)


def embed_vector_rows_to_qdrant_points(
    embedder: Embedder,
    vector_rows: list[dict],
) -> list[dict]:
    """
    Точки для upsert из строк SupabaseChunksUpserter: text, point_id, payload.
    """
    if not vector_rows:
        qdrant_logger.warning("Нет векторных строк для вставки")
        return []

    qdrant_points: list[dict] = []
    for row in vector_rows:
        text = row.get("text") or ""
        vector = embedder.embed_text(text)
        qdrant_points.append(
            {
                "id": row["point_id"],
                "vector": vector,
                "payload": row.get("payload"),
            }
        )
    return qdrant_points


class QdrantInsertor:
    def __init__(
        self,
        *,
        qdrant_client: QdrantClient,
    ):
        self.qdrant_client = qdrant_client


    def insert_chunks(
        self,
        *,
        chunks: list[dict],
        collection_name: str = "test_chunks",
    ) -> None:
        """
        Вставляет чанки в Qdrant.
        """
        if not chunks:
            qdrant_logger.warning("Нет чанков для вставки")
            return

        first_chunk = chunks[0]
        doc_id = first_chunk.get("payload").get("doc_id")

        if not doc_id:
            raise ValueError("В chunks.metadata не найден doc_id")

        self.delete_chunks_by_doc_id(
            collection_name=collection_name,
            doc_id=doc_id,
        )

        self.qdrant_client.upsert(
            collection_name=collection_name,
            points=chunks,
        )
        qdrant_logger.info(f"Вставлено {len(chunks)} чанков в коллекцию {collection_name}")

    def delete_chunks_by_doc_id(
        self,
        *,
        collection_name: str,
        doc_id: str,
    ) -> None:
        """
        Удаляет из коллекции все векторы документа по payload.doc_id.

        Args:
            collection_name: название коллекции
            doc_id: id документа
        """
        qdrant_logger.info(f"Удаляем чанки из коллекции {collection_name} по doc_id {doc_id}")
        self.qdrant_client.delete(
            collection_name=collection_name,
            points_selector=models.FilterSelector(
                filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="doc_id",
                            match=models.MatchValue(value=doc_id),
                        )
                    ]
                )
            ),
        )
        qdrant_logger.info(f"Удалено все чанки из коллекции {collection_name} по doc_id {doc_id}")
