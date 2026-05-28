from qdrant_client import QdrantClient, models
import logging as lg

qdrant_logger = lg.getLogger(__name__)
qdrant_logger.setLevel(lg.INFO)


class QdrantHelper:
    def __init__(
        self,
        *,
        qdrant_host: str,
        qdrant_port: str,
        qdrant_api_key: str = "",
        timeout: int = 180,
    ):
        self.client = QdrantClient(
            url=f"http://{qdrant_host}:{qdrant_port}",
            api_key=qdrant_api_key,
            timeout=timeout,
        )

    def get_qdrant_client(self) -> QdrantClient:
        """
        Возвращает Qdrant client.
        """
        return self.client

    def check_connection(self) -> bool:
        """
        Проверяет соединение с Qdrant.
        """
        try:
            self.get_qdrant_client().get_collections()
            return True
        except Exception as e:
            qdrant_logger.error(f"Ошибка при проверке соединения с Qdrant: {e}")
            return False

    def create_collection(
        self,
        *,
        collection_name: str,
        vector_size: int = 1024,
        distance: models.Distance = models.Distance.COSINE,
    ) -> None:
        """
        Создает коллекцию в Qdrant.
        Если коллекция уже существует, то ничего не делает.
        """
        if self.check_collection(collection_name=collection_name):
            qdrant_logger.warning(f"Коллекция {collection_name} уже существует")
            return

        self.client.create_collection(
            collection_name=collection_name,
            vectors_config=models.VectorParams(
                size=vector_size,
                distance=distance,
            ),
        )
        qdrant_logger.info(f"Коллекция {collection_name} создана")

    def delete_collection(
        self,
        *,
        collection_name: str,
    ) -> None:
        """
        Удаляет коллекцию в Qdrant. Если коллекция не существует, то ничего не делает.
        """
        if not self.check_collection(collection_name=collection_name):
            qdrant_logger.warning(f"Коллекция {collection_name} не существует")
            return

        self.client.delete_collection(collection_name=collection_name)
        qdrant_logger.info(f"Коллекция {collection_name} удалена")

    def check_collection(
        self,
        *,
        collection_name: str,
    ) -> bool:
        """
        Проверяет наличие коллекции в Qdrant.
        True - коллекция существует, False - коллекция не существует.
        """
        return self.client.collection_exists(collection_name=collection_name)
