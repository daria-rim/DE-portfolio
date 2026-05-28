from sentence_transformers import SentenceTransformer


EMBEDDING_MODEL = "ai-forever/ru-en-RoSBERTa"


class Embedder:
    def __init__(self, model_name: str = EMBEDDING_MODEL):
        self.model = SentenceTransformer(model_name)

    def embed_text(self, text: str) -> list[float]:
        """
        Эмбедит текст и возвращает их векторное представление

        Args:
            text: текст для эмбединга

        Returns:
            векторное представление текста
        """
        return self.model.encode(text).tolist()

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """
        Эмбедит список текстов и возвращает их векторное представление

        Args:
            texts: список текстов для эмбединга

        Returns:
            список векторных представлений текстов
        """
        return self.model.encode(texts).tolist()

    def get_embedding_dimension(self) -> int:
        """
        Возвращает размерность векторного представления текста
        """
        return self.model.get_sentence_embedding_dimension()

    def count_tokens(self, text: str) -> int:
        """
        Число токенов в тексте по токенайзеру выбранной модели эмбеддинга.

        Args:
            text: входной текст
        """
        if not text:
            return 0
        ids = self.model.tokenizer.encode(
            text,
            add_special_tokens=True,
            verbose=False,  # не предупреждать о длине при подсчёте токенов
        )
        return len(ids)
