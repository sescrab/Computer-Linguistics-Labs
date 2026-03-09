from Driver import *


def test_embedding_pipeline(
        test_text: str = None,
        chunk_size: int = 128,
):

    # ── Тестовый текст (если не передан свой) ──
    if test_text is None:
        test_text = (
                        "Это тестовый текст для проверки разбиения и эмбеддингов. "
                        "Он содержит несколько предложений разной длины. "
                        "Python — отличный язык. Мы проверяем функции. "
                        "Очень длинное предложение, чтобы точно получить несколько чанков: "
                        "вот оно идёт и идёт и идёт и кажется никогда не закончится, "
                        "потому что мы специально его удлиняем для теста разбиения."
                    ) * 3
    chunks = get_chunks(test_text, chunk_size=chunk_size)
    print(chunks)
    embeddings = get_embeddings(chunks)
    print(embeddings)
    sim1 = cos_compare(embeddings[0], embeddings[1])
    print(sim1)

# Запуск детальных тестов
if __name__ == "__main__":
    test_embedding_pipeline()