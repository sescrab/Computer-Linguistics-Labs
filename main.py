from embedding_system import *


def test_embedding_pipeline(
        test_text: str = None,
        chunk_size: int = 64,
):
    if test_text is None:
        test_text = """ Маша живет в Новосибирске. Маша изучает информационные технологии.
        
        В Новосибирском государственном университете есть факультет информационных технологий"""
    chunks = get_chunks(test_text)
    print(chunks)
    embeddings = get_embeddings(chunks)
    print(embeddings)
    sim1 = cos_compare(embeddings[0], embeddings[1])
    print(sim1)

if __name__ == "__main__":
    test_embedding_pipeline()