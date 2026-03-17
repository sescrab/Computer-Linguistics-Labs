from typing import List, Dict, Any, Optional
import re
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer

EMBEDDING_MODEL = SentenceTransformer("sentence-transformers/paraphrase-multilingual-mpnet-base-v2")

#Lab3 ===========

#Разбивает текст либо на предложения (по умолчанию) либо на абзацы
def get_chunks(text: str, split_by_sentences=False) -> list[str]:
    if not text or not text.strip():
        return []

    text = text.strip()

    if not split_by_sentences:
        #Разделяем по двойным переводам строк (абзацы)
        chunks = re.split(r'\n\s*\n', text)
        return [chunk.strip() for chunk in chunks if chunk.strip()]

    else:
        #Разделяем на предложения
        sentence_pattern = r'(?<!\w\.\w.)(?<=\.|\?|\!|\…)\s+'
        chunks = re.split(sentence_pattern, text)

        # Убираем пустые строки и обрезаем пробелы
        return [chunk.strip() for chunk in chunks if chunk.strip()]

#По фрагментам текста возвращает их эмбединги сразу в виде нампаевских массивов
def get_embeddings(chunks: List[str]):
    embeddings = EMBEDDING_MODEL.encode(chunks)
    return embeddings

#По эмбедингам после get_embeddings возвращает степень сходства в виде числа от 0 до 1
def cos_compare(a: np.ndarray, b: np.ndarray):
    #sklearn ожидает 2D массивы
    a_2d = a.reshape(1, -1)
    b_2d = b.reshape(1, -1)

    # возвращает матрицу виде [[числа(схожесть)]]
    similarity_matrix = cosine_similarity(a_2d, b_2d)

    return float(similarity_matrix[0, 0])