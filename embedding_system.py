from typing import List, Dict, Any, Optional
import re
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer
import json
from openai import OpenAI
from myenv import API_KEY

EMBEDDING_MODEL = SentenceTransformer("sentence-transformers/paraphrase-multilingual-mpnet-base-v2")

client = OpenAI(
    api_key=API_KEY,
    base_url="https://polza.ai/api/v1"
)
LLM_MODEL = "deepseek/deepseek-v4-flash"


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

#Lab6 ===========

def get_ontology_data_vectors(path):
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    nodes = data.get('nodes', [])
    arcs = data.get('arcs', [])
    node_data = {}

    for node in nodes:
        id = node.get('id')
        params = node.get('data', {}).get('params_values', {})
        labels = params.get('http://www.w3.org/2000/01/rdf-schema#label',  ['Нет названия'])
        name = labels[0].split('@')[0]
        desc = params.get('http://www.w3.org/2000/01/rdf-schema#comment', 'Нет описания')
        node_data[id] = {'name': name, 'desc': desc, 'relations': []}

    for arc in arcs:
        source_id = arc.get('source')
        target_id = arc.get('target')
        uri = arc.get('data', {}).get('uri', '')

        if source_id in node_data and target_id in node_data:
            if uri in node_data:
                rel_name = node_data[uri]['name']
            elif '#' in uri:
                rel_name = uri.split('#')[-1]
            else:
                rel_name = uri.split('/')[-1]

            target_name = node_data[target_id]['name']
            node_data[source_id]['relations'].append(f"{rel_name}: {target_name}")

    ans = []
    for id, dt in node_data.items():
        lines = [f"Название: {dt['name']}", f"Описание: {dt['desc']}"]
        lines.extend(dt['relations'])

        node_text = "\n".join(lines)
        embedding = get_embeddings([node_text])[0]

        ans.append({'id': id, 'text': node_text, 'embedding': embedding})
    return ans


def make_prompt(user_request, ontology_data):
    prompt = f"Дай ответ на данный вопрос, используя информацию из текста:\n{user_request}\nТекст:\n{ontology_data}"
    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content

def iterate_llm(user_request, ontology_data, helping_nodes_saved=[], helping_nodes_new=3):
    request_embedding = get_embeddings([user_request])[0]
    for el in ontology_data:
        el['score'] = cos_compare(el['embedding'], request_embedding)
    ontology_data.sort(key=lambda x: x['score'], reverse=True)
    saved_ids = set([node['id'] for node in helping_nodes_saved])

    for node in ontology_data[:helping_nodes_new]:
        if node['id'] not in saved_ids:
            helping_nodes_saved.append(node)
    context_text = ""
    for node in helping_nodes_saved:
        context_text += node['text'] + "\n\n"

    answer = make_prompt(user_request, context_text)
    return answer

