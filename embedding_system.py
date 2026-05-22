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


def iterate_llm(user_request, ontology_data, helping_nodes_saved=[], helping_nodes_new=3, entities_data={}, L=3):
    request_embedding = get_embeddings([user_request])[0]
    for el in ontology_data:
        el['score'] = cos_compare(el['embedding'], request_embedding)
    ontology_data.sort(key=lambda x: x['score'], reverse=True)
    saved_ids = set([node['id'] for node in helping_nodes_saved])

    for node in ontology_data[:helping_nodes_new]:
        if node['id'] not in saved_ids:
            helping_nodes_saved.append(node)
            saved_ids.add(node['id'])
    context_text = ""
    for node in helping_nodes_saved:
        context_text += node['text'] + "\n\n"

    if entities_data == {}:
        answer = make_prompt(user_request, context_text)
    else:
        found_fragments = set()
        for id in saved_ids:
            if id in entities_data:
                for fragment in entities_data[id]:
                    found_fragments.add(fragment)
        found_fragments = list(found_fragments)
        fragments_embeddings = get_embeddings(found_fragments)
        fragments_scored = []
        for i in range(len(found_fragments)):
            fragments_scored.append([found_fragments[i], cos_compare(fragments_embeddings[i], request_embedding)])
        fragments_scored.sort(key=lambda x: x[1], reverse=True)
        fragments_text = "\n\n".join([fragment[0] for fragment in fragments_scored[:L]])
        answer = make_prompt_upgraded(user_request, context_text, fragments_text)
    return answer

#Lab7 ===========

def make_prompt_upgraded(user_request, ontology_data, text_fragments):
    prompt = f"Ответь на заданный вопрос: {user_request}\n\nИспользуя основной текст:\n{ontology_data}\n\nДополняя свой ответ данными текстами:\n{text_fragments}"

    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content


def get_markup_data(markup_files, K=5):
    entities_data = {}

    for path in markup_files:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        entities = data.get('entites', [])
        text_with_ids = data.get('textWithIds', {})

        paragraphs = {}

        for id, text in text_with_ids.items():
            p_id = int(id) // 1000

            if p_id not in paragraphs:
                paragraphs[p_id] = []
            paragraphs[p_id].append((int(id), text))

        for p_id in paragraphs:
            paragraphs[p_id].sort(key=lambda x: x[0])
            paragraphs[p_id] = " ".join([word[1].strip() for word in paragraphs[p_id]])

        for el in entities:
            uri = el.get('node_uri')
            if not uri:
                continue

            if uri not in entities_data:
                entities_data[uri] = set()

            pos_start = el.get('pos_start')
            if pos_start is not None:
                p_id = pos_start // 1000

                fragment_parts = []
                for i in range(p_id - K, p_id + K + 1):
                    if i in paragraphs:
                        fragment_parts.append(paragraphs[i])

                fragment = " ".join(fragment_parts).strip()
                if fragment:
                    entities_data[uri].add(fragment)

    return entities_data
