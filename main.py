from embedding_system import *

ONTOLOGY_PATH = "graph.json"
MARKUP_FILES = ["Hobbit_markup.json", "Fellowship_markup.json"]

def rag_implementation(request):
    #Фаза 1
    ontology_data = get_ontology_data_vectors(ONTOLOGY_PATH)

    #Фаза 2
    saved_nodes = []
    answer = iterate_llm(request, ontology_data, saved_nodes)

    #Фаза 3
    answer = iterate_llm(answer, ontology_data, saved_nodes)

    #Добавление фрагментов
    entities_data = get_markup_data(MARKUP_FILES)
    answer = iterate_llm(request, ontology_data, saved_nodes, entities_data=entities_data)

    return answer

if __name__ == "__main__":
    request = input("Введите запрос: ")
    print(rag_implementation(request))