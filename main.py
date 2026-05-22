from embedding_system import *

ONTOLOGY_PATH = "graph.json"

def rag_implementation(request):
    #Фаза 1
    ontology_data = get_ontology_data_vectors(ONTOLOGY_PATH)

    #Фаза 2
    saved_nodes = []
    answer = iterate_llm(request, ontology_data, saved_nodes)

    #Фаза 3
    answer = iterate_llm(answer, ontology_data, saved_nodes)
    return answer

if __name__ == "__main__":
    pass