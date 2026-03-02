import json
import random
import string
from typing import List, Dict, Any, Optional
from neo4j import GraphDatabase


class Neo4jRepository:
    # Подключение к базе данных
    def __init__(self, uri: str, user: str, password: str, database: str = "neo4j", namespace_title: str = "default"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database
        self.namespace_title = namespace_title

    def close(self):
        self.driver.close()

    def run_custom_query(self, query: str, parameters=None) -> List:
        if parameters is None:
            parameters = {}
        with self.driver.session(database=self.database) as session:
            result = session.run(query, parameters)
            return list(result)

    def transform_labels(self, labels, separator=':'):
        if len(labels) == 0:
            return ''
        res = ''
        for l in labels:
            i = separator + '{l}'.format(l=l)
            res += i
        return res

    def transform_props(self, props):
        if len(props) == 0:
            return ''
        data = "{"
        for p in props:
            temp = "{p}".format(p=p)
            temp += ':'
            temp += "{val}".format(val=json.dumps(props[p]))
            data += temp + ','
        data = data[:-1]
        data += "}"
        return data

    def uri_exists(self, uri: str) -> bool:
        query = "MATCH (n {uri: $uri}) RETURN count(n) as count"
        result = self.run_custom_query(query, {'uri': uri})
        return result[0]['count'] > 0

    def generate_random_string(self, length: int = 8):
        chars = string.ascii_letters + string.digits
        return ''.join(random.choice(chars) for _ in range(length))

    def generate_uri(self, length: int = 8) -> str:
        random_string = self.generate_random_string(length)
        return f"http://{self.namespace_title}.com/{random_string}"

    def generate_unique_uri(self, length: int = 8, max_attempts: int = 5) -> str:
        for attempt in range(max_attempts):
            uri = self.generate_uri(length)
            if not self.uri_exists(uri):
                return uri
        raise ValueError(f"Failed to generate unique URI after {max_attempts} attempts")

    def collect_node(self, node) -> Dict[str, Any]:
        data = node.data()
        if len(data) == 0:
            raise ValueError(f"Node has no data")
        for record in data:
            labels = list(node[record].labels)
            data = data[record]
        tnode = {
            'labels': labels
        }
        for field in data:
            tnode[field] = data[field]
        return tnode

    def collect_arc(self, path) -> Dict[str, Any]:
        node1 = path[0].start_node
        node2 = path[0].end_node
        for record in path[0]:
            arc = record
        tarc = {
            'id': arc.element_id,
            'uri': arc.type,
            'node_uri_from': node1.get('uri'),
            'node_uri_to': node2.get('uri'),
        }
        return tarc

    # Выдает словарь с ключами nodes и arcs, содержащий списки узлов и связей
    def get_all_nodes_and_arcs(self) -> Dict[str, List]:
        nodes = []
        arcs = []

        query_nodes = """
        MATCH (n)
        RETURN n
        """
        result_nodes = self.run_custom_query(query_nodes)

        for record in result_nodes:
            nodes.append(self.collect_node(record))

        query_arcs = """
        MATCH arc = ()-[]->()
        RETURN arc
        """
        result_arcs = self.run_custom_query(query_arcs)

        for record in result_arcs:
            arcs.append(self.collect_arc(record))

        return {'nodes': nodes, 'arcs': arcs}

    def get_nodes_by_labels(self, labels: List[str]) -> List[Dict]:
        label_str = self.transform_labels(labels)
        query = f"MATCH (n{label_str}) RETURN n"

        result = self.run_custom_query(query)
        return [self.collect_node(record) for record in result]

    def get_node_by_uri(self, uri: str) -> Optional[Dict]:
        query = "MATCH (n {uri:'$uri'}) RETURN n"
        result = self.run_custom_query(query, {'uri': uri})

        if result:
            return self.collect_node(result[0])
        return None

    def create_node(self, params: Dict) -> Dict:
        labels = []
        if ('labels' in params):
            labels = params['labels']
        label_str = self.transform_labels(labels, ':')

        params['uri'] = self.generate_unique_uri()
        props = {k: v for k, v in params.items() if k != 'labels'}
        props_str = self.transform_props(props)

        query = f"CREATE (n{label_str} {props_str}) RETURN n"
        result = self.run_custom_query(query)

        return self.collect_node(result[0])

    def create_arc(self, node1_uri: str, node2_uri: str, arc_type: str, properties: Dict = None) -> Dict:
        if properties is None:
            properties = {}

        query = """
        MATCH (a {uri: $uri1})
        MATCH (b {uri: $uri2})
        CREATE r = (a)-[:""" + arc_type + """ $props]->(b)
        RETURN r
        """

        result = self.run_custom_query(query, {
            'uri1': node1_uri,
            'uri2': node2_uri,
            'props': properties
        })

        return self.collect_arc(result[0])

    def delete_node_by_uri(self, uri: str) -> bool:
        query = """
        MATCH (n {uri: $uri})
        DETACH DELETE n
        RETURN count(n) as deleted_count
        """

        result = self.run_custom_query(query, {'uri': uri})
        return result[0]['deleted_count'] > 0

    def delete_arc_by_id(self, arc_id: int) -> bool:
        query = """
        MATCH ()-[r]->()
        WHERE elementId(r) = $arc_id
        DELETE r
        RETURN count(r) as deleted_count
        """

        result = self.run_custom_query(query, {'arc_id': arc_id})
        return result[0]['deleted_count'] > 0

    # Возвращает обновленный узел, если получилось обновить
    def update_node(self, uri: str, updates: Dict = None) -> Optional[Dict]:
        if updates is None:
            updates = {}

        new_labels = updates.pop('labels', {})

        set_clauses = []
        params = {'uri': uri}

        for i, (key, value) in enumerate(updates.items()):
            param_name = f'val_{i}'
            set_clauses.append(f"n.`{key}` = ${param_name}")
            params[param_name] = value

        for label in new_labels:
            set_clauses.append(f"n:`{label}`")

        if not set_clauses:
            return self.get_node_by_uri(uri)

        set_str = ', '.join(set_clauses)
        query = f"""
        MATCH (n {{uri: $uri}})
        SET {set_str}
        RETURN n
        """

        result = self.run_custom_query(query, params)

        if result:
            return self.collect_node(result[0])
        return None

    def clear_db(self):
        query = """
        MATCH (p)
        DETACH DELETE p
        """
        return self.run_custom_query(query)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

# Пример использования:
if __name__ == "__main__":
    with Neo4jRepository(
            uri="neo4j://127.0.0.1:7687",
            user="neo4j",
            password="12345678"
    ) as repo:
        node1 = repo.create_node({
            'labels': ['Person'],
            'description': 'Natalia',
        })

        node2 = repo.create_node({
            'labels': ['Person'],
            'description': 'Jane',
        })

        node3 = repo.create_node({
            'labels': ['Person'],
            'description': 'Ivan',
        })

        print("Created nodes:", node1, node2, node3)

        arc1 = repo.create_arc(
            node1_uri=node1['uri'],
            node2_uri=node2['uri'],
            arc_type='knows',
        )
        arc2 = repo.create_arc(
            node1_uri=node1['uri'],
            node2_uri=node3['uri'],
            arc_type='knows',
        )
        arc3 = repo.create_arc(
            node1_uri=node3['uri'],
            node2_uri=node2['uri'],
            arc_type='knows',
        )

        print("Created arcs:", arc1, arc2, arc3)

        all_data = repo.get_all_nodes_and_arcs()
        print("All nodes and arcs:", all_data)

        persons = repo.get_nodes_by_labels(['Person'])
        print("All persons:", persons)

        updated = repo.update_node(node1['uri'], {'description': 'Updated person'})
        print("Updated node:", updated)

        print(repo.delete_arc_by_id(arc2['id']))

        print(repo.delete_node_by_uri(node3['uri']))

        repo.clear_db()
