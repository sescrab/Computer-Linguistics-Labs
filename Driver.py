import json
import random
import string
from typing import List, Dict, Any, Optional
from neo4j import GraphDatabase


class Neo4jRepository:

    # Lab1 =================
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
        query = "MATCH (n {uri:$uri}) RETURN n"
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

    def delete_arc_by_id(self, arc_id: str) -> bool:
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


    # Lab2 =========

    def get_uri_unique_part(self, uri):
        return uri.split('/')[-1]

    def create_class(self, title, description='', parent_uri=None):
        node = self.create_node({'title': title, 'description': description, 'labels': ['Class']})
        node = self.update_node(node['uri'], {'labels': [self.get_uri_unique_part(node['uri'])]})
        if parent_uri is not None:
             self.create_arc(node['uri'], parent_uri, 'subclassOf')
        return node

    def update_class(self, class_uri, title, description):
        node = self.update_node(class_uri, {'title': title, 'description': description})
        return node

    def add_class_parent(self, class_uri, parent_uri):
        arc = self.create_arc(class_uri, parent_uri, 'subclassOf')
        return arc

    def get_class(self, class_uri):
        return self.get_node_by_uri(class_uri)

    def get_class_parents(self, class_uri):
        query = """
        MATCH path = (n:Class {uri: $uri})-[:subclassOf*]->(ancestor)
        RETURN DISTINCT ancestor
        """
        nodes = self.run_custom_query(query, {'uri': class_uri});
        result = []
        for node in nodes:
            result.append(self.collect_node(node))
        return result

    def get_class_children(self, class_uri):
        query = """
        MATCH path = (n:Class {uri: $uri})<-[:subclassOf*]-(child)
        RETURN DISTINCT child
        """
        nodes = self.run_custom_query(query, {'uri': class_uri});
        result = []
        for node in nodes:
            result.append(self.collect_node(node))
        return result

    def get_ontology_parent_classes(self):
        query = """
        MATCH (n:Class)
        WHERE NOT (n)-[:subclassOf]->()
        RETURN n
        """
        nodes = self.run_custom_query(query);
        result = []
        for node in nodes:
            result.append(self.collect_node(node))
        return result

    def collect_single_signature(self, class_uri, acc):
        seen_params = {p['title'] for p in acc['params']}
        seen_obj_params = {p['title'] for p in acc['obj_params']}
        query1 = """
        MATCH (p:DatatypeProperty)-[:domain]->(:Class {uri: $uri})
        RETURN p
        """
        nodes_datatype = self.run_custom_query(query1, {'uri' : class_uri});
        for node in nodes_datatype:
            collected_node = self.collect_node(node)
            if collected_node['title'] not in seen_params:
                acc['params'].append(collected_node)
                seen_params.add(collected_node['title'])

        query2 = """
        MATCH (:Class {uri: $uri})<-[:domain]-(p:ObjectProperty)-[:range]->(:Class)
        RETURN p
        """
        nodes_objtype1 = self.run_custom_query(query2, {'uri' : class_uri});
        for node in nodes_objtype1:
            tDict = self.collect_node(node)
            tDict['relation_direction'] = 1
            if tDict['title'] not in seen_obj_params:
                acc['obj_params'].append(tDict)
                seen_obj_params.add(tDict['title'])

        query3 = """
        MATCH (:Class {uri: $uri})<-[:range]-(p:ObjectProperty)-[:domain]->(:Class)
        RETURN p
        """
        nodes_objtype2 = self.run_custom_query(query3, {'uri' : class_uri});
        for node in nodes_objtype2:
            tDict = self.collect_node(node)
            tDict['relation_direction'] = -1
            if tDict['title'] not in seen_obj_params:
                acc['obj_params'].append(tDict)
                seen_obj_params.add(tDict['title'])
        return acc

    def collect_signature(self, class_uri):
        acc = {'params': [], 'obj_params': []}
        self.collect_single_signature(class_uri, acc)
        parents = self.get_class_parents(class_uri)
        for parent in parents:
            self.collect_single_signature(parent['uri'], acc)
        return acc

    def check_sign_props(self, signature, properties):
        sign_titles_data = [p['title'] for p in signature['params']]
        sign_titles_obj = [p['title'] for p in signature['obj_params']]
        prop_titles_data = [p for p in properties['params']]
        prop_titles_obj = [p for p in properties['obj_params']]

        for title in sign_titles_data:
            if title not in prop_titles_data:
                return False
        for title in sign_titles_obj:
            if title not in prop_titles_obj:
                return False
        for title in prop_titles_data:
            if title not in sign_titles_data:
                return False
        for title in prop_titles_obj:
            if title not in sign_titles_obj:
                return False
        return True

    def update_object(self, obj_uri, properties):
        query = """
        MATCH (p:Class)<-[:type]-(:Object {uri: $uri})
        RETURN p
        """
        node = self.run_custom_query(query, {'uri': obj_uri})
        node = self.collect_node(node[0])
        class_sign = self.collect_signature(node['uri'])
        # if not self.check_sign_props(class_sign, properties):
        #     raise ValueError("Wrong properties given")
        node = self.update_node(obj_uri, properties['params'])

        query1 = """
        MATCH (n:Object {uri: $uri})-[r]->(:Object)
        DELETE r
        """
        self.run_custom_query(query1, {'uri': obj_uri})

        for arc in properties['obj_params']:
            self.create_arc(obj_uri, properties['obj_params'][arc], arc)

        return node


    def create_object(self, class_uri, title, properties, description=""):
        node = self.create_node({'title': title, 'description': description, 'labels': ['Object', self.get_uri_unique_part(class_uri)]})
        node = self.update_node(node['uri'], {'labels': [self.get_uri_unique_part(node['uri'])]})
        self.create_arc(node['uri'], class_uri, 'type')
        node = self.update_object(node['uri'], properties)
        return node

    def get_object(self, object_uri):
        node = self.get_node_by_uri(object_uri)
        query = """
        MATCH r = (n:Object {uri: $uri})-[]->(:Object)
        RETURN r
        """
        arcs = self.run_custom_query(query, {'uri': self.get_uri_unique_part(object_uri)})
        arcs = [self.collect_arc(arc) for arc in arcs]
        for arc in arcs:
            node[arc['title']] = arc['node_uri_to']
        return node

    def delete_object(self, object_uri):
        return self.delete_node_by_uri(object_uri)

    def get_class_objects(self, class_uri):
        classes = self.get_class_children(class_uri)
        classes.append(self.get_class(class_uri))

        result = []
        for c in classes:
            tResult = self.get_nodes_by_labels([self.get_uri_unique_part(c['uri'])])
            for node in tResult:
                if 'Object' in node['labels']:
                    result.append(node)
        return result

    def get_ontology(self):
        classes = self.get_nodes_by_labels(['Class'])
        objects = self.get_nodes_by_labels(['Object'])
        return {
            'Classes': classes,
            'Objects': objects
        }

    def add_class_attribute(self, class_uri, title):
        node = self.create_node({
            'title': title,
            'labels': ['DatatypeProperty']
        })
        self.create_arc(node['uri'], class_uri, 'domain')
        return node

    def delete_class_attribute(self, property_uri):
        node = self.get_node_by_uri(property_uri)
        title = node['title']

        query = """
        MATCH (p:DatatypeProperty {uri: $uri})-[:domain]->(c:Class)
        RETURN c
        """
        class_result = self.run_custom_query(query, {'uri': property_uri})
        target_class = self.collect_node(class_result[0])

        objects = self.get_class_objects(target_class['uri'])
        for obj in objects:
            obj_updates = {}
            if title in obj:
                obj_updates[title] = None
                self.update_node(obj['uri'], obj_updates)

        return self.delete_node_by_uri(property_uri)

    def add_class_object_attribute(self, class_uri, range_class_uri, title):
        node = self.create_node({
            'title': title,
            'labels': ['ObjectProperty']
        })
        self.create_arc(node['uri'], class_uri, 'domain')
        self.create_arc(node['uri'], range_class_uri, 'range')
        return node

    def delete_class_object_attribute(self, object_property_uri):
        node = self.get_node_by_uri(object_property_uri)
        title = node.get('title')

        query1 = """
        MATCH (p:ObjectProperty {uri: $uri})-[:domain]->(c:Class)
        RETURN c
        """
        domain_result = self.run_custom_query(query1, {'uri': object_property_uri})
        domain_class = self.collect_node(domain_result[0])
        objects = self.get_class_objects(domain_class['uri'])

        for obj in objects:
            query2 = """
            MATCH (obj:Object {uri: $uri})-[r:""" + title + """]->()
            DELETE r
            """
            self.run_custom_query(
                query2,
                {'uri': obj['uri']}
            )
        return self.delete_node_by_uri(object_property_uri)

    def delete_class(self, class_uri):
        classes = self.get_class_children(class_uri)
        classes.append(self.get_class(class_uri))

        for obj in self.get_class_objects(class_uri):
            self.delete_node_by_uri(obj['uri'])

        for cls in classes:
            query1 = """
            MATCH (p:DatatypeProperty)-[:domain]->(c:Class {uri: $uri})
            DETACH DELETE p
            """
            self.run_custom_query(query1, {'uri': cls['uri']})

            query2 = """
            MATCH (p:ObjectProperty)-[:domain]->(c:Class {uri: $uri})
            DETACH DELETE p
            """
            self.run_custom_query(query2, {'uri': cls['uri']})

        deleted_count = 0
        for cls in classes:
            if self.delete_node_by_uri(cls['uri']):
                deleted_count += 1

        return deleted_count > 0

