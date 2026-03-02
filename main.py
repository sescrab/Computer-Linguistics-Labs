from Driver import *
def run_detailed_tests():
    """Функция для детального тестирования всех функций Lab2"""

    with Neo4jRepository(
            uri="neo4j://127.0.0.1:7687",
            user="neo4j",
            password="12345678",
            namespace_title="test-ontology"
    ) as repo:

        # Очищаем базу
        repo.clear_db()

        # Тест 1: Создание иерархии классов
        print("\n=== Тест 1: Создание иерархии классов ===")
        thing = repo.create_class("Thing", "Базовый класс всего")
        living = repo.create_class("Living", "Живое", thing['uri'])
        nonliving = repo.create_class("NonLiving", "Неживое", thing['uri'])
        animal = repo.create_class("Animal", "Животные", living['uri'])
        plant = repo.create_class("Plant", "Растения", living['uri'])
        mammal = repo.create_class("Mammal", "Млекопитающие", animal['uri'])
        dog = repo.create_class("Dog", "Собаки", mammal['uri'])
        classes = [thing, living, nonliving, animal, plant, mammal, dog]
        print(f"Созданы классоы:")
        for cls in classes:
            print(cls)

        # Тест 2: Проверка родителей
        print("\n=== Тест 2: Проверка родителей ===")
        dog_parents = repo.get_class_parents(dog['uri'])
        print(f"Родители Dog ({len(dog_parents)}):")
        for p in dog_parents:
            print(f"  - {p['title']}")

        # Тест 3: Проверка детей
        print("\n=== Тест 3: Проверка детей ===")
        thing_children = repo.get_class_children(thing['uri'])
        print(f"Дети Thing ({len(thing_children)}):")
        for c in thing_children:
            print(f"  - {c['title']}")

        # Тест 4: Корневые классы
        print("\n=== Тест 4: Корневые классы ===")
        roots = repo.get_ontology_parent_classes()
        print(f"Корневых классов: {len(roots)}")
        for r in roots:
            print(f"  - {r['title']}")

        # Тест 5: Добавление атрибутов
        print("\n=== Тест 5: Добавление атрибутов ===")
        age_attr = repo.add_class_attribute(animal['uri'], "age")
        name_attr = repo.add_class_attribute(animal['uri'], "name")
        breed_attr = repo.add_class_attribute(dog['uri'], "breed")
        print(f"Добавлены атрибуты: age, name, breed")

        # Тест 6: Добавление объектных атрибутов
        print("\n=== Тест 6: Добавление объектных атрибутов ===")
        friend_attr = repo.add_class_object_attribute(animal['uri'], animal['uri'], "hasFriend")
        owner_attr = repo.add_class_object_attribute(dog['uri'], thing['uri'], "hasOwner")
        print(f"Добавлены объектные атрибуты: hasFriend, hasOwner")

        # Тест 7: Сигнатура класса
        print("\n=== Тест 7: Сигнатура класса ===")
        dog_sign = repo.collect_signature(dog['uri'])
        print(f"Сигнатура Dog:")
        print(f"  Дата-атрибуты: {[p['title'] for p in dog_sign['params']]}")
        print(f"  Объектные атрибуты: {[p['title'] for p in dog_sign['obj_params']]}")

        # Тест 8: Создание объектов
        print("\n=== Тест 8: Создание объектов ===")
        obj1 = repo.create_object(
            dog['uri'],
            "Rex",
            {"params": {"age": 3, "name": "Rex", "breed": "Shepherd"}, "obj_params":[]},
            "Первая собака"
        )
        obj2 = repo.create_object(
            dog['uri'],
            "Bobik",
            {"params": {"age": 2, "name": "Bobik", "breed": "Mixed"}, "obj_params":[]},
            "Вторая собака"
        )
        print(f"Создано объекты:")
        print(obj1)
        print(obj2)

        # Тест 9: Получение объектов класса
        print("\n=== Тест 9: Получение объектов класса ===")
        animal_objects = repo.get_class_objects(animal['uri'])
        print(f"Объекты Animal и подклассов: {len(animal_objects)}")
        for obj in animal_objects:
            print(f"  - {obj['title']} (возраст: {obj.get('age')})")

        # Тест 10: Получение всей онтологии
        print("\n=== Тест 10: Получение всей онтологии ===")
        ontology = repo.get_ontology()
        print(f"Классы: {len(ontology['Classes'])}")
        print(f"Объекты: {len(ontology['Objects'])}")

        # Тест 11: Удаление атрибута
        print("\n=== Тест 11: Удаление атрибута ===")
        repo.delete_class_attribute(breed_attr['uri'])
        updated_obj = repo.get_object(obj1['uri'])
        print(f"После удаления breed у объекта Rex: breed={updated_obj.get('breed', 'отсутствует')}")

        # Тест 12: Удаление класса
        print("\n=== Тест 12: Удаление класса ===")
        repo.delete_class(animal['uri'])
        final_ontology = repo.get_ontology()
        print(f"После удаления Animal:")
        print(f"  Классов: {len(final_ontology['Classes'])}")
        print(f"  Объектов: {len(final_ontology['Objects'])}")

        print("\n=== Все тесты завершены ===")


# Запуск детальных тестов
if __name__ == "__main__":
    run_detailed_tests()