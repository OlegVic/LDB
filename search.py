from sqlalchemy.orm import aliased, selectinload
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Dict, List, Any, Optional
from sqlalchemy.sql import func, or_, and_
from sqlalchemy.future import select
from models import Product, ProductCharacteristic, CharacteristicClarify, ClassClarify
import time
from datetime import datetime

class ProductSearch:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def search_by_article(self, article: str):
        """Поиск по артикулу"""
        stmt = (select(Product)
        .options(
            selectinload(Product.characteristics),
            selectinload(Product.certificates),
            selectinload(Product.photos),
            selectinload(Product.analogs),
        )
        .where(Product.article == article))
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def search_by_name(self, name_query: str, limit=200):
        """Полнотекстовый поиск по наименованию (и артикулу)"""
        ts_query = func.plainto_tsquery('russian', name_query)
        stmt = (
            select(Product)
            .where(Product.search_vector.op('@@')(ts_query))
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def search_by_characteristics(self, filters: dict, limit=200):
        """
        Поиск по нескольким характеристикам.
        filters = {'Длина': '3м', 'Цвет': 'красный'}
        """
        stmt = select(Product)
        for idx, (char_name, value) in enumerate(filters.items()):
            PC_alias = aliased(ProductCharacteristic, name=f"pc_{idx}")
            CC_alias = aliased(CharacteristicClarify, name=f"cc_{idx}")
            stmt = stmt.join(PC_alias, Product.id == PC_alias.product_id) \
                .join(CC_alias, PC_alias.characteristic_id == CC_alias.id) \
                .where(
                CC_alias.characteristic_good == char_name,
                PC_alias.value == value
            )
        stmt = stmt.distinct().limit(limit)
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def smart_search(self, text_query: str, char_filters: dict = None, limit=200):
        """
        Комбинированный поиск по тексту + характеристикам.
        """
        base = await self.search_by_name(text_query, limit=100)
        if char_filters:
            product_ids = [prod.id for prod in base]
            stmt = select(Product)
            for char_name, value in char_filters.items():
                stmt = stmt.join(ProductCharacteristic, Product.id == ProductCharacteristic.product_id)\
                     .join(CharacteristicClarify, ProductCharacteristic.characteristic_id == CharacteristicClarify.id)\
                     .where(
                        CharacteristicClarify.characteristic_good == char_name,
                        ProductCharacteristic.value == value,
                        Product.id.in_(product_ids)
                     )
            stmt = stmt.distinct().limit(limit)
            result = await self.session.execute(stmt)
            return result.scalars().all()
        else:
            return base[:limit]

    async def search_by_group(self, group_name: str, limit=200):
        """
        Поиск по группе товаров.
        """
        stmt = (
            select(Product)
            .join(ClassClarify, Product.class_id == ClassClarify.id)
            .where(ClassClarify.group_name == group_name)
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def search_by_keys(self, key_phrase: str, limit=200):
        """
        Поиск по ключевым фразам в следующем порядке:
        1. group_name в ClassClarify
        2. purpose в ClassClarify
        3. class_rusname в ClassClarify
        4. name в Product

        Ключевая фраза разбивается на отдельные слова, исключаются предлоги и другие 
        бессмысленные слова, затем выполняется поиск по каждому слову отдельно.

        Args:
            key_phrase: Ключевая фраза для поиска
            limit: Максимальное количество результатов. Если None, возвращаются все найденные результаты.
        """
        # Список предлогов и других бессмысленных слов для исключения
        stop_words = [
            "и", "в", "во", "не", "что", "он", "на", "я", "с", "со", "как", "а", "то", "все", "она", 
            "так", "его", "но", "да", "ты", "к", "у", "же", "вы", "за", "бы", "по", "только", "ее", 
            "мне", "было", "вот", "от", "меня", "еще", "нет", "о", "из", "ему", "теперь", "когда", 
            "даже", "ну", "вдруг", "ли", "если", "уже", "или", "ни", "быть", "был", "него", "до", 
            "вас", "нибудь", "опять", "уж", "вам", "ведь", "там", "потом", "себя", "ничего", "ей", 
            "может", "они", "тут", "где", "есть", "надо", "ней", "для", "мы", "тебя", "их", "чем", 
            "была", "сам", "чтоб", "без", "будто", "чего", "раз", "тоже", "себе", "под", "будет", 
            "ж", "тогда", "кто", "этот", "того", "потому", "этого", "какой", "совсем", "ним", "здесь", 
            "этом", "один", "почти", "мой", "тем", "чтобы", "нее", "сейчас", "были", "куда", "зачем", 
            "всех", "никогда", "можно", "при", "наконец", "два", "об", "другой", "хоть", "после", 
            "над", "больше", "тот", "через", "эти", "нас", "про", "всего", "них", "какая", "много", 
            "разве", "три", "эту", "моя", "впрочем", "хорошо", "свою", "этой", "перед", "иногда", 
            "лучше", "чуть", "том", "нельзя", "такой", "им", "более", "всегда", "конечно", "всю", 
            "между"
        ]

        # Разбиваем ключевую фразу на отдельные слова и фильтруем стоп-слова
        words = key_phrase.lower().split()
        filtered_words = [word.strip(',.!?:;()[]{}"\'-') for word in words if word.lower() not in stop_words]

        # Если после фильтрации не осталось слов, используем исходную фразу
        if not filtered_words:
            filtered_words = [key_phrase]

        # Инициализация результатов
        results = []
        seen_ids = set()

        # Поиск по каждому слову
        for word in filtered_words:
            if len(word) < 3:  # Пропускаем слишком короткие слова
                continue

            # Создаем запрос для поиска по group_name, purpose, class_rusname
            class_stmt = (
                select(Product)
                .join(ClassClarify, Product.class_id == ClassClarify.id)
                .where(
                    or_(
                        ClassClarify.group_name.ilike(f"%{word}%"),
                        ClassClarify.purpose.ilike(f"%{word}%"),
                        ClassClarify.class_rusname.ilike(f"%{word}%")
                    )
                )
            )

            # Создаем запрос для поиска по названию товара
            name_stmt = (
                select(Product)
                .where(Product.name.ilike(f"%{word}%"))
            )

            # Добавляем результаты поиска по текущему слову
            class_result = await self.session.execute(class_stmt)
            class_products = class_result.scalars().all()

            name_result = await self.session.execute(name_stmt)
            name_products = name_result.scalars().all()

            word_results = class_products + name_products

            # Добавляем только уникальные результаты
            for product in word_results:
                if product.id not in seen_ids:
                    results.append(product)
                    seen_ids.add(product.id)

        if limit is None:
            return results
        return results[:limit]

    async def structured_search(self, search_criteria: Dict[str, Any], limit=200) -> Dict[str, Any]:
        """
        Структурированный поиск по заданным критериям.

        Формат входных данных:
        {
          "include": {
            "articles": ["01-0023", "KR-91-0840"],
            "keys": ["Кабель силовой", "Патч-корд"],
            "characteristics": {
              "Длина": ["3м", "2м"],
              "Цвет": ["синий"]
            }
          },
          "exclude": { ... }
        }

        Формат выходных данных:
        {
          "articles": ["01-0023", ...],
          "clarifications": {
            "classes": ["Кабель связи акустический", ...],
            "groups": ["Патч-корды", ...],
            "characteristics": {
              "Длина": ["3м", "2м", ...],
              "Цвет": ["синий", "красный", ...]
            }
          },
          "metadata": {
            "start_time": "2023-05-20 12:34:56",
            "end_time": "2023-05-20 12:34:57",
            "duration_seconds": 1.23
          }
        }
        """
        print("Начало структурированного поиска...")
        # Запись времени начала поиска
        start_time = datetime.now()
        print(f"Время начала поиска: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Инициализация результатов
        results = []
        print("Инициализация пустого списка результатов")

        # Обработка включающих критериев
        include = search_criteria.get("include", {})
        print(f"Получены включающие критерии: {include}")

        # Поиск по артикулам
        if "articles" in include and include["articles"]:
            print(f"Начинаем поиск по артикулам: {include['articles']}")
            article_results = []
            for article in include["articles"]:
                print(f"  Поиск по артикулу: {article}")
                article_found = await self.search_by_article(article)
                print(f"  Найдено товаров с артикулом {article}: {len(article_found)}")
                article_results.extend(article_found)
            print(f"Всего найдено товаров по артикулам: {len(article_results)}")
            results.extend(article_results)

        # Поиск по ключевым фразам
        if "keys" in include and include["keys"]:
            print(f"Начинаем поиск по ключевым фразам: {include['keys']}")
            keys_results = []
            for key_phrase in include["keys"]:
                print(f"  Поиск по ключевой фразе: {key_phrase}")
                key_found = await self.search_by_keys(key_phrase, limit)
                print(f"  Найдено товаров по ключевой фразе '{key_phrase}': {len(key_found)}")
                keys_results.extend(key_found)
            print(f"Всего найдено товаров по ключевым фразам: {len(keys_results)}")
            results.extend(keys_results)

        # Поиск по характеристикам
        if "characteristics" in include and include["characteristics"]:
            print(f"Начинаем поиск по характеристикам: {include['characteristics']}")
            char_results = []
            for char_name, values in include["characteristics"].items():
                print(f"  Поиск по характеристике: {char_name}")
                for value in values:
                    print(f"    Поиск по значению: {value}")
                    char_found = await self.search_by_characteristics({char_name: value}, limit)
                    print(f"    Найдено товаров с характеристикой '{char_name}={value}': {len(char_found)}")
                    char_results.extend(char_found)
            print(f"Всего найдено товаров по характеристикам: {len(char_results)}")
            results.extend(char_results)

        # Обработка исключающих критериев
        exclude = search_criteria.get("exclude", {})
        print(f"Получены исключающие критерии: {exclude}")

        # Исключение по артикулам
        if "articles" in exclude and exclude["articles"]:
            print(f"Исключаем товары по артикулам: {exclude['articles']}")
            before_count = len(results)
            results = [r for r in results if r.article not in exclude["articles"]]
            print(f"Исключено товаров по артикулам: {before_count - len(results)}")
            print(f"Осталось товаров после исключения по артикулам: {len(results)}")

        # Исключение по ключевым фразам
        if "keys" in exclude and exclude["keys"]:
            print(f"Исключаем товары по ключевым фразам: {exclude['keys']}")
            for key_phrase in exclude["keys"]:
                print(f"  Обработка исключения по ключевой фразе: {key_phrase}")

                # Получаем имена классов, соответствующих ключевой фразе
                print(f"  Поиск классов, соответствующих ключевой фразе '{key_phrase}'")
                excluded_class_names = self.session.query(ClassClarify.class_rusname).filter(
                    or_(
                        ClassClarify.group_name.ilike(f"%{key_phrase}%"),
                        ClassClarify.purpose.ilike(f"%{key_phrase}%"),
                        ClassClarify.class_rusname.ilike(f"%{key_phrase}%")
                    )
                ).all()
                excluded_class_names = [c[0] for c in excluded_class_names]
                print(f"  Найдены классы для исключения: {excluded_class_names}")

                # Исключаем продукты по классам
                # Получаем ID классов, соответствующих исключаемым именам классов
                print(f"  Получение ID классов для исключения")
                excluded_class_ids = self.session.query(ClassClarify.id).filter(
                    ClassClarify.class_rusname.in_(excluded_class_names)
                ).all()
                excluded_class_ids = [c[0] for c in excluded_class_ids]
                print(f"  Найдены ID классов для исключения: {excluded_class_ids}")

                # Исключаем продукты по ID классов
                before_count = len(results)
                results = [r for r in results if r.class_id not in excluded_class_ids]
                print(f"  Исключено товаров по классам: {before_count - len(results)}")
                print(f"  Осталось товаров после исключения по классам: {len(results)}")

                # Исключаем продукты по названию
                before_count = len(results)
                results = [r for r in results if key_phrase.lower() not in r.name.lower()]
                print(f"  Исключено товаров по названию: {before_count - len(results)}")
                print(f"  Осталось товаров после исключения по названию: {len(results)}")

        # Исключение по характеристикам
        if "characteristics" in exclude and exclude["characteristics"]:
            print(f"Исключаем товары по характеристикам: {exclude['characteristics']}")
            for char_name, values in exclude["characteristics"].items():
                print(f"  Обработка исключения по характеристике: {char_name} со значениями {values}")

                # Получаем ID характеристики
                print(f"  Поиск ID характеристики '{char_name}'")
                char_ids = self.session.query(CharacteristicClarify.id).filter(
                    CharacteristicClarify.characteristic_good == char_name
                ).all()
                char_ids = [c[0] for c in char_ids]
                print(f"  Найдены ID характеристики: {char_ids}")

                # Получаем ID продуктов с исключаемыми характеристиками
                print(f"  Поиск товаров с характеристикой '{char_name}' и значениями {values}")
                excluded_product_ids = self.session.query(ProductCharacteristic.product_id).filter(
                    ProductCharacteristic.characteristic_id.in_(char_ids),
                    ProductCharacteristic.value.in_(values)
                ).all()
                excluded_product_ids = [p[0] for p in excluded_product_ids]
                print(f"  Найдено товаров для исключения: {len(excluded_product_ids)}")

                # Исключаем продукты
                before_count = len(results)
                results = [r for r in results if r.id not in excluded_product_ids]
                print(f"  Исключено товаров по характеристике '{char_name}': {before_count - len(results)}")
                print(f"  Осталось товаров после исключения по характеристике: {len(results)}")

        # Удаляем дубликаты
        print("Удаление дубликатов из результатов поиска")
        unique_results = []
        seen_ids = set()
        for product in results:
            if product.id not in seen_ids:
                unique_results.append(product)
                seen_ids.add(product.id)
        print(f"Найдено уникальных товаров: {len(unique_results)} из {len(results)}")

        # Ограничиваем количество результатов
        print(f"Ограничение количества результатов до {limit}")
        if len(unique_results) > limit:
            print(f"Результаты ограничены: показаны первые {limit} из {len(unique_results)}")
        unique_results = unique_results[:limit]

        # Формируем выходные данные
        print("Формирование выходных данных")
        output = {
            "articles": [product.article for product in unique_results],
        }
        print(f"Добавлено {len(output['articles'])} артикулов в результаты")

        # Если найдено более 10 товаров, добавляем уточняющие характеристики
        if len(unique_results) > 10:
            print(f"Найдено более 10 товаров ({len(unique_results)}), добавляем уточняющие характеристики")
            clarifications = {}

            # Получаем уникальные классы из результатов
            print("Получение уникальных классов из результатов")
            product_class_ids = list(set(product.class_id for product in unique_results if product.class_id))
            print(f"Найдено уникальных классов: {len(product_class_ids)}")

            # Получаем имена классов по их ID
            if product_class_ids:
                print("Получение имен классов по их ID")
                class_names = self.session.query(ClassClarify.class_rusname).filter(
                    ClassClarify.id.in_(product_class_ids)
                ).all()
                classes = [c[0] for c in class_names]
                if classes:
                    print(f"Добавление {len(classes)} классов в уточнения")
                    clarifications["classes"] = classes

            # Получаем уникальные группы из результатов
            if product_class_ids:
                print("Получение уникальных групп из результатов")
                groups = self.session.query(ClassClarify.group_name).filter(
                    ClassClarify.id.in_(product_class_ids)
                ).distinct().all()
                groups = [g[0] for g in groups if g[0]]
                if groups:
                    print(f"Добавление {len(groups)} групп в уточнения")
                    clarifications["groups"] = groups

            # Получаем уникальные характеристики из результатов
            product_ids = [product.id for product in unique_results]
            if product_ids:
                print("Получение уникальных характеристик из результатов")
                # Получаем все характеристики для найденных продуктов
                char_query = self.session.query(
                    CharacteristicClarify.characteristic_good,
                    ProductCharacteristic.value
                ).join(
                    ProductCharacteristic, 
                    CharacteristicClarify.id == ProductCharacteristic.characteristic_id
                ).filter(
                    ProductCharacteristic.product_id.in_(product_ids)
                ).distinct()

                char_values = {}
                for char_name, value in char_query.all():
                    if char_name not in char_values:
                        char_values[char_name] = []
                    char_values[char_name].append(value)

                if char_values:
                    print(f"Добавление {len(char_values)} характеристик в уточнения")
                    clarifications["characteristics"] = char_values

            output["clarifications"] = clarifications
            print("Уточнения добавлены в результаты")

        # Запись времени окончания поиска и расчет длительности
        end_time = datetime.now()
        duration_seconds = (end_time - start_time).total_seconds()
        print(f"Время окончания поиска: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Длительность поиска: {round(duration_seconds, 3)} секунд")

        # Добавление метаданных о времени поиска
        print("Добавление метаданных о времени поиска")
        output["metadata"] = {
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_seconds": round(duration_seconds, 3)
        }

        print("Поиск завершен. Возвращаем результаты.")
        return output

    async def structured_search_v2(self, search_criteria: Dict[str, Any], limit=200) -> Dict[str, Any]:
        """
        Структурированный поиск по заданным критериям с измененной логикой.

        В отличие от structured_search, эта функция сначала собирает все возможные артикулы
        по articles и keys, а затем фильтрует их по characteristics.

        Формат входных данных:
        {
          "include": {
            "articles": ["01-0023", "KR-91-0840"],
            "keys": ["Кабель силовой", "Патч-корд"],
            "characteristics": {
              "Длина": ["3м", "2м"],
              "Цвет": ["синий"]
            }
          },
          "exclude": { ... }
        }

        Формат выходных данных:
        {
          "articles": ["01-0023", ...],
          "clarifications": {
            "classes": ["Кабель связи акустический", ...],
            "groups": ["Патч-корды", ...],
            "characteristics": {
              "Длина": ["3м", "2м", ...],
              "Цвет": ["синий", "красный", ...]
            }
          },
          "metadata": {
            "start_time": "2023-05-20 12:34:56",
            "end_time": "2023-05-20 12:34:57",
            "duration_seconds": 1.23
          }
        }
        """
        print("Начало структурированного поиска v2...")
        # Запись времени начала поиска
        start_time = datetime.now()
        print(f"Время начала поиска: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Инициализация результатов
        results = []
        print("Инициализация пустого списка результатов")

        # Обработка включающих критериев
        include = search_criteria.get("include", {})
        print(f"Получены включающие критерии: {include}")

        # Шаг 1: Сначала собираем все возможные артикулы по articles и keys
        print("Шаг 1: Сбор артикулов по критериям articles и keys")

        article_results = []
        include_articles = []
        # Поиск по артикулам
        if "articles" in include and include["articles"]:
            print(f"Начинаем поиск по артикулам: {include['articles']}")
            for article in include["articles"]:
                # print(f"  Поиск по артикулу: {article}")
                article_found = await self.search_by_article(article)
                # print(f"  Найдено товаров с артикулом {article}: {len(article_found)}")
                article_results.extend(article_found)
            print(f"Всего найдено товаров по артикулам: {len(article_results)}")
            # results.extend(article_results)
            include_articles.extend(article_results)

        # Применение жестких фильтров по классам и группам
        class_group_filtered_ids = None

        # Фильтрация по классам
        if "classes" in include and include["classes"]:
            print(f"Применение жесткого фильтра по классам: {include['classes']}")
            class_stmt = (
                select(Product.id)
                .join(ClassClarify, Product.class_id == ClassClarify.id)
                .where(ClassClarify.class_rusname.in_(include["classes"]))
            )
            class_result = await self.session.execute(class_stmt)
            class_filtered_ids = set([id[0] for id in class_result.all()])
            print(f"Найдено товаров по классам: {len(class_filtered_ids)}")

            if class_group_filtered_ids is None:
                class_group_filtered_ids = class_filtered_ids
            else:
                class_group_filtered_ids &= class_filtered_ids

        # Фильтрация по группам
        if "groups" in include and include["groups"]:
            print(f"Применение жесткого фильтра по группам: {include['groups']}")
            group_stmt = (
                select(Product.id)
                .join(ClassClarify, Product.class_id == ClassClarify.id)
                .where(ClassClarify.group_name.in_(include["groups"]))
            )
            group_result = await self.session.execute(group_stmt)
            group_filtered_ids = set([id[0] for id in group_result.all()])
            print(f"Найдено товаров по группам: {len(group_filtered_ids)}")

            if class_group_filtered_ids is None:
                class_group_filtered_ids = group_filtered_ids
            else:
                class_group_filtered_ids &= group_filtered_ids

        # Поиск по ключевым фразам
        if "keys" in include and include["keys"]:
            print(f"Начинаем поиск по ключевым фразам: {include['keys']}")
            keys_results = []
            for key_phrase in include["keys"]:
                print(f"  Поиск по ключевой фразе: {key_phrase}")

                # Если есть фильтры по классам или группам, ограничиваем поиск по ключам
                if class_group_filtered_ids is not None:
                    print(f"  Применение фильтра по классам/группам к поиску по ключевой фразе")
                    # Получаем все товары по ключевой фразе
                    all_key_found = await self.search_by_keys(key_phrase, limit=None)  # Без ограничения, чтобы получить все
                    # Фильтруем только те, которые прошли фильтр по классам/группам
                    key_found = [p for p in all_key_found if p.id in class_group_filtered_ids]
                else:
                    key_found = await self.search_by_keys(key_phrase, limit)

                print(f"  Найдено товаров по ключевой фразе '{key_phrase}': {len(key_found)}")
                keys_results.extend(key_found)
            print(f"Всего найдено товаров по ключевым фразам: {len(keys_results)}")
            results.extend(keys_results)

        # Удаляем дубликаты после поиска по артикулам и ключам
        print("Удаление дубликатов из промежуточных результатов поиска")
        unique_results = []
        seen_ids = set()
        for product in results:
            if product.id not in seen_ids:
                unique_results.append(product)
                seen_ids.add(product.id)
        print(f"Найдено уникальных товаров после поиска по артикулам и ключам: {len(unique_results)} из {len(results)}")
        results = unique_results

        # Шаг 2: Фильтрация результатов по характеристикам
        if "characteristics" in include and include["characteristics"] and results:
            print(f"Шаг 2: Фильтрация результатов по характеристикам: {include['characteristics']}")
            product_ids = [product.id for product in results]

            for char_name, values in include["characteristics"].items():
                print(f"  Фильтрация по характеристике: {char_name} со значениями {values}")

                # Получаем ID характеристики
                char_stmt = (
                    select(CharacteristicClarify.id)
                    .where(
                        or_(
                            CharacteristicClarify.characteristic == char_name,
                            CharacteristicClarify.characteristic_good == char_name
                        )
                    )
                )
                char_result = await self.session.execute(char_stmt)
                char_ids = [c[0] for c in char_result.all()]
                print(f"  Найдены ID характеристики '{char_name}': {char_ids}")

                if not char_ids:
                    print(f"  Характеристика '{char_name}' не найдена, пропускаем фильтрацию")
                    continue

                # Получаем ID продуктов с подходящими характеристиками
                matching_product_ids = set()
                for value in values:
                    print(f"    Поиск товаров с характеристикой '{char_name}' и значением '{value}'")
                    matching_stmt = (
                        select(ProductCharacteristic.product_id)
                        .where(
                            ProductCharacteristic.characteristic_id.in_(char_ids),
                            or_(
                                func.lower(ProductCharacteristic.value) == value.lower(),
                                ProductCharacteristic.extra_value.ilike(f"%;{value.lower()}%;"),
                            ),
                            ProductCharacteristic.product_id.in_(product_ids)
                        )
                    )
                    matching_result = await self.session.execute(matching_stmt)
                    matching_ids = [p[0] for p in matching_result.all()]
                    print(f"    Найдено товаров с характеристикой '{char_name}={value}': {len(matching_ids)}")
                    matching_product_ids.update(matching_ids)

                # Фильтруем результаты, оставляя только товары с подходящими характеристиками
                before_count = len(results)
                results = [r for r in results if r.id in matching_product_ids]
                print(f"  После фильтрации по характеристике '{char_name}' осталось товаров: {len(results)} из {before_count}")

                # Обновляем список ID продуктов для следующей итерации
                product_ids = [product.id for product in results]

                results_articles = set([prod.article for prod in results])
                for prod in article_results:
                    if prod.article not in results_articles:
                        results.append(prod)

                if not results:
                    print("  После фильтрации не осталось товаров, прекращаем поиск")
                    break

        # Обработка исключающих критериев
        exclude = search_criteria.get("exclude", {})
        print(f"Получены исключающие критерии: {exclude}")

        # Исключение по артикулам
        if "articles" in exclude and exclude["articles"]:
            print(f"Исключаем товары по артикулам: {exclude['articles']}")
            before_count = len(results)
            results = [r for r in results if r.article not in exclude["articles"]]
            print(f"Исключено товаров по артикулам: {before_count - len(results)}")
            print(f"Осталось товаров после исключения по артикулам: {len(results)}")

        # Исключение по ключевым фразам
        if "keys" in exclude and exclude["keys"]:
            print(f"Исключаем товары по ключевым фразам: {exclude['keys']}")
            for key_phrase in exclude["keys"]:
                print(f"  Обработка исключения по ключевой фразе: {key_phrase}")

                # Получаем имена классов, соответствующих ключевой фразе
                print(f"  Поиск классов, соответствующих ключевой фразе '{key_phrase}'")
                excluded_class_stmt = select(ClassClarify.class_rusname).where(
                    or_(
                        ClassClarify.group_name.ilike(f"%{key_phrase}%"),
                        ClassClarify.purpose.ilike(f"%{key_phrase}%"),
                        ClassClarify.class_rusname.ilike(f"%{key_phrase}%")
                    )
                )
                excluded_class_result = await self.session.execute(excluded_class_stmt)
                excluded_class_names = [c[0] for c in excluded_class_result.all()]
                print(f"  Найдены классы для исключения: {excluded_class_names}")

                # Исключаем продукты по классам
                # Получаем ID классов, соответствующих исключаемым именам классов
                print(f"  Получение ID классов для исключения")
                excluded_class_id_stmt = select(ClassClarify.id).where(
                    ClassClarify.class_rusname.in_(excluded_class_names)
                )
                excluded_class_id_result = await self.session.execute(excluded_class_id_stmt)
                excluded_class_ids = [c[0] for c in excluded_class_id_result.all()]
                print(f"  Найдены ID классов для исключения: {excluded_class_ids}")

                # Исключаем продукты по ID классов
                before_count = len(results)
                results = [r for r in results if r.class_id not in excluded_class_ids]
                print(f"  Исключено товаров по классам: {before_count - len(results)}")
                print(f"  Осталось товаров после исключения по классам: {len(results)}")

                # Исключаем продукты по названию
                before_count = len(results)
                results = [r for r in results if key_phrase.lower() not in r.name.lower()]
                print(f"  Исключено товаров по названию: {before_count - len(results)}")
                print(f"  Осталось товаров после исключения по названию: {len(results)}")

        # Исключение по характеристикам
        if "characteristics" in exclude and exclude["characteristics"]:
            print(f"Исключаем товары по характеристикам: {exclude['characteristics']}")
            for char_name, values in exclude["characteristics"].items():
                print(f"  Обработка исключения по характеристике: {char_name} со значениями {values}")

                # Получаем ID характеристики
                print(f"  Поиск ID характеристики '{char_name}'")
                char_id_stmt = select(CharacteristicClarify.id).where(
                    CharacteristicClarify.characteristic_good == char_name
                )
                char_id_result = await self.session.execute(char_id_stmt)
                char_ids = [c[0] for c in char_id_result.all()]
                print(f"  Найдены ID характеристики: {char_ids}")

                # Получаем ID продуктов с исключаемыми характеристиками
                print(f"  Поиск товаров с характеристикой '{char_name}' и значениями {values}")
                excluded_product_id_stmt = select(ProductCharacteristic.product_id).where(
                    ProductCharacteristic.characteristic_id.in_(char_ids),
                    ProductCharacteristic.value.in_(values)
                )
                excluded_product_id_result = await self.session.execute(excluded_product_id_stmt)
                excluded_product_ids = [p[0] for p in excluded_product_id_result.all()]
                print(f"  Найдено товаров для исключения: {len(excluded_product_ids)}")

                # Исключаем продукты
                before_count = len(results)
                results = [r for r in results if r.id not in excluded_product_ids]
                print(f"  Исключено товаров по характеристике '{char_name}': {before_count - len(results)}")
                print(f"  Осталось товаров после исключения по характеристике: {len(results)}")

        # Ограничиваем количество результатов
        print(f"Ограничение количества результатов до {limit}")
        if len(results) > limit:
            print(f"Результаты ограничены: показаны первые {limit} из {len(results)}")
        results = results[:limit]

        # Формируем выходные данные
        print("Формирование выходных данных")
        output = {
            "articles": [product.article for product in results],
        }
        output['articles'].extend([product.article for product in include_articles])
        print(f"Добавлено {len(output['articles'])} артикулов в результаты")

        # Если найдено более 10 товаров, добавляем уточняющие характеристики
        if len(results) > 10:
            print(f"Найдено более 10 товаров ({len(results)}), добавляем уточняющие характеристики")
            clarifications = {}

            # Получаем уникальные классы из результатов
            print("Получение уникальных классов из результатов")
            product_class_ids = list(set(product.class_id for product in results if product.class_id))
            print(f"Найдено уникальных классов: {len(product_class_ids)}")

            # Получаем имена классов по их ID
            if product_class_ids:
                print("Получение имен классов по их ID")
                class_stmt = select(ClassClarify.class_rusname).where(
                    ClassClarify.id.in_(product_class_ids)
                )
                class_result = await self.session.execute(class_stmt)
                class_names = class_result.all()
                classes = [c[0] for c in class_names]
                if classes:
                    print(f"Добавление {len(classes)} классов в уточнения")
                    clarifications["classes"] = classes

            # Получаем уникальные группы из результатов
            if product_class_ids:
                print("Получение уникальных групп из результатов")
                group_stmt = select(ClassClarify.group_name).where(
                    ClassClarify.id.in_(product_class_ids)
                ).distinct()
                group_result = await self.session.execute(group_stmt)
                groups = [g[0] for g in group_result.all() if g[0]]
                if groups:
                    print(f"Добавление {len(groups)} групп в уточнения")
                    clarifications["groups"] = groups

            # Получаем уникальные характеристики из результатов
            product_ids = [product.id for product in results]
            if product_ids:
                print("Получение уникальных характеристик из результатов")
                # Получаем все характеристики для найденных продуктов
                char_stmt = select(
                    CharacteristicClarify.characteristic,
                    CharacteristicClarify.characteristic_good,
                    ProductCharacteristic.value
                ).join(
                    ProductCharacteristic, 
                    CharacteristicClarify.id == ProductCharacteristic.characteristic_id
                ).where(
                    ProductCharacteristic.product_id.in_(product_ids)
                ).distinct()

                char_result = await self.session.execute(char_stmt)
                char_query_results = char_result.all()

                char_values = {}
                for char_name, char_name_good, value in char_query_results:
                    char_ = char_name_good if char_name_good else char_name
                    if char_ not in char_values:
                        char_values[char_] = []
                    char_values[char_].append(value)

                if char_values:
                    print(f"Добавление {len(char_values)} характеристик в уточнения")
                    clarifications["characteristics"] = char_values

            output["clarifications"] = clarifications
            print("Уточнения добавлены в результаты")

        # Запись времени окончания поиска и расчет длительности
        end_time = datetime.now()
        duration_seconds = (end_time - start_time).total_seconds()
        print(f"Время окончания поиска: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Длительность поиска: {round(duration_seconds, 3)} секунд")

        # Добавление метаданных о времени поиска
        print("Добавление метаданных о времени поиска")
        output["metadata"] = {
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_seconds": round(duration_seconds, 3)
        }

        print("Поиск завершен. Возвращаем результаты.")
        return output
        # Запись времени начала поиска
        start_time = datetime.now()
        print(f"Время начала поиска: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Инициализация результатов
        results = []
        print("Инициализация пустого списка результатов")

        # Обработка включающих критериев
        include = search_criteria.get("include", {})
        print(f"Получены включающие критерии: {include}")

        # Поиск по артикулам
        if "articles" in include and include["articles"]:
            print(f"Начинаем поиск по артикулам: {include['articles']}")
            article_results = []
            for article in include["articles"]:
                print(f"  Поиск по артикулу: {article}")
                article_found = self.search_by_article(article)
                print(f"  Найдено товаров с артикулом {article}: {len(article_found)}")
                article_results.extend(article_found)
            print(f"Всего найдено товаров по артикулам: {len(article_results)}")
            results.extend(article_results)

        # Поиск по ключевым фразам
        if "keys" in include and include["keys"]:
            print(f"Начинаем поиск по ключевым фразам: {include['keys']}")
            keys_results = []
            for key_phrase in include["keys"]:
                print(f"  Поиск по ключевой фразе: {key_phrase}")
                key_found = self.search_by_keys(key_phrase)
                print(f"  Найдено товаров по ключевой фразе '{key_phrase}': {len(key_found)}")
                keys_results.extend(key_found)
            print(f"Всего найдено товаров по ключевым фразам: {len(keys_results)}")
            results.extend(keys_results)

        # Поиск по характеристикам
        if "characteristics" in include and include["characteristics"]:
            print(f"Начинаем поиск по характеристикам: {include['characteristics']}")
            char_results = []
            for char_name, values in include["characteristics"].items():
                print(f"  Поиск по характеристике: {char_name}")
                for value in values:
                    print(f"    Поиск по значению: {value}")
                    char_found = self.search_by_characteristics({char_name: value})
                    print(f"    Найдено товаров с характеристикой '{char_name}={value}': {len(char_found)}")
                    char_results.extend(char_found)
            print(f"Всего найдено товаров по характеристикам: {len(char_results)}")
            results.extend(char_results)

        # Обработка исключающих критериев
        exclude = search_criteria.get("exclude", {})
        print(f"Получены исключающие критерии: {exclude}")

        # Исключение по артикулам
        if "articles" in exclude and exclude["articles"]:
            print(f"Исключаем товары по артикулам: {exclude['articles']}")
            before_count = len(results)
            results = [r for r in results if r.article not in exclude["articles"]]
            print(f"Исключено товаров по артикулам: {before_count - len(results)}")
            print(f"Осталось товаров после исключения по артикулам: {len(results)}")

        # Исключение по ключевым фразам
        if "keys" in exclude and exclude["keys"]:
            print(f"Исключаем товары по ключевым фразам: {exclude['keys']}")
            for key_phrase in exclude["keys"]:
                print(f"  Обработка исключения по ключевой фразе: {key_phrase}")

                # Получаем имена классов, соответствующих ключевой фразе
                print(f"  Поиск классов, соответствующих ключевой фразе '{key_phrase}'")
                excluded_class_names = self.session.query(ClassClarify.class_rusname).filter(
                    or_(
                        ClassClarify.group_name.ilike(f"%{key_phrase}%"),
                        ClassClarify.purpose.ilike(f"%{key_phrase}%"),
                        ClassClarify.class_rusname.ilike(f"%{key_phrase}%")
                    )
                ).all()
                excluded_class_names = [c[0] for c in excluded_class_names]
                print(f"  Найдены классы для исключения: {excluded_class_names}")

                # Исключаем продукты по классам
                # Получаем ID классов, соответствующих исключаемым именам классов
                print(f"  Получение ID классов для исключения")
                excluded_class_ids = self.session.query(ClassClarify.id).filter(
                    ClassClarify.class_rusname.in_(excluded_class_names)
                ).all()
                excluded_class_ids = [c[0] for c in excluded_class_ids]
                print(f"  Найдены ID классов для исключения: {excluded_class_ids}")

                # Исключаем продукты по ID классов
                before_count = len(results)
                results = [r for r in results if r.class_id not in excluded_class_ids]
                print(f"  Исключено товаров по классам: {before_count - len(results)}")
                print(f"  Осталось товаров после исключения по классам: {len(results)}")

                # Исключаем продукты по названию
                before_count = len(results)
                results = [r for r in results if key_phrase.lower() not in r.name.lower()]
                print(f"  Исключено товаров по названию: {before_count - len(results)}")
                print(f"  Осталось товаров после исключения по названию: {len(results)}")

        # Исключение по характеристикам
        if "characteristics" in exclude and exclude["characteristics"]:
            print(f"Исключаем товары по характеристикам: {exclude['characteristics']}")
            for char_name, values in exclude["characteristics"].items():
                print(f"  Обработка исключения по характеристике: {char_name} со значениями {values}")

                # Получаем ID характеристики
                print(f"  Поиск ID характеристики '{char_name}'")
                char_ids = self.session.query(CharacteristicClarify.id).filter(
                    CharacteristicClarify.characteristic_good == char_name
                ).all()
                char_ids = [c[0] for c in char_ids]
                print(f"  Найдены ID характеристики: {char_ids}")

                # Получаем ID продуктов с исключаемыми характеристиками
                print(f"  Поиск товаров с характеристикой '{char_name}' и значениями {values}")
                excluded_product_ids = self.session.query(ProductCharacteristic.product_id).filter(
                    ProductCharacteristic.characteristic_id.in_(char_ids),
                    ProductCharacteristic.value.in_(values)
                ).all()
                excluded_product_ids = [p[0] for p in excluded_product_ids]
                print(f"  Найдено товаров для исключения: {len(excluded_product_ids)}")

                # Исключаем продукты
                before_count = len(results)
                results = [r for r in results if r.id not in excluded_product_ids]
                print(f"  Исключено товаров по характеристике '{char_name}': {before_count - len(results)}")
                print(f"  Осталось товаров после исключения по характеристике: {len(results)}")

        # Удаляем дубликаты
        print("Удаление дубликатов из результатов поиска")
        unique_results = []
        seen_ids = set()
        for product in results:
            if product.id not in seen_ids:
                unique_results.append(product)
                seen_ids.add(product.id)
        print(f"Найдено уникальных товаров: {len(unique_results)} из {len(results)}")

        # Ограничиваем количество результатов
        print(f"Ограничение количества результатов до {limit}")
        if len(unique_results) > limit:
            print(f"Результаты ограничены: показаны первые {limit} из {len(unique_results)}")
        unique_results = unique_results[:limit]

        # Формируем выходные данные
        print("Формирование выходных данных")
        output = {
            "articles": [product.article for product in unique_results],
        }
        print(f"Добавлено {len(output['articles'])} артикулов в результаты")

        # Если найдено более 10 товаров, добавляем уточняющие характеристики
        if len(unique_results) > 10:
            print(f"Найдено более 10 товаров ({len(unique_results)}), добавляем уточняющие характеристики")
            clarifications = {}

            # Получаем уникальные классы из результатов
            print("Получение уникальных классов из результатов")
            product_class_ids = list(set(product.class_id for product in unique_results if product.class_id))
            print(f"Найдено уникальных классов: {len(product_class_ids)}")

            # Получаем имена классов по их ID
            if product_class_ids:
                print("Получение имен классов по их ID")
                class_names = self.session.query(ClassClarify.class_rusname).filter(
                    ClassClarify.id.in_(product_class_ids)
                ).all()
                classes = [c[0] for c in class_names]
                if classes:
                    print(f"Добавление {len(classes)} классов в уточнения")
                    clarifications["classes"] = classes

            # Получаем уникальные группы из результатов
            if product_class_ids:
                print("Получение уникальных групп из результатов")
                groups = self.session.query(ClassClarify.group_name).filter(
                    ClassClarify.id.in_(product_class_ids)
                ).distinct().all()
                groups = [g[0] for g in groups if g[0]]
                if groups:
                    print(f"Добавление {len(groups)} групп в уточнения")
                    clarifications["groups"] = groups

            # Получаем уникальные характеристики из результатов
            product_ids = [product.id for product in unique_results]
            if product_ids:
                print("Получение уникальных характеристик из результатов")
                # Получаем все характеристики для найденных продуктов
                char_query = self.session.query(
                    CharacteristicClarify.characteristic_good,
                    ProductCharacteristic.value
                ).join(
                    ProductCharacteristic, 
                    CharacteristicClarify.id == ProductCharacteristic.characteristic_id
                ).filter(
                    ProductCharacteristic.product_id.in_(product_ids)
                ).distinct()

                char_values = {}
                for char_name, value in char_query.all():
                    if char_name not in char_values:
                        char_values[char_name] = []
                    char_values[char_name].append(value)

                if char_values:
                    print(f"Добавление {len(char_values)} характеристик в уточнения")
                    clarifications["characteristics"] = char_values

            output["clarifications"] = clarifications
            print("Уточнения добавлены в результаты")

        # Запись времени окончания поиска и расчет длительности
        end_time = datetime.now()
        duration_seconds = (end_time - start_time).total_seconds()
        print(f"Время окончания поиска: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Длительность поиска: {round(duration_seconds, 3)} секунд")

        # Добавление метаданных о времени поиска
        print("Добавление метаданных о времени поиска")
        output["metadata"] = {
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_seconds": round(duration_seconds, 3)
        }

        print("Поиск завершен. Возвращаем результаты.")
        return output
