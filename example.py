from db import AsyncSessionLocal
from search import ProductSearch
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
import json
import asyncio

# # Пример поиска по артикулу
# async def test_search_by_article():
#     session = SessionLocal()
#     search = ProductSearch(session)
#     print("Поиск по артикулу:")
#     results = await search.search_by_article("01-0023")
#     for p in results:
#         print(p.article, p.name)
#
# # Пример поиска по наименованию
# async def test_search_by_name():
#     session = SessionLocal()
#     search = ProductSearch(session)
#     print("Поиск по наименованию:")
#     results = await search.search_by_name("патч-корд utp 3 метра")
#     for p in results:
#         print(p.article, p.name)
#
# # Пример поиска по характеристикам
# async def test_search_by_characteristics():
#     session = SessionLocal()
#     search = ProductSearch(session)
#     print("Поиск по характеристикам:")
#     results = await search.search_by_characteristics({'Длина': '3м', 'Цвет': 'красный'})
#     for p in results:
#         print(p.article, p.name)
#
# # Пример комбинированного поиска
# async def test_smart_search():
#     session = SessionLocal()
#     search = ProductSearch(session)
#     print("Умный поиск:")
#     results = await search.smart_search("патч-корд", {'Длина': '3м'})
#     for p in results:
#         print(p.article, p.name)

async def main():
    # Пример структурированного поиска
    print("\nСтруктурированный поиск:")

    # Создаем сессию и экземпляр поисковика
    session = AsyncSessionLocal()
    search = ProductSearch(session)

    search_criteria_ = {
        "include": {
            "articles": ["01-0023", "KR-91-0840"],
            "keys": ["Кабель связи акустический", "Сверло/бур", "Патч-корды", "Акустические кабели"],
            "classes": [],
            "groups": ["Кабели и сопутствующие товары"],
            "characteristics": {
                "Цвет": ["Белый"],
                "Тип крепления": [
                    "Винт",
                    "Дюбель",
                ],
            }
        },
        "exclude": {
            "articles": [],
            "keys": [],
            "characteristics": {}
        }
    }

    search_criteria = {
        "include": {
            "articles": [],
            "keys": ["кабе"],
            "classes": [],
            "groups": [],
            "characteristics": {}
        },
        "exclude": {
            "articles": [],
            "keys": [],
            "characteristics": {}
        },
        "metadata": {
            "session_id": 999,
            "parameters": {}
        }
    }

    search_criteria_ = {
        "include": {
            "articles": [],
            "keys": ["кабе"],
            "classes": [],
            "groups": [],
            "characteristics": {
                "Длина": [
                    "305 м."
                ],
            }
        },
        "exclude": {
            "articles": [],
            "keys": [],
            "characteristics": {}
        },
        "metadata": {
            "session_id": 999,
            "parameters": {}
        }
    }

    search_criteria_ = {
        "include": {
            "articles": [],
            "keys": ["Кабель"],
            "characteristics": {}
        },
        "exclude": {
            "articles": [],
            "keys": [],
            "clarifications": [],
            "characteristics": {}
        }
    }

    # print("Результаты оригинального структурированного поиска:")
    # results = await search.structured_search(search_criteria, 200)
    # print(json.dumps(results, ensure_ascii=False, indent=2))

    print("\nСтруктурированный поиск v2 (сначала поиск по артикулам и ключам, затем фильтрация по характеристикам):")
    results_v2 = await search.structured_search_v2(search_criteria, 10000)
    print(json.dumps(results_v2, ensure_ascii=False, indent=2))

    # Закрываем сессию после использования
    await session.close()

# Запускаем асинхронную функцию main
if __name__ == "__main__":
    asyncio.run(main())
