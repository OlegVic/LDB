
"""
Consolidated imports for the LDB project.

This file contains all the imports needed across the project, as well as
the main data import functionality from the 1C API.
"""

# Standard library imports
import asyncio
import time
import json
import os
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from io import StringIO

# Third-party imports
import pandas as pd
import requests
import aiohttp
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session, sessionmaker, aliased
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.future import select
from sqlalchemy import update, func, or_, and_
from sqlalchemy.dialects.postgresql import TSVECTOR

# Local imports
from models import (
    # Base models
    Base,
    # Main models
    Product,
    ClassClarify,
    CharacteristicClarify,
    # Relationship models
    ProductCharacteristic,
    ProductAnalog,
    ProductBarcode,
    ProductCertificate,
    ProductInstruction,
    ProductPhoto,
    ProductPrice,
)
from api1C import ApiClient
from db import AsyncSessionLocal, load_dotenv
import os

# Загрузка переменных окружения из файла .env
load_dotenv()

# Получение строки подключения к базе данных и токена из переменных окружения
DATABASE_URL = os.getenv("DATABASE_URL")
TOKEN = os.getenv("API_TOKEN")

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def process_products(products, session, product_attributes=None, analogs_data=None, barcodes_data=None, 
                      certificates_data=None, photos_data=None, instructions_data=None, prices_data=None, stock_data=None):
    for prod in products:
        article = prod.get('article')
        name = prod.get('name')
        class_rusname = prod.get('sdsclass', {}).get('rusname')

        unit = prod.get('unit')
        unitpak = prod.get('unitpak')
        comunit = prod.get('comunit')
        comunitpak = prod.get('comunitpak')

        unit_list = ['бухта', 'метр', 'м.','см.', 'мм.', 'м', 'см', 'мм']
        # бухта >> метр
        # бухта >>
        # метр >> бухта
        # метр >>
        # метр >> шт
        # шт >> м
        # м >>
        # шт >> метр

        length_unit_found = False
        if unit and unit in unit_list:
            length_unit_found = True
        elif comunit and comunit in unit_list:
            length_unit_found = True

        # Фильтруем товары с пустым или None class_rusname
        if not class_rusname or not class_rusname.strip():
            print(f"Пропущен товар {article} — нет class_rusname")
            continue

        # 0. Авто-добавление класса (classes_clarify)
        class_obj = None
        if class_rusname:
            stmt = select(ClassClarify).where(ClassClarify.class_rusname == class_rusname)
            result = await session.execute(stmt)
            class_obj = result.scalar_one_or_none()
            if not class_obj:
                class_obj = ClassClarify(class_rusname=class_rusname)
                session.add(class_obj)
                await session.flush()

        # 1. Добавим товар
        stmt = select(Product).where(Product.article == article)
        result = await session.execute(stmt)
        product = result.scalar_one_or_none()
        if not product:
            product = Product(article=article, name=name, class_id=class_obj.id)
            session.add(product)
            await session.flush()

        # 2. Добавляем характеристики из предварительно загруженных атрибутов
        if product_attributes and article in product_attributes:
            attributes = product_attributes[article]

            if attributes:
                print(f"{article} - attributes:", len(attributes))

            for char in attributes:
                char_name = char.get('characteristic')
                if not char_name:
                    continue
                value1 = char.get('value1')
                value2 = char.get('value2')
                unit = char.get('unit')
                value = " ".join(str(x) for x in [value1, value2, unit] if x)

                # 2.1 Авто-добавление характеристики в справочник (characteristics_clarify)
                stmt = select(CharacteristicClarify).where(CharacteristicClarify.characteristic == char_name)
                result = await session.execute(stmt)
                char_obj = result.scalar_one_or_none()
                if not char_obj:
                    char_obj = CharacteristicClarify(
                        characteristic=char_name,
                        characteristic_good=char_name,
                        priority=1,
                    )
                    session.add(char_obj)
                    await session.flush()

                # 2.2 Добавляем характеристику товара (product_characteristics)
                stmt = select(ProductCharacteristic).where(
                    ProductCharacteristic.product_id == product.id,
                    ProductCharacteristic.characteristic_id == char_obj.id
                )
                result = await session.execute(stmt)
                pc = result.scalar_one_or_none()
                if not pc:
                    pc = ProductCharacteristic(
                        product_id=product.id,
                        characteristic_id=char_obj.id,
                        value=value
                    )
                    session.add(pc)

        # Добавляем специальную характеристику "Длина" для товаров с единицами измерения из unit_list
        # Это необходимо для правильного хранения и поиска товаров с единицами измерения длины
        # Значение хранится в extra_value в формате ";X метр;X м.;X м;" для метров или ";X unit;" для других единиц
        # Такой формат позволяет искать товары по запросам вида ";бухта;" или ";X метр;"
        if length_unit_found:
            # Создаем или получаем характеристику "Длина"
            stmt = select(CharacteristicClarify).where(CharacteristicClarify.characteristic == "Длина")
            result = await session.execute(stmt)
            length_char_obj = result.scalar_one_or_none()
            if not length_char_obj:
                length_char_obj = CharacteristicClarify(
                    characteristic="Длина",
                    characteristic_good="Длина",
                    priority=1,
                )
                session.add(length_char_obj)
                await session.flush()

            # Форматируем значение в требуемом формате
            formatted_extra_value = None
            if unit in ["метр", "м", "м."]:
                if comunit in ["бухта"]:
                    formatted_extra_value = f";{comunit};"
            elif unit in ["бухта"]:
                formatted_value = f"{unit}"
                if comunit in ["метр", "м.", "м"]:
                    formatted_extra_value = f";{comunitpak} метр;{comunitpak} м.;{comunitpak} м;{comunitpak*100} см.;{comunitpak*100} см;{comunitpak*1000} мм.;{comunitpak*1000} мм;"
                elif comunit in ["см", "см."]:
                    formatted_extra_value = f";{comunitpak*100} метр;{comunitpak*100} м.;{comunitpak*100} м;{comunitpak} см.;{comunitpak} см;{comunitpak/10} мм.;{comunitpak/10} мм;"

            # Проверяем, существует ли уже такая характеристика для товара
            stmt = select(ProductCharacteristic).where(
                ProductCharacteristic.product_id == product.id,
                ProductCharacteristic.characteristic_id == length_char_obj.id
            )
            result = await session.execute(stmt)
            length_pc = result.scalar_one_or_none()

            if not length_pc:
                # Создаем новую характеристику
                length_pc = ProductCharacteristic(
                    product_id=product.id,
                    characteristic_id=length_char_obj.id,
                    value=f"{unitpak} {unit}",
                    extra_value=formatted_extra_value if formatted_extra_value and formatted_extra_value.strip() != '' else None
                )
                session.add(length_pc)
            else:
                # Обновляем существующую характеристику
                length_pc.value = f"{unitpak} {unit}"
                length_pc.extra_value = formatted_extra_value

        # Обработка аналогов
        if analogs_data and article in analogs_data:
            # Сначала удаляем существующие аналоги для этого товара
            stmt = select(ProductAnalog).where(ProductAnalog.product_id == product.id)
            result = await session.execute(stmt)
            existing_analogs = result.scalars().all()
            for existing_analog in existing_analogs:
                await session.delete(existing_analog)

            # Добавляем новые аналоги
            for analog_article in analogs_data[article]:
                analog = ProductAnalog(
                    product_id=product.id,
                    article=analog_article
                )
                session.add(analog)

        # Обработка штрихкодов
        if barcodes_data and article in barcodes_data:
            # Сначала удаляем существующие штрихкоды для этого товара
            stmt = select(ProductBarcode).where(ProductBarcode.product_id == product.id)
            result = await session.execute(stmt)
            existing_barcodes = result.scalars().all()
            for existing_barcode in existing_barcodes:
                await session.delete(existing_barcode)

            # Добавляем новые штрихкоды
            for barcode in barcodes_data[article]:
                barcode_obj = ProductBarcode(
                    product_id=product.id,
                    barcode=barcode
                )
                session.add(barcode_obj)

        # Обработка сертификатов временно отключена из-за технических проблем в API
        # if certificates_data and article in certificates_data:
        #     # Сначала удаляем существующие сертификаты для этого товара
        #     stmt = select(ProductCertificate).where(ProductCertificate.product_id == product.id)
        #     result = await session.execute(stmt)
        #     existing_certificates = result.scalars().all()
        #     for existing_certificate in existing_certificates:
        #         await session.delete(existing_certificate)
        #
        #     # Добавляем новые сертификаты
        #     for cert_link in certificates_data[article]:
        #         certificate = ProductCertificate(
        #             product_id=product.id,
        #             certificate_link=cert_link
        #         )
        #         session.add(certificate)

        # Обработка фотографий
        if photos_data and article in photos_data:
            # Сначала удаляем существующие фотографии для этого товара
            stmt = select(ProductPhoto).where(ProductPhoto.product_id == product.id)
            result = await session.execute(stmt)
            existing_photos = result.scalars().all()
            for existing_photo in existing_photos:
                await session.delete(existing_photo)

            # Добавляем новые фотографии
            for photo_link in photos_data[article]:
                photo = ProductPhoto(
                    product_id=product.id,
                    photo_link=photo_link
                )
                session.add(photo)

        # Обработка инструкций временно отключена из-за технических проблем в API
        # if instructions_data and article in instructions_data:
        #     # Сначала удаляем существующие инструкции для этого товара
        #     stmt = select(ProductInstruction).where(ProductInstruction.product_id == product.id)
        #     result = await session.execute(stmt)
        #     existing_instructions = result.scalars().all()
        #     for existing_instruction in existing_instructions:
        #         await session.delete(existing_instruction)
        #
        #     # Добавляем новые инструкции
        #     for instr_link in instructions_data[article]:
        #         instruction = ProductInstruction(
        #             product_id=product.id,
        #             instruction_link=instr_link
        #         )
        #         session.add(instruction)

        # Обработка цен
        if prices_data and article in prices_data:
            # Сначала удаляем существующие цены для этого товара
            stmt = select(ProductPrice).where(ProductPrice.product_id == product.id)
            result = await session.execute(stmt)
            existing_prices = result.scalars().all()
            for existing_price in existing_prices:
                await session.delete(existing_price)

            # Добавляем новые цены
            for price_data in prices_data[article]:
                price = ProductPrice(
                    product_id=product.id,
                    price_type=price_data['price_type'],
                    price=price_data['price']
                )
                session.add(price)

        # Обновление общего остатка
        if stock_data and article in stock_data:
            total_stock = stock_data[article]['total'] - stock_data[article]['reserve']
            product.total_stock = max(0, total_stock)  # Убедимся, что остаток не отрицательный

    await session.commit()

async def main():
    # Start timing the entire process
    total_start_time = time.time()
    start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"=== Starting import process at {start_datetime} ===")

    # Initialize statistics dictionary
    stats = {
        "attributes": 0,
        "analogs": 0,
        "barcodes": 0,
        "certificates": 0,
        "photos": 0,
        "instructions": 0,
        "prices": 0,
        "stock_items": 0,
        "products": 0,
        "timings": {}
    }

    client = ApiClient(token=TOKEN)

    # Сначала загружаем все атрибуты продуктов
    print("\n[1/9] Fetching all product attributes...")
    attr_start_time = time.time()
    product_attributes = {}
    attr_limit = 100000
    attr_offset = 0
    total_attr_count = 0

    while True:
        attr_data = await client.get_etim_product_attributes(limit=attr_limit, offset=attr_offset)
        attr_results = attr_data['result']['results']
        if not attr_results:
            break

        total_attr_count += len(attr_results)

        # Группируем атрибуты по артикулу продукта
        for item in attr_results:
            article = item.get('article')
            if not article:
                continue

            if article not in product_attributes:
                product_attributes[article] = []

            # Извлекаем все атрибуты из текущего элемента
            attributes = item.get('attribute', [])
            etimclasskey = item.get('etimclasskey', '')
            version = item.get('version', '')

            # Добавляем информацию о etimclasskey и version к каждому атрибуту
            for attr in attributes:
                attr['etimclasskey'] = etimclasskey
                attr['version'] = version

                # Проверяем, есть ли уже такая характеристика для этого артикула
                char_name = attr.get('characteristic')
                if char_name and not any(existing.get('characteristic') == char_name for existing in product_attributes[article]):
                    product_attributes[article].append(attr)
                    stats["attributes"] += 1

        print(f"  Progress: Loaded {total_attr_count} product attribute sets (offset={attr_offset})")
        if len(attr_results) < attr_limit:
            break
        attr_offset += attr_limit

    attr_end_time = time.time()
    attr_elapsed = attr_end_time - attr_start_time
    stats["timings"]["attributes"] = attr_elapsed

    print(f"  Completed: Total products with attributes: {len(product_attributes)}")
    print(f"  Time taken: {attr_elapsed:.2f} seconds")

    # Загружаем дополнительные данные для товаров
    print("\n=== Fetching additional product data ===")
    analogs_data = {}
    barcodes_data = {}
    certificates_data = {}
    photos_data = {}
    instructions_data = {}
    prices_data = {}
    stock_data = {}

    # Загружаем аналоги
    print("\n[2/9] Fetching analogs...")
    analogs_start_time = time.time()
    analogs_limit = 100000
    analogs_offset = 0
    total_analogs_count = 0

    while True:
        analogs_response = await client.get_analogs(limit=analogs_limit, offset=analogs_offset)
        analogs_results = analogs_response['result']['results']
        if not analogs_results:
            break

        total_analogs_count += len(analogs_results)

        for analog in analogs_results:
            article = analog.get('article')
            if not article:
                continue

            if article not in analogs_data:
                analogs_data[article] = []

            # Обрабатываем атрибуты аналогов в новом формате
            attributes = analog.get('attribute', [])
            if not isinstance(attributes, list):
                attributes = [attributes]

            for attr in attributes:
                analog_article = attr.get('article')
                if analog_article and attr.get('type') == 'Аналоги':
                    analogs_data[article].append(analog_article)
                    stats["analogs"] += 1

        print(f"  Progress: Loaded {total_analogs_count} analogs (offset={analogs_offset})")
        if len(analogs_results) < analogs_limit:
            break
        analogs_offset += analogs_limit

    analogs_end_time = time.time()
    analogs_elapsed = analogs_end_time - analogs_start_time
    stats["timings"]["analogs"] = analogs_elapsed

    print(f"  Completed: Total products with analogs: {len(analogs_data)}")
    print(f"  Time taken: {analogs_elapsed:.2f} seconds")

    # Загружаем штрихкоды
    print("\n[3/9] Fetching barcodes...")
    barcodes_start_time = time.time()
    barcodes_limit = 100000
    barcodes_offset = 0
    total_barcodes_count = 0

    while True:
        barcodes_response = await client.get_barcodes(limit=barcodes_limit, offset=barcodes_offset)
        barcodes_results = barcodes_response['result']['results']
        if not barcodes_results:
            break

        total_barcodes_count += len(barcodes_results)

        for barcode_item in barcodes_results:
            article = barcode_item.get('article')
            if not article:
                continue

            if article not in barcodes_data:
                barcodes_data[article] = []

            # Обрабатываем атрибуты штрихкодов в новом формате
            attribute = barcode_item.get('attribute', {})
            barcode = attribute.get('barcode')
            if barcode:
                barcodes_data[article].append(barcode)
                stats["barcodes"] += 1

        print(f"  Progress: Loaded {total_barcodes_count} barcodes (offset={barcodes_offset})")
        if len(barcodes_results) < barcodes_limit:
            break
        barcodes_offset += barcodes_limit

    barcodes_end_time = time.time()
    barcodes_elapsed = barcodes_end_time - barcodes_start_time
    stats["timings"]["barcodes"] = barcodes_elapsed

    print(f"  Completed: Total products with barcodes: {len(barcodes_data)}")
    print(f"  Time taken: {barcodes_elapsed:.2f} seconds")

    # Временно отключаем загрузку сертификатов из-за технических проблем в API
    print("\n[4/9] Skipping certificates due to API technical issues...")
    certificates_start_time = time.time()
    certificates_data = {}
    stats["certificates"] = 0

    certificates_end_time = time.time()
    certificates_elapsed = certificates_end_time - certificates_start_time
    stats["timings"]["certificates"] = certificates_elapsed

    print(f"  Certificates loading skipped due to API technical issues")
    print(f"  Time taken: {certificates_elapsed:.2f} seconds")

    # Загружаем фотографии
    print("\n[5/9] Fetching photos...")
    photos_start_time = time.time()
    photos_limit = 100000
    photos_offset = 0
    total_photos_count = 0

    while True:
        photos_response = await client.get_photos(limit=photos_limit, offset=photos_offset)
        photos_results = photos_response['result']['results']
        if not photos_results:
            break

        total_photos_count += len(photos_results)

        for photo_item in photos_results:
            article = photo_item.get('article')
            if not article:
                continue

            if article not in photos_data:
                photos_data[article] = []

            photo_link = photo_item.get('filelink')
            if photo_link:
                photos_data[article].append(photo_link)
                stats["photos"] += 1

        print(f"  Progress: Loaded {total_photos_count} photos (offset={photos_offset})")
        if len(photos_results) < photos_limit:
            break
        photos_offset += photos_limit

    photos_end_time = time.time()
    photos_elapsed = photos_end_time - photos_start_time
    stats["timings"]["photos"] = photos_elapsed

    print(f"  Completed: Total products with photos: {len(photos_data)}")
    print(f"  Time taken: {photos_elapsed:.2f} seconds")

    # Временно отключаем загрузку инструкций из-за технических проблем в API
    print("\n[6/9] Skipping instructions due to API technical issues...")
    instructions_start_time = time.time()
    instructions_data = {}
    stats["instructions"] = 0

    instructions_end_time = time.time()
    instructions_elapsed = instructions_end_time - instructions_start_time
    stats["timings"]["instructions"] = instructions_elapsed

    print(f"  Instructions loading skipped due to API technical issues")
    print(f"  Time taken: {instructions_elapsed:.2f} seconds")

    # Загружаем цены с использованием limit и offset
    print("\n[7/9] Fetching prices...")
    prices_start_time = time.time()
    prices_data = {}
    prices_limit = 100000
    prices_offset = 0
    total_prices_count = 0

    while True:
        try:
            prices_response = await client.get_price_list(limit=prices_limit, offset=prices_offset)
            prices_results = prices_response['result']['results']
            if not prices_results:
                break

            batch_count = len(prices_results)
            print(f"  Progress: Received {batch_count} products with price data (offset={prices_offset})")

            for product in prices_results:
                article = product.get('article')
                if not article:
                    continue

                if article not in prices_data:
                    prices_data[article] = []

                # Обрабатываем атрибуты цен в новом формате
                attributes = product.get('attribute', [])
                for attr in attributes:
                    price_type = attr.get('ratename')
                    price = attr.get('value')

                    if price_type and price is not None:
                        prices_data[article].append({
                            'price_type': price_type,
                            'price': price
                        })
                        stats["prices"] += 1
                        total_prices_count += 1

            if len(prices_results) < prices_limit:
                break
            prices_offset += prices_limit

        except Exception as e:
            print(f"  Error fetching prices at offset {prices_offset}: {str(e)}")
            print("  Will continue with already fetched price data")
            break

    print(f"  Processed {total_prices_count} price entries for {len(prices_data)} products")

    prices_end_time = time.time()
    prices_elapsed = prices_end_time - prices_start_time
    stats["timings"]["prices"] = prices_elapsed

    print(f"  Completed: Total products with prices: {len(prices_data)}")
    print(f"  Time taken: {prices_elapsed:.2f} seconds")

    # Загружаем остатки на складах
    print("\n[8/9] Fetching warehouse stock...")
    stock_start_time = time.time()
    stock_limit = 100000
    stock_offset = 0
    total_stock_count = 0

    while True:
        stock_response = await client.get_warehouse_stock(limit=stock_limit, offset=stock_offset)
        stock_results = stock_response['result']['results']
        if not stock_results:
            break

        total_stock_count += len(stock_results)

        for stock_item in stock_results:
            article = stock_item.get('article')
            if not article:
                continue

            if article not in stock_data:
                stock_data[article] = {
                    'total': 0,
                    'reserve': 0
                }

            # Обрабатываем атрибуты остатков в новом формате
            attributes = stock_item.get('attribute', [])
            if not isinstance(attributes, list):
                attributes = [attributes]

            for attr in attributes:
                count = attr.get('count', 0)
                reserv = attr.get('reserv', 0)

                stock_data[article]['total'] += count
                stock_data[article]['reserve'] += reserv
                stats["stock_items"] += 1

        print(f"  Progress: Loaded {total_stock_count} stock items (offset={stock_offset})")
        if len(stock_results) < stock_limit:
            break
        stock_offset += stock_limit

    stock_end_time = time.time()
    stock_elapsed = stock_end_time - stock_start_time
    stats["timings"]["stock"] = stock_elapsed

    print(f"  Completed: Total products with stock data: {len(stock_data)}")
    print(f"  Time taken: {stock_elapsed:.2f} seconds")

    # Теперь загружаем продукты и используем предварительно загруженные данные
    print("\n[9/9] Processing products...")
    products_start_time = time.time()
    limit = 1000
    offset = 0
    total_products_count = 0

    while True:
        data = await client.get_full_products(limit=limit, offset=offset)
        results = data['result']['results']
        if not results:
            break

        total_products_count += len(results)

        async with AsyncSessionLocal() as session:
            await process_products(
                results, 
                session, 
                product_attributes,
                analogs_data,
                barcodes_data,
                certificates_data,
                photos_data,
                instructions_data,
                prices_data,
                stock_data
            )

        stats["products"] += len(results)
        print(f"  Progress: Processed {total_products_count} products (offset={offset})")

        if len(results) < limit:
            break
        offset += limit

    products_end_time = time.time()
    products_elapsed = products_end_time - products_start_time
    stats["timings"]["products"] = products_elapsed

    print(f"  Completed: Total products processed: {total_products_count}")
    print(f"  Time taken: {products_elapsed:.2f} seconds")

    # Закрываем клиент API
    await client.close()

    # Выводим итоговую статистику
    total_end_time = time.time()
    total_elapsed = total_end_time - total_start_time
    end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("\n" + "="*50)
    print(f"IMPORT PROCESS COMPLETED AT {end_datetime}")
    print("="*50)
    print(f"Total time: {total_elapsed:.2f} seconds ({total_elapsed/60:.2f} minutes)")
    print("\nItems processed:")
    print(f"  Products: {stats['products']}")
    print(f"  Attributes: {stats['attributes']}")
    print(f"  Analogs: {stats['analogs']}")
    print(f"  Barcodes: {stats['barcodes']}")
    print(f"  Certificates: {stats['certificates']}")
    print(f"  Photos: {stats['photos']}")
    print(f"  Instructions: {stats['instructions']}")
    print(f"  Prices: {stats['prices']}")
    print(f"  Stock items: {stats['stock_items']}")

    print("\nTime breakdown:")
    for operation, elapsed in stats["timings"].items():
        percentage = (elapsed / total_elapsed) * 100
        print(f"  {operation}: {elapsed:.2f} seconds ({percentage:.1f}%)")

    print("="*50)

if __name__ == "__main__":
    asyncio.run(main())
