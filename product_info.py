"""
Product Information Display Module

This module provides a unified way to display product information to clients.
It allows customization of what information to display for each product.

Usage:
    from product_info import ProductInfoDisplay

    # Create an instance
    info_display = ProductInfoDisplay(session)

    # Get product information
    result = info_display.get_product_info(
        articles=["01-0023", "KR-91-0840"],
        show_name=True,
        show_prices=True,
        show_stock=True,
        show_expected=False,
        show_certificates=False,
        show_photos=False,
        show_analogs=False,
        show_characteristics=False,
        price_types=[]
    )
"""

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.future import select
from typing import List, Dict, Any, Optional
from models import Product, ProductCharacteristic, CharacteristicClarify, ProductPrice, ProductCertificate, ProductPhoto, ProductAnalog
from search import ProductSearch
import json

class ProductInfoDisplay:
    """
    A class that provides a unified way to display product information to clients.
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize the ProductInfoDisplay with a database session.

        Args:
            session: SQLAlchemy async session for database access
        """
        self.session = session
        self.search = ProductSearch(session)

    async def get_product_info(
        self,
        articles: List[str],
        show_name: bool = True,
        show_prices: bool = True,
        show_stock: bool = True,
        show_expected: bool = False,
        show_certificates: bool = False,
        show_photos: bool = False,
        show_analogs: bool = False,
        show_characteristics: bool = False,
        price_types: List[str] = []
    ) -> Dict[str, Any]:
        """
        Get information about products based on their articles and display preferences.

        Args:
            articles: List of product articles to retrieve information for
            show_name: Whether to include product names (default: True)
            show_prices: Whether to include product prices (default: True)
            show_stock: Whether to include product stock information (default: True)
            show_expected: Whether to include expected deliveries (default: False)
            show_certificates: Whether to include certificates (default: False)
            show_photos: Whether to include photos (default: False)
            show_analogs: Whether to include analogs (default: False)
            show_characteristics: Whether to include characteristics (default: False)
            price_types: List of price types to include (if empty, only retail prices are included)

        Returns:
            A dictionary with product articles as keys and product information as values
        """
        result = {}

        # Get product information from the database
        for article in articles:
            products = await self.search.search_by_article(article)
            if not products:
                # Product not found
                result[article] = {"error": "Product not found"}
                continue

            # Use the first product found (there should be only one with this article)
            product = products[0]

            # Initialize product info dictionary
            product_info = {}

            # Add name if requested
            if show_name:
                product_info["name"] = product.name

            # Add characteristics if requested
            if show_characteristics:
                characteristics = {}
                for char in product.characteristics:
                    char_stmt = select(CharacteristicClarify).where(CharacteristicClarify.id == char.characteristic_id)
                    char_result = await self.session.execute(char_stmt)
                    char_name = char_result.scalars().first()
                    if char_name:
                        characteristics[char_name.characteristic] = char.value
                product_info["characteristics"] = characteristics

            # Add prices if requested
            if show_prices:
                prices = {}
                # Get prices from the database
                price_stmt = select(ProductPrice).where(ProductPrice.product_id == product.id)

                # Filter by price types if specified
                if price_types:
                    price_stmt = price_stmt.where(ProductPrice.price_type.in_(price_types))

                # Get all prices
                price_result = await self.session.execute(price_stmt)
                product_prices = price_result.scalars().all()

                # Add prices to the result
                for price in product_prices:
                    prices[price.price_type] = price.price

                # If no prices were found, add a default retail price
                if not prices:
                    prices["retail"] = "N/A"

                product_info["prices"] = prices

            # Add stock information if requested
            if show_stock:
                # Get stock information from the database
                stock_info = {
                    "total": product.total_stock if product.total_stock is not None else 0,
                    "warehouses": {}  # We don't have warehouse-specific stock in the database
                }
                product_info["stock"] = stock_info

            # Add expected deliveries if requested
            if show_expected:
                # We don't have expected deliveries in the database, so use a placeholder
                product_info["expected"] = "N/A"

            # Add certificates if requested
            if show_certificates:
                certificates = []
                # Get certificates from the database
                for cert in product.certificates:
                    certificates.append({"link": cert.certificate_link})
                product_info["certificates"] = certificates

            # Add photos if requested
            if show_photos:
                photos = []
                # Get photos from the database
                for photo in product.photos:
                    photos.append({"link": photo.photo_link})
                product_info["photos"] = photos

            # Add analogs if requested
            if show_analogs:
                analogs = []
                # Get analogs from the database
                for analog in product.analogs:
                    analogs.append({"article": analog.article})
                product_info["analogs"] = analogs

            # Add product info to result
            result[article] = product_info

        return result


# Example usage
if __name__ == "__main__":
    import asyncio
    from db import AsyncSessionLocal

    async def main():
        session = AsyncSessionLocal()

        try:
            # Create an instance without API token (will use only database information)
            info_display = ProductInfoDisplay(session)

            # Get product information for two articles
            result = await info_display.get_product_info(
                articles=["07-0900"],
                show_name=True,
                show_prices=True,
                show_stock=True,
                show_expected=False,
                show_certificates=False,
                show_photos=False,
                show_analogs=False,
                show_characteristics=True
            )

            # Print the result
            print(json.dumps(result, ensure_ascii=False, indent=2))
        finally:
            # Close the session to release resources
            await session.close()

    # Run the async function
    asyncio.run(main())
