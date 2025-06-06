"""
Combined test file for the LDB project.

This file contains tests for:
1. API client (api1C.py)
2. Product info display (product_info.py)
"""

import asyncio
import json
from api1C import ApiClient
from db import AsyncSessionLocal
from product_info import ProductInfoDisplay

# API Token for testing
TOKEN = "e28b426f3a703733b9aab5af251c1026"

# Test functions for ProductInfoDisplay
async def test_product_info():
    print("\n=== Testing ProductInfoDisplay ===")
    # Create a database session using the engine directly
    async with AsyncSessionLocal() as session:
        # Create a ProductInfoDisplay instance without API token
        # (will use only database information)
        print("Testing ProductInfoDisplay without API token...")
        info_display = ProductInfoDisplay(session)

        # Define test articles
        test_articles = ["01-0023", "KR-91-0840"]

        # Test 1: Basic information (name, prices, stock)
        print("\nTest 1: Basic information (name, prices, stock)")
        result = await info_display.get_product_info(
            articles=test_articles,
            show_name=True,
            show_prices=True,
            show_stock=True,
            show_expected=False,
            show_certificates=False,
            show_photos=False,
            show_analogs=False,
            show_characteristics=False
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))

        # Test 2: Include characteristics
        print("\nTest 2: Include characteristics")
        result = await info_display.get_product_info(
            articles=test_articles,
            show_name=True,
            show_prices=True,
            show_stock=True,
            show_expected=False,
            show_certificates=False,
            show_photos=False,
            show_analogs=False,
            show_characteristics=True
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))

        # Test 3: All information
        print("\nTest 3: All information")
        result = await info_display.get_product_info(
            articles=test_articles,
            show_name=True,
            show_prices=True,
            show_stock=True,
            show_expected=True,
            show_certificates=True,
            show_photos=True,
            show_analogs=True,
            show_characteristics=True
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))

        # Test 4: Only name
        print("\nTest 4: Only name")
        result = await info_display.get_product_info(
            articles=test_articles,
            show_name=True,
            show_prices=False,
            show_stock=False,
            show_expected=False,
            show_certificates=False,
            show_photos=False,
            show_analogs=False,
            show_characteristics=False
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))

        # Test 5: Non-existent article
        print("\nTest 5: Non-existent article")
        result = await info_display.get_product_info(
            articles=["non-existent-article"],
            show_name=True,
            show_prices=True,
            show_stock=True
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))

async def main():
    # Run ProductInfoDisplay tests
    await test_product_info()

if __name__ == "__main__":
    asyncio.run(main())
