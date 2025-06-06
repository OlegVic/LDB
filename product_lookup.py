"""
Product Lookup Script

This script allows users to look up product information by entering a product article.
It retrieves and displays all available information about the product in a user-friendly format.

Usage:
    python product_lookup.py
    
    Then follow the prompts to enter a product article.
"""

import json
from db import AsyncSessionLocal
from product_info import ProductInfoDisplay

def format_product_info(product_data):
    """
    Format product information for display.
    
    Args:
        product_data: Dictionary containing product information
        
    Returns:
        Formatted string with product information
    """
    if "error" in product_data:
        return f"Error: {product_data['error']}"
    
    output = []
    
    # Add product name
    if "name" in product_data:
        output.append(f"Product Name: {product_data['name']}")
        output.append("-" * 50)
    
    # Add characteristics
    if "characteristics" in product_data and product_data["characteristics"]:
        output.append("Characteristics:")
        for char_name, char_value in product_data["characteristics"].items():
            output.append(f"  {char_name}: {char_value}")
        output.append("-" * 50)
    
    # Add prices
    if "prices" in product_data and product_data["prices"]:
        output.append("Prices:")
        for price_type, price in product_data["prices"].items():
            if not isinstance(price, dict) or "error" not in price:
                output.append(f"  {price_type}: {price}")
        output.append("-" * 50)
    
    # Add stock information
    if "stock" in product_data:
        if isinstance(product_data["stock"], dict):
            output.append("Stock:")
            if "total" in product_data["stock"]:
                output.append(f"  Total: {product_data['stock']['total']}")
            
            if "warehouses" in product_data["stock"] and product_data["stock"]["warehouses"]:
                output.append("  Warehouses:")
                for warehouse_id, warehouse_info in product_data["stock"]["warehouses"].items():
                    output.append(f"    {warehouse_info['name']}: {warehouse_info['quantity']}")
        else:
            output.append(f"Stock: {product_data['stock']}")
        output.append("-" * 50)
    
    # Add expected deliveries
    if "expected" in product_data and product_data["expected"] != "N/A":
        output.append(f"Expected Deliveries: {product_data['expected']}")
        output.append("-" * 50)
    
    # Add certificates
    if "certificates" in product_data and product_data["certificates"]:
        output.append("Certificates:")
        if isinstance(product_data["certificates"], list):
            for cert in product_data["certificates"]:
                if isinstance(cert, dict) and "link" in cert:
                    output.append(f"  {cert['link']}")
                else:
                    output.append(f"  {cert}")
        else:
            output.append(f"  {product_data['certificates']}")
        output.append("-" * 50)
    
    # Add photos
    if "photos" in product_data and product_data["photos"]:
        output.append("Photos:")
        if isinstance(product_data["photos"], list):
            for photo in product_data["photos"]:
                if isinstance(photo, dict) and "link" in photo:
                    output.append(f"  {photo['link']}")
                else:
                    output.append(f"  {photo}")
        else:
            output.append(f"  {product_data['photos']}")
        output.append("-" * 50)
    
    # Add analogs
    if "analogs" in product_data and product_data["analogs"]:
        output.append("Analogs:")
        if isinstance(product_data["analogs"], list):
            for analog in product_data["analogs"]:
                if isinstance(analog, dict) and "article" in analog:
                    output.append(f"  {analog['article']}")
                else:
                    output.append(f"  {analog}")
        else:
            output.append(f"  {product_data['analogs']}")
        output.append("-" * 50)
    
    return "\n".join(output)

def main():
    """
    Main function to run the product lookup script.
    """
    print("Product Lookup Tool")
    print("==================")
    print("Enter a product article to look up information.")
    print("Type 'exit' to quit.")
    print()
    
    # Create database session
    session = AsyncSessionLocal()
    
    # Create ProductInfoDisplay instance
    info_display = ProductInfoDisplay(session)
    
    while True:
        # Get product article from user
        article = input("Enter product article: ").strip()
        
        # Exit if user types 'exit'
        if article.lower() == 'exit':
            break
        
        if not article:
            print("Please enter a valid article.")
            continue
        
        # Get product information
        result = info_display.get_product_info(
            articles=[article],
            show_name=True,
            show_prices=True,
            show_stock=True,
            show_expected=True,
            show_certificates=True,
            show_photos=True,
            show_analogs=True,
            show_characteristics=True
        )
        
        # Display product information
        if article in result:
            print("\nProduct Information:")
            print("===================")
            print(format_product_info(result[article]))
        else:
            print(f"\nNo information found for article: {article}")
        
        print("\n")
    
    # Close session
    session.close()
    print("Thank you for using Product Lookup Tool. Goodbye!")

if __name__ == "__main__":
    main()