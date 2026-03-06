"""
Business logic for ERP data transformation.
"""
import json
from pathlib import Path
from django.conf import settings


def parse_and_transform_erp_data(file_path: str = 'erp_data.json') -> list[dict]:
    """
    Parse ERP JSON data and transform it for e-shop API.
    
    Handles edge cases:
    - Duplicate SKUs (keeps the last occurrence)
    - Invalid stock values (e.g., "N/A") → treated as 0
    - Null or negative prices → treated as 0.0
    - Missing or null attributes/color → defaults to "N/A"
    
    Returns:
        List of transformed product dictionaries with keys:
        - sku: Product SKU
        - title: Product title
        - price_vat_incl: Price including 21% VAT, rounded to 2 decimals
        - stock_total: Sum of all stock values
        - color: Product color or "N/A"
    """
    # Resolve file path relative to BASE_DIR
    full_path = Path(settings.BASE_DIR) / file_path
    
    with open(full_path, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)
    
    # Use dict for deduplication (last occurrence wins)
    processed_data = {}
    
    for item in raw_data:
        sku = item.get('id')
        if not sku:
            continue
        
        # Transform and store (overwrites duplicates)
        processed_data[sku] = _transform_product(item)
    
    return list(processed_data.values())


def _transform_product(item: dict) -> dict:
    """
    Transform a single product record from ERP format to e-shop format.
    """
    sku = item.get('id', '')
    title = item.get('title', '')
    
    # Calculate total stock
    stock_total = _calculate_stock_total(item.get('stocks', {}))
    
    # Calculate price with VAT
    price_vat_incl = _calculate_price_with_vat(item.get('price_vat_excl'))
    
    # Extract color
    color = _extract_color(item.get('attributes'))
    
    return {
        'sku': sku,
        'title': title,
        'price_vat_incl': price_vat_incl,
        'stock_total': stock_total,
        'color': color,
    }


def _calculate_stock_total(stocks: dict | None) -> int:
    """
    Sum all stock values. Invalid values (non-numeric, "N/A") are treated as 0.
    """
    if not stocks or not isinstance(stocks, dict):
        return 0
    
    total = 0
    for value in stocks.values():
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            total += int(value)
        # Non-numeric values like "N/A" are ignored (treated as 0)
    
    return total


def _calculate_price_with_vat(price: float | None) -> float:
    """
    Calculate price including 21% VAT.
    Null or negative prices are treated as 0.0.
    """
    if price is None or not isinstance(price, (int, float)) or price < 0:
        base_price = 0.0
    else:
        base_price = float(price)
    
    # Add 21% VAT and round to 2 decimal places
    # Add tiny epsilon to handle banker's rounding edge cases (e.g., 0.5 rounds up)
    return round((base_price * 1.21) + 0.000001, 2)


def _extract_color(attributes: dict | None) -> str:
    """
    Extract color from attributes. Returns "N/A" if not available.
    """
    if not attributes or not isinstance(attributes, dict):
        return "N/A"
    
    color = attributes.get('color')
    if not color:
        return "N/A"
    
    return str(color)
