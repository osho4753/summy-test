"""
Business logic for ERP data transformation.
"""
import json
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from django.conf import settings


def parse_and_transform_erp_data(file_path: str = 'erp_data.json') -> list[dict]:
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


def _calculate_price_with_vat(price: float | None) -> Decimal:
    """
    Calculate price including 21% VAT.
    Null or negative prices are treated as Decimal('0.00').
    
    Returns Decimal to preserve precision for financial calculations.
    Conversion to float/string should only happen at system boundaries (e.g., JSON serialization).
    """
    if price is None or not isinstance(price, (int, float)) or price < 0:
        return Decimal('0.00')
    
    # Use Decimal for precise financial calculations
    base_price = Decimal(str(price))
    vat_rate = Decimal('1.21')
    
    # Calculate price with VAT and round to 2 decimal places using ROUND_HALF_UP
    price_with_vat = (base_price * vat_rate).quantize(
        Decimal('0.01'), rounding=ROUND_HALF_UP
    )
    
    return price_with_vat


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
