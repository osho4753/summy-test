from typing import Any
from decimal import Decimal, ROUND_HALF_UP
from pydantic import BaseModel, Field, model_validator

class ProductSchema(BaseModel):
    sku: str = Field(alias="id")
    title: str
    price_vat_incl: Decimal = Decimal('0.00')
    stock_total: int = 0
    color: str = "N/A"

    @model_validator(mode='before')
    @classmethod
    def transform_erp_data(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
            
        price_excl = data.get('price_vat_excl')
        if isinstance(price_excl, (int, float)) and price_excl > 0:
            price_incl = (Decimal(str(price_excl)) * Decimal('1.21')).quantize(
                Decimal('0.01'), rounding=ROUND_HALF_UP
            )
            data['price_vat_incl'] = price_incl
        else:
            data['price_vat_incl'] = Decimal('0.00')

        stocks = data.get('stocks')
        stock_total = 0
        if isinstance(stocks, dict):
            for v in stocks.values():
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    stock_total += int(v)
        data['stock_total'] = stock_total

        attributes = data.get('attributes')
        if isinstance(attributes, dict) and attributes.get('color'):
            data['color'] = str(attributes['color'])
        else:
            data['color'] = "N/A"

        return data