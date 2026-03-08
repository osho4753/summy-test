from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, computed_field

class ProductSchema(BaseModel):
    sku: str = Field(alias="id")
    title: str
    price_vat_excl: Optional[float] = None
    stocks: Optional[Dict[str, Any]] = None
    attributes: Optional[Dict[str, Any]] = None

    @computed_field
    @property
    def price_vat_incl(self) -> Decimal:
        if self.price_vat_excl is not None and self.price_vat_excl > 0:
            val = Decimal(str(self.price_vat_excl)) * Decimal('1.21')
            return val.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        return Decimal('0.00')

    @computed_field
    @property
    def stock_total(self) -> int:
        if not self.stocks:
            return 0
        return sum(
            int(v) for v in self.stocks.values()
            if isinstance(v, (int, float)) and not isinstance(v, bool)
        )

    @computed_field
    @property
    def color(self) -> str:
        if self.attributes and isinstance(self.attributes, dict):
            return str(self.attributes.get('color', 'N/A'))
        return 'N/A'

    def to_eshop_payload(self) -> dict:
        return {
            "sku": self.sku,
            "title": self.title,
            "price_vat_incl": str(self.price_vat_incl), 
            "stock_total": self.stock_total,
            "color": self.color,
        }