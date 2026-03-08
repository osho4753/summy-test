import logging
import ijson
from pathlib import Path
from django.conf import settings
from pydantic import ValidationError
from .schemas import ProductSchema

logger = logging.getLogger(__name__)

def stream_erp_data(file_path: str = 'erp_data.json'):
    full_path = Path(settings.BASE_DIR) / file_path
    with open(full_path, 'rb') as f:
        for item in ijson.items(f, 'item'):
            if item.get('id'):
                try:
                    yield ProductSchema(**item)
                except ValidationError as e:
                    logger.error(f"Error validating SKU {item.get('id')}: {e.errors()}")
                    continue