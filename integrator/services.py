import ijson
from pathlib import Path
from django.conf import settings
from .schemas import ProductSchema

def stream_erp_data(file_path: str = 'erp_data.json'):
    full_path = Path(settings.BASE_DIR) / file_path
    with open(full_path, 'rb') as f:
        for item in ijson.items(f, 'item'):
            if item.get('id'):
                yield ProductSchema(**item)