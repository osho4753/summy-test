import ijson
from pathlib import Path
from django.conf import settings
from .schemas import ProductSchema

def stream_erp_data_in_batches(file_path: str = 'erp_data.json', batch_size: int = 500):
    full_path = Path(settings.BASE_DIR) / file_path
    
    batch = {}
    with open(full_path, 'rb') as f:
        for item in ijson.items(f, 'item'):
            if not item.get('id'):
                continue
            
            product = ProductSchema(**item)
            batch[product.sku] = product.model_dump(mode='json')
            
            if len(batch) >= batch_size:
                yield list(batch.values())
                batch = {}
                
        if batch:
            yield list(batch.values())