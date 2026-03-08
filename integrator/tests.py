import json
import tempfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest
import responses

from integrator.models import ProductSyncState
from integrator.schemas import ProductSchema
from integrator.services import stream_erp_data
from integrator.tasks import sync_erp_to_eshop, sync_single_product, init_worker_session


# =============================================================================
# 1: Validation tests (Pydantic)
# =============================================================================

class TestProductSchemaValidation:

    def test_valid_product_transformation(self):
        raw_data = {
            "id": "SKU-001",
            "title": "Kávovar",
            "price_vat_excl": 100.0,
            "stocks": {"praha": 5, "brno": 3},
            "attributes": {"color": "stříbrná"}
        }
        product = ProductSchema(**raw_data)
        
        assert product.sku == "SKU-001"
        assert product.price_vat_incl == Decimal('121.00')
        assert product.stock_total == 8                    
        assert product.color == "stříbrná"
        
        payload = product.to_eshop_payload()
        assert payload['price_vat_incl'] == '121.00' 

    def test_edge_cases_and_nulls(self):
        raw_data = {
            "id": "SKU-002",
            "title": "Chyba",
            "price_vat_excl": -150.0,       
            "stocks": {"praha": "N/A"},     
            "attributes": None            
        }
        product = ProductSchema(**raw_data)
        
        assert product.price_vat_incl == Decimal('0.00')
        assert product.stock_total == 0
        assert product.color == "N/A"


# =============================================================================
# 2: Testing streaming data processing from ERP (ijson + Pydantic) 
# =============================================================================

@pytest.mark.django_db
class TestStreamErpData:

    def test_stream_erp_data_yields_schemas(self, settings):
        test_data = [
            {"id": "TEST-001", "title": "First", "price_vat_excl": 100},
            {"id": "TEST-002", "title": "Second", "price_vat_excl": 200},
        ]
        
        with tempfile.TemporaryDirectory() as tmpdirname:
            tmp_dir = Path(tmpdirname)
            temp_path = tmp_dir / 'erp_data.json'
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(test_data, f)
            
            with patch.object(settings, 'BASE_DIR', tmp_dir):
                items = list(stream_erp_data())
        
        assert len(items) == 2
        assert isinstance(items[0], ProductSchema)
        assert items[0].sku == 'TEST-001'
        assert items[1].price_vat_incl == Decimal('242.00')


# =============================================================================
# 3: Celery tasks orchestrator
# =============================================================================

@pytest.mark.django_db
class TestCeleryTasks:
    """Тесты оркестратора и атомарных задач."""

    def setup_method(self):
        init_worker_session()

    @patch('integrator.tasks.sync_single_product.delay')
    def test_sync_erp_to_eshop_orchestrator(self, mock_delay, settings):
        test_data = [
            {"id": "NEW-001", "title": "New", "price_vat_excl": 100},
            {"id": "SKIP-001", "title": "Old", "price_vat_excl": 100},
        ]
        
        with tempfile.TemporaryDirectory() as tmpdirname:
            tmp_dir = Path(tmpdirname)
            temp_path = tmp_dir / 'erp_data.json'
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(test_data, f)
            
            with patch.object(settings, 'BASE_DIR', tmp_dir):
                schema = ProductSchema(**test_data[1])
                payload = schema.to_eshop_payload()
                import hashlib
                import json as base_json
                hash_val = hashlib.sha256(base_json.dumps(payload, sort_keys=True).encode()).hexdigest()
                ProductSyncState.objects.create(sku="SKIP-001", data_hash=hash_val)

                result = sync_erp_to_eshop()
        
        assert result['dispatched_tasks'] == 1
        assert mock_delay.call_count == 1
        
        called_payload = mock_delay.call_args[0][0]
        assert called_payload['sku'] == 'NEW-001'

    @responses.activate
    def test_sync_single_product_success(self, settings):
        payload = {'sku': 'NEW-001', 'title': 'P1', 'price_vat_incl': '121.00', 'stock_total': 10, 'color': 'red'}
        prod_hash = "fake_hash_123"
        
        responses.add(
            responses.POST,
            f"{settings.ESHOP_API_BASE_URL}/",
            json={"status": "created"},
            status=201
        )
        
        sync_single_product(payload, prod_hash, is_new=True)
        
        assert ProductSyncState.objects.filter(sku='NEW-001', data_hash=prod_hash).exists()

    @responses.activate
    def test_sync_single_product_rate_limit_retry(self, settings):
        payload = {'sku': 'RATE-001', 'title': 'Test', 'price_vat_incl': '50.00', 'stock_total': 0, 'color': 'N/A'}
        prod_hash = "fake_hash_456"
        
        responses.add(
            responses.POST,
            f"{settings.ESHOP_API_BASE_URL}/",
            json={"error": "rate limit exceeded"},
            status=429,
            headers={"Retry-After": "15"}
        )
        
        with patch.object(sync_single_product, 'retry', side_effect=Exception("Rate Limit 429")) as mock_retry:
            with pytest.raises(Exception, match="Rate Limit 429"):
                sync_single_product(payload, prod_hash, is_new=True)
            
            mock_retry.assert_called_once()
            call_kwargs = mock_retry.call_args[1]
            assert call_kwargs['countdown'] == 15