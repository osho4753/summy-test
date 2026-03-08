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
from integrator.tasks import sync_single_product, bulk_save_sync_states

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

    def test_stream_erp_data_skips_invalid_items(self, settings):
        test_data = [
            {"id": "VALID-001", "title": "Good", "price_vat_excl": 100},
            {"id": "BROKEN-001", "title": "Bad", "price_vat_excl": "not_a_num"}, 
            {"id": "VALID-002", "title": "Good 2", "price_vat_excl": 200},
        ]
        
        with tempfile.TemporaryDirectory() as tmpdirname:
            tmp_dir = Path(tmpdirname)
            temp_path = tmp_dir / 'erp_data.json'
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(test_data, f)
            
            with patch.object(settings, 'BASE_DIR', tmp_dir):
                items = list(stream_erp_data())
        
        assert len(items) == 2
        assert items[0].sku == 'VALID-001'
        assert items[1].sku == 'VALID-002'

# =============================================================================
# 3: Celery tasks, API Client & Database Writes
# =============================================================================

@pytest.mark.django_db
class TestCeleryTasksAndDB:

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
        
        result = sync_single_product(payload, prod_hash, is_new=True)
        assert result == {'sku': 'NEW-001', 'hash': 'fake_hash_123'}

    def test_bulk_save_sync_states_creates_and_updates(self):
        results = [
            {'sku': 'SKU-A', 'hash': 'hash_A'},
            {'sku': 'SKU-B', 'hash': 'hash_B'},
            None, 
            {'invalid_key': 'data'} 
        ]
        
        bulk_save_sync_states(results)
        
        assert ProductSyncState.objects.count() == 2
        assert ProductSyncState.objects.get(sku='SKU-A').data_hash == 'hash_A'
        assert ProductSyncState.objects.get(sku='SKU-B').data_hash == 'hash_B'

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
        
        with patch('integrator.tasks.sync_single_product.retry') as mock_retry:
            mock_retry.side_effect = Exception("Retry triggered") 
            
            with pytest.raises(Exception, match="Retry triggered"):
                sync_single_product(payload, prod_hash, is_new=True)
            
            mock_retry.assert_called_once()
            assert mock_retry.call_args.kwargs['countdown'] == 15

    @responses.activate
    def test_sync_single_product_fallback_to_patch(self, settings):
        payload = {'sku': 'EXISTING-001', 'title': 'Test', 'price_vat_incl': '50.00', 'stock_total': 0, 'color': 'N/A'}
        prod_hash = "hash_789"
        
        responses.add(
            responses.POST,
            f"{settings.ESHOP_API_BASE_URL}/",
            json={"error": "already exists"},
            status=409
        )
        
        responses.add(
            responses.PATCH,
            f"{settings.ESHOP_API_BASE_URL}/EXISTING-001/",
            json={"status": "updated"},
            status=200
        )
        
        result = sync_single_product(payload, prod_hash, is_new=True)
        
        assert len(responses.calls) == 2
        assert responses.calls[0].request.method == 'POST'
        assert responses.calls[1].request.method == 'PATCH'
        assert result == {'sku': 'EXISTING-001', 'hash': 'hash_789'}