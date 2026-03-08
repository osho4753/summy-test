import json
import tempfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest
import responses

from integrator.models import ProductSyncState
from integrator.schemas import ProductSchema
from integrator.services import stream_erp_data_in_batches
from integrator.tasks import sync_erp_to_eshop, process_sync_batch, init_worker_session

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
        assert product.price_vat_incl == Decimal('121.00')  # 100 * 1.21
        assert product.stock_total == 8                     # 5 + 3
        assert product.color == "stříbrná"

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
# 2: batches generator tests
# =============================================================================

@pytest.mark.django_db
class TestStreamErpData:
    def test_stream_erp_data_in_batches(self, settings):
        test_data = [
            {"id": "TEST-001", "title": "First", "price_vat_excl": 100},
            {"id": "TEST-002", "title": "Second", "price_vat_excl": 200},
            {"id": "TEST-003", "title": "Third", "price_vat_excl": 300},
        ]
        
        with tempfile.TemporaryDirectory() as tmpdirname:
            tmp_dir = Path(tmpdirname)
            temp_path = tmp_dir / 'erp_data.json'
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(test_data, f)
            
            with patch.object(settings, 'BASE_DIR', tmp_dir):
                batches = list(stream_erp_data_in_batches(batch_size=2))
        
        assert len(batches) == 2
        assert len(batches[0]) == 2
        assert len(batches[1]) == 1
        assert batches[0][0]['sku'] == 'TEST-001'
        assert batches[1][0]['sku'] == 'TEST-003'

    def test_deduplication_keeps_last_occurrence(self, settings):
        test_data = [
            {"id": "DUP-001", "title": "Old", "price_vat_excl": 100},
            {"id": "DUP-001", "title": "New", "price_vat_excl": 200},
        ]
        
        with tempfile.TemporaryDirectory() as tmpdirname:
            tmp_dir = Path(tmpdirname)
            temp_path = tmp_dir / 'erp_data.json'
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(test_data, f)
            
            with patch.object(settings, 'BASE_DIR', tmp_dir):
                batches = list(stream_erp_data_in_batches(batch_size=5))
        
        assert len(batches) == 1
        assert len(batches[0]) == 1
        assert batches[0][0]['title'] == 'New'
        assert batches[0][0]['price_vat_incl'] == '242.00'


# =============================================================================
# 3: Celery tasks tests
# =============================================================================

@pytest.mark.django_db
class TestCeleryTasks:
    def setup_method(self):
        init_worker_session()

    @patch('integrator.tasks.process_sync_batch.delay')
    @patch('integrator.tasks.stream_erp_data_in_batches')
    def test_sync_erp_to_eshop_orchestrator(self, mock_stream, mock_delay):
        mock_stream.return_value = [[{"sku": "1"}], [{"sku": "2"}], [{"sku": "3"}]]
        
        result = sync_erp_to_eshop()
        
        assert result['dispatched_batches'] == 3
        assert mock_delay.call_count == 3

    @responses.activate
    def test_process_sync_batch_success(self, settings):
        batch = [
            {'sku': 'NEW-001', 'title': 'P1', 'price_vat_incl': '121.00', 'stock_total': 10, 'color': 'red'},
            {'sku': 'NEW-002', 'title': 'P2', 'price_vat_incl': '242.00', 'stock_total': 5, 'color': 'blue'}
        ]
        
        responses.add(
            responses.POST,
            f"{settings.ESHOP_API_BASE_URL}/",
            json={"status": "created"},
            status=201
        )
        
        process_sync_batch(batch)
        
        assert ProductSyncState.objects.count() == 2
        assert ProductSyncState.objects.filter(sku='NEW-001').exists()
        assert ProductSyncState.objects.filter(sku='NEW-002').exists()

    @responses.activate
    def test_process_sync_batch_rate_limit_retry(self, settings):
        batch = [
            {'sku': 'RATE-001', 'title': 'Test', 'price_vat_incl': '50.00', 'stock_total': 0, 'color': 'N/A'}
        ]
        
        responses.add(
            responses.POST,
            f"{settings.ESHOP_API_BASE_URL}/",
            json={"error": "rate limit exceeded"},
            status=429,
            headers={"Retry-After": "15"}
        )
        
        with patch.object(process_sync_batch, 'retry', side_effect=Exception("Retry Triggered")) as mock_retry:
            with pytest.raises(Exception, match="Retry Triggered"):
                process_sync_batch(batch)
            
            mock_retry.assert_called_once()
            call_kwargs = mock_retry.call_args[1]
            assert call_kwargs['countdown'] == 15