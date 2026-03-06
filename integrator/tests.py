"""
Tests for ERP to E-shop integration.

Block A: Unit tests for data transformation (parse_and_transform_erp_data)
Block B: Integration tests for Celery task (sync_erp_to_eshop)
"""
import json
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import responses

from integrator.models import ProductSyncState
from integrator.services import (
    parse_and_transform_erp_data,
    _calculate_price_with_vat,
    _calculate_stock_total,
    _extract_color,
)
from integrator.tasks import (
    sync_erp_to_eshop,
    sync_product_to_eshop,
    sync_product_chunk,
    ESHOP_API_BASE_URL,
    _calculate_hash,
    CHUNK_SIZE,
)


# =============================================================================
# BLOCK A: Unit tests for data transformation
# =============================================================================

class TestCalculatePriceWithVat:
    """Tests for VAT calculation (21%)."""
    
    def test_valid_price_adds_21_percent_vat(self):
        """VAT (21%) is correctly added to valid price."""
        # 100 * 1.21 = 121.0
        assert _calculate_price_with_vat(100.0) == 121.0
        
        # 12400.5 * 1.21 = 15004.605 → rounded to 15004.61
        assert _calculate_price_with_vat(12400.5) == 15004.61
    
    def test_null_price_returns_zero(self):
        """Null price results in 0.0."""
        assert _calculate_price_with_vat(None) == 0.0
    
    def test_negative_price_returns_zero(self):
        """Negative price (like -150.0 from SKU-002) results in 0.0."""
        assert _calculate_price_with_vat(-150.0) == 0.0
    
    def test_zero_price_returns_zero(self):
        """Zero price stays zero."""
        assert _calculate_price_with_vat(0) == 0.0


class TestCalculateStockTotal:
    """Tests for stock summation."""
    
    def test_valid_stocks_are_summed(self):
        """Valid numeric stocks are correctly summed."""
        stocks = {"praha": 5, "brno": 3}
        assert _calculate_stock_total(stocks) == 8
    
    def test_string_na_treated_as_zero(self):
        """String 'N/A' (like in SKU-008) is treated as 0."""
        stocks = {"praha": "N/A"}
        assert _calculate_stock_total(stocks) == 0
    
    def test_mixed_valid_and_invalid_stocks(self):
        """Mix of valid and invalid values - only valid are summed."""
        stocks = {"praha": 10, "brno": "N/A", "externi": 5}
        assert _calculate_stock_total(stocks) == 15
    
    def test_null_stocks_returns_zero(self):
        """Null stocks dict returns 0."""
        assert _calculate_stock_total(None) == 0
    
    def test_empty_stocks_returns_zero(self):
        """Empty stocks dict returns 0."""
        assert _calculate_stock_total({}) == 0


class TestExtractColor:
    """Tests for color extraction from attributes."""
    
    def test_valid_color_extracted(self):
        """Color is correctly extracted from attributes."""
        attributes = {"color": "stříbrná"}
        assert _extract_color(attributes) == "stříbrná"
    
    def test_null_attributes_returns_na(self):
        """Null attributes (like SKU-003) returns 'N/A'."""
        assert _extract_color(None) == "N/A"
    
    def test_empty_attributes_returns_na(self):
        """Empty attributes dict returns 'N/A'."""
        assert _extract_color({}) == "N/A"
    
    def test_missing_color_key_returns_na(self):
        """Attributes without 'color' key returns 'N/A'."""
        attributes = {"size": "large"}
        assert _extract_color(attributes) == "N/A"


@pytest.mark.django_db
class TestParseAndTransformErpData:
    """Integration tests for full transformation pipeline."""
    
    def test_full_transformation_with_temp_file(self, settings):
        """Test complete transformation with temporary JSON file."""
        # Create test data with known edge cases
        test_data = [
            {
                "id": "TEST-001",
                "title": "Test Product",
                "price_vat_excl": 100.0,
                "stocks": {"warehouse1": 10, "warehouse2": 5},
                "attributes": {"color": "red"}
            },
            {
                "id": "TEST-002",
                "title": "Negative Price",
                "price_vat_excl": -50.0,
                "stocks": {"warehouse1": 3},
                "attributes": None
            },
        ]
        
        with tempfile.TemporaryDirectory() as tmpdirname:
            tmp_dir_path = Path(tmpdirname)
            temp_path = tmp_dir_path / 'erp_data.json'
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(test_data, f)
            
            with patch.object(settings, 'BASE_DIR', tmp_dir_path):
                with patch('integrator.services.settings', settings):
                    result = parse_and_transform_erp_data()
            
            assert len(result) == 2
            
            # Check first product
            product1 = next(p for p in result if p['sku'] == 'TEST-001')
            assert product1['price_vat_incl'] == 121.0  # 100 * 1.21
            assert product1['stock_total'] == 15  # 10 + 5
            assert product1['color'] == 'red'
            
            # Check second product (edge cases)
            product2 = next(p for p in result if p['sku'] == 'TEST-002')
            assert product2['price_vat_incl'] == 0.0  # Negative → 0
            assert product2['color'] == 'N/A'  # None attributes
    
    def test_deduplication_keeps_last_occurrence(self, settings):
        """Duplicate SKUs are deduplicated (last occurrence wins)."""
        test_data = [
            {"id": "DUP-001", "title": "First", "price_vat_excl": 100, "stocks": {}, "attributes": {}},
            {"id": "DUP-001", "title": "Second", "price_vat_excl": 200, "stocks": {}, "attributes": {}},
        ]
        
        with tempfile.TemporaryDirectory() as tmpdirname:
            tmp_dir_path = Path(tmpdirname)
            temp_path = tmp_dir_path / 'erp_data.json'
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(test_data, f)
            
            with patch.object(settings, 'BASE_DIR', tmp_dir_path):
                with patch('integrator.services.settings', settings):
                    result = parse_and_transform_erp_data()
            
            # Only one product should exist
            assert len(result) == 1
            # Last occurrence should be kept
            assert result[0]['title'] == 'Second'
            assert result[0]['price_vat_incl'] == 242.0  # 200 * 1.21


# =============================================================================
# BLOCK B: Integration tests for Celery task
# =============================================================================

@pytest.mark.django_db
class TestSyncErpToEshopTask:
    """Tests for sync_erp_to_eshop orchestrator task."""
    
    def test_orchestrator_dispatches_new_products(self, settings):
        """Orchestrator dispatches sub-tasks for new products."""
        test_data = [
            {
                "id": "NEW-001",
                "title": "New Product",
                "price_vat_excl": 100.0,
                "stocks": {"warehouse": 10},
                "attributes": {"color": "blue"}
            }
        ]
        
        with tempfile.TemporaryDirectory() as tmpdirname:
            tmp_dir_path = Path(tmpdirname)
            temp_path = tmp_dir_path / 'erp_data.json'
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(test_data, f)
            
            with patch.object(settings, 'BASE_DIR', tmp_dir_path):
                with patch('integrator.services.settings', settings):
                    with patch('integrator.tasks.group') as mock_group:
                        mock_group.return_value.apply_async = MagicMock()
                        result = sync_erp_to_eshop()
            
            assert result['dispatched'] == 1
            assert result['skipped'] == 0
            assert result['total'] == 1
            assert result['chunks'] == 1
    
    def test_unchanged_product_is_skipped(self, settings):
        """Products with unchanged hash are skipped by orchestrator."""
        test_data = [
            {
                "id": "SKIP-001",
                "title": "Already Synced",
                "price_vat_excl": 100.0,
                "stocks": {"warehouse": 5},
                "attributes": {"color": "green"}
            }
        ]
        
        with tempfile.TemporaryDirectory() as tmpdirname:
            tmp_dir_path = Path(tmpdirname)
            temp_path = tmp_dir_path / 'erp_data.json'
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(test_data, f)
            
            with patch.object(settings, 'BASE_DIR', tmp_dir_path):
                with patch('integrator.services.settings', settings):
                    # Pre-create sync state with matching hash
                    products = parse_and_transform_erp_data()
                    product_hash = _calculate_hash(products[0])
                    ProductSyncState.objects.create(sku='SKIP-001', data_hash=product_hash)
                    
                    with patch('integrator.tasks.group') as mock_group:
                        result = sync_erp_to_eshop()
            
            assert result['skipped'] == 1
            assert result['dispatched'] == 0
            mock_group.assert_not_called()


@pytest.mark.django_db
class TestSyncProductToEshopTask:
    """Tests for sync_product_to_eshop sub-task."""
    
    @responses.activate
    def test_successful_post_creates_sync_state(self):
        """Successful POST creates ProductSyncState record."""
        product = {
            'sku': 'NEW-001',
            'title': 'New Product',
            'price_vat_incl': 121.0,
            'stock_total': 10,
            'color': 'blue'
        }
        product_hash = _calculate_hash(product)
        
        responses.add(
            responses.POST,
            f"{ESHOP_API_BASE_URL}/",
            json={"status": "created"},
            status=201
        )
        
        result = sync_product_to_eshop(product, product_hash, is_new=True)
        
        assert result['status'] == 'created'
        assert ProductSyncState.objects.filter(sku='NEW-001').exists()
        sync_state = ProductSyncState.objects.get(sku='NEW-001')
        assert sync_state.data_hash == product_hash
    
    @responses.activate
    def test_successful_patch_updates_sync_state(self):
        """Successful PATCH updates existing ProductSyncState."""
        product = {
            'sku': 'MOD-001',
            'title': 'Modified Product',
            'price_vat_incl': 150.0,
            'stock_total': 5,
            'color': 'red'
        }
        old_hash = 'old_hash_value'
        new_hash = _calculate_hash(product)
        
        ProductSyncState.objects.create(sku='MOD-001', data_hash=old_hash)
        
        responses.add(
            responses.PATCH,
            f"{ESHOP_API_BASE_URL}/MOD-001/",
            json={"status": "updated"},
            status=200
        )
        
        result = sync_product_to_eshop(product, new_hash, is_new=False)
        
        assert result['status'] == 'updated'
        sync_state = ProductSyncState.objects.get(sku='MOD-001')
        assert sync_state.data_hash == new_hash
    
    @responses.activate
    def test_rate_limit_429_triggers_retry(self):
        """429 response triggers task retry."""
        product = {
            'sku': 'RATE-001',
            'title': 'Rate Limited Product',
            'price_vat_incl': 50.0,
            'stock_total': 0,
            'color': 'N/A'
        }
        product_hash = _calculate_hash(product)
        
        responses.add(
            responses.POST,
            f"{ESHOP_API_BASE_URL}/",
            json={"error": "rate limit exceeded"},
            status=429
        )
        
        with patch.object(sync_product_to_eshop, 'retry', side_effect=Exception("Retry triggered")) as mock_retry:
            with pytest.raises(Exception, match="Retry triggered"):
                sync_product_to_eshop(product, product_hash, is_new=True)
            
            mock_retry.assert_called_once()
            call_kwargs = mock_retry.call_args[1]
            assert call_kwargs['countdown'] == 10


@pytest.mark.django_db
class TestSyncProductChunkTask:
    """Tests for sync_product_chunk sub-task (batch processing)."""
    
    @responses.activate
    def test_chunk_creates_multiple_products(self):
        """Chunk task successfully creates multiple products."""
        chunk = [
            {
                'product': {'sku': 'CHUNK-001', 'title': 'Product 1', 'price_vat_incl': 121.0, 'stock_total': 10, 'color': 'red'},
                'hash': _calculate_hash({'sku': 'CHUNK-001', 'title': 'Product 1', 'price_vat_incl': 121.0, 'stock_total': 10, 'color': 'red'}),
                'is_new': True,
            },
            {
                'product': {'sku': 'CHUNK-002', 'title': 'Product 2', 'price_vat_incl': 242.0, 'stock_total': 5, 'color': 'blue'},
                'hash': _calculate_hash({'sku': 'CHUNK-002', 'title': 'Product 2', 'price_vat_incl': 242.0, 'stock_total': 5, 'color': 'blue'}),
                'is_new': True,
            },
        ]
        
        responses.add(responses.POST, f"{ESHOP_API_BASE_URL}/", json={"status": "created"}, status=201)
        responses.add(responses.POST, f"{ESHOP_API_BASE_URL}/", json={"status": "created"}, status=201)
        
        result = sync_product_chunk(chunk)
        
        assert result['created'] == 2
        assert result['updated'] == 0
        assert result['errors'] == 0
        assert ProductSyncState.objects.filter(sku='CHUNK-001').exists()
        assert ProductSyncState.objects.filter(sku='CHUNK-002').exists()
    
    @responses.activate
    def test_chunk_rate_limit_retries_entire_chunk(self):
        """429 on any product in chunk triggers retry for entire chunk."""
        chunk = [
            {
                'product': {'sku': 'RATE-CHUNK-001', 'title': 'Product 1', 'price_vat_incl': 100.0, 'stock_total': 1, 'color': 'N/A'},
                'hash': 'hash1',
                'is_new': True,
            },
            {
                'product': {'sku': 'RATE-CHUNK-002', 'title': 'Product 2', 'price_vat_incl': 200.0, 'stock_total': 2, 'color': 'N/A'},
                'hash': 'hash2',
                'is_new': True,
            },
        ]
        
        # First product succeeds, second gets rate limited
        responses.add(responses.POST, f"{ESHOP_API_BASE_URL}/", json={"status": "created"}, status=201)
        responses.add(responses.POST, f"{ESHOP_API_BASE_URL}/", json={"error": "rate limit"}, status=429)
        
        with patch.object(sync_product_chunk, 'retry', side_effect=Exception("Chunk retry triggered")) as mock_retry:
            with pytest.raises(Exception, match="Chunk retry triggered"):
                sync_product_chunk(chunk)
            
            mock_retry.assert_called_once()
            call_kwargs = mock_retry.call_args[1]
            assert call_kwargs['countdown'] == 10
    
    @responses.activate
    def test_chunk_mixed_create_and_update(self):
        """Chunk handles mix of new and existing products."""
        # Pre-create one product
        ProductSyncState.objects.create(sku='MIX-002', data_hash='old_hash')
        
        chunk = [
            {
                'product': {'sku': 'MIX-001', 'title': 'New Product', 'price_vat_incl': 100.0, 'stock_total': 1, 'color': 'green'},
                'hash': 'new_hash_1',
                'is_new': True,
            },
            {
                'product': {'sku': 'MIX-002', 'title': 'Updated Product', 'price_vat_incl': 200.0, 'stock_total': 2, 'color': 'yellow'},
                'hash': 'new_hash_2',
                'is_new': False,
            },
        ]
        
        responses.add(responses.POST, f"{ESHOP_API_BASE_URL}/", json={"status": "created"}, status=201)
        responses.add(responses.PATCH, f"{ESHOP_API_BASE_URL}/MIX-002/", json={"status": "updated"}, status=200)
        
        result = sync_product_chunk(chunk)
        
        assert result['created'] == 1
        assert result['updated'] == 1
        assert result['errors'] == 0

