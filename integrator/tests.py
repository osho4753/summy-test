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
from integrator.tasks import sync_erp_to_eshop, ESHOP_API_BASE_URL


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
    """Tests for sync_erp_to_eshop Celery task."""
    
    @responses.activate
    def test_successful_post_creates_sync_state(self, settings):
        """Successful POST creates ProductSyncState record."""
        # Prepare test data
        test_data = [
            {
                "id": "NEW-001",
                "title": "New Product",
                "price_vat_excl": 100.0,
                "stocks": {"warehouse": 10},
                "attributes": {"color": "blue"}
            }
        ]
        
        # Mock API response - 201 Created
        responses.add(
            responses.POST,
            f"{ESHOP_API_BASE_URL}/",
            json={"status": "created"},
            status=201
        )
        
        with tempfile.TemporaryDirectory() as tmpdirname:
            tmp_dir_path = Path(tmpdirname)
            temp_path = tmp_dir_path / 'erp_data.json'
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(test_data, f)
            
            with patch.object(settings, 'BASE_DIR', tmp_dir_path):
                with patch('integrator.services.settings', settings):
                    # Call task synchronously (without .delay())
                    result = sync_erp_to_eshop()
            
            # Verify sync state was created
            assert ProductSyncState.objects.filter(sku='NEW-001').exists()
            sync_state = ProductSyncState.objects.get(sku='NEW-001')
            assert len(sync_state.data_hash) == 64  # SHA-256 hex length
            
            # Verify stats
            assert result['created'] == 1
            assert result['errors'] == 0
    
    @responses.activate
    def test_rate_limit_429_triggers_retry(self, settings):
        """429 response triggers task retry."""
        # Prepare test data
        test_data = [
            {
                "id": "RATE-001",
                "title": "Rate Limited Product",
                "price_vat_excl": 50.0,
                "stocks": {},
                "attributes": {}
            }
        ]
        
        # Mock API response - 429 Rate Limit
        responses.add(
            responses.POST,
            f"{ESHOP_API_BASE_URL}/",
            json={"error": "rate limit exceeded"},
            status=429
        )
        
        with tempfile.TemporaryDirectory() as tmpdirname:
            tmp_dir_path = Path(tmpdirname)
            temp_path = tmp_dir_path / 'erp_data.json'
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(test_data, f)
            
            with patch.object(settings, 'BASE_DIR', tmp_dir_path):
                with patch('integrator.services.settings', settings):
                    # Patch retry method on the Celery task itself
                    with patch('integrator.tasks.sync_erp_to_eshop.retry', side_effect=Exception("Retry triggered")) as mock_retry:
                        # Call task normally
                        with pytest.raises(Exception, match="Retry triggered"):
                            sync_erp_to_eshop()
                        
                        # Verify retry was called with correct parameters
                        mock_retry.assert_called_once()
                        call_kwargs = mock_retry.call_args[1]
                        assert call_kwargs['countdown'] == 10
    
    @responses.activate
    def test_unchanged_product_is_skipped(self, settings):
        """Products with unchanged hash are skipped."""
        # Prepare test data
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
                    # First sync - should create
                    responses.add(
                        responses.POST,
                        f"{ESHOP_API_BASE_URL}/",
                        json={"status": "created"},
                        status=201
                    )
                    result1 = sync_erp_to_eshop()
                    assert result1['created'] == 1
                    
                    # Second sync - should skip (no API call needed)
                    result2 = sync_erp_to_eshop()
                    assert result2['skipped'] == 1
                    assert result2['created'] == 0
                    
                    # Verify only one API call was made
                    assert len(responses.calls) == 1
    
    @responses.activate
    def test_modified_product_triggers_patch(self, settings):
        """Modified product (different hash) triggers PATCH."""
        # First version of product
        test_data_v1 = [
            {
                "id": "MOD-001",
                "title": "Original Title",
                "price_vat_excl": 100.0,
                "stocks": {"warehouse": 5},
                "attributes": {"color": "red"}
            }
        ]
        
        # Modified version
        test_data_v2 = [
            {
                "id": "MOD-001",
                "title": "Updated Title",  # Changed!
                "price_vat_excl": 150.0,    # Changed!
                "stocks": {"warehouse": 5},
                "attributes": {"color": "red"}
            }
        ]
        
        with tempfile.TemporaryDirectory() as tmpdirname:
            tmp_dir_path = Path(tmpdirname)
            temp_path = tmp_dir_path / 'erp_data.json'
            
            # Write first version
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(test_data_v1, f)
            
            with patch.object(settings, 'BASE_DIR', tmp_dir_path):
                with patch('integrator.services.settings', settings):
                    # First sync - POST
                    responses.add(
                        responses.POST,
                        f"{ESHOP_API_BASE_URL}/",
                        json={"status": "created"},
                        status=201
                    )
                    result1 = sync_erp_to_eshop()
                    assert result1['created'] == 1
                    
                    old_hash = ProductSyncState.objects.get(sku='MOD-001').data_hash
                    
                    # Update file with new data
                    with open(temp_path, 'w', encoding='utf-8') as f:
                        json.dump(test_data_v2, f)
                    
                    # Second sync - should PATCH
                    responses.add(
                        responses.PATCH,
                        f"{ESHOP_API_BASE_URL}/MOD-001/",
                        json={"status": "updated"},
                        status=200
                    )
                    result2 = sync_erp_to_eshop()
                    assert result2['updated'] == 1
                    
                    new_hash = ProductSyncState.objects.get(sku='MOD-001').data_hash
                    assert old_hash != new_hash  # Hash should be different

