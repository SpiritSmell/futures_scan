"""
Unit tests for ResilientExchangeManager
Tests cover exchange initialization, resilience components, and error handling.
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import Dict, Any

from exchange_manager_v3 import ResilientExchangeManager, ResilientExchange
from config_manager import ExchangeConfig


class TestResilientExchangeManager:
    """Test suite for ResilientExchangeManager functionality."""
    
    @pytest.fixture
    def sample_exchange_configs(self):
        """Sample exchange configurations for testing."""
        return [
            ExchangeConfig(
                name="binance",
                enabled=True,
                timeout=30.0,
                rate_limit=1200,
                sandbox=False
            ),
            ExchangeConfig(
                name="bybit",
                enabled=True,
                timeout=25.0,
                rate_limit=600,
                sandbox=True
            ),
            ExchangeConfig(
                name="disabled_exchange",
                enabled=False,
                timeout=30.0,
                rate_limit=1000
            )
        ]
    
    @pytest.fixture
    def mock_resilience_components(self):
        """Mock resilience components for testing."""
        return {
            'circuit_breaker_manager': Mock(),
            'retry_manager': Mock(),
            'health_monitor': Mock(),
            'cache_manager': Mock(),
            'connection_pool': Mock()
        }
    
    def test_exchange_manager_initialization(self):
        """Test ResilientExchangeManager initialization."""
        manager = ResilientExchangeManager()
        
        assert manager.exchanges == {}
        assert manager.circuit_breaker_manager is None
        assert manager.retry_manager is None
        assert manager.health_monitor is None
        assert manager.cache_manager is None
        assert manager.connection_pool is None
        assert manager.logger is not None
    
    def test_set_resilience_components(self, mock_resilience_components):
        """Test setting resilience components."""
        manager = ResilientExchangeManager()
        manager.set_resilience_components(**mock_resilience_components)
        
        assert manager.circuit_breaker_manager == mock_resilience_components['circuit_breaker_manager']
        assert manager.retry_manager == mock_resilience_components['retry_manager']
        assert manager.health_monitor == mock_resilience_components['health_monitor']
        assert manager.cache_manager == mock_resilience_components['cache_manager']
        assert manager.connection_pool == mock_resilience_components['connection_pool']
    
    @pytest.mark.asyncio
    async def test_initialize_exchanges_success(self, sample_exchange_configs, mock_resilience_components):
        """Test successful exchange initialization."""
        manager = ResilientExchangeManager()
        manager.set_resilience_components(**mock_resilience_components)
        
        # Mock the _create_resilient_exchange method
        with patch.object(manager, '_create_resilient_exchange') as mock_create:
            mock_exchange = Mock(spec=ResilientExchange)
            mock_exchange.name = "binance"
            mock_create.return_value = mock_exchange
            
            await manager.initialize_exchanges(sample_exchange_configs)
            
            # Should only create enabled exchanges
            assert mock_create.call_count == 2  # binance and bybit
            assert len(manager.exchanges) == 2
            assert "binance" in manager.exchanges
            assert "bybit" in manager.exchanges
            assert "disabled_exchange" not in manager.exchanges
    
    @pytest.mark.asyncio
    async def test_initialize_exchanges_with_failures(self, sample_exchange_configs, mock_resilience_components):
        """Test exchange initialization with some failures."""
        manager = ResilientExchangeManager()
        manager.set_resilience_components(**mock_resilience_components)
        
        def mock_create_side_effect(config):
            if config.name == "bybit":
                raise Exception("Failed to initialize bybit")
            mock_exchange = Mock(spec=ResilientExchange)
            mock_exchange.name = config.name
            return mock_exchange
        
        with patch.object(manager, '_create_resilient_exchange', side_effect=mock_create_side_effect):
            await manager.initialize_exchanges(sample_exchange_configs)
            
            # Should only have successfully initialized exchanges
            assert len(manager.exchanges) == 1
            assert "binance" in manager.exchanges
            assert "bybit" not in manager.exchanges
    
    def test_get_exchange_success(self, mock_resilience_components):
        """Test getting an existing exchange."""
        manager = ResilientExchangeManager()
        manager.set_resilience_components(**mock_resilience_components)
        
        mock_exchange = Mock(spec=ResilientExchange)
        manager.exchanges["binance"] = mock_exchange
        
        result = manager.get_exchange("binance")
        assert result == mock_exchange
    
    def test_get_exchange_not_found(self):
        """Test getting a non-existent exchange."""
        manager = ResilientExchangeManager()
        
        result = manager.get_exchange("nonexistent")
        assert result is None
    
    def test_get_available_exchanges(self, mock_resilience_components):
        """Test getting list of available exchanges."""
        manager = ResilientExchangeManager()
        manager.set_resilience_components(**mock_resilience_components)
        
        manager.exchanges["binance"] = Mock()
        manager.exchanges["bybit"] = Mock()
        
        available = manager.get_available_exchanges()
        assert set(available) == {"binance", "bybit"}
    
    def test_get_exchange_count(self, mock_resilience_components):
        """Test getting exchange count."""
        manager = ResilientExchangeManager()
        manager.set_resilience_components(**mock_resilience_components)
        
        manager.exchanges["binance"] = Mock()
        manager.exchanges["bybit"] = Mock()
        
        count = manager.get_exchange_count()
        assert count == 2
    
    @pytest.mark.asyncio
    async def test_close_all_exchanges(self, mock_resilience_components):
        """Test closing all exchanges."""
        manager = ResilientExchangeManager()
        manager.set_resilience_components(**mock_resilience_components)
        
        mock_exchange1 = AsyncMock()
        mock_exchange2 = AsyncMock()
        manager.exchanges["binance"] = mock_exchange1
        manager.exchanges["bybit"] = mock_exchange2
        
        await manager.close_all()
        
        mock_exchange1.close.assert_called_once()
        mock_exchange2.close.assert_called_once()
        assert len(manager.exchanges) == 0


class TestResilientExchange:
    """Test suite for ResilientExchange functionality."""
    
    @pytest.fixture
    def sample_exchange_config(self):
        """Sample exchange configuration."""
        return ExchangeConfig(
            name="binance",
            enabled=True,
            timeout=30.0,
            rate_limit=1200,
            sandbox=False
        )
    
    @pytest.fixture
    def mock_ccxt_exchange(self):
        """Mock CCXT exchange instance."""
        mock_exchange = AsyncMock()
        mock_exchange.id = "binance"
        mock_exchange.name = "Binance"
        mock_exchange.has = {
            'fetchTickers': True,
            'fetchFundingRates': True,
            'fetchOrderBook': True
        }
        mock_exchange.markets = {
            'BTC/USDT': {'symbol': 'BTC/USDT', 'active': True},
            'ETH/USDT': {'symbol': 'ETH/USDT', 'active': True}
        }
        return mock_exchange
    
    @pytest.fixture
    def mock_resilience_components(self):
        """Mock resilience components."""
        return {
            'circuit_breaker': Mock(),
            'retry_manager': Mock(),
            'health_check': Mock(),
            'cache_manager': Mock(),
            'connection_pool': Mock()
        }
    
    def test_resilient_exchange_initialization(self, sample_exchange_config, mock_ccxt_exchange, mock_resilience_components):
        """Test ResilientExchange initialization."""
        exchange = ResilientExchange(
            config=sample_exchange_config,
            ccxt_exchange=mock_ccxt_exchange,
            **mock_resilience_components
        )
        
        assert exchange.name == "binance"
        assert exchange.config == sample_exchange_config
        assert exchange.ccxt_exchange == mock_ccxt_exchange
        assert exchange.circuit_breaker == mock_resilience_components['circuit_breaker']
        assert exchange.retry_manager == mock_resilience_components['retry_manager']
        assert exchange.health_check == mock_resilience_components['health_check']
        assert exchange.is_healthy is True
        assert exchange.last_error is None
    
    @pytest.mark.asyncio
    async def test_fetch_tickers_success(self, sample_exchange_config, mock_ccxt_exchange, mock_resilience_components):
        """Test successful ticker fetching."""
        mock_tickers = {
            'BTC/USDT': {'symbol': 'BTC/USDT', 'last': 50000.0},
            'ETH/USDT': {'symbol': 'ETH/USDT', 'last': 3000.0}
        }
        mock_ccxt_exchange.fetch_tickers.return_value = mock_tickers
        
        exchange = ResilientExchange(
            config=sample_exchange_config,
            ccxt_exchange=mock_ccxt_exchange,
            **mock_resilience_components
        )
        
        # Mock circuit breaker to allow execution
        mock_resilience_components['circuit_breaker'].is_open = False
        mock_resilience_components['circuit_breaker'].call = AsyncMock(return_value=mock_tickers)
        
        result = await exchange.fetch_tickers()
        
        assert result == mock_tickers
        mock_resilience_components['circuit_breaker'].call.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_fetch_tickers_circuit_breaker_open(self, sample_exchange_config, mock_ccxt_exchange, mock_resilience_components):
        """Test ticker fetching when circuit breaker is open."""
        exchange = ResilientExchange(
            config=sample_exchange_config,
            ccxt_exchange=mock_ccxt_exchange,
            **mock_resilience_components
        )
        
        # Mock circuit breaker as open
        mock_resilience_components['circuit_breaker'].is_open = True
        
        result = await exchange.fetch_tickers()
        
        assert result == {}
        assert exchange.is_healthy is False
    
    @pytest.mark.asyncio
    async def test_fetch_funding_rates_success(self, sample_exchange_config, mock_ccxt_exchange, mock_resilience_components):
        """Test successful funding rate fetching."""
        mock_funding_rates = [
            {'symbol': 'BTC/USDT', 'fundingRate': 0.0001},
            {'symbol': 'ETH/USDT', 'fundingRate': 0.0002}
        ]
        mock_ccxt_exchange.fetch_funding_rates.return_value = mock_funding_rates
        
        exchange = ResilientExchange(
            config=sample_exchange_config,
            ccxt_exchange=mock_ccxt_exchange,
            **mock_resilience_components
        )
        
        # Mock circuit breaker to allow execution
        mock_resilience_components['circuit_breaker'].is_open = False
        mock_resilience_components['circuit_breaker'].call = AsyncMock(return_value=mock_funding_rates)
        
        result = await exchange.fetch_funding_rates()
        
        assert result == mock_funding_rates
        mock_resilience_components['circuit_breaker'].call.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_fetch_with_retry_on_failure(self, sample_exchange_config, mock_ccxt_exchange, mock_resilience_components):
        """Test fetch operation with retry on failure."""
        exchange = ResilientExchange(
            config=sample_exchange_config,
            ccxt_exchange=mock_ccxt_exchange,
            **mock_resilience_components
        )
        
        # Mock circuit breaker to raise exception first, then succeed
        mock_resilience_components['circuit_breaker'].is_open = False
        mock_resilience_components['circuit_breaker'].call = AsyncMock(
            side_effect=[Exception("Network error"), {'BTC/USDT': {'last': 50000.0}}]
        )
        
        # Mock retry manager
        async def mock_retry_call(func, *args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception:
                # Simulate retry
                return await func(*args, **kwargs)
        
        mock_resilience_components['retry_manager'].call = mock_retry_call
        
        result = await exchange.fetch_tickers()
        
        assert 'BTC/USDT' in result
        assert exchange.last_error is not None
    
    @pytest.mark.asyncio
    async def test_health_check_update(self, sample_exchange_config, mock_ccxt_exchange, mock_resilience_components):
        """Test health check status updates."""
        exchange = ResilientExchange(
            config=sample_exchange_config,
            ccxt_exchange=mock_ccxt_exchange,
            **mock_resilience_components
        )
        
        # Initially healthy
        assert exchange.is_healthy is True
        
        # Simulate error
        exchange.last_error = Exception("Test error")
        exchange._update_health_status()
        
        # Should update health check
        mock_resilience_components['health_check'].record_failure.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_close_exchange(self, sample_exchange_config, mock_ccxt_exchange, mock_resilience_components):
        """Test exchange cleanup on close."""
        exchange = ResilientExchange(
            config=sample_exchange_config,
            ccxt_exchange=mock_ccxt_exchange,
            **mock_resilience_components
        )
        
        await exchange.close()
        
        mock_ccxt_exchange.close.assert_called_once()
        mock_resilience_components['health_check'].stop.assert_called_once()


# Integration tests for exchange manager
class TestExchangeManagerIntegration:
    """Integration tests for exchange manager with real-like scenarios."""
    
    @pytest.mark.asyncio
    async def test_full_exchange_lifecycle(self, sample_exchange_configs):
        """Test complete exchange lifecycle from initialization to cleanup."""
        manager = ResilientExchangeManager()
        
        # Mock all dependencies
        with patch('exchange_manager_v3.getattr') as mock_getattr, \
             patch('exchange_manager_v3.ccxt') as mock_ccxt:
            
            # Setup mocks
            mock_exchange_class = AsyncMock()
            mock_exchange_instance = AsyncMock()
            mock_exchange_instance.load_markets = AsyncMock()
            mock_exchange_instance.markets = {'BTC/USDT': {}}
            mock_exchange_class.return_value = mock_exchange_instance
            mock_getattr.return_value = mock_exchange_class
            
            # Mock resilience components
            mock_components = {
                'circuit_breaker_manager': Mock(),
                'retry_manager': Mock(),
                'health_monitor': Mock(),
                'cache_manager': Mock(),
                'connection_pool': Mock()
            }
            manager.set_resilience_components(**mock_components)
            
            # Initialize exchanges
            await manager.initialize_exchanges([sample_exchange_configs[0]])  # Only binance
            
            # Verify exchange was created
            assert len(manager.exchanges) == 1
            assert "binance" in manager.exchanges
            
            # Test getting exchange
            exchange = manager.get_exchange("binance")
            assert exchange is not None
            
            # Test cleanup
            await manager.close_all()
            assert len(manager.exchanges) == 0


# Performance tests
class TestExchangeManagerPerformance:
    """Performance tests for exchange manager."""
    
    @pytest.mark.asyncio
    async def test_concurrent_exchange_initialization(self):
        """Test concurrent initialization of multiple exchanges."""
        import time
        
        manager = ResilientExchangeManager()
        
        # Create multiple exchange configs
        configs = [
            ExchangeConfig(name=f"exchange_{i}", enabled=True, timeout=30.0, rate_limit=1000)
            for i in range(10)
        ]
        
        # Mock components
        mock_components = {
            'circuit_breaker_manager': Mock(),
            'retry_manager': Mock(),
            'health_monitor': Mock(),
            'cache_manager': Mock(),
            'connection_pool': Mock()
        }
        manager.set_resilience_components(**mock_components)
        
        with patch.object(manager, '_create_resilient_exchange') as mock_create:
            mock_create.return_value = Mock(spec=ResilientExchange)
            
            start_time = time.time()
            await manager.initialize_exchanges(configs)
            end_time = time.time()
            
            # Should initialize all exchanges quickly
            assert end_time - start_time < 1.0  # Less than 1 second
            assert len(manager.exchanges) == 10


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
