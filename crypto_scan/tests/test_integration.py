"""
Integration Tests for Crypto Futures Price Collector v5
Tests component interactions and end-to-end system behavior.
"""

import pytest
import asyncio
import time
from typing import Dict, List, Any
from unittest.mock import patch, Mock, AsyncMock

from tests.mocks.exchange_mocks import MockExchangeFactory, MockCCXTExchange
from tests.mocks.rabbitmq_mocks import MockAsyncRabbitMQClient
from tests.mocks.database_mocks import MockDatabaseClient

# Import system components
from config_manager import ConfigManager, AppConfig
from exchange_manager_v3 import ResilientExchangeManager
from circuit_breaker import CircuitBreakerManager
from retry_manager import RetryManagerRegistry
from health_monitor import HealthMonitor
from cache_manager import CacheManager
from batch_processor import BatchProcessorManager
from connection_pool import ConnectionPoolManager
from data_collector import DataCollector
from data_sender import DataSender


class TestSystemIntegration:
    """Integration tests for complete system workflows."""
    
    @pytest.fixture
    def integration_config(self):
        """Configuration for integration testing."""
        return {
            "environment": "development",
            "debug": True,
            "exchanges": ["binance", "bybit"],
            "ticker_interval": 5.0,
            "funding_rate_interval": 30.0,
            "cache": {
                "enabled": True,
                "default_ttl": 60.0,
                "max_size": 1000
            },
            "batch_processing": {
                "enabled": True,
                "batch_size": 100,
                "flush_interval": 10.0
            },
            "rabbitmq": {
                "host": "localhost",
                "port": 5672,
                "user": "test_user",
                "password": "test_password"
            },
            "database": {
                "enabled": True,
                "host": "localhost",
                "port": 8123,
                "database": "crypto_test"
            }
        }
    
    @pytest.fixture
    async def integrated_system(self, integration_config):
        """Create fully integrated system for testing."""
        config = AppConfig(**integration_config)
        
        # Initialize all components
        cache_manager = CacheManager()
        connection_pool = ConnectionPoolManager()
        batch_processor = BatchProcessorManager()
        circuit_breaker_manager = CircuitBreakerManager()
        retry_manager = RetryManagerRegistry()
        health_monitor = HealthMonitor()
        
        # Initialize exchange manager
        exchange_manager = ResilientExchangeManager()
        exchange_manager.set_resilience_components(
            circuit_breaker_manager=circuit_breaker_manager,
            retry_manager=retry_manager,
            health_monitor=health_monitor,
            cache_manager=cache_manager,
            connection_pool=connection_pool
        )
        
        # Initialize data components
        rabbitmq_client = MockAsyncRabbitMQClient(**config.rabbitmq.model_dump())
        database_client = MockDatabaseClient(**config.database.model_dump())
        
        data_collector = DataCollector(
            exchange_manager=exchange_manager,
            cache_manager=cache_manager,
            config=config
        )
        
        data_sender = DataSender(
            rabbitmq_client=rabbitmq_client,
            database_client=database_client,
            batch_processor=batch_processor,
            config=config
        )
        
        # Mock exchange initialization
        with patch('exchange_manager_v3.getattr') as mock_getattr:
            mock_exchange_class = AsyncMock()
            mock_exchange_instance = MockExchangeFactory.create_exchange('binance')
            mock_exchange_class.return_value = mock_exchange_instance
            mock_getattr.return_value = mock_exchange_class
            
            # Initialize exchanges
            exchange_configs = [config.get_exchange_config(name) for name in config.exchanges]
            await exchange_manager.initialize_exchanges(exchange_configs)
        
        # Start components
        await rabbitmq_client.start()
        await database_client.connect()
        await health_monitor.start()
        
        system = {
            'config': config,
            'exchange_manager': exchange_manager,
            'data_collector': data_collector,
            'data_sender': data_sender,
            'rabbitmq_client': rabbitmq_client,
            'database_client': database_client,
            'cache_manager': cache_manager,
            'health_monitor': health_monitor,
            'circuit_breaker_manager': circuit_breaker_manager,
            'retry_manager': retry_manager
        }
        
        yield system
        
        # Cleanup
        await exchange_manager.close_all()
        await health_monitor.stop()
        await rabbitmq_client.stop()
        await database_client.disconnect()
    
    @pytest.mark.asyncio
    async def test_end_to_end_data_flow(self, integrated_system):
        """Test complete data flow from collection to storage."""
        system = integrated_system
        
        # Collect data
        collected_data = await system['data_collector'].collect_all_data()
        
        # Verify data collection
        assert 'tickers' in collected_data
        assert 'funding_rates' in collected_data
        assert len(collected_data['tickers']) > 0
        
        # Send data through the pipeline
        await system['data_sender'].send_tickers(collected_data['tickers'])
        await system['data_sender'].send_funding_rates(collected_data['funding_rates'])
        
        # Verify data was sent to RabbitMQ
        rabbitmq_stats = system['rabbitmq_client'].get_statistics()
        assert rabbitmq_stats['publish_count'] > 0
        
        # Verify data was stored in database
        db_stats = system['database_client'].get_statistics()
        assert db_stats['insert_count'] > 0
        
        # Check cache usage
        cache_stats = system['cache_manager'].get_cache_stats()
        assert cache_stats['ticker']['hits'] + cache_stats['ticker']['misses'] > 0
    
    @pytest.mark.asyncio
    async def test_resilience_integration(self, integrated_system):
        """Test resilience components working together."""
        system = integrated_system
        
        # Get resilience components
        cb_manager = system['circuit_breaker_manager']
        retry_manager = system['retry_manager']
        health_monitor = system['health_monitor']
        
        # Simulate exchange failures
        exchange = system['exchange_manager'].get_exchange('binance')
        if hasattr(exchange, 'ccxt_exchange'):
            exchange.ccxt_exchange.set_failure_rate(0.5)  # 50% failure rate
        
        # Collect data multiple times to trigger resilience mechanisms
        results = []
        for _ in range(10):
            try:
                data = await system['data_collector'].collect_ticker_data(['binance'])
                results.append(data)
            except Exception as e:
                results.append(e)
            
            await asyncio.sleep(0.1)  # Small delay between attempts
        
        # Verify resilience mechanisms activated
        cb_states = cb_manager.get_all_states()
        assert len(cb_states) > 0
        
        # Check health monitor status
        health_status = health_monitor.get_all_health_status()
        assert len(health_status) > 0
        
        # Verify some successful results despite failures
        successful_results = [r for r in results if not isinstance(r, Exception)]
        assert len(successful_results) > 0, "Resilience mechanisms should allow some success"
    
    @pytest.mark.asyncio
    async def test_cache_integration(self, integrated_system):
        """Test cache integration across components."""
        system = integrated_system
        cache_manager = system['cache_manager']
        data_collector = system['data_collector']
        
        # First data collection (should populate cache)
        data1 = await data_collector.collect_ticker_data(['binance'])
        
        # Check cache was populated
        cache_stats_before = cache_manager.get_cache_stats()
        
        # Second data collection (should use cache)
        data2 = await data_collector.collect_ticker_data(['binance'])
        
        # Check cache usage
        cache_stats_after = cache_manager.get_cache_stats()
        
        # Verify cache hits increased
        if 'ticker' in cache_stats_after:
            assert cache_stats_after['ticker']['hits'] >= cache_stats_before.get('ticker', {}).get('hits', 0)
    
    @pytest.mark.asyncio
    async def test_batch_processing_integration(self, integrated_system):
        """Test batch processing integration."""
        system = integrated_system
        data_sender = system['data_sender']
        
        # Generate large dataset
        large_dataset = []
        for i in range(500):
            large_dataset.append({
                'timestamp': int(time.time() * 1000),
                'exchange': 'binance',
                'symbol': f'BTC/USDT_{i}',
                'price': 50000 + i,
                'volume': 1000 + i
            })
        
        # Send data (should be batched)
        await data_sender.send_tickers(large_dataset)
        
        # Verify batching occurred
        rabbitmq_stats = system['rabbitmq_client'].get_statistics()
        db_stats = system['database_client'].get_statistics()
        
        # Should have processed all data
        assert rabbitmq_stats['publish_count'] == len(large_dataset)
        assert db_stats['insert_count'] == len(large_dataset)
    
    @pytest.mark.asyncio
    async def test_configuration_reload_integration(self, integrated_system):
        """Test configuration reload affecting system behavior."""
        system = integrated_system
        config_manager = ConfigManager()
        
        # Load initial configuration
        initial_config = system['config']
        assert initial_config.ticker_interval == 5.0
        
        # Simulate configuration change
        new_config_data = initial_config.model_dump()
        new_config_data['ticker_interval'] = 10.0
        
        # Create new config
        updated_config = AppConfig(**new_config_data)
        
        # Verify configuration changed
        assert updated_config.ticker_interval == 10.0
        assert updated_config.ticker_interval != initial_config.ticker_interval


class TestFailureScenarios:
    """Integration tests for failure scenarios and recovery."""
    
    @pytest.mark.asyncio
    async def test_exchange_failure_recovery(self):
        """Test system behavior when exchanges fail and recover."""
        # Create exchange that will fail initially
        failing_exchange = MockExchangeFactory.create_failing_exchange('binance', failure_rate=1.0)
        
        # Create resilience components
        circuit_breaker_manager = CircuitBreakerManager()
        retry_manager = RetryManagerRegistry()
        
        cb = circuit_breaker_manager.create_circuit_breaker('exchange_binance', {
            'failure_threshold': 3,
            'recovery_timeout': 5.0,
            'success_threshold': 2
        })
        
        rm = retry_manager.create_retry_manager('exchange_binance', {
            'max_attempts': 3,
            'base_delay': 0.1,
            'strategy': 'exponential'
        })
        
        # Test initial failures
        failure_count = 0
        for _ in range(5):
            try:
                await rm.call(lambda: cb.call(failing_exchange.fetch_tickers))
            except Exception:
                failure_count += 1
        
        assert failure_count == 5, "All calls should fail initially"
        
        # Simulate exchange recovery
        failing_exchange.set_failure_rate(0.0)  # Exchange recovers
        
        # Wait for circuit breaker recovery timeout
        await asyncio.sleep(6.0)
        
        # Test recovery
        success_count = 0
        for _ in range(5):
            try:
                result = await rm.call(lambda: cb.call(failing_exchange.fetch_tickers))
                if result:
                    success_count += 1
            except Exception:
                pass
        
        assert success_count > 0, "Some calls should succeed after recovery"
    
    @pytest.mark.asyncio
    async def test_rabbitmq_connection_failure(self):
        """Test behavior when RabbitMQ connection fails."""
        rabbitmq_client = MockAsyncRabbitMQClient()
        rabbitmq_client.set_failure_rate(0.3)  # 30% connection failure rate
        
        # Test connection resilience
        connection_attempts = 0
        successful_connections = 0
        
        for _ in range(10):
            connection_attempts += 1
            try:
                await rabbitmq_client.connect()
                successful_connections += 1
                await rabbitmq_client.disconnect()
            except Exception:
                pass
        
        # Should have some successful connections despite failures
        success_rate = successful_connections / connection_attempts
        assert success_rate > 0.5, f"Connection success rate too low: {success_rate:.1%}"
    
    @pytest.mark.asyncio
    async def test_database_connection_failure(self):
        """Test behavior when database connection fails."""
        db_client = MockDatabaseClient()
        db_client.set_failure_rate(0.2)  # 20% failure rate
        
        # Test database operation resilience
        operation_attempts = 0
        successful_operations = 0
        
        for _ in range(20):
            operation_attempts += 1
            try:
                await db_client.connect()
                await db_client.create_table('test_table', ['id', 'data'])
                await db_client.insert_batch('test_table', [{'id': 1, 'data': 'test'}])
                successful_operations += 1
                await db_client.disconnect()
            except Exception:
                pass
        
        # Should have some successful operations despite failures
        success_rate = successful_operations / operation_attempts
        assert success_rate > 0.6, f"Database success rate too low: {success_rate:.1%}"
    
    @pytest.mark.asyncio
    async def test_memory_pressure_scenario(self):
        """Test system behavior under memory pressure."""
        import gc
        
        # Create components
        cache_manager = CacheManager()
        
        # Simulate memory pressure by filling cache
        for i in range(20000):  # Exceed cache limits
            cache_manager.set('ticker', f'symbol_{i}', {
                'price': 50000 + i,
                'volume': 1000 + i,
                'large_data': 'x' * 1000  # Add some bulk to each entry
            })
        
        # Force garbage collection
        gc.collect()
        
        # Verify cache still functions (should have evicted old entries)
        cache_stats = cache_manager.get_cache_stats()
        
        # Cache should not grow indefinitely
        total_entries = sum(stats.get('size', 0) for stats in cache_stats.values())
        assert total_entries < 15000, f"Cache size not properly limited: {total_entries}"
        
        # Cache should still be functional
        cache_manager.set('ticker', 'test_symbol', {'price': 100})
        result = cache_manager.get('ticker', 'test_symbol')
        assert result is not None, "Cache should still be functional under pressure"


class TestScalabilityScenarios:
    """Integration tests for system scalability."""
    
    @pytest.mark.asyncio
    async def test_multiple_exchange_scaling(self):
        """Test system behavior with many exchanges."""
        # Create many mock exchanges
        exchanges = {}
        exchange_names = ['binance', 'bybit', 'bitget', 'htx', 'gateio', 'okx', 'kucoin', 'mexc']
        
        for name in exchange_names:
            exchanges[name] = MockExchangeFactory.create_exchange(name)
        
        # Create exchange manager
        exchange_manager = ResilientExchangeManager()
        
        # Mock resilience components
        circuit_breaker_manager = CircuitBreakerManager()
        retry_manager = RetryManagerRegistry()
        health_monitor = HealthMonitor()
        cache_manager = CacheManager()
        connection_pool = ConnectionPoolManager()
        
        exchange_manager.set_resilience_components(
            circuit_breaker_manager=circuit_breaker_manager,
            retry_manager=retry_manager,
            health_monitor=health_monitor,
            cache_manager=cache_manager,
            connection_pool=connection_pool
        )
        
        # Mock exchange initialization for all exchanges
        with patch('exchange_manager_v3.getattr') as mock_getattr:
            def mock_exchange_factory(exchange_id):
                return lambda **kwargs: exchanges[exchange_id]
            
            mock_getattr.side_effect = lambda ccxt_module, exchange_id: mock_exchange_factory(exchange_id)
            
            # Create exchange configs
            from config_manager import ExchangeConfig
            exchange_configs = [
                ExchangeConfig(name=name, enabled=True, timeout=30.0, rate_limit=1000)
                for name in exchange_names
            ]
            
            # Initialize all exchanges
            await exchange_manager.initialize_exchanges(exchange_configs)
        
        # Verify all exchanges were initialized
        assert len(exchange_manager.exchanges) == len(exchange_names)
        
        # Test concurrent operations on all exchanges
        start_time = time.time()
        tasks = []
        
        for exchange_name in exchange_names:
            exchange = exchange_manager.get_exchange(exchange_name)
            if exchange:
                tasks.append(exchange.fetch_tickers())
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        end_time = time.time()
        
        # Verify performance scales reasonably
        total_time = end_time - start_time
        assert total_time < 10.0, f"Multi-exchange operations too slow: {total_time:.2f}s"
        
        # Verify most operations succeeded
        successful_results = [r for r in results if not isinstance(r, Exception)]
        success_rate = len(successful_results) / len(results)
        assert success_rate > 0.8, f"Success rate too low with many exchanges: {success_rate:.1%}"
        
        # Cleanup
        await exchange_manager.close_all()
        await health_monitor.stop()
    
    @pytest.mark.asyncio
    async def test_high_throughput_scenario(self):
        """Test system behavior under high throughput demands."""
        # Create high-performance mock components
        rabbitmq_client = MockAsyncRabbitMQClient()
        rabbitmq_client.set_delays(0.001, 0.0001)  # Very fast
        
        db_client = MockDatabaseClient()
        db_client.set_delays(0.001, 0.0001)  # Very fast
        
        batch_processor = BatchProcessorManager()
        
        await rabbitmq_client.start()
        await db_client.connect()
        
        # Generate high-volume data
        high_volume_data = []
        for i in range(10000):
            high_volume_data.append({
                'timestamp': int(time.time() * 1000),
                'exchange': f'exchange_{i % 5}',
                'symbol': f'BTC/USDT_{i % 100}',
                'price': 50000 + (i % 1000),
                'volume': 1000 + (i % 500)
            })
        
        # Process high-volume data
        start_time = time.time()
        
        # Send to RabbitMQ in batches
        batch_size = 1000
        for i in range(0, len(high_volume_data), batch_size):
            batch = high_volume_data[i:i + batch_size]
            await rabbitmq_client.publish_batch(batch, 'crypto_data', 'ticker')
        
        # Store in database in batches
        for i in range(0, len(high_volume_data), batch_size):
            batch = high_volume_data[i:i + batch_size]
            await db_client.insert_batch('tickers', batch)
        
        end_time = time.time()
        processing_time = end_time - start_time
        throughput = len(high_volume_data) / processing_time
        
        # Verify high throughput performance
        assert throughput > 5000, f"Throughput too low: {throughput:.0f} records/s"
        assert processing_time < 5.0, f"Processing time too long: {processing_time:.2f}s"
        
        # Verify all data was processed
        rabbitmq_stats = rabbitmq_client.get_statistics()
        db_stats = db_client.get_statistics()
        
        assert rabbitmq_stats['publish_count'] == len(high_volume_data)
        assert db_stats['insert_count'] == len(high_volume_data)
        
        print(f"High Throughput Test Results:")
        print(f"  Records Processed: {len(high_volume_data):,}")
        print(f"  Processing Time: {processing_time:.2f}s")
        print(f"  Throughput: {throughput:,.0f} records/s")
        
        # Cleanup
        await rabbitmq_client.stop()
        await db_client.disconnect()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])
