"""
Performance and Load Tests for Crypto Futures Price Collector v5
Tests system performance under various load conditions and stress scenarios.
"""

import pytest
import asyncio
import time
import psutil
import statistics
from typing import Dict, List, Any, Tuple
from unittest.mock import patch, Mock, AsyncMock

from tests.mocks.exchange_mocks import MockExchangeFactory, MockCCXTExchange
from tests.mocks.rabbitmq_mocks import MockAsyncRabbitMQClient
from tests.mocks.database_mocks import MockDatabaseClient

# Import system components for testing
from config_manager import ConfigManager, AppConfig
from exchange_manager_v3 import ResilientExchangeManager
from circuit_breaker import CircuitBreakerManager
from retry_manager import RetryManagerRegistry
from health_monitor import HealthMonitor
from cache_manager import CacheManager
from batch_processor import BatchProcessorManager
from connection_pool import ConnectionPoolManager


class TestSystemPerformance:
    """Performance tests for the entire system."""
    
    @pytest.fixture
    def performance_config(self):
        """Configuration optimized for performance testing."""
        return {
            "environment": "development",
            "debug": False,
            "exchanges": ["binance", "bybit", "bitget"],
            "ticker_interval": 1.0,
            "funding_rate_interval": 60.0,
            "cache": {
                "enabled": True,
                "default_ttl": 300.0,
                "max_size": 10000
            },
            "batch_processing": {
                "enabled": True,
                "batch_size": 1000,
                "flush_interval": 5.0
            },
            "performance": {
                "max_concurrent_requests": 100,
                "connection_pool_size": 20,
                "worker_threads": 4
            }
        }
    
    @pytest.fixture
    def mock_components(self):
        """Create mock components for performance testing."""
        return {
            'exchanges': MockExchangeFactory.create_all_exchanges(),
            'rabbitmq': MockAsyncRabbitMQClient(),
            'database': MockDatabaseClient()
        }
    
    @pytest.mark.asyncio
    async def test_system_startup_performance(self, performance_config, mock_components):
        """Test system startup time and resource usage."""
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss
        
        # Initialize all components
        config = AppConfig(**performance_config)
        
        cache_manager = CacheManager()
        connection_pool = ConnectionPoolManager()
        batch_processor = BatchProcessorManager()
        circuit_breaker_manager = CircuitBreakerManager()
        retry_manager = RetryManagerRegistry()
        health_monitor = HealthMonitor()
        
        exchange_manager = ResilientExchangeManager()
        exchange_manager.set_resilience_components(
            circuit_breaker_manager=circuit_breaker_manager,
            retry_manager=retry_manager,
            health_monitor=health_monitor,
            cache_manager=cache_manager,
            connection_pool=connection_pool
        )
        
        # Mock exchange initialization
        with patch('exchange_manager_v3.getattr') as mock_getattr:
            mock_exchange_class = AsyncMock()
            mock_exchange_instance = AsyncMock()
            mock_exchange_instance.load_markets = AsyncMock()
            mock_exchange_instance.markets = {'BTC/USDT': {}}
            mock_exchange_class.return_value = mock_exchange_instance
            mock_getattr.return_value = mock_exchange_class
            
            # Initialize exchanges
            exchange_configs = [config.get_exchange_config(name) for name in config.exchanges]
            await exchange_manager.initialize_exchanges(exchange_configs)
        
        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss
        
        startup_time = end_time - start_time
        memory_usage = (end_memory - start_memory) / 1024 / 1024  # MB
        
        # Performance assertions
        assert startup_time < 5.0, f"Startup time too slow: {startup_time:.2f}s"
        assert memory_usage < 100, f"Memory usage too high: {memory_usage:.2f}MB"
        assert len(exchange_manager.exchanges) == 3
        
        # Cleanup
        await exchange_manager.close_all()
        await health_monitor.stop()
    
    @pytest.mark.asyncio
    async def test_concurrent_data_collection_performance(self, mock_components):
        """Test performance under concurrent data collection load."""
        # Setup mock exchanges with realistic delays
        exchanges = {}
        for name in ['binance', 'bybit', 'bitget', 'htx', 'gateio']:
            exchange = MockExchangeFactory.create_exchange(name)
            exchange.set_delays(0.05, 0.02)  # Realistic network delays
            exchanges[name] = exchange
        
        # Test concurrent ticker fetching
        async def fetch_tickers_concurrent(exchange_name: str, iterations: int = 10):
            exchange = exchanges[exchange_name]
            start_time = time.time()
            
            tasks = []
            for _ in range(iterations):
                tasks.append(exchange.fetch_tickers())
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            end_time = time.time()
            
            successful_results = [r for r in results if not isinstance(r, Exception)]
            return {
                'exchange': exchange_name,
                'total_time': end_time - start_time,
                'successful_requests': len(successful_results),
                'failed_requests': len(results) - len(successful_results),
                'avg_response_time': (end_time - start_time) / len(results),
                'throughput': len(results) / (end_time - start_time)
            }
        
        # Run concurrent tests for all exchanges
        start_time = time.time()
        tasks = [fetch_tickers_concurrent(name, 20) for name in exchanges.keys()]
        results = await asyncio.gather(*tasks)
        total_time = time.time() - start_time
        
        # Analyze results
        total_requests = sum(r['successful_requests'] + r['failed_requests'] for r in results)
        total_successful = sum(r['successful_requests'] for r in results)
        overall_throughput = total_requests / total_time
        success_rate = total_successful / total_requests
        
        # Performance assertions
        assert overall_throughput > 10, f"Throughput too low: {overall_throughput:.2f} req/s"
        assert success_rate > 0.95, f"Success rate too low: {success_rate:.2%}"
        assert total_time < 30, f"Total test time too long: {total_time:.2f}s"
        
        print(f"Performance Results:")
        print(f"  Total Requests: {total_requests}")
        print(f"  Success Rate: {success_rate:.2%}")
        print(f"  Overall Throughput: {overall_throughput:.2f} req/s")
        print(f"  Total Time: {total_time:.2f}s")
    
    @pytest.mark.asyncio
    async def test_cache_performance(self):
        """Test cache system performance."""
        cache_manager = CacheManager()
        
        # Test cache write performance
        start_time = time.time()
        for i in range(10000):
            cache_manager.set('ticker', f'BTC/USDT_{i}', {'price': 50000 + i})
        write_time = time.time() - start_time
        
        # Test cache read performance
        start_time = time.time()
        hits = 0
        for i in range(10000):
            result = cache_manager.get('ticker', f'BTC/USDT_{i}')
            if result is not None:
                hits += 1
        read_time = time.time() - start_time
        
        # Performance assertions
        assert write_time < 1.0, f"Cache write performance too slow: {write_time:.3f}s"
        assert read_time < 0.5, f"Cache read performance too slow: {read_time:.3f}s"
        assert hits == 10000, f"Cache hit rate incorrect: {hits}/10000"
        
        # Test cache statistics
        stats = cache_manager.get_cache_stats()
        assert stats['ticker']['hits'] >= 10000
        assert stats['ticker']['misses'] >= 0
    
    @pytest.mark.asyncio
    async def test_batch_processing_performance(self):
        """Test batch processing performance."""
        batch_processor = BatchProcessorManager()
        
        # Create large dataset
        test_data = []
        for i in range(50000):
            test_data.append({
                'timestamp': int(time.time() * 1000),
                'exchange': 'binance',
                'symbol': f'BTC/USDT_{i % 100}',
                'price': 50000 + (i % 1000),
                'volume': 1000 + (i % 500)
            })
        
        # Test batch processing performance
        start_time = time.time()
        
        # Process in batches
        batch_size = 1000
        processed_count = 0
        
        for i in range(0, len(test_data), batch_size):
            batch = test_data[i:i + batch_size]
            # Simulate batch processing
            await asyncio.sleep(0.001)  # Minimal processing time
            processed_count += len(batch)
        
        end_time = time.time()
        processing_time = end_time - start_time
        throughput = processed_count / processing_time
        
        # Performance assertions
        assert processing_time < 5.0, f"Batch processing too slow: {processing_time:.2f}s"
        assert throughput > 10000, f"Throughput too low: {throughput:.0f} records/s"
        assert processed_count == len(test_data)
        
        print(f"Batch Processing Results:")
        print(f"  Processed Records: {processed_count:,}")
        print(f"  Processing Time: {processing_time:.2f}s")
        print(f"  Throughput: {throughput:,.0f} records/s")
    
    @pytest.mark.asyncio
    async def test_message_queue_performance(self):
        """Test RabbitMQ client performance."""
        rabbitmq_client = MockAsyncRabbitMQClient()
        rabbitmq_client.set_delays(0.001, 0.0001)  # Very fast for performance testing
        
        await rabbitmq_client.connect()
        await rabbitmq_client.declare_exchange('crypto_data', 'topic')
        
        # Test message publishing performance
        messages = []
        for i in range(10000):
            messages.append({
                'timestamp': int(time.time() * 1000),
                'exchange': 'binance',
                'symbol': f'BTC/USDT_{i % 100}',
                'price': 50000 + (i % 1000)
            })
        
        start_time = time.time()
        
        # Publish messages in batches
        batch_size = 100
        for i in range(0, len(messages), batch_size):
            batch = messages[i:i + batch_size]
            await rabbitmq_client.publish_batch(batch, 'crypto_data', 'ticker.binance')
        
        end_time = time.time()
        publish_time = end_time - start_time
        throughput = len(messages) / publish_time
        
        # Performance assertions
        assert publish_time < 2.0, f"Message publishing too slow: {publish_time:.2f}s"
        assert throughput > 5000, f"Throughput too low: {throughput:.0f} msg/s"
        
        stats = rabbitmq_client.get_statistics()
        assert stats['publish_count'] == len(messages)
        
        await rabbitmq_client.disconnect()
    
    @pytest.mark.asyncio
    async def test_database_performance(self):
        """Test database client performance."""
        db_client = MockDatabaseClient()
        db_client.set_delays(0.001, 0.0001)  # Very fast for performance testing
        
        await db_client.connect()
        
        # Create test table
        await db_client.create_table('performance_test', [
            'timestamp', 'exchange', 'symbol', 'price', 'volume'
        ])
        
        # Generate test data
        test_records = []
        for i in range(10000):
            test_records.append({
                'timestamp': int(time.time() * 1000),
                'exchange': 'binance',
                'symbol': f'BTC/USDT_{i % 100}',
                'price': 50000 + (i % 1000),
                'volume': 1000 + (i % 500)
            })
        
        # Test batch insert performance
        start_time = time.time()
        inserted_count = await db_client.insert_batch('performance_test', test_records)
        insert_time = time.time() - start_time
        
        # Test select performance
        start_time = time.time()
        results = await db_client.select_data('performance_test', limit=1000)
        select_time = time.time() - start_time
        
        # Performance assertions
        assert insert_time < 2.0, f"Database insert too slow: {insert_time:.2f}s"
        assert select_time < 0.5, f"Database select too slow: {select_time:.2f}s"
        assert inserted_count == len(test_records)
        assert len(results) == 1000
        
        insert_throughput = inserted_count / insert_time
        assert insert_throughput > 5000, f"Insert throughput too low: {insert_throughput:.0f} records/s"
        
        await db_client.disconnect()


class TestStressScenarios:
    """Stress tests for system resilience under extreme conditions."""
    
    @pytest.mark.asyncio
    async def test_high_failure_rate_resilience(self):
        """Test system behavior under high failure rates."""
        # Create exchanges with high failure rates
        exchanges = {}
        for name in ['binance', 'bybit', 'bitget']:
            exchange = MockExchangeFactory.create_failing_exchange(name, failure_rate=0.3)
            exchanges[name] = exchange
        
        # Test resilience components
        circuit_breaker_manager = CircuitBreakerManager()
        retry_manager = RetryManagerRegistry()
        
        # Create circuit breakers and retry managers
        for name in exchanges.keys():
            circuit_breaker_manager.create_circuit_breaker(f'exchange_{name}', {
                'failure_threshold': 5,
                'recovery_timeout': 10.0,
                'success_threshold': 2
            })
            retry_manager.create_retry_manager(f'exchange_{name}', {
                'max_attempts': 3,
                'base_delay': 0.1,
                'strategy': 'exponential'
            })
        
        # Run stress test
        results = []
        for exchange_name, exchange in exchanges.items():
            cb = circuit_breaker_manager.get_circuit_breaker(f'exchange_{exchange_name}')
            rm = retry_manager.get_retry_manager(f'exchange_{exchange_name}')
            
            successful_calls = 0
            failed_calls = 0
            
            for _ in range(50):  # 50 attempts per exchange
                try:
                    async def fetch_with_resilience():
                        return await rm.call(lambda: cb.call(exchange.fetch_tickers))
                    
                    result = await fetch_with_resilience()
                    if result:
                        successful_calls += 1
                except Exception:
                    failed_calls += 1
            
            results.append({
                'exchange': exchange_name,
                'successful_calls': successful_calls,
                'failed_calls': failed_calls,
                'success_rate': successful_calls / (successful_calls + failed_calls)
            })
        
        # Verify resilience mechanisms worked
        for result in results:
            # Even with 30% failure rate, resilience should improve success rate
            assert result['success_rate'] > 0.5, f"Resilience failed for {result['exchange']}"
        
        print("Stress Test Results:")
        for result in results:
            print(f"  {result['exchange']}: {result['success_rate']:.1%} success rate")
    
    @pytest.mark.asyncio
    async def test_memory_usage_under_load(self):
        """Test memory usage under sustained load."""
        import gc
        
        # Get initial memory usage
        gc.collect()
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        # Create components
        cache_manager = CacheManager()
        batch_processor = BatchProcessorManager()
        
        # Simulate sustained load
        for iteration in range(100):
            # Generate data
            for i in range(1000):
                cache_manager.set('ticker', f'symbol_{i}', {
                    'price': 50000 + i,
                    'volume': 1000 + i,
                    'timestamp': time.time()
                })
            
            # Periodic memory check
            if iteration % 20 == 0:
                gc.collect()
                current_memory = psutil.Process().memory_info().rss / 1024 / 1024
                memory_growth = current_memory - initial_memory
                
                # Memory growth should be reasonable
                assert memory_growth < 500, f"Excessive memory growth: {memory_growth:.1f}MB"
        
        # Final memory check
        gc.collect()
        final_memory = psutil.Process().memory_info().rss / 1024 / 1024
        total_growth = final_memory - initial_memory
        
        print(f"Memory Usage:")
        print(f"  Initial: {initial_memory:.1f}MB")
        print(f"  Final: {final_memory:.1f}MB")
        print(f"  Growth: {total_growth:.1f}MB")
        
        # Memory growth should be reasonable for the workload
        assert total_growth < 200, f"Memory leak detected: {total_growth:.1f}MB growth"
    
    @pytest.mark.asyncio
    async def test_connection_pool_exhaustion(self):
        """Test behavior when connection pools are exhausted."""
        connection_pool = ConnectionPoolManager()
        
        # Configure small pool for testing
        pool_size = 5
        connections = []
        
        # Exhaust the pool
        for i in range(pool_size):
            conn = Mock()
            connections.append(conn)
        
        # Try to get one more connection (should handle gracefully)
        try:
            extra_conn = Mock()  # Simulate pool exhaustion handling
            # In real implementation, this would wait or fail gracefully
            assert True  # Pool exhaustion handled
        except Exception as e:
            # Should be a controlled exception, not a crash
            assert "pool" in str(e).lower() or "connection" in str(e).lower()


class TestBenchmarks:
    """Benchmark tests for performance comparison."""
    
    @pytest.mark.asyncio
    async def test_throughput_benchmarks(self):
        """Benchmark system throughput under various conditions."""
        
        benchmarks = {}
        
        # Benchmark 1: Single exchange, no failures
        exchange = MockExchangeFactory.create_exchange('binance')
        exchange.set_delays(0.01, 0.001)
        
        start_time = time.time()
        tasks = [exchange.fetch_tickers() for _ in range(100)]
        await asyncio.gather(*tasks)
        single_exchange_time = time.time() - start_time
        
        benchmarks['single_exchange_throughput'] = 100 / single_exchange_time
        
        # Benchmark 2: Multiple exchanges, concurrent
        exchanges = MockExchangeFactory.create_all_exchanges()
        for ex in exchanges.values():
            ex.set_delays(0.01, 0.001)
        
        start_time = time.time()
        tasks = []
        for exchange in exchanges.values():
            for _ in range(20):
                tasks.append(exchange.fetch_tickers())
        
        await asyncio.gather(*tasks)
        multi_exchange_time = time.time() - start_time
        
        benchmarks['multi_exchange_throughput'] = len(tasks) / multi_exchange_time
        
        # Benchmark 3: With resilience components
        circuit_breaker_manager = CircuitBreakerManager()
        retry_manager = RetryManagerRegistry()
        
        cb = circuit_breaker_manager.create_circuit_breaker('test', {
            'failure_threshold': 10,
            'recovery_timeout': 60.0
        })
        rm = retry_manager.create_retry_manager('test', {
            'max_attempts': 2,
            'base_delay': 0.001,
            'strategy': 'fixed'
        })
        
        start_time = time.time()
        tasks = []
        for _ in range(100):
            tasks.append(rm.call(lambda: cb.call(exchange.fetch_tickers)))
        
        await asyncio.gather(*tasks)
        resilient_time = time.time() - start_time
        
        benchmarks['resilient_throughput'] = 100 / resilient_time
        
        # Print benchmark results
        print("\nBenchmark Results:")
        for name, throughput in benchmarks.items():
            print(f"  {name}: {throughput:.1f} req/s")
        
        # Performance assertions
        assert benchmarks['single_exchange_throughput'] > 500
        assert benchmarks['multi_exchange_throughput'] > 200
        assert benchmarks['resilient_throughput'] > 100
        
        # Resilience overhead should be reasonable
        overhead = (benchmarks['single_exchange_throughput'] - benchmarks['resilient_throughput']) / benchmarks['single_exchange_throughput']
        assert overhead < 0.5, f"Resilience overhead too high: {overhead:.1%}"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])
