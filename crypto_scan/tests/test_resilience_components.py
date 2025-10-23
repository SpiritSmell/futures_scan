"""
Unit tests for Resilience Components: Circuit Breaker, Retry Manager, Health Monitor
Tests cover failure detection, recovery mechanisms, and adaptive behavior.
"""

import pytest
import asyncio
import time
from unittest.mock import Mock, AsyncMock, patch
from typing import Dict, Any

from circuit_breaker import CircuitBreakerManager, CircuitBreaker, CircuitState
from retry_manager import RetryManagerRegistry, RetryManager, RetryStrategy
from health_monitor import HealthMonitor, HealthCheck, HealthStatus


class TestCircuitBreakerManager:
    """Test suite for CircuitBreakerManager functionality."""
    
    def test_circuit_breaker_manager_initialization(self):
        """Test CircuitBreakerManager initialization."""
        manager = CircuitBreakerManager()
        
        assert manager.circuit_breakers == {}
        assert manager.logger is not None
    
    def test_create_circuit_breaker(self):
        """Test creating a circuit breaker."""
        manager = CircuitBreakerManager()
        
        config = {
            'failure_threshold': 5,
            'recovery_timeout': 60.0,
            'success_threshold': 3,
            'timeout': 30.0
        }
        
        cb = manager.create_circuit_breaker('test_service', config)
        
        assert isinstance(cb, CircuitBreaker)
        assert cb.name == 'test_service'
        assert cb.failure_threshold == 5
        assert cb.recovery_timeout == 60.0
        assert cb.success_threshold == 3
        assert cb.timeout == 30.0
        assert cb.state == CircuitState.CLOSED
    
    def test_get_circuit_breaker(self):
        """Test getting an existing circuit breaker."""
        manager = CircuitBreakerManager()
        
        config = {'failure_threshold': 3, 'recovery_timeout': 30.0}
        cb = manager.create_circuit_breaker('test_service', config)
        
        retrieved_cb = manager.get_circuit_breaker('test_service')
        assert retrieved_cb == cb
        
        # Test non-existent circuit breaker
        non_existent = manager.get_circuit_breaker('non_existent')
        assert non_existent is None
    
    def test_get_all_states(self):
        """Test getting states of all circuit breakers."""
        manager = CircuitBreakerManager()
        
        config = {'failure_threshold': 3, 'recovery_timeout': 30.0}
        manager.create_circuit_breaker('service1', config)
        manager.create_circuit_breaker('service2', config)
        
        states = manager.get_all_states()
        
        assert len(states) == 2
        assert 'service1' in states
        assert 'service2' in states
        assert states['service1'] == CircuitState.CLOSED
        assert states['service2'] == CircuitState.CLOSED


class TestCircuitBreaker:
    """Test suite for CircuitBreaker functionality."""
    
    @pytest.fixture
    def circuit_breaker(self):
        """Create a circuit breaker for testing."""
        return CircuitBreaker(
            name='test_cb',
            failure_threshold=3,
            recovery_timeout=10.0,
            success_threshold=2,
            timeout=5.0
        )
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_closed_state_success(self, circuit_breaker):
        """Test circuit breaker in closed state with successful calls."""
        async def successful_operation():
            return "success"
        
        result = await circuit_breaker.call(successful_operation)
        
        assert result == "success"
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0
        assert circuit_breaker.success_count == 1
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_failure_threshold(self, circuit_breaker):
        """Test circuit breaker opening after failure threshold."""
        async def failing_operation():
            raise Exception("Test failure")
        
        # Execute failures up to threshold
        for i in range(3):
            with pytest.raises(Exception):
                await circuit_breaker.call(failing_operation)
        
        assert circuit_breaker.state == CircuitState.OPEN
        assert circuit_breaker.failure_count == 3
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_open_state_rejection(self, circuit_breaker):
        """Test circuit breaker rejecting calls in open state."""
        # Force circuit breaker to open state
        circuit_breaker.state = CircuitState.OPEN
        circuit_breaker.last_failure_time = time.time()
        
        async def any_operation():
            return "should not execute"
        
        with pytest.raises(Exception, match="Circuit breaker is OPEN"):
            await circuit_breaker.call(any_operation)
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_half_open_recovery(self, circuit_breaker):
        """Test circuit breaker recovery through half-open state."""
        # Force to open state
        circuit_breaker.state = CircuitState.OPEN
        circuit_breaker.last_failure_time = time.time() - 15.0  # Past recovery timeout
        
        async def successful_operation():
            return "success"
        
        # First call should transition to half-open
        result = await circuit_breaker.call(successful_operation)
        assert result == "success"
        assert circuit_breaker.state == CircuitState.HALF_OPEN
        
        # Second successful call should close the circuit
        result = await circuit_breaker.call(successful_operation)
        assert result == "success"
        assert circuit_breaker.state == CircuitState.CLOSED
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_timeout_handling(self, circuit_breaker):
        """Test circuit breaker timeout handling."""
        async def slow_operation():
            await asyncio.sleep(10)  # Longer than timeout
            return "should timeout"
        
        with pytest.raises(asyncio.TimeoutError):
            await circuit_breaker.call(slow_operation)
        
        assert circuit_breaker.failure_count == 1
    
    def test_circuit_breaker_reset(self, circuit_breaker):
        """Test circuit breaker reset functionality."""
        # Set some state
        circuit_breaker.failure_count = 5
        circuit_breaker.success_count = 3
        circuit_breaker.state = CircuitState.OPEN
        
        circuit_breaker.reset()
        
        assert circuit_breaker.failure_count == 0
        assert circuit_breaker.success_count == 0
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.last_failure_time is None


class TestRetryManagerRegistry:
    """Test suite for RetryManagerRegistry functionality."""
    
    def test_retry_manager_registry_initialization(self):
        """Test RetryManagerRegistry initialization."""
        registry = RetryManagerRegistry()
        
        assert registry.retry_managers == {}
        assert registry.logger is not None
    
    def test_create_retry_manager(self):
        """Test creating a retry manager."""
        registry = RetryManagerRegistry()
        
        config = {
            'max_attempts': 3,
            'base_delay': 1.0,
            'max_delay': 30.0,
            'strategy': 'exponential'
        }
        
        rm = registry.create_retry_manager('test_service', config)
        
        assert isinstance(rm, RetryManager)
        assert rm.name == 'test_service'
        assert rm.max_attempts == 3
        assert rm.base_delay == 1.0
        assert rm.max_delay == 30.0
        assert rm.strategy == RetryStrategy.EXPONENTIAL
    
    def test_get_retry_manager(self):
        """Test getting an existing retry manager."""
        registry = RetryManagerRegistry()
        
        config = {'max_attempts': 3, 'base_delay': 1.0, 'strategy': 'linear'}
        rm = registry.create_retry_manager('test_service', config)
        
        retrieved_rm = registry.get_retry_manager('test_service')
        assert retrieved_rm == rm
        
        # Test non-existent retry manager
        non_existent = registry.get_retry_manager('non_existent')
        assert non_existent is None


class TestRetryManager:
    """Test suite for RetryManager functionality."""
    
    @pytest.fixture
    def retry_manager(self):
        """Create a retry manager for testing."""
        return RetryManager(
            name='test_rm',
            max_attempts=3,
            base_delay=0.1,  # Short delay for testing
            max_delay=1.0,
            strategy=RetryStrategy.EXPONENTIAL
        )
    
    @pytest.mark.asyncio
    async def test_retry_manager_success_first_attempt(self, retry_manager):
        """Test successful operation on first attempt."""
        call_count = 0
        
        async def successful_operation():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = await retry_manager.call(successful_operation)
        
        assert result == "success"
        assert call_count == 1
        assert retry_manager.total_attempts == 1
        assert retry_manager.successful_attempts == 1
    
    @pytest.mark.asyncio
    async def test_retry_manager_success_after_retries(self, retry_manager):
        """Test successful operation after some retries."""
        call_count = 0
        
        async def eventually_successful_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception(f"Attempt {call_count} failed")
            return "success"
        
        result = await retry_manager.call(eventually_successful_operation)
        
        assert result == "success"
        assert call_count == 3
        assert retry_manager.total_attempts == 3
        assert retry_manager.successful_attempts == 1
    
    @pytest.mark.asyncio
    async def test_retry_manager_max_attempts_exceeded(self, retry_manager):
        """Test retry manager when max attempts are exceeded."""
        call_count = 0
        
        async def always_failing_operation():
            nonlocal call_count
            call_count += 1
            raise Exception(f"Attempt {call_count} failed")
        
        with pytest.raises(Exception, match="Attempt 3 failed"):
            await retry_manager.call(always_failing_operation)
        
        assert call_count == 3
        assert retry_manager.total_attempts == 3
        assert retry_manager.failed_attempts == 1
    
    @pytest.mark.asyncio
    async def test_retry_delay_calculation(self, retry_manager):
        """Test retry delay calculation for different strategies."""
        # Test exponential backoff
        delay1 = retry_manager._calculate_delay(1)
        delay2 = retry_manager._calculate_delay(2)
        delay3 = retry_manager._calculate_delay(3)
        
        assert delay1 == 0.1  # base_delay
        assert delay2 == 0.2  # base_delay * 2
        assert delay3 == 0.4  # base_delay * 4
        
        # Test max delay cap
        delay_large = retry_manager._calculate_delay(10)
        assert delay_large <= retry_manager.max_delay
    
    def test_retry_manager_statistics(self, retry_manager):
        """Test retry manager statistics."""
        retry_manager.total_attempts = 10
        retry_manager.successful_attempts = 7
        retry_manager.failed_attempts = 3
        
        stats = retry_manager.get_statistics()
        
        assert stats['total_attempts'] == 10
        assert stats['successful_attempts'] == 7
        assert stats['failed_attempts'] == 3
        assert stats['success_rate'] == 0.7
        assert stats['failure_rate'] == 0.3


class TestHealthMonitor:
    """Test suite for HealthMonitor functionality."""
    
    def test_health_monitor_initialization(self):
        """Test HealthMonitor initialization."""
        monitor = HealthMonitor()
        
        assert monitor.health_checks == {}
        assert monitor.is_running is False
        assert monitor.logger is not None
    
    def test_add_health_check(self):
        """Test adding a health check."""
        monitor = HealthMonitor()
        
        config = {
            'check_interval': 60.0,
            'timeout': 30.0,
            'failure_threshold': 3,
            'recovery_threshold': 2
        }
        
        hc = monitor.add_health_check('test_service', config)
        
        assert isinstance(hc, HealthCheck)
        assert hc.name == 'test_service'
        assert hc.check_interval == 60.0
        assert hc.timeout == 30.0
        assert hc.failure_threshold == 3
        assert hc.recovery_threshold == 2
        assert hc.status == HealthStatus.UNKNOWN
    
    def test_get_health_check(self):
        """Test getting an existing health check."""
        monitor = HealthMonitor()
        
        config = {'check_interval': 60.0, 'timeout': 30.0}
        hc = monitor.add_health_check('test_service', config)
        
        retrieved_hc = monitor.get_health_check('test_service')
        assert retrieved_hc == hc
        
        # Test non-existent health check
        non_existent = monitor.get_health_check('non_existent')
        assert non_existent is None
    
    def test_get_all_health_status(self):
        """Test getting health status of all checks."""
        monitor = HealthMonitor()
        
        config = {'check_interval': 60.0, 'timeout': 30.0}
        hc1 = monitor.add_health_check('service1', config)
        hc2 = monitor.add_health_check('service2', config)
        
        hc1.status = HealthStatus.HEALTHY
        hc2.status = HealthStatus.UNHEALTHY
        
        statuses = monitor.get_all_health_status()
        
        assert len(statuses) == 2
        assert statuses['service1'] == HealthStatus.HEALTHY
        assert statuses['service2'] == HealthStatus.UNHEALTHY
    
    @pytest.mark.asyncio
    async def test_health_monitor_start_stop(self):
        """Test starting and stopping health monitor."""
        monitor = HealthMonitor()
        
        config = {'check_interval': 0.1, 'timeout': 30.0}  # Short interval for testing
        monitor.add_health_check('test_service', config)
        
        # Start monitoring
        await monitor.start()
        assert monitor.is_running is True
        
        # Let it run briefly
        await asyncio.sleep(0.2)
        
        # Stop monitoring
        await monitor.stop()
        assert monitor.is_running is False


class TestHealthCheck:
    """Test suite for HealthCheck functionality."""
    
    @pytest.fixture
    def health_check(self):
        """Create a health check for testing."""
        return HealthCheck(
            name='test_hc',
            check_interval=60.0,
            timeout=30.0,
            failure_threshold=3,
            recovery_threshold=2
        )
    
    def test_health_check_initialization(self, health_check):
        """Test HealthCheck initialization."""
        assert health_check.name == 'test_hc'
        assert health_check.check_interval == 60.0
        assert health_check.timeout == 30.0
        assert health_check.failure_threshold == 3
        assert health_check.recovery_threshold == 2
        assert health_check.status == HealthStatus.UNKNOWN
        assert health_check.consecutive_failures == 0
        assert health_check.consecutive_successes == 0
        assert health_check.is_running is False
    
    def test_record_success(self, health_check):
        """Test recording successful health check."""
        # Set initial failure state
        health_check.consecutive_failures = 2
        health_check.status = HealthStatus.UNHEALTHY
        
        health_check.record_success()
        
        assert health_check.consecutive_failures == 0
        assert health_check.consecutive_successes == 1
        assert health_check.total_checks == 1
        assert health_check.successful_checks == 1
        # Should still be unhealthy until recovery threshold
        assert health_check.status == HealthStatus.UNHEALTHY
        
        # Record another success to reach recovery threshold
        health_check.record_success()
        assert health_check.status == HealthStatus.HEALTHY
    
    def test_record_failure(self, health_check):
        """Test recording failed health check."""
        # Set initial healthy state
        health_check.consecutive_successes = 2
        health_check.status = HealthStatus.HEALTHY
        
        health_check.record_failure(Exception("Test error"))
        
        assert health_check.consecutive_successes == 0
        assert health_check.consecutive_failures == 1
        assert health_check.total_checks == 1
        assert health_check.failed_checks == 1
        assert health_check.last_error is not None
        # Should still be healthy until failure threshold
        assert health_check.status == HealthStatus.HEALTHY
        
        # Record more failures to reach threshold
        for _ in range(2):
            health_check.record_failure(Exception("Test error"))
        
        assert health_check.status == HealthStatus.UNHEALTHY
    
    def test_health_check_statistics(self, health_check):
        """Test health check statistics."""
        health_check.total_checks = 10
        health_check.successful_checks = 8
        health_check.failed_checks = 2
        
        stats = health_check.get_statistics()
        
        assert stats['total_checks'] == 10
        assert stats['successful_checks'] == 8
        assert stats['failed_checks'] == 2
        assert stats['success_rate'] == 0.8
        assert stats['failure_rate'] == 0.2
        assert stats['status'] == HealthStatus.UNKNOWN
    
    @pytest.mark.asyncio
    async def test_health_check_start_stop(self, health_check):
        """Test starting and stopping health check."""
        # Mock check function
        async def mock_check():
            return True
        
        health_check.check_function = mock_check
        health_check.check_interval = 0.1  # Short interval for testing
        
        # Start health check
        await health_check.start()
        assert health_check.is_running is True
        
        # Let it run briefly
        await asyncio.sleep(0.2)
        
        # Stop health check
        await health_check.stop()
        assert health_check.is_running is False


# Performance tests for resilience components
class TestResiliencePerformance:
    """Performance tests for resilience components."""
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_performance(self):
        """Test circuit breaker performance under load."""
        cb = CircuitBreaker(
            name='perf_test',
            failure_threshold=100,
            recovery_timeout=60.0,
            timeout=1.0
        )
        
        async def fast_operation():
            return "success"
        
        start_time = time.time()
        
        # Execute many operations
        tasks = [cb.call(fast_operation) for _ in range(1000)]
        results = await asyncio.gather(*tasks)
        
        end_time = time.time()
        
        assert len(results) == 1000
        assert all(r == "success" for r in results)
        assert end_time - start_time < 1.0  # Should complete in less than 1 second
    
    @pytest.mark.asyncio
    async def test_retry_manager_performance(self):
        """Test retry manager performance."""
        rm = RetryManager(
            name='perf_test',
            max_attempts=1,  # No retries for performance test
            base_delay=0.001,
            strategy=RetryStrategy.FIXED
        )
        
        async def fast_operation():
            return "success"
        
        start_time = time.time()
        
        # Execute many operations
        tasks = [rm.call(fast_operation) for _ in range(1000)]
        results = await asyncio.gather(*tasks)
        
        end_time = time.time()
        
        assert len(results) == 1000
        assert all(r == "success" for r in results)
        assert end_time - start_time < 1.0  # Should complete in less than 1 second


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
