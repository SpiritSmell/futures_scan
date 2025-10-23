"""
Unit tests for ConfigManager - Advanced Configuration System with Pydantic v2
Tests cover validation, environment profiles, dynamic reloading, and rollback functionality.
"""

import pytest
import tempfile
import os
import json
import yaml
from pathlib import Path
from unittest.mock import patch, mock_open
from typing import Dict, Any

from config_manager import (
    ConfigManager, AppConfig, ExchangeConfig, CircuitBreakerConfig,
    RetryConfig, HealthCheckConfig, Environment, LogLevel
)


class TestConfigManager:
    """Test suite for ConfigManager functionality."""
    
    @pytest.fixture
    def temp_config_dir(self):
        """Create temporary directory for test configs."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)
    
    @pytest.fixture
    def sample_config_data(self) -> Dict[str, Any]:
        """Sample configuration data for testing."""
        return {
            "environment": "development",
            "debug": True,
            "app_name": "Test Crypto Collector",
            "version": "1.0.0",
            "exchanges": ["binance", "bybit"],
            "ticker_interval": 30.0,
            "funding_rate_interval": 300.0,
            "cache": {
                "enabled": True,
                "default_ttl": 300.0,
                "max_size": 1000
            },
            "rabbitmq": {
                "host": "localhost",
                "port": 5672,
                "user": "test_user",
                "password": "test_password"
            },
            "exchange_configs": {
                "binance": {
                    "name": "binance",
                    "enabled": True,
                    "timeout": 30.0,
                    "rate_limit": 1200
                }
            }
        }
    
    def test_config_manager_initialization(self):
        """Test ConfigManager initialization."""
        manager = ConfigManager()
        assert manager.config is None
        assert manager.snapshots == []
        assert manager.max_snapshots == 10
    
    def test_load_config_from_json(self, temp_config_dir, sample_config_data):
        """Test loading configuration from JSON file."""
        config_file = temp_config_dir / "test_config.json"
        with open(config_file, 'w') as f:
            json.dump(sample_config_data, f)
        
        manager = ConfigManager(str(config_file))
        config = manager.load_config()
        
        assert isinstance(config, AppConfig)
        assert config.environment == Environment.DEVELOPMENT
        assert config.debug is True
        assert config.app_name == "Test Crypto Collector"
        assert len(config.exchanges) == 2
        assert "binance" in config.exchanges
        assert "bybit" in config.exchanges
    
    def test_load_config_from_yaml(self, temp_config_dir, sample_config_data):
        """Test loading configuration from YAML file."""
        config_file = temp_config_dir / "test_config.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(sample_config_data, f)
        
        manager = ConfigManager(str(config_file))
        config = manager.load_config()
        
        assert isinstance(config, AppConfig)
        assert config.environment == Environment.DEVELOPMENT
        assert config.cache.enabled is True
        assert config.rabbitmq.host == "localhost"
    
    def test_config_validation_success(self, sample_config_data):
        """Test successful configuration validation."""
        config = AppConfig(**sample_config_data)
        
        assert config.ticker_interval == 30.0
        assert config.funding_rate_interval == 300.0
        assert config.cache.max_size == 1000
        assert config.rabbitmq.port == 5672
    
    def test_config_validation_failures(self):
        """Test configuration validation failures."""
        # Test invalid ticker interval
        with pytest.raises(ValueError):
            AppConfig(ticker_interval=-1.0)
        
        # Test invalid port
        with pytest.raises(ValueError):
            AppConfig(rabbitmq={"port": 70000})
        
        # Test invalid log level
        with pytest.raises(ValueError):
            AppConfig(logging={"level": "INVALID_LEVEL"})
    
    def test_environment_overrides(self, temp_config_dir, sample_config_data):
        """Test environment-specific configuration overrides."""
        # Create base config
        base_config = temp_config_dir / "test_config.yaml"
        with open(base_config, 'w') as f:
            yaml.dump(sample_config_data, f)
        
        # Create environment override
        prod_override = {
            "environment": "production",
            "debug": False,
            "logging": {"level": "WARNING"},
            "ticker_interval": 15.0
        }
        env_config = temp_config_dir / "test_config.production.yaml"
        with open(env_config, 'w') as f:
            yaml.dump(prod_override, f)
        
        manager = ConfigManager(str(base_config))
        config = manager.load_config(environment="production")
        
        assert config.environment == Environment.PRODUCTION
        assert config.debug is False
        assert config.logging.level == LogLevel.WARNING
        assert config.ticker_interval == 15.0
        # Base config values should still be present
        assert len(config.exchanges) == 2
    
    @patch.dict(os.environ, {
        'CRYPTO_COLLECTOR_DEBUG': 'false',
        'CRYPTO_COLLECTOR_TICKER_INTERVAL': '60.0',
        'CRYPTO_COLLECTOR_RABBITMQ__HOST': 'prod-rabbitmq.com',
        'CRYPTO_COLLECTOR_EXCHANGES': '["binance","bybit","bitget"]'
    })
    def test_environment_variables(self, temp_config_dir, sample_config_data):
        """Test environment variable overrides."""
        config_file = temp_config_dir / "test_config.json"
        with open(config_file, 'w') as f:
            json.dump(sample_config_data, f)
        
        manager = ConfigManager(str(config_file))
        config = manager.load_config()
        
        assert config.debug is False
        assert config.ticker_interval == 60.0
        assert config.rabbitmq.host == "prod-rabbitmq.com"
        assert len(config.exchanges) == 3
        assert "bitget" in config.exchanges
    
    def test_config_snapshots(self, temp_config_dir, sample_config_data):
        """Test configuration snapshot functionality."""
        config_file = temp_config_dir / "test_config.json"
        with open(config_file, 'w') as f:
            json.dump(sample_config_data, f)
        
        manager = ConfigManager(str(config_file))
        manager.load_config()
        
        # Should have one snapshot after loading
        assert len(manager.snapshots) == 1
        assert manager.snapshots[0].environment == "development"
        
        # Load with different environment
        manager.load_config(environment="production")
        assert len(manager.snapshots) == 2
    
    def test_config_rollback(self, temp_config_dir, sample_config_data):
        """Test configuration rollback functionality."""
        config_file = temp_config_dir / "test_config.json"
        with open(config_file, 'w') as f:
            json.dump(sample_config_data, f)
        
        manager = ConfigManager(str(config_file))
        
        # Load initial config
        config1 = manager.load_config()
        assert config1.environment == Environment.DEVELOPMENT
        
        # Load different environment
        config2 = manager.load_config(environment="production")
        assert config2.environment == Environment.PRODUCTION
        
        # Rollback to previous
        success = manager.rollback_config(steps=1)
        assert success is True
        assert manager.config.environment == Environment.DEVELOPMENT
    
    def test_config_reload_detection(self, temp_config_dir, sample_config_data):
        """Test configuration file change detection."""
        config_file = temp_config_dir / "test_config.json"
        with open(config_file, 'w') as f:
            json.dump(sample_config_data, f)
        
        manager = ConfigManager(str(config_file))
        manager.load_config()
        
        # Modify config file
        modified_config = sample_config_data.copy()
        modified_config["ticker_interval"] = 45.0
        with open(config_file, 'w') as f:
            json.dump(modified_config, f)
        
        # Should detect change and reload
        reloaded = manager.reload_config()
        assert reloaded is True
        assert manager.config.ticker_interval == 45.0
    
    def test_get_exchange_config(self, sample_config_data):
        """Test getting exchange-specific configuration."""
        config = AppConfig(**sample_config_data)
        
        # Get existing exchange config
        binance_config = config.get_exchange_config("binance")
        assert binance_config.name == "binance"
        assert binance_config.enabled is True
        assert binance_config.timeout == 30.0
        
        # Get non-existing exchange (should create default)
        htx_config = config.get_exchange_config("htx")
        assert htx_config.name == "htx"
        assert htx_config.enabled is True  # default value
    
    def test_config_summary(self, temp_config_dir, sample_config_data):
        """Test configuration summary generation."""
        config_file = temp_config_dir / "test_config.json"
        with open(config_file, 'w') as f:
            json.dump(sample_config_data, f)
        
        manager = ConfigManager(str(config_file))
        manager.load_config()
        
        summary = manager.get_config_summary()
        assert summary["environment"] == "development"
        assert summary["exchanges_count"] == 2
        assert "binance" in summary["exchanges"]
        assert "bybit" in summary["exchanges"]
        assert summary["snapshots_count"] == 1


class TestExchangeConfig:
    """Test suite for ExchangeConfig validation."""
    
    def test_valid_exchange_config(self):
        """Test valid exchange configuration."""
        config = ExchangeConfig(
            name="binance",
            enabled=True,
            timeout=30.0,
            rate_limit=1200
        )
        
        assert config.name == "binance"
        assert config.enabled is True
        assert config.timeout == 30.0
        assert config.rate_limit == 1200
        assert config.sandbox is False  # default
    
    def test_exchange_config_validation(self):
        """Test exchange configuration validation."""
        # Test invalid timeout
        with pytest.raises(ValueError):
            ExchangeConfig(name="test", timeout=0.5)  # Below minimum
        
        # Test invalid rate limit
        with pytest.raises(ValueError):
            ExchangeConfig(name="test", rate_limit=50)  # Below minimum
        
        # Test empty name
        with pytest.raises(ValueError):
            ExchangeConfig(name="")


class TestCircuitBreakerConfig:
    """Test suite for CircuitBreakerConfig validation."""
    
    def test_valid_circuit_breaker_config(self):
        """Test valid circuit breaker configuration."""
        config = CircuitBreakerConfig(
            failure_threshold=5,
            recovery_timeout=60.0,
            success_threshold=3,
            timeout=30.0
        )
        
        assert config.failure_threshold == 5
        assert config.recovery_timeout == 60.0
        assert config.success_threshold == 3
        assert config.timeout == 30.0
    
    def test_circuit_breaker_validation(self):
        """Test circuit breaker configuration validation."""
        # Test invalid failure threshold
        with pytest.raises(ValueError):
            CircuitBreakerConfig(failure_threshold=0)
        
        # Test invalid recovery timeout
        with pytest.raises(ValueError):
            CircuitBreakerConfig(recovery_timeout=0.5)


class TestRetryConfig:
    """Test suite for RetryConfig validation."""
    
    def test_valid_retry_config(self):
        """Test valid retry configuration."""
        config = RetryConfig(
            max_attempts=3,
            base_delay=1.0,
            max_delay=30.0,
            strategy="adaptive"
        )
        
        assert config.max_attempts == 3
        assert config.base_delay == 1.0
        assert config.max_delay == 30.0
        assert config.strategy == "adaptive"
    
    def test_retry_config_validation(self):
        """Test retry configuration validation."""
        # Test invalid strategy
        with pytest.raises(ValueError):
            RetryConfig(strategy="invalid_strategy")
        
        # Test invalid max attempts
        with pytest.raises(ValueError):
            RetryConfig(max_attempts=0)


class TestHealthCheckConfig:
    """Test suite for HealthCheckConfig validation."""
    
    def test_valid_health_check_config(self):
        """Test valid health check configuration."""
        config = HealthCheckConfig(
            check_interval=120.0,
            timeout=30.0,
            failure_threshold=3,
            recovery_threshold=2
        )
        
        assert config.check_interval == 120.0
        assert config.timeout == 30.0
        assert config.failure_threshold == 3
        assert config.recovery_threshold == 2
    
    def test_health_check_validation(self):
        """Test health check configuration validation."""
        # Test invalid check interval
        with pytest.raises(ValueError):
            HealthCheckConfig(check_interval=5.0)  # Below minimum
        
        # Test invalid failure threshold
        with pytest.raises(ValueError):
            HealthCheckConfig(failure_threshold=0)


# Performance tests
class TestConfigPerformance:
    """Performance tests for configuration system."""
    
    def test_config_loading_performance(self, tmp_path, sample_config_data):
        """Test configuration loading performance."""
        import time
        
        config_file = tmp_path / "perf_test_config.json"
        with open(config_file, 'w') as f:
            json.dump(sample_config_data, f)
        
        manager = ConfigManager(str(config_file))
        
        start_time = time.time()
        for _ in range(100):
            manager.load_config()
        end_time = time.time()
        
        avg_time = (end_time - start_time) / 100
        assert avg_time < 0.01  # Should load in less than 10ms on average
    
    def test_validation_performance(self, sample_config_data):
        """Test configuration validation performance."""
        import time
        
        start_time = time.time()
        for _ in range(1000):
            AppConfig(**sample_config_data)
        end_time = time.time()
        
        avg_time = (end_time - start_time) / 1000
        assert avg_time < 0.001  # Should validate in less than 1ms on average


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
