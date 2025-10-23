"""
Advanced Configuration Manager v5 - Structured configuration with Pydantic validation.
Supports multiple formats (YAML, JSON, TOML), environment profiles, and dynamic reloading.
"""

import os
import json
import logging
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from enum import Enum

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

try:
    import toml
    TOML_AVAILABLE = True
except ImportError:
    TOML_AVAILABLE = False

from pydantic import BaseModel, Field, field_validator, model_validator
try:
    from pydantic_settings import BaseSettings
except ImportError:
    from pydantic import BaseSettings

logger = logging.getLogger(__name__)


class Environment(str, Enum):
    """Supported environment types."""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TESTING = "testing"


class LogLevel(str, Enum):
    """Supported log levels."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class CircuitBreakerConfig(BaseModel):
    """Circuit Breaker configuration."""
    failure_threshold: int = Field(default=5, ge=1, le=100)
    recovery_timeout: float = Field(default=60.0, ge=1.0, le=3600.0)
    success_threshold: int = Field(default=3, ge=1, le=20)
    timeout: float = Field(default=30.0, ge=1.0, le=300.0)
    max_failure_threshold: int = Field(default=15, ge=1, le=200)
    backoff_multiplier: float = Field(default=1.5, ge=1.0, le=5.0)
    max_recovery_timeout: float = Field(default=300.0, ge=60.0, le=7200.0)


class RetryConfig(BaseModel):
    """Retry mechanism configuration."""
    max_attempts: int = Field(default=3, ge=1, le=10)
    base_delay: float = Field(default=1.0, ge=0.1, le=60.0)
    max_delay: float = Field(default=30.0, ge=1.0, le=300.0)
    strategy: str = Field(default="adaptive", pattern="^(fixed|linear|exponential|fibonacci|adaptive)$")
    backoff_multiplier: float = Field(default=2.0, ge=1.0, le=10.0)
    jitter: bool = Field(default=True)


class HealthCheckConfig(BaseModel):
    """Health check configuration."""
    check_interval: float = Field(default=120.0, ge=10.0, le=3600.0)
    timeout: float = Field(default=30.0, ge=1.0, le=300.0)
    failure_threshold: int = Field(default=3, ge=1, le=20)
    recovery_threshold: int = Field(default=2, ge=1, le=10)
    adaptive_scaling: bool = Field(default=True)


class ExchangeApiConfig(BaseModel):
    """Exchange API configuration."""
    apiKey: str = Field(default="")
    secret: str = Field(default="")
    password: Optional[str] = Field(default=None)
    uid: Optional[str] = Field(default=None)
    
    model_config = {
        "str_strip_whitespace": True,
        "validate_assignment": True
    }


class ExchangeConfig(BaseModel):
    """Individual exchange configuration."""
    name: str = Field(..., min_length=1)
    enabled: bool = Field(default=True)
    timeout: float = Field(default=30.0, ge=1.0, le=300.0)
    rate_limit: Optional[int] = Field(default=None, ge=100, le=10000)
    sandbox: bool = Field(default=False)
    
    # Resilience components
    circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    health_check: HealthCheckConfig = Field(default_factory=HealthCheckConfig)


class CacheConfig(BaseModel):
    """Cache configuration."""
    enabled: bool = Field(default=True)
    default_ttl: float = Field(default=300.0, ge=1.0, le=3600.0)
    max_size: int = Field(default=10000, ge=100, le=100000)
    cleanup_interval: float = Field(default=60.0, ge=10.0, le=600.0)


class BatchConfig(BaseModel):
    """Batch processing configuration."""
    enabled: bool = Field(default=True)
    max_size: int = Field(default=100, ge=1, le=10000)
    max_wait_time: float = Field(default=5.0, ge=0.1, le=60.0)
    strategy: str = Field(default="size_based", pattern="^(size_based|time_based|adaptive|priority_based)$")
    compression_enabled: bool = Field(default=True)


class RabbitMQConfig(BaseModel):
    """RabbitMQ configuration."""
    host: str = Field(default="localhost")
    port: int = Field(default=5672, ge=1, le=65535)
    user: str = Field(default="rmuser")
    password: str = Field(default="rmpassword")
    vhost: str = Field(default="/")
    exchange: str = Field(default="futures_price_collector")
    out_exchange: str = Field(default="futures_price_collector_out")
    
    class Config:
        # Скрываем пароль в логах
        json_encoders = {
            str: lambda v: "***" if "password" in str(v).lower() else v
        }


class DatabaseConfig(BaseModel):
    """Database configuration."""
    enabled: bool = Field(default=False)
    host: str = Field(default="localhost")
    port: int = Field(default=9000, ge=1, le=65535)
    database: str = Field(default="crypto_data")
    user: str = Field(default="default")
    password: str = Field(default="")
    
    model_config = {
        "str_strip_whitespace": True,
        "validate_assignment": True
    }


class PerformanceConfig(BaseModel):
    """Performance and monitoring configuration."""
    enable_metrics: bool = Field(default=True)
    metrics_interval: float = Field(default=60.0, ge=10.0, le=600.0)
    enable_profiling: bool = Field(default=False)
    max_memory_usage: int = Field(default=2048, ge=256, le=16384)  # MB
    max_cpu_usage: float = Field(default=80.0, ge=10.0, le=100.0)  # %


class LoggingConfig(BaseModel):
    """Logging configuration."""
    level: LogLevel = Field(default=LogLevel.INFO)
    format: str = Field(default="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_enabled: bool = Field(default=True)
    file_path: str = Field(default="logs/crypto_collector.log")
    console_enabled: bool = Field(default=True)
    max_file_size: int = Field(default=100, ge=1, le=1000)  # MB
    backup_count: int = Field(default=5, ge=1, le=20)


class AppConfig(BaseSettings):
    """Main application configuration with Pydantic validation."""
    
    # Environment and basic settings
    environment: Environment = Field(default=Environment.DEVELOPMENT)
    debug: bool = Field(default=False)
    app_name: str = Field(default="Crypto Futures Collector v4")
    version: str = Field(default="4.0.0")
    
    # Exchange settings
    exchanges: List[str] = Field(default_factory=lambda: ["binance", "bybit", "bitget"])
    api_keys: Dict[str, ExchangeApiConfig] = Field(default_factory=dict)
    exchange_configs: Dict[str, ExchangeConfig] = Field(default_factory=dict)
    
    # Data collection settings
    ticker_interval: float = Field(default=30.0, ge=1.0, le=300.0)
    funding_rate_interval: float = Field(default=300.0, ge=60.0, le=3600.0)
    enable_change_detection: bool = Field(default=True)
    
    # Component configurations
    cache: CacheConfig = Field(default_factory=CacheConfig)
    batch: BatchConfig = Field(default_factory=BatchConfig)
    rabbitmq: RabbitMQConfig = Field(default_factory=RabbitMQConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    performance: PerformanceConfig = Field(default_factory=PerformanceConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "env_nested_delimiter": "__",
        "case_sensitive": False,
        "extra": "ignore"
    }
    
    @field_validator('exchanges')
    @classmethod
    def validate_exchanges(cls, v):
        """Validate exchange names."""
        supported_exchanges = {
            'binance', 'bybit', 'bitget', 'htx', 'gateio', 'okx', 'kucoin'
        }
        for exchange in v:
            if exchange not in supported_exchanges:
                logger.warning(f"Exchange '{exchange}' may not be supported")
        return v
    
    @model_validator(mode='after')
    def validate_intervals(self):
        """Validate that intervals make sense."""
        if self.ticker_interval > self.funding_rate_interval:
            logger.warning("Ticker interval is greater than funding rate interval")
        return self
    
    def get_exchange_config(self, exchange_name: str) -> ExchangeConfig:
        """Get configuration for specific exchange."""
        if exchange_name in self.exchange_configs:
            return self.exchange_configs[exchange_name]
        
        # Create default config
        config = ExchangeConfig(name=exchange_name)
        self.exchange_configs[exchange_name] = config
        return config
    
    def get_api_config(self, exchange_name: str) -> ExchangeApiConfig:
        """Get API configuration for specific exchange."""
        if exchange_name in self.api_keys:
            return self.api_keys[exchange_name]
        
        # Create default config
        config = ExchangeApiConfig()
        self.api_keys[exchange_name] = config
        return config


@dataclass
class ConfigSnapshot:
    """Configuration snapshot for rollback functionality."""
    config: Dict[str, Any]
    timestamp: float
    checksum: str
    environment: str


class ConfigManager:
    """
    Advanced Configuration Manager with multiple format support,
    environment profiles, and dynamic reloading.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = Path(config_path) if config_path else None
        self.config: Optional[AppConfig] = None
        self.snapshots: List[ConfigSnapshot] = []
        self.max_snapshots = 10
        
        # File watchers for dynamic reloading
        self._last_modified = {}
        self._config_hash = None
    
    def load_config(self, config_path: Optional[str] = None, environment: Optional[str] = None) -> AppConfig:
        """
        Load configuration from file with format auto-detection.
        
        Args:
            config_path: Path to configuration file
            environment: Environment override
            
        Returns:
            Loaded and validated configuration
        """
        if config_path:
            self.config_path = Path(config_path)
        
        if not self.config_path or not self.config_path.exists():
            logger.warning(f"Config file not found: {self.config_path}, using defaults")
            self.config = AppConfig()
            return self.config
        
        try:
            # Detect file format and load
            config_data = self._load_config_file(self.config_path)
            
            # Apply environment override
            if environment:
                config_data['environment'] = environment
            
            # Load environment-specific overrides
            config_data = self._apply_environment_overrides(config_data)
            
            # Create validated config
            self.config = AppConfig(**config_data)
            
            # Create snapshot for rollback
            self._create_snapshot()
            
            logger.info(f"Configuration loaded successfully from {self.config_path}")
            logger.info(f"Environment: {self.config.environment}")
            logger.info(f"Exchanges: {', '.join(self.config.exchanges)}")
            
            return self.config
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            if self.config:
                logger.info("Using previous configuration")
                return self.config
            else:
                logger.info("Using default configuration")
                self.config = AppConfig()
                return self.config
    
    def _load_config_file(self, config_path: Path) -> Dict[str, Any]:
        """Load configuration file based on extension."""
        suffix = config_path.suffix.lower()
        
        with open(config_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if suffix == '.json':
            return json.loads(content)
        elif suffix in ['.yml', '.yaml'] and YAML_AVAILABLE:
            return yaml.safe_load(content) or {}
        elif suffix == '.toml' and TOML_AVAILABLE:
            return toml.loads(content)
        elif suffix == '.cfg':
            # Legacy INI-style format - convert to dict
            return self._parse_legacy_config(content)
        else:
            # Try to detect format by content
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                if YAML_AVAILABLE:
                    try:
                        return yaml.safe_load(content) or {}
                    except yaml.YAMLError:
                        pass
                raise ValueError(f"Unsupported config format: {suffix}")
    
    def _parse_legacy_config(self, content: str) -> Dict[str, Any]:
        """Parse legacy INI-style configuration."""
        config = {}
        current_section = None
        
        for line in content.split('\n'):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            
            if line.startswith('[') and line.endswith(']'):
                current_section = line[1:-1]
                if current_section not in config:
                    config[current_section] = {}
            elif '=' in line and current_section:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()
                
                # Try to convert to appropriate type
                if value.lower() in ['true', 'false']:
                    value = value.lower() == 'true'
                elif value.isdigit():
                    value = int(value)
                elif '.' in value and value.replace('.', '').isdigit():
                    value = float(value)
                
                config[current_section][key] = value
        
        return config
    
    def _apply_environment_overrides(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply environment-specific configuration overrides."""
        environment = config_data.get('environment', 'development')
        
        # Look for environment-specific config files
        if self.config_path:
            env_config_path = self.config_path.parent / f"{self.config_path.stem}.{environment}{self.config_path.suffix}"
            if env_config_path.exists():
                try:
                    env_config = self._load_config_file(env_config_path)
                    config_data = self._deep_merge(config_data, env_config)
                    logger.info(f"Applied environment overrides from {env_config_path}")
                except Exception as e:
                    logger.warning(f"Failed to load environment config {env_config_path}: {e}")
        
        # Apply environment variables
        config_data = self._apply_env_variables(config_data)
        
        return config_data
    
    def _apply_env_variables(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply environment variable overrides."""
        env_prefix = "CRYPTO_COLLECTOR_"
        
        for key, value in os.environ.items():
            if key.startswith(env_prefix):
                config_key = key[len(env_prefix):].lower()
                
                # Handle nested keys (e.g., CRYPTO_COLLECTOR_RABBITMQ__HOST)
                if '__' in config_key:
                    keys = config_key.split('__')
                    current = config_data
                    for k in keys[:-1]:
                        if k not in current:
                            current[k] = {}
                        current = current[k]
                    current[keys[-1]] = self._convert_env_value(value)
                else:
                    config_data[config_key] = self._convert_env_value(value)
        
        return config_data
    
    def _convert_env_value(self, value: str) -> Any:
        """Convert environment variable string to appropriate type."""
        if value.lower() in ['true', 'false']:
            return value.lower() == 'true'
        elif value.isdigit():
            return int(value)
        elif '.' in value and value.replace('.', '').replace('-', '').isdigit():
            return float(value)
        elif value.startswith('[') and value.endswith(']'):
            # Simple list parsing
            return [item.strip().strip('"\'') for item in value[1:-1].split(',') if item.strip()]
        else:
            return value
    
    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two dictionaries."""
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def _create_snapshot(self):
        """Create configuration snapshot for rollback."""
        if not self.config:
            return
        
        config_dict = self.config.dict()
        config_json = json.dumps(config_dict, sort_keys=True)
        checksum = hashlib.md5(config_json.encode()).hexdigest()
        
        snapshot = ConfigSnapshot(
            config=config_dict,
            timestamp=os.path.getmtime(self.config_path) if self.config_path else 0,
            checksum=checksum,
            environment=self.config.environment.value
        )
        
        self.snapshots.append(snapshot)
        
        # Keep only recent snapshots
        if len(self.snapshots) > self.max_snapshots:
            self.snapshots = self.snapshots[-self.max_snapshots:]
        
        self._config_hash = checksum
    
    def reload_config(self) -> bool:
        """
        Reload configuration if file has changed.
        
        Returns:
            True if configuration was reloaded, False otherwise
        """
        if not self.config_path or not self.config_path.exists():
            return False
        
        try:
            current_mtime = os.path.getmtime(self.config_path)
            last_mtime = self._last_modified.get(str(self.config_path), 0)
            
            if current_mtime > last_mtime:
                logger.info("Configuration file changed, reloading...")
                
                # Try to load new configuration
                new_config = self.load_config()
                
                # Validate new configuration
                if self._validate_config_changes(new_config):
                    self.config = new_config
                    self._last_modified[str(self.config_path)] = current_mtime
                    logger.info("Configuration reloaded successfully")
                    return True
                else:
                    logger.error("New configuration validation failed, keeping current config")
                    return False
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to reload configuration: {e}")
            return False
    
    def _validate_config_changes(self, new_config: AppConfig) -> bool:
        """Validate configuration changes before applying."""
        try:
            # Basic validation - Pydantic already did most of it
            
            # Check critical settings
            if not new_config.exchanges:
                logger.error("No exchanges configured")
                return False
            
            # Check that we have API keys for enabled exchanges
            missing_keys = []
            for exchange in new_config.exchanges:
                api_config = new_config.get_api_config(exchange)
                if not api_config.apiKey and not api_config.secret:
                    missing_keys.append(exchange)
            
            if missing_keys:
                logger.warning(f"Missing API keys for exchanges: {', '.join(missing_keys)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            return False
    
    def rollback_config(self, steps: int = 1) -> bool:
        """
        Rollback configuration to previous snapshot.
        
        Args:
            steps: Number of steps to rollback
            
        Returns:
            True if rollback was successful, False otherwise
        """
        if len(self.snapshots) < steps + 1:
            logger.error(f"Not enough snapshots for rollback (have {len(self.snapshots)}, need {steps + 1})")
            return False
        
        try:
            # Get target snapshot
            target_snapshot = self.snapshots[-(steps + 1)]
            
            # Restore configuration
            self.config = AppConfig(**target_snapshot.config)
            
            # Remove newer snapshots
            self.snapshots = self.snapshots[:-(steps)]
            
            logger.info(f"Configuration rolled back {steps} step(s) to {target_snapshot.environment}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to rollback configuration: {e}")
            return False
    
    def save_config(self, output_path: Optional[str] = None, format: str = "json") -> bool:
        """
        Save current configuration to file.
        
        Args:
            output_path: Output file path
            format: Output format (json, yaml, toml)
            
        Returns:
            True if save was successful, False otherwise
        """
        if not self.config:
            logger.error("No configuration to save")
            return False
        
        try:
            output_path = Path(output_path) if output_path else self.config_path
            if not output_path:
                logger.error("No output path specified")
                return False
            
            config_dict = self.config.dict()
            
            # Remove sensitive data for export
            config_dict = self._sanitize_config_for_export(config_dict)
            
            if format.lower() == 'json':
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(config_dict, f, indent=2, default=str)
            elif format.lower() in ['yml', 'yaml'] and YAML_AVAILABLE:
                with open(output_path, 'w', encoding='utf-8') as f:
                    yaml.dump(config_dict, f, default_flow_style=False, indent=2)
            elif format.lower() == 'toml' and TOML_AVAILABLE:
                with open(output_path, 'w', encoding='utf-8') as f:
                    toml.dump(config_dict, f)
            else:
                logger.error(f"Unsupported format: {format}")
                return False
            
            logger.info(f"Configuration saved to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")
            return False
    
    def _sanitize_config_for_export(self, config_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Remove sensitive data from configuration for export."""
        sensitive_keys = ['password', 'secret', 'apikey', 'key']
        
        def sanitize_recursive(obj):
            if isinstance(obj, dict):
                return {
                    k: "***HIDDEN***" if any(sk in k.lower() for sk in sensitive_keys) else sanitize_recursive(v)
                    for k, v in obj.items()
                }
            elif isinstance(obj, list):
                return [sanitize_recursive(item) for item in obj]
            else:
                return obj
        
        return sanitize_recursive(config_dict)
    
    def get_config_summary(self) -> Dict[str, Any]:
        """Get configuration summary for monitoring."""
        if not self.config:
            return {}
        
        return {
            'environment': self.config.environment.value,
            'exchanges_count': len(self.config.exchanges),
            'exchanges': self.config.exchanges,
            'ticker_interval': self.config.ticker_interval,
            'funding_rate_interval': self.config.funding_rate_interval,
            'cache_enabled': self.config.cache.enabled,
            'batch_enabled': self.config.batch.enabled,
            'rabbitmq_host': self.config.rabbitmq.host,
            'snapshots_count': len(self.snapshots),
            'config_hash': self._config_hash
        }
