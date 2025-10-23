# 📚 API Documentation - Crypto Futures Price Collector v5

## 🏗️ Architecture Overview

The Crypto Futures Price Collector v5 is built with a modern, modular architecture featuring:

- **Configuration Management**: Pydantic v2 validation with multiple formats
- **Resilience Components**: Circuit Breaker, Retry Manager, Health Monitor
- **Performance Optimization**: Caching, Connection Pooling, Batch Processing
- **Data Pipeline**: Collection → Processing → Storage/Messaging

## 📋 Table of Contents

1. [Configuration System](#configuration-system)
2. [Exchange Management](#exchange-management)
3. [Resilience Components](#resilience-components)
4. [Data Collection](#data-collection)
5. [Data Processing](#data-processing)
6. [Storage & Messaging](#storage--messaging)
7. [Monitoring & Health](#monitoring--health)
8. [Error Handling](#error-handling)

---

## 🔧 Configuration System

### ConfigManager

**Purpose**: Advanced configuration management with Pydantic v2 validation, environment profiles, and dynamic reloading.

#### Class: `ConfigManager`

```python
class ConfigManager:
    def __init__(self, config_path: Optional[str] = None)
    def load_config(self, environment: Optional[str] = None) -> AppConfig
    def reload_config(self) -> bool
    def rollback_config(self, steps: int = 1) -> bool
    def get_config_summary() -> Dict[str, Any]
```

**Key Features**:
- ✅ Multiple format support (JSON, YAML, TOML)
- ✅ Environment-specific profiles (dev, staging, production)
- ✅ Dynamic configuration reloading
- ✅ Configuration snapshots and rollback
- ✅ Environment variable overrides

#### Configuration Models

##### AppConfig
```python
class AppConfig(BaseSettings):
    environment: Environment = Environment.DEVELOPMENT
    debug: bool = True
    app_name: str = "Crypto Futures Collector"
    version: str = "5.0.0"
    exchanges: List[str] = ["binance", "bybit"]
    ticker_interval: float = Field(ge=1.0, le=3600.0, default=30.0)
    funding_rate_interval: float = Field(ge=60.0, le=86400.0, default=300.0)
    
    # Nested configurations
    cache: CacheConfig
    rabbitmq: RabbitMQConfig
    database: DatabaseConfig
    logging: LoggingConfig
    performance: PerformanceConfig
    exchange_configs: Dict[str, ExchangeConfig]
```

##### ExchangeConfig
```python
class ExchangeConfig(BaseModel):
    name: str = Field(min_length=1)
    enabled: bool = True
    timeout: float = Field(ge=1.0, le=300.0, default=30.0)
    rate_limit: int = Field(ge=100, le=10000, default=1000)
    sandbox: bool = False
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
```

**Usage Example**:
```python
# Load configuration
config_manager = ConfigManager("./config/base_config.yaml")
config = config_manager.load_config(environment="production")

# Access configuration
print(f"Environment: {config.environment}")
print(f"Exchanges: {config.exchanges}")

# Get exchange-specific config
binance_config = config.get_exchange_config("binance")
print(f"Binance timeout: {binance_config.timeout}s")
```

---

## 🏢 Exchange Management

### ResilientExchangeManager

**Purpose**: Manages multiple cryptocurrency exchanges with built-in resilience and error handling.

#### Class: `ResilientExchangeManager`

```python
class ResilientExchangeManager:
    def __init__(self)
    def set_resilience_components(self, **components)
    async def initialize_exchanges(self, configs: List[ExchangeConfig])
    def get_exchange(self, name: str) -> Optional[ResilientExchange]
    def get_available_exchanges(self) -> List[str]
    def get_exchange_count(self) -> int
    async def close_all(self)
```

#### Class: `ResilientExchange`

```python
class ResilientExchange:
    def __init__(self, config: ExchangeConfig, ccxt_exchange, **resilience_components)
    async def fetch_tickers(self, symbols: Optional[List[str]] = None) -> Dict[str, Dict]
    async def fetch_funding_rates(self, symbols: Optional[List[str]] = None) -> List[Dict]
    async def fetch_order_book(self, symbol: str, limit: Optional[int] = None) -> Dict
    async def close(self)
    
    # Properties
    @property
    def is_healthy(self) -> bool
    @property
    def last_error(self) -> Optional[Exception]
```

**Usage Example**:
```python
# Initialize exchange manager
exchange_manager = ResilientExchangeManager()
exchange_manager.set_resilience_components(
    circuit_breaker_manager=cb_manager,
    retry_manager=retry_manager,
    health_monitor=health_monitor,
    cache_manager=cache_manager,
    connection_pool=connection_pool
)

# Initialize exchanges
exchange_configs = [config.get_exchange_config(name) for name in config.exchanges]
await exchange_manager.initialize_exchanges(exchange_configs)

# Use exchanges
binance = exchange_manager.get_exchange("binance")
if binance and binance.is_healthy:
    tickers = await binance.fetch_tickers()
    print(f"Fetched {len(tickers)} tickers from Binance")
```

---

## 🛡️ Resilience Components

### Circuit Breaker

**Purpose**: Prevents cascade failures by temporarily blocking requests to failing services.

#### Class: `CircuitBreakerManager`

```python
class CircuitBreakerManager:
    def create_circuit_breaker(self, name: str, config: Dict) -> CircuitBreaker
    def get_circuit_breaker(self, name: str) -> Optional[CircuitBreaker]
    def get_all_states(self) -> Dict[str, CircuitBreakerState]
```

#### Class: `CircuitBreaker`

```python
class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int, recovery_timeout: float, ...)
    async def call(self, func: Callable) -> Any
    def reset(self)
    
    # Properties
    @property
    def state(self) -> CircuitBreakerState
    @property
    def failure_count(self) -> int
    @property
    def success_count(self) -> int
```

**States**: `CLOSED` → `OPEN` → `HALF_OPEN` → `CLOSED`

### Retry Manager

**Purpose**: Implements intelligent retry strategies with exponential backoff.

#### Class: `RetryManagerRegistry`

```python
class RetryManagerRegistry:
    def create_retry_manager(self, name: str, config: Dict) -> RetryManager
    def get_retry_manager(self, name: str) -> Optional[RetryManager]
```

#### Class: `RetryManager`

```python
class RetryManager:
    def __init__(self, name: str, max_attempts: int, base_delay: float, ...)
    async def call(self, func: Callable) -> Any
    def get_statistics(self) -> Dict[str, Any]
```

**Strategies**: `fixed`, `linear`, `exponential`, `adaptive`

### Health Monitor

**Purpose**: Continuously monitors service health and provides status reporting.

#### Class: `HealthMonitor`

```python
class HealthMonitor:
    def add_health_check(self, name: str, config: Dict) -> HealthCheck
    def get_health_check(self, name: str) -> Optional[HealthCheck]
    def get_all_health_status(self) -> Dict[str, HealthStatus]
    async def start(self)
    async def stop(self)
```

**Usage Example**:
```python
# Create resilience components
cb_manager = CircuitBreakerManager()
retry_manager = RetryManagerRegistry()
health_monitor = HealthMonitor()

# Configure circuit breaker
cb = cb_manager.create_circuit_breaker('binance_api', {
    'failure_threshold': 5,
    'recovery_timeout': 60.0,
    'success_threshold': 3
})

# Configure retry manager
rm = retry_manager.create_retry_manager('binance_api', {
    'max_attempts': 3,
    'base_delay': 1.0,
    'strategy': 'exponential'
})

# Use together
async def resilient_api_call():
    return await rm.call(lambda: cb.call(api_function))
```

---

## 📊 Data Collection

### DataCollector

**Purpose**: Orchestrates data collection from multiple exchanges with caching and error handling.

#### Class: `DataCollector`

```python
class DataCollector:
    def __init__(self, exchange_manager: ResilientExchangeManager, cache_manager: CacheManager, config: AppConfig)
    async def collect_all_data(self) -> Dict[str, Any]
    async def collect_ticker_data(self, exchanges: Optional[List[str]] = None) -> List[Dict]
    async def collect_funding_rate_data(self, exchanges: Optional[List[str]] = None) -> List[Dict]
    async def collect_order_book_data(self, symbols: List[str], exchanges: Optional[List[str]] = None) -> List[Dict]
    def get_collection_statistics(self) -> Dict[str, Any]
```

**Usage Example**:
```python
# Initialize data collector
data_collector = DataCollector(
    exchange_manager=exchange_manager,
    cache_manager=cache_manager,
    config=config
)

# Collect all data
all_data = await data_collector.collect_all_data()
print(f"Collected tickers: {len(all_data['tickers'])}")
print(f"Collected funding rates: {len(all_data['funding_rates'])}")

# Collect specific data
binance_tickers = await data_collector.collect_ticker_data(['binance'])
```

---

## ⚡ Performance Components

### Cache Manager

**Purpose**: Multi-tier caching system with TTL support and statistics.

#### Class: `CacheManager`

```python
class CacheManager:
    def __init__(self)
    def get(self, cache_type: str, key: str) -> Optional[Any]
    def set(self, cache_type: str, key: str, value: Any, ttl: Optional[float] = None)
    def delete(self, cache_type: str, key: str) -> bool
    def clear(self, cache_type: Optional[str] = None)
    def get_cache_stats(self) -> Dict[str, Dict[str, Any]]
```

**Cache Types**: `ticker`, `funding_rate`, `order_book`, `market_data`

### Batch Processor

**Purpose**: Efficient batch processing with compression and automatic flushing.

#### Class: `BatchProcessorManager`

```python
class BatchProcessorManager:
    def __init__(self)
    def add_to_batch(self, batch_type: str, data: Any)
    def process_batch(self, batch_type: str, processor_func: Callable)
    def flush_all_batches(self)
    def get_batch_statistics(self) -> Dict[str, Any]
```

### Connection Pool

**Purpose**: Manages connection pools for databases and message queues.

#### Class: `ConnectionPoolManager`

```python
class ConnectionPoolManager:
    def __init__(self)
    def create_pool(self, pool_name: str, factory: Callable, max_size: int = 10)
    async def get_connection(self, pool_name: str)
    async def return_connection(self, pool_name: str, connection)
    def get_pool_statistics(self) -> Dict[str, Any]
```

---

## 💾 Storage & Messaging

### Data Sender

**Purpose**: Handles data transmission to RabbitMQ and database storage with batching.

#### Class: `DataSender`

```python
class DataSender:
    def __init__(self, rabbitmq_client, database_client, batch_processor, config)
    async def send_tickers(self, tickers: List[Dict])
    async def send_funding_rates(self, funding_rates: List[Dict])
    async def send_order_books(self, order_books: List[Dict])
    async def start(self)
    async def stop(self)
    def get_sender_statistics(self) -> Dict[str, Any]
```

### RabbitMQ Integration

#### Class: `AsyncRabbitMQClient`

```python
class AsyncRabbitMQClient:
    def __init__(self, host: str, port: int, user: str, password: str, **kwargs)
    async def connect(self) -> bool
    async def disconnect(self)
    async def publish_message(self, exchange: str, routing_key: str, message: Dict) -> bool
    async def publish_batch(self, messages: List[Dict], exchange: str, routing_key: str) -> int
    def get_statistics(self) -> Dict[str, Any]
```

### Database Integration

#### Class: `DatabaseClient`

```python
class DatabaseClient:
    def __init__(self, host: str, port: int, database: str, **kwargs)
    async def connect(self) -> bool
    async def disconnect(self)
    async def insert_batch(self, table: str, records: List[Dict]) -> int
    async def select_data(self, table: str, conditions: Optional[Dict] = None) -> List[Dict]
    def get_statistics(self) -> Dict[str, Any]
```

---

## 📈 Monitoring & Health

### System Metrics

All components provide comprehensive metrics and statistics:

```python
# Exchange Manager Statistics
exchange_stats = exchange_manager.get_statistics()
# {
#     'total_exchanges': 5,
#     'healthy_exchanges': 4,
#     'failed_exchanges': 1,
#     'total_requests': 15420,
#     'successful_requests': 14891,
#     'failed_requests': 529,
#     'average_response_time': 0.245
# }

# Cache Statistics
cache_stats = cache_manager.get_cache_stats()
# {
#     'ticker': {
#         'hits': 8945,
#         'misses': 1234,
#         'hit_rate': 0.879,
#         'size': 2341,
#         'max_size': 5000
#     }
# }

# Circuit Breaker States
cb_states = circuit_breaker_manager.get_all_states()
# {
#     'exchange_binance': 'CLOSED',
#     'exchange_bybit': 'HALF_OPEN',
#     'exchange_bitget': 'OPEN'
# }
```

### Health Check Endpoints

```python
# Overall system health
health_status = health_monitor.get_all_health_status()
# {
#     'exchange_binance': 'HEALTHY',
#     'exchange_bybit': 'UNHEALTHY',
#     'rabbitmq_connection': 'HEALTHY',
#     'database_connection': 'HEALTHY'
# }
```

---

## ❌ Error Handling

### Exception Hierarchy

```python
class CryptoCollectorException(Exception):
    """Base exception for all crypto collector errors."""

class ConfigurationError(CryptoCollectorException):
    """Configuration-related errors."""

class ExchangeError(CryptoCollectorException):
    """Exchange-related errors."""

class NetworkError(CryptoCollectorException):
    """Network connectivity errors."""

class DataProcessingError(CryptoCollectorException):
    """Data processing and validation errors."""
```

### Error Handling Patterns

```python
# Resilient API call with full error handling
async def resilient_fetch_data(exchange_name: str):
    try:
        exchange = exchange_manager.get_exchange(exchange_name)
        if not exchange or not exchange.is_healthy:
            raise ExchangeError(f"Exchange {exchange_name} not available")
        
        # Use circuit breaker and retry manager
        cb = circuit_breaker_manager.get_circuit_breaker(f'exchange_{exchange_name}')
        rm = retry_manager.get_retry_manager(f'exchange_{exchange_name}')
        
        data = await rm.call(lambda: cb.call(exchange.fetch_tickers))
        return data
        
    except CircuitBreakerOpenError:
        logger.warning(f"Circuit breaker open for {exchange_name}")
        return {}
    except RetryExhaustedError:
        logger.error(f"All retry attempts failed for {exchange_name}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error for {exchange_name}: {e}")
        return {}
```

---

## 🚀 Usage Examples

### Complete System Setup

```python
import asyncio
from config_manager import ConfigManager
from exchange_manager_v3 import ResilientExchangeManager
from circuit_breaker import CircuitBreakerManager
from retry_manager import RetryManagerRegistry
from health_monitor import HealthMonitor
from cache_manager import CacheManager
from data_collector import DataCollector
from data_sender import DataSender

async def main():
    # Load configuration
    config_manager = ConfigManager("./config/base_config.yaml")
    config = config_manager.load_config(environment="production")
    
    # Initialize performance components
    cache_manager = CacheManager()
    connection_pool = ConnectionPoolManager()
    batch_processor = BatchProcessorManager()
    
    # Initialize resilience components
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
    
    # Initialize exchanges
    exchange_configs = [config.get_exchange_config(name) for name in config.exchanges]
    await exchange_manager.initialize_exchanges(exchange_configs)
    
    # Initialize data components
    data_collector = DataCollector(exchange_manager, cache_manager, config)
    data_sender = DataSender(rabbitmq_client, database_client, batch_processor, config)
    
    # Start monitoring
    await health_monitor.start()
    await data_sender.start()
    
    # Main data collection loop
    while True:
        try:
            # Collect data
            all_data = await data_collector.collect_all_data()
            
            # Send data
            await data_sender.send_tickers(all_data['tickers'])
            await data_sender.send_funding_rates(all_data['funding_rates'])
            
            # Wait for next interval
            await asyncio.sleep(config.ticker_interval)
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Main loop error: {e}")
            await asyncio.sleep(5)  # Brief pause before retry
    
    # Cleanup
    await exchange_manager.close_all()
    await health_monitor.stop()
    await data_sender.stop()

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 📝 Configuration Examples

### Production Configuration (YAML)

```yaml
environment: production
debug: false
app_name: "Crypto Futures Collector"
version: "5.0.0"

exchanges:
  - binance
  - bybit
  - bitget
  - htx
  - gateio

ticker_interval: 15.0
funding_rate_interval: 300.0

cache:
  enabled: true
  default_ttl: 300.0
  max_size: 10000

performance:
  max_concurrent_requests: 50
  connection_pool_size: 20
  worker_threads: 8

rabbitmq:
  host: "${RABBITMQ_HOST:localhost}"
  port: 5672
  user: "${RABBITMQ_USER}"
  password: "${RABBITMQ_PASSWORD}"

database:
  enabled: true
  host: "${DATABASE_HOST:localhost}"
  port: 8123
  database: "${DATABASE_NAME:crypto_data}"

logging:
  level: WARNING
  console: true
  file: true
  file_path: "./logs/collector.log"
```

### Environment Variables (.env)

```bash
# RabbitMQ Configuration
CRYPTO_COLLECTOR_RABBITMQ__HOST=prod-rabbitmq.company.com
CRYPTO_COLLECTOR_RABBITMQ__USER=crypto_collector
CRYPTO_COLLECTOR_RABBITMQ__PASSWORD=secure_password_123

# Database Configuration  
CRYPTO_COLLECTOR_DATABASE__HOST=prod-clickhouse.company.com
CRYPTO_COLLECTOR_DATABASE__NAME=crypto_production

# API Keys (Exchange-specific)
CRYPTO_COLLECTOR_EXCHANGE_CONFIGS__BINANCE__API_KEY=your_binance_api_key
CRYPTO_COLLECTOR_EXCHANGE_CONFIGS__BINANCE__API_SECRET=your_binance_api_secret

# Performance Tuning
CRYPTO_COLLECTOR_TICKER_INTERVAL=10.0
CRYPTO_COLLECTOR_PERFORMANCE__MAX_CONCURRENT_REQUESTS=100
```

---

## 🔍 Troubleshooting

### Common Issues

1. **Configuration Validation Errors**
   - Check Pydantic validation messages
   - Verify environment variable formats
   - Ensure required fields are present

2. **Exchange Connection Issues**
   - Check API keys and permissions
   - Verify network connectivity
   - Review rate limiting settings

3. **Circuit Breaker Activation**
   - Monitor failure rates
   - Adjust thresholds if needed
   - Check underlying service health

4. **Memory Usage Growth**
   - Monitor cache sizes
   - Check for connection leaks
   - Review batch processing settings

### Debug Mode

Enable debug mode for detailed logging:

```yaml
debug: true
logging:
  level: DEBUG
  console: true
```

---

## 📊 Performance Benchmarks

### Typical Performance Metrics

- **Startup Time**: < 5 seconds
- **Memory Usage**: 50-150 MB (depending on cache size)
- **Throughput**: 1000+ requests/second
- **Latency**: < 100ms average response time
- **Reliability**: 99.9% uptime with resilience components

### Optimization Tips

1. **Caching**: Enable caching for frequently accessed data
2. **Batching**: Use batch processing for high-volume operations
3. **Connection Pooling**: Configure appropriate pool sizes
4. **Circuit Breakers**: Set reasonable failure thresholds
5. **Monitoring**: Use health checks and metrics for optimization

---

*This documentation covers the complete API surface of the Crypto Futures Price Collector v5. For implementation details, see the source code and unit tests.*
