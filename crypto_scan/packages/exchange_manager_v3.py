"""
Exchange Manager v3 с интеграцией Circuit Breaker, retry механизмов и health monitoring.
Обеспечивает максимальную устойчивость при работе с проблемными биржами.
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass

import ccxt.async_support as ccxt_async
import ccxt

from circuit_breaker import CircuitBreakerManager, CircuitBreakerConfig, CircuitBreakerError
from retry_manager import RetryManagerRegistry, RetryConfig, RetryStrategy
from health_monitor import HealthMonitor, HealthCheckConfig, HealthStatus

logger = logging.getLogger(__name__)


@dataclass
class ExchangeConfig:
    """Конфигурация биржи с расширенными параметрами устойчивости."""
    name: str
    api_key: str = ""
    secret: str = ""
    enabled: bool = True
    
    # Параметры Circuit Breaker
    circuit_breaker_config: Optional[CircuitBreakerConfig] = None
    
    # Параметры Retry
    retry_config: Optional[RetryConfig] = None
    
    # Параметры Health Check
    health_check_config: Optional[HealthCheckConfig] = None
    
    # Дополнительные параметры
    rate_limit: Optional[int] = None
    timeout: float = 30.0
    sandbox: bool = False


class ResilientExchange:
    """
    Устойчивая обертка для биржи с Circuit Breaker, retry и health monitoring.
    """
    
    def __init__(
        self,
        config: ExchangeConfig,
        circuit_breaker_manager: CircuitBreakerManager,
        retry_registry: RetryManagerRegistry,
        health_monitor: HealthMonitor
    ):
        self.config = config
        self.name = config.name
        
        # Компоненты устойчивости
        self.circuit_breaker_manager = circuit_breaker_manager
        self.retry_registry = retry_registry
        self.health_monitor = health_monitor
        
        # CCXT exchanges
        self.async_exchange: Optional[ccxt_async.Exchange] = None
        self.sync_exchange: Optional[ccxt.Exchange] = None
        
        # Статистика
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'circuit_breaker_blocks': 0,
            'retry_attempts': 0,
            'last_success_time': None,
            'last_failure_time': None,
            'initialization_time': None,
            'markets_count': 0
        }
        
        # Инициализируем компоненты устойчивости
        self._setup_resilience_components()
        
        logger.info(f"Resilient Exchange '{self.name}' created")
    
    def _setup_resilience_components(self):
        """Настройка компонентов устойчивости."""
        # Circuit Breaker
        cb_config = self.config.circuit_breaker_config or CircuitBreakerConfig(
            failure_threshold=5,
            recovery_timeout=60.0,
            success_threshold=3,
            timeout=self.config.timeout
        )
        self.circuit_breaker = self.circuit_breaker_manager.get_breaker(
            f"exchange_{self.name}",
            cb_config
        )
        
        # Retry Manager
        retry_config = self.config.retry_config or RetryConfig(
            max_attempts=3,
            base_delay=1.0,
            strategy=RetryStrategy.EXPONENTIAL,
            retryable_exceptions=(
                ccxt.NetworkError,
                ccxt.RequestTimeout,
                ccxt.ExchangeNotAvailable,
                ConnectionError,
                TimeoutError,
                asyncio.TimeoutError
            )
        )
        self.retry_manager = self.retry_registry.get_manager(
            f"exchange_{self.name}",
            retry_config
        )
        
        # Health Check
        health_config = self.config.health_check_config or HealthCheckConfig(
            check_interval=120.0,  # 2 минуты
            timeout=30.0,
            failure_threshold=3,
            recovery_threshold=2
        )
        
        self.health_check = self.health_monitor.add_health_check(
            f"exchange_{self.name}",
            self._perform_health_check,
            health_config
        )
    
    async def initialize(self) -> bool:
        """Инициализация биржи с устойчивостью."""
        start_time = time.time()
        
        try:
            # Используем retry механизм для инициализации
            success = await self.retry_manager.execute_with_retry(
                self._initialize_exchange
            )
            
            if success:
                self.stats['initialization_time'] = time.time() - start_time
                self.stats['last_success_time'] = time.time()
                logger.info(f"Exchange '{self.name}' initialized successfully in {self.stats['initialization_time']:.2f}s")
            
            return success
            
        except Exception as e:
            self.stats['last_failure_time'] = time.time()
            logger.error(f"Failed to initialize exchange '{self.name}': {e}")
            return False
    
    async def _initialize_exchange(self) -> bool:
        """Внутренняя инициализация биржи."""
        try:
            # Создаем async exchange
            exchange_class = getattr(ccxt_async, self.name)
            exchange_params = {
                'apiKey': self.config.api_key,
                'secret': self.config.secret,
                'timeout': self.config.timeout * 1000,  # CCXT использует миллисекунды
                'sandbox': self.config.sandbox,
                'enableRateLimit': True
            }
            
            # Добавляем rate limit только если указан
            if self.config.rate_limit:
                exchange_params['rateLimit'] = self.config.rate_limit
            
            self.async_exchange = exchange_class(exchange_params)
            
            # Создаем sync exchange для некоторых операций
            sync_exchange_class = getattr(ccxt, self.name)
            sync_params = {
                'apiKey': self.config.api_key,
                'secret': self.config.secret,
                'timeout': self.config.timeout * 1000,
                'sandbox': self.config.sandbox,
                'enableRateLimit': True
            }
            
            # Добавляем rate limit только если указан
            if self.config.rate_limit:
                sync_params['rateLimit'] = self.config.rate_limit
            
            self.sync_exchange = sync_exchange_class(sync_params)
            
            # Загружаем рынки
            markets = await self.async_exchange.load_markets()
            self.stats['markets_count'] = len(markets)
            
            logger.info(f"Exchange '{self.name}' loaded {len(markets)} markets")
            return True
            
        except Exception as e:
            logger.error(f"Exchange '{self.name}' initialization failed: {e}")
            raise
    
    async def fetch_tickers(self) -> Optional[Dict[str, Any]]:
        """Получение тикеров с устойчивостью."""
        return await self._execute_with_resilience(
            self._fetch_tickers_impl,
            "fetch_tickers"
        )
    
    async def _fetch_tickers_impl(self) -> Dict[str, Any]:
        """Внутренняя реализация получения тикеров."""
        if not self.async_exchange:
            raise RuntimeError(f"Exchange '{self.name}' not initialized")
        
        tickers = await self.async_exchange.fetch_tickers()
        return tickers
    
    async def fetch_funding_rates(self) -> Optional[Dict[str, Any]]:
        """Получение funding rates с устойчивостью."""
        return await self._execute_with_resilience(
            self._fetch_funding_rates_impl,
            "fetch_funding_rates"
        )
    
    async def _fetch_funding_rates_impl(self) -> Dict[str, Any]:
        """Внутренняя реализация получения funding rates."""
        if not self.async_exchange:
            raise RuntimeError(f"Exchange '{self.name}' not initialized")
        
        # Проверяем поддержку funding rates
        if not hasattr(self.async_exchange, 'fetch_funding_rates'):
            logger.warning(f"Exchange '{self.name}' does not support funding rates")
            return {}
        
        funding_rates = await self.async_exchange.fetch_funding_rates()
        return funding_rates
    
    async def _execute_with_resilience(
        self,
        func: Callable,
        operation_name: str
    ) -> Optional[Any]:
        """Выполнение операции с полной устойчивостью."""
        self.stats['total_requests'] += 1
        
        try:
            # Используем Circuit Breaker
            result = await self.circuit_breaker.call(
                # Используем Retry Manager
                self.retry_manager.execute_with_retry,
                func
            )
            
            self.stats['successful_requests'] += 1
            self.stats['last_success_time'] = time.time()
            
            logger.debug(f"Exchange '{self.name}' {operation_name} successful")
            return result
            
        except CircuitBreakerError:
            self.stats['circuit_breaker_blocks'] += 1
            logger.warning(f"Exchange '{self.name}' {operation_name} blocked by circuit breaker")
            return None
            
        except Exception as e:
            self.stats['failed_requests'] += 1
            self.stats['last_failure_time'] = time.time()
            logger.error(f"Exchange '{self.name}' {operation_name} failed: {e}")
            return None
    
    async def _perform_health_check(self) -> bool:
        """Выполнение health check биржи."""
        try:
            if not self.async_exchange:
                return False
            
            # Простая проверка - получение времени сервера или статуса
            if hasattr(self.async_exchange, 'fetch_time'):
                await asyncio.wait_for(
                    self.async_exchange.fetch_time(),
                    timeout=10.0
                )
            elif hasattr(self.async_exchange, 'fetch_status'):
                status = await asyncio.wait_for(
                    self.async_exchange.fetch_status(),
                    timeout=10.0
                )
                return status.get('status') == 'ok'
            else:
                # Fallback - пробуем получить один тикер
                symbols = list(self.async_exchange.markets.keys())[:1]
                if symbols:
                    await asyncio.wait_for(
                        self.async_exchange.fetch_ticker(symbols[0]),
                        timeout=10.0
                    )
            
            return True
            
        except Exception as e:
            logger.debug(f"Health check failed for '{self.name}': {e}")
            return False
    
    async def close(self):
        """Закрытие соединений биржи."""
        if self.async_exchange:
            await self.async_exchange.close()
        
        logger.info(f"Exchange '{self.name}' connections closed")
    
    def get_status(self) -> Dict[str, Any]:
        """Получение статуса биржи."""
        return {
            'name': self.name,
            'config': {
                'enabled': self.config.enabled,
                'timeout': self.config.timeout,
                'rate_limit': self.config.rate_limit,
                'sandbox': self.config.sandbox
            },
            'stats': self.stats,
            'circuit_breaker': self.circuit_breaker.get_status(),
            'retry_manager': self.retry_manager.get_status(),
            'health_check': self.health_check.get_status() if self.health_check else None,
            'is_initialized': self.async_exchange is not None
        }


class ResilientExchangeManager:
    """
    Менеджер устойчивых бирж с интегрированными компонентами устойчивости.
    """
    
    def __init__(self):
        self.exchanges: Dict[str, ResilientExchange] = {}
        
        # Компоненты устойчивости
        self.circuit_breaker_manager = CircuitBreakerManager()
        self.retry_registry = RetryManagerRegistry()
        self.health_monitor = HealthMonitor()
        
        # Статистика
        self.stats = {
            'total_exchanges': 0,
            'initialized_exchanges': 0,
            'failed_exchanges': 0,
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0
        }
        
        logger.info("Resilient Exchange Manager initialized")
    
    async def initialize_exchanges(self, configs: List[ExchangeConfig]) -> Dict[str, bool]:
        """Инициализация всех бирж."""
        self.stats['total_exchanges'] = len(configs)
        results = {}
        
        logger.info(f"Initializing {len(configs)} exchanges with resilience components...")
        
        # Запускаем health monitor
        await self.health_monitor.start()
        
        # Инициализируем биржи параллельно
        init_tasks = []
        for config in configs:
            if config.enabled:
                exchange = ResilientExchange(
                    config,
                    self.circuit_breaker_manager,
                    self.retry_registry,
                    self.health_monitor
                )
                self.exchanges[config.name] = exchange
                init_tasks.append(self._initialize_single_exchange(exchange))
        
        # Ждем завершения инициализации
        init_results = await asyncio.gather(*init_tasks, return_exceptions=True)
        
        # Обрабатываем результаты
        for i, (config, result) in enumerate(zip(configs, init_results)):
            if config.enabled:
                if isinstance(result, Exception):
                    logger.error(f"Failed to initialize {config.name}: {result}")
                    results[config.name] = False
                    self.stats['failed_exchanges'] += 1
                else:
                    results[config.name] = result
                    if result:
                        self.stats['initialized_exchanges'] += 1
                    else:
                        self.stats['failed_exchanges'] += 1
            else:
                results[config.name] = False
                logger.info(f"Exchange {config.name} disabled in config")
        
        logger.info(
            f"Exchange initialization complete: "
            f"{self.stats['initialized_exchanges']}/{self.stats['total_exchanges']} successful"
        )
        
        return results
    
    async def _initialize_single_exchange(self, exchange: ResilientExchange) -> bool:
        """Инициализация одной биржи."""
        try:
            return await exchange.initialize()
        except Exception as e:
            logger.error(f"Exception during {exchange.name} initialization: {e}")
            return False
    
    def get_exchange(self, name: str) -> Optional[ResilientExchange]:
        """Получение биржи по имени."""
        return self.exchanges.get(name)
    
    def get_healthy_exchanges(self) -> List[ResilientExchange]:
        """Получение списка здоровых бирж."""
        healthy = []
        for exchange in self.exchanges.values():
            if (exchange.health_check and 
                exchange.health_check.status == HealthStatus.HEALTHY):
                healthy.append(exchange)
        return healthy
    
    def get_available_exchanges(self) -> List[ResilientExchange]:
        """Получение списка доступных бирж (здоровые + деградированные)."""
        available = []
        for exchange in self.exchanges.values():
            if (exchange.circuit_breaker.state.value != 'open' and
                exchange.async_exchange is not None):
                available.append(exchange)
        return available
    
    async def fetch_all_tickers(self) -> Dict[str, Optional[Dict[str, Any]]]:
        """Получение тикеров со всех доступных бирж."""
        available_exchanges = self.get_available_exchanges()
        
        if not available_exchanges:
            logger.warning("No available exchanges for fetching tickers")
            return {}
        
        # Параллельное получение тикеров
        fetch_tasks = {
            exchange.name: exchange.fetch_tickers()
            for exchange in available_exchanges
        }
        
        results = await asyncio.gather(*fetch_tasks.values(), return_exceptions=True)
        
        # Обрабатываем результаты
        tickers_data = {}
        for exchange_name, result in zip(fetch_tasks.keys(), results):
            if isinstance(result, Exception):
                logger.error(f"Failed to fetch tickers from {exchange_name}: {result}")
                tickers_data[exchange_name] = None
            else:
                tickers_data[exchange_name] = result
                if result:
                    self.stats['successful_requests'] += 1
                else:
                    self.stats['failed_requests'] += 1
        
        self.stats['total_requests'] += len(fetch_tasks)
        return tickers_data
    
    async def fetch_all_funding_rates(self) -> Dict[str, Optional[Dict[str, Any]]]:
        """Получение funding rates со всех доступных бирж."""
        available_exchanges = self.get_available_exchanges()
        
        if not available_exchanges:
            logger.warning("No available exchanges for fetching funding rates")
            return {}
        
        # Параллельное получение funding rates
        fetch_tasks = {
            exchange.name: exchange.fetch_funding_rates()
            for exchange in available_exchanges
        }
        
        results = await asyncio.gather(*fetch_tasks.values(), return_exceptions=True)
        
        # Обрабатываем результаты
        funding_data = {}
        for exchange_name, result in zip(fetch_tasks.keys(), results):
            if isinstance(result, Exception):
                logger.error(f"Failed to fetch funding rates from {exchange_name}: {result}")
                funding_data[exchange_name] = None
            else:
                funding_data[exchange_name] = result
                if result:
                    self.stats['successful_requests'] += 1
                else:
                    self.stats['failed_requests'] += 1
        
        self.stats['total_requests'] += len(fetch_tasks)
        return funding_data
    
    def get_system_status(self) -> Dict[str, Any]:
        """Получение общего статуса системы."""
        # Статус health monitor
        health_status = self.health_monitor.get_overall_status()
        
        # Статус circuit breakers
        cb_status = self.circuit_breaker_manager.get_health_summary()
        
        # Статус retry managers
        retry_status = self.retry_registry.get_performance_summary()
        
        return {
            'exchanges': {
                'total': self.stats['total_exchanges'],
                'initialized': self.stats['initialized_exchanges'],
                'failed': self.stats['failed_exchanges'],
                'healthy': health_status['healthy_count'],
                'degraded': health_status['degraded_count'],
                'unhealthy': health_status['unhealthy_count']
            },
            'requests': {
                'total': self.stats['total_requests'],
                'successful': self.stats['successful_requests'],
                'failed': self.stats['failed_requests'],
                'success_rate': (self.stats['successful_requests'] / max(1, self.stats['total_requests'])) * 100
            },
            'health_monitor': health_status,
            'circuit_breakers': cb_status,
            'retry_managers': retry_status
        }
    
    async def close_all(self):
        """Закрытие всех соединений."""
        # Останавливаем health monitor
        await self.health_monitor.stop()
        
        # Закрываем все биржи
        close_tasks = [exchange.close() for exchange in self.exchanges.values()]
        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
        
        logger.info("All exchanges closed")
    
    def get_status(self) -> Dict[str, Any]:
        """Получение общего статуса менеджера бирж."""
        health_status = self.health_monitor.get_overall_status()
        
        return {
            'healthy_count': health_status['healthy_count'],
            'total_count': len(self.exchanges),
            'degraded_count': health_status['degraded_count'],
            'unhealthy_count': health_status['unhealthy_count'],
            'initialized_exchanges': self.stats['initialized_exchanges'],
            'failed_exchanges': self.stats['failed_exchanges'],
            'success_rate': (self.stats['successful_requests'] / max(1, self.stats['total_requests'])) * 100
        }
    
    def get_all_status(self) -> Dict[str, Dict[str, Any]]:
        """Получение статуса всех бирж."""
        return {name: exchange.get_status() 
                for name, exchange in self.exchanges.items()}
