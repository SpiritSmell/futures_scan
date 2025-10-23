"""
Data Collector v3 с интеграцией Circuit Breaker, retry механизмов и health monitoring.
Максимальная устойчивость при сборе данных с бирж.
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field

from interfaces import DataCollectorInterface
from exchange_manager_v3 import ResilientExchangeManager, ExchangeConfig
from cache_manager import CacheManager
from connection_pool import ConnectionPoolManager
from circuit_breaker import CircuitBreakerError
from retry_manager import RetryConfig, RetryStrategy

logger = logging.getLogger(__name__)


@dataclass
class CollectionStats:
    """Расширенная статистика сбора данных."""
    total_collections: int = 0
    successful_collections: int = 0
    failed_collections: int = 0
    cached_collections: int = 0
    circuit_breaker_blocks: int = 0
    retry_attempts: int = 0
    
    # Детальная статистика по типам данных
    ticker_collections: int = 0
    funding_collections: int = 0
    
    # Временные метрики
    total_collection_time: float = 0.0
    last_collection_time: Optional[float] = None
    last_success_time: Optional[float] = None
    last_failure_time: Optional[float] = None
    
    # История производительности
    recent_collection_times: List[float] = field(default_factory=list)
    recent_success_rates: List[float] = field(default_factory=list)
    
    @property
    def success_rate(self) -> float:
        """Процент успешных сборов данных."""
        if self.total_collections == 0:
            return 0.0
        return (self.successful_collections / self.total_collections) * 100
    
    @property
    def cache_hit_rate(self) -> float:
        """Процент попаданий в кэш."""
        if self.total_collections == 0:
            return 0.0
        return (self.cached_collections / self.total_collections) * 100
    
    @property
    def average_collection_time(self) -> float:
        """Среднее время сбора данных."""
        if self.successful_collections == 0:
            return 0.0
        return self.total_collection_time / self.successful_collections
    
    @property
    def recent_average_time(self) -> float:
        """Среднее время недавних сборов."""
        if not self.recent_collection_times:
            return 0.0
        return sum(self.recent_collection_times) / len(self.recent_collection_times)


class ResilientDataCollector(DataCollectorInterface):
    """
    Устойчивый сборщик данных с полной интеграцией компонентов устойчивости.
    """
    
    def __init__(
        self,
        exchange_configs: List[ExchangeConfig],
        cache_manager: CacheManager,
        connection_pool_manager: ConnectionPoolManager
    ):
        self.exchange_configs = exchange_configs
        self.cache_manager = cache_manager
        self.connection_pool_manager = connection_pool_manager
        
        # Создаем устойчивый менеджер бирж
        self.exchange_manager = ResilientExchangeManager()
        
        # Статистика
        self.stats = CollectionStats()
        
        # Конфигурация retry для операций сбора данных
        self._setup_retry_configs()
        
        logger.info("Resilient Data Collector initialized")
    
    def _setup_retry_configs(self):
        """Настройка конфигураций retry для различных операций."""
        # Конфигурация для тикеров (более частые запросы)
        self.ticker_retry_config = RetryConfig(
            max_attempts=2,
            base_delay=0.5,
            max_delay=5.0,
            strategy=RetryStrategy.EXPONENTIAL,
            backoff_multiplier=1.5
        )
        
        # Конфигурация для funding rates (менее частые запросы)
        self.funding_retry_config = RetryConfig(
            max_attempts=3,
            base_delay=1.0,
            max_delay=10.0,
            strategy=RetryStrategy.ADAPTIVE,
            backoff_multiplier=2.0
        )
    
    async def start(self):
        """Запуск сборщика данных."""
        logger.info("Starting Resilient Data Collector...")
        
        # Инициализируем биржи с компонентами устойчивости
        init_results = await self.exchange_manager.initialize_exchanges(self.exchange_configs)
        
        successful_exchanges = sum(1 for success in init_results.values() if success)
        total_exchanges = len(self.exchange_configs)
        
        logger.info(
            f"Data Collector started: {successful_exchanges}/{total_exchanges} exchanges initialized"
        )
        
        if successful_exchanges == 0:
            logger.error("No exchanges initialized successfully!")
            raise RuntimeError("Failed to initialize any exchanges")
    
    async def stop(self):
        """Остановка сборщика данных."""
        logger.info("Stopping Resilient Data Collector...")
        await self.exchange_manager.close_all()
        logger.info("Data Collector stopped")
    
    async def collect_tickers(self, symbols: Optional[List[str]] = None) -> Dict[str, Any]:
        """Сбор тикеров с максимальной устойчивостью."""
        start_time = time.time()
        self.stats.total_collections += 1
        self.stats.ticker_collections += 1
        
        try:
            # Проверяем кэш
            cache_key = f"tickers:all:{hash(tuple(symbols) if symbols else 'all')}"
            cached_data = self.cache_manager.get('tickers', cache_key)
            
            if cached_data:
                self.stats.cached_collections += 1
                logger.debug("Using cached tickers data")
                return cached_data
            
            # Собираем данные со всех доступных бирж
            logger.debug(f"Collecting tickers from available exchanges...")
            
            tickers_data = await self.exchange_manager.fetch_all_tickers()
            
            # Фильтруем по символам если указаны
            if symbols:
                filtered_data = {}
                for exchange_name, exchange_tickers in tickers_data.items():
                    if exchange_tickers:
                        filtered_data[exchange_name] = {
                            symbol: ticker for symbol, ticker in exchange_tickers.items()
                            if symbol in symbols
                        }
                    else:
                        filtered_data[exchange_name] = None
                tickers_data = filtered_data
            
            # Подготавливаем результат
            result = {
                'type': 'tickers',
                'timestamp': time.time(),
                'data': tickers_data,
                'collection_stats': {
                    'exchanges_queried': len(tickers_data),
                    'successful_exchanges': sum(1 for data in tickers_data.values() if data),
                    'failed_exchanges': sum(1 for data in tickers_data.values() if not data),
                    'collection_time': time.time() - start_time
                }
            }
            
            # Кэшируем результат
            self.cache_manager.set('tickers', cache_key, result, ttl=30)
            
            # Обновляем статистику
            collection_time = time.time() - start_time
            self.stats.successful_collections += 1
            self.stats.total_collection_time += collection_time
            self.stats.last_collection_time = time.time()
            self.stats.last_success_time = time.time()
            
            # Обновляем историю производительности
            self.stats.recent_collection_times.append(collection_time)
            if len(self.stats.recent_collection_times) > 50:
                self.stats.recent_collection_times.pop(0)
            
            successful_count = sum(1 for data in tickers_data.values() if data)
            logger.info(
                f"Collected tickers from {successful_count}/{len(tickers_data)} exchanges "
                f"in {collection_time:.2f}s"
            )
            
            return result
            
        except CircuitBreakerError:
            self.stats.circuit_breaker_blocks += 1
            self.stats.failed_collections += 1
            self.stats.last_failure_time = time.time()
            logger.warning("Ticker collection blocked by circuit breaker")
            return self._create_empty_result('tickers', start_time)
            
        except Exception as e:
            self.stats.failed_collections += 1
            self.stats.last_failure_time = time.time()
            logger.error(f"Failed to collect tickers: {e}")
            return self._create_empty_result('tickers', start_time)
    
    async def collect_funding_rates(self, symbols: Optional[List[str]] = None) -> Dict[str, Any]:
        """Сбор funding rates с максимальной устойчивостью."""
        start_time = time.time()
        self.stats.total_collections += 1
        self.stats.funding_collections += 1
        
        try:
            # Проверяем кэш
            cache_key = f"funding_rates:all:{hash(tuple(symbols) if symbols else 'all')}"
            cached_data = self.cache_manager.get('funding_rates', cache_key)
            
            if cached_data:
                self.stats.cached_collections += 1
                logger.debug("Using cached funding rates data")
                return cached_data
            
            # Собираем данные со всех доступных бирж
            logger.debug(f"Collecting funding rates from available exchanges...")
            
            funding_data = await self.exchange_manager.fetch_all_funding_rates()
            
            # Фильтруем по символам если указаны
            if symbols:
                filtered_data = {}
                for exchange_name, exchange_funding in funding_data.items():
                    if exchange_funding:
                        filtered_data[exchange_name] = {
                            symbol: rate for symbol, rate in exchange_funding.items()
                            if symbol in symbols
                        }
                    else:
                        filtered_data[exchange_name] = None
                funding_data = filtered_data
            
            # Подготавливаем результат
            result = {
                'type': 'funding_rates',
                'timestamp': time.time(),
                'data': funding_data,
                'collection_stats': {
                    'exchanges_queried': len(funding_data),
                    'successful_exchanges': sum(1 for data in funding_data.values() if data),
                    'failed_exchanges': sum(1 for data in funding_data.values() if not data),
                    'collection_time': time.time() - start_time
                }
            }
            
            # Кэшируем результат
            self.cache_manager.set('funding_rates', cache_key, result, ttl=300)  # 5 минут
            
            # Обновляем статистику
            collection_time = time.time() - start_time
            self.stats.successful_collections += 1
            self.stats.total_collection_time += collection_time
            self.stats.last_collection_time = time.time()
            self.stats.last_success_time = time.time()
            
            # Обновляем историю производительности
            self.stats.recent_collection_times.append(collection_time)
            if len(self.stats.recent_collection_times) > 50:
                self.stats.recent_collection_times.pop(0)
            
            successful_count = sum(1 for data in funding_data.values() if data)
            logger.info(
                f"Collected funding rates from {successful_count}/{len(funding_data)} exchanges "
                f"in {collection_time:.2f}s"
            )
            
            return result
            
        except CircuitBreakerError:
            self.stats.circuit_breaker_blocks += 1
            self.stats.failed_collections += 1
            self.stats.last_failure_time = time.time()
            logger.warning("Funding rates collection blocked by circuit breaker")
            return self._create_empty_result('funding_rates', start_time)
            
        except Exception as e:
            self.stats.failed_collections += 1
            self.stats.last_failure_time = time.time()
            logger.error(f"Failed to collect funding rates: {e}")
            return self._create_empty_result('funding_rates', start_time)
    
    def _create_empty_result(self, data_type: str, start_time: float) -> Dict[str, Any]:
        """Создание пустого результата при ошибке."""
        return {
            'type': data_type,
            'timestamp': time.time(),
            'data': {},
            'collection_stats': {
                'exchanges_queried': 0,
                'successful_exchanges': 0,
                'failed_exchanges': 0,
                'collection_time': time.time() - start_time,
                'error': True
            }
        }
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """Получение статистики сбора данных (реализация интерфейса)."""
        return {
            'basic_stats': {
                'total_collections': self.stats.total_collections,
                'successful_collections': self.stats.successful_collections,
                'failed_collections': self.stats.failed_collections,
                'success_rate': self.stats.success_rate,
                'cache_hit_rate': self.stats.cache_hit_rate
            },
            'performance_stats': {
                'average_collection_time': self.stats.average_collection_time,
                'recent_average_time': self.stats.recent_average_time,
                'total_collection_time': self.stats.total_collection_time
            },
            'resilience_stats': {
                'circuit_breaker_blocks': self.stats.circuit_breaker_blocks,
                'retry_attempts': self.stats.retry_attempts
            },
            'data_type_stats': {
                'ticker_collections': self.stats.ticker_collections,
                'funding_collections': self.stats.funding_collections
            },
            'timing': {
                'last_collection_time': self.stats.last_collection_time,
                'last_success_time': self.stats.last_success_time,
                'last_failure_time': self.stats.last_failure_time
            }
        }
    
    def get_exchange_status(self) -> Dict[str, Any]:
        """Получение статуса всех бирж."""
        return self.exchange_manager.get_system_status()
    
    def get_healthy_exchanges(self) -> List[str]:
        """Получение списка здоровых бирж."""
        healthy_exchanges = self.exchange_manager.get_healthy_exchanges()
        return [exchange.name for exchange in healthy_exchanges]
    
    def get_available_exchanges(self) -> List[str]:
        """Получение списка доступных бирж."""
        available_exchanges = self.exchange_manager.get_available_exchanges()
        return [exchange.name for exchange in available_exchanges]
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Получение детальных метрик производительности."""
        exchange_status = self.exchange_manager.get_system_status()
        
        return {
            'collector_metrics': self.get_collection_stats(),
            'exchange_metrics': exchange_status,
            'cache_metrics': self.cache_manager.get_stats(),
            'connection_pool_metrics': self.connection_pool_manager.get_all_stats(),
            'overall_health': {
                'healthy_exchanges': len(self.get_healthy_exchanges()),
                'available_exchanges': len(self.get_available_exchanges()),
                'total_exchanges': len(self.exchange_configs),
                'system_efficiency': self._calculate_system_efficiency()
            }
        }
    
    def _calculate_system_efficiency(self) -> float:
        """Вычисление общей эффективности системы."""
        if self.stats.total_collections == 0:
            return 0.0
        
        # Базовая эффективность на основе успешности
        base_efficiency = self.stats.success_rate
        
        # Бонус за использование кэша
        cache_bonus = min(self.stats.cache_hit_rate * 0.1, 10.0)
        
        # Штраф за блокировки circuit breaker
        cb_penalty = min(self.stats.circuit_breaker_blocks * 2.0, 20.0)
        
        # Итоговая эффективность
        efficiency = base_efficiency + cache_bonus - cb_penalty
        return max(0.0, min(100.0, efficiency))
    
    async def restart_exchange(self, exchange_name: str) -> bool:
        """Перезапуск конкретной биржи (реализация интерфейса)."""
        exchange = self.exchange_manager.get_exchange(exchange_name)
        if not exchange:
            logger.warning(f"Exchange '{exchange_name}' not found")
            return False
        
        try:
            # Закрываем текущие соединения
            await exchange.close()
            
            # Сбрасываем circuit breaker
            await self.exchange_manager.circuit_breaker_manager.reset_breaker(
                f"exchange_{exchange_name}"
            )
            
            # Переинициализируем
            success = await exchange.initialize()
            
            if success:
                logger.info(f"Exchange '{exchange_name}' restarted successfully")
            else:
                logger.error(f"Failed to restart exchange '{exchange_name}'")
            
            return success
            
        except Exception as e:
            logger.error(f"Error restarting exchange '{exchange_name}': {e}")
            return False
