"""
DataCollector v2 - оптимизированный модуль для сбора данных с криптобирж.
Включает кэширование, connection pooling и batch processing.
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field

from .interfaces import DataCollectorInterface, CollectionResult
from .exchange_manager import ExchangeManager
from .cache_manager import CacheManager, cached
from .connection_pool import ConnectionPoolManager
from .batch_processor import BatchProcessorManager, BatchConfig

logger = logging.getLogger(__name__)


@dataclass
class OptimizedCollectionStats:
    """Расширенная статистика сбора данных с метриками производительности."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    cached_requests: int = 0
    total_response_time: float = 0.0
    last_collection_time: float = 0.0
    
    # Метрики производительности
    cache_hit_rate: float = 0.0
    avg_batch_size: float = 0.0
    connection_pool_efficiency: float = 0.0
    
    @property
    def success_rate(self) -> float:
        """Процент успешных запросов."""
        if self.total_requests == 0:
            return 0.0
        return (self.successful_requests / self.total_requests) * 100
    
    @property
    def average_response_time(self) -> float:
        """Среднее время ответа."""
        if self.successful_requests == 0:
            return 0.0
        return self.total_response_time / self.successful_requests
    
    @property
    def efficiency_score(self) -> float:
        """Общий показатель эффективности."""
        return (
            self.success_rate * 0.4 +
            self.cache_hit_rate * 0.3 +
            self.connection_pool_efficiency * 0.3
        )


class OptimizedDataCollector(DataCollectorInterface):
    """
    Оптимизированный коллектор данных с кэшированием, connection pooling и batch processing.
    """
    
    def __init__(self, 
                 exchange_manager: ExchangeManager,
                 cache_manager: Optional[CacheManager] = None,
                 connection_pool_manager: Optional[ConnectionPoolManager] = None,
                 batch_processor_manager: Optional[BatchProcessorManager] = None):
        self.exchange_manager = exchange_manager
        self.cache_manager = cache_manager
        self.connection_pool_manager = connection_pool_manager
        self.batch_processor_manager = batch_processor_manager
        
        self.stats = OptimizedCollectionStats()
        self._running = False
        
        # Настройка кэширования для методов
        if self.cache_manager:
            self._setup_caching()
    
    def _setup_caching(self):
        """Настройка кэширования для методов класса."""
        # Привязываем cache_manager к декорированным методам
        if hasattr(self._collect_exchange_tickers_cached, '_cache_manager'):
            self._collect_exchange_tickers_cached._cache_manager = self.cache_manager
        if hasattr(self._collect_exchange_funding_rates_cached, '_cache_manager'):
            self._collect_exchange_funding_rates_cached._cache_manager = self.cache_manager
    
    async def start(self):
        """Запуск коллектора."""
        if self._running:
            return
        
        self._running = True
        
        # Запускаем компоненты оптимизации
        if self.cache_manager:
            await self.cache_manager.start()
        
        if self.connection_pool_manager:
            await self.connection_pool_manager.start()
        
        if self.batch_processor_manager:
            await self.batch_processor_manager.start()
        
        logger.info("Optimized Data Collector started with performance enhancements")
    
    async def stop(self):
        """Остановка коллектора."""
        if not self._running:
            return
        
        self._running = False
        
        # Останавливаем компоненты оптимизации
        if self.batch_processor_manager:
            await self.batch_processor_manager.stop()
        
        if self.connection_pool_manager:
            await self.connection_pool_manager.stop()
        
        if self.cache_manager:
            await self.cache_manager.stop()
        
        logger.info("Optimized Data Collector stopped")
    
    async def collect_tickers(self, exchanges: Optional[List[str]] = None) -> Dict[str, Any]:
        """Оптимизированный параллельный сбор тикеров с кэшированием."""
        start_time = time.time()
        
        target_exchanges = exchanges or self.exchange_manager.get_healthy_exchanges()
        if not target_exchanges:
            logger.warning("No healthy exchanges available for ticker collection")
            return {}
        
        logger.debug(f"Collecting tickers from {len(target_exchanges)} exchanges")
        
        # Проверяем кэш сначала
        cached_results = {}
        exchanges_to_fetch = []
        
        if self.cache_manager:
            ticker_cache = self.cache_manager.get_cache('tickers')
            if ticker_cache:
                for exchange_name in target_exchanges:
                    cache_key = f"tickers:{exchange_name}"
                    cached_data = ticker_cache.get(cache_key)
                    if cached_data:
                        cached_results[exchange_name] = cached_data
                        self.stats.cached_requests += 1
                        logger.debug(f"Using cached tickers for {exchange_name}")
                    else:
                        exchanges_to_fetch.append(exchange_name)
            else:
                exchanges_to_fetch = target_exchanges
        else:
            exchanges_to_fetch = target_exchanges
        
        # Создаем задачи для параллельного сбора
        tasks = []
        for exchange_name in exchanges_to_fetch:
            task = asyncio.create_task(
                self._collect_exchange_tickers_optimized(exchange_name),
                name=f"tickers_{exchange_name}"
            )
            tasks.append((exchange_name, task))
        
        # Собираем результаты
        all_tickers = cached_results.copy()
        successful_exchanges = len(cached_results)
        
        for exchange_name, task in tasks:
            try:
                tickers = await task
                if tickers:
                    all_tickers[exchange_name] = tickers
                    successful_exchanges += 1
                    self.stats.successful_requests += 1
                    
                    # Кэшируем результат
                    if self.cache_manager:
                        ticker_cache = self.cache_manager.get_cache('tickers')
                        if ticker_cache:
                            cache_key = f"tickers:{exchange_name}"
                            ticker_cache.set(cache_key, tickers, ttl=30.0)  # 30 секунд TTL
                else:
                    self.stats.failed_requests += 1
                    
            except Exception as e:
                logger.error(f"Failed to collect tickers from {exchange_name}: {e}")
                self.stats.failed_requests += 1
        
        collection_time = time.time() - start_time
        self.stats.total_response_time += collection_time
        self.stats.total_requests += len(target_exchanges)
        
        # Обновляем метрики производительности
        if self.stats.total_requests > 0:
            self.stats.cache_hit_rate = (self.stats.cached_requests / self.stats.total_requests) * 100
        
        logger.info(f"Collected tickers from {successful_exchanges}/{len(target_exchanges)} exchanges in {collection_time:.2f}s (cached: {len(cached_results)})")
        
        return all_tickers
    
    async def collect_funding_rates(self, exchanges: Optional[List[str]] = None) -> Dict[str, Any]:
        """Оптимизированный параллельный сбор funding rates с кэшированием."""
        start_time = time.time()
        
        target_exchanges = exchanges or self.exchange_manager.get_healthy_exchanges()
        if not target_exchanges:
            logger.warning("No healthy exchanges available for funding rates collection")
            return {}
        
        logger.debug(f"Collecting funding rates from {len(target_exchanges)} exchanges")
        
        # Проверяем кэш сначала
        cached_results = {}
        exchanges_to_fetch = []
        
        if self.cache_manager:
            funding_cache = self.cache_manager.get_cache('funding_rates')
            if funding_cache:
                for exchange_name in target_exchanges:
                    cache_key = f"funding_rates:{exchange_name}"
                    cached_data = funding_cache.get(cache_key)
                    if cached_data:
                        cached_results[exchange_name] = cached_data
                        self.stats.cached_requests += 1
                        logger.debug(f"Using cached funding rates for {exchange_name}")
                    else:
                        exchanges_to_fetch.append(exchange_name)
            else:
                exchanges_to_fetch = target_exchanges
        else:
            exchanges_to_fetch = target_exchanges
        
        # Создаем задачи для параллельного сбора
        tasks = []
        for exchange_name in exchanges_to_fetch:
            task = asyncio.create_task(
                self._collect_exchange_funding_rates_optimized(exchange_name),
                name=f"funding_{exchange_name}"
            )
            tasks.append((exchange_name, task))
        
        # Собираем результаты
        all_funding_rates = cached_results.copy()
        successful_exchanges = len(cached_results)
        
        for exchange_name, task in tasks:
            try:
                funding_rates = await task
                if funding_rates:
                    all_funding_rates[exchange_name] = funding_rates
                    successful_exchanges += 1
                    self.stats.successful_requests += 1
                    
                    # Кэшируем результат
                    if self.cache_manager:
                        funding_cache = self.cache_manager.get_cache('funding_rates')
                        if funding_cache:
                            cache_key = f"funding_rates:{exchange_name}"
                            funding_cache.set(cache_key, funding_rates, ttl=300.0)  # 5 минут TTL
                else:
                    self.stats.failed_requests += 1
                    
            except Exception as e:
                logger.error(f"Failed to collect funding rates from {exchange_name}: {e}")
                self.stats.failed_requests += 1
        
        collection_time = time.time() - start_time
        self.stats.total_response_time += collection_time
        self.stats.total_requests += len(target_exchanges)
        
        # Обновляем метрики производительности
        if self.stats.total_requests > 0:
            self.stats.cache_hit_rate = (self.stats.cached_requests / self.stats.total_requests) * 100
        
        logger.info(f"Collected funding rates from {successful_exchanges}/{len(target_exchanges)} exchanges in {collection_time:.2f}s (cached: {len(cached_results)})")
        
        return all_funding_rates
    
    async def _collect_exchange_tickers_optimized(self, exchange_name: str) -> Optional[Dict[str, Any]]:
        """Оптимизированный сбор тикеров с одной биржи."""
        exchange = self.exchange_manager.get_exchange(exchange_name)
        if not exchange:
            logger.error(f"Exchange {exchange_name} not found")
            return None
        
        try:
            # Используем connection pool если доступен
            if self.connection_pool_manager:
                pool = await self.connection_pool_manager.get_pool(exchange_name)
                # Здесь можно добавить логику использования pool для HTTP запросов
            
            # Выполняем сбор данных
            tickers = await exchange.fetch_tickers()
            
            # Обновляем метрики connection pool
            if self.connection_pool_manager:
                pool_stats = pool.get_stats()
                self.stats.connection_pool_efficiency = pool_stats.success_rate * 100
            
            return tickers
            
        except Exception as e:
            logger.error(f"Error collecting tickers from {exchange_name}: {e}")
            return None
    
    async def _collect_exchange_funding_rates_optimized(self, exchange_name: str) -> Optional[Dict[str, Any]]:
        """Оптимизированный сбор funding rates с одной биржи."""
        exchange = self.exchange_manager.get_exchange(exchange_name)
        if not exchange:
            logger.error(f"Exchange {exchange_name} not found")
            return None
        
        try:
            # Используем connection pool если доступен
            if self.connection_pool_manager:
                pool = await self.connection_pool_manager.get_pool(exchange_name)
            
            # Выполняем сбор данных
            funding_rates = await exchange.fetch_funding_rates()
            
            # Обновляем метрики connection pool
            if self.connection_pool_manager:
                pool_stats = pool.get_stats()
                self.stats.connection_pool_efficiency = pool_stats.success_rate * 100
            
            return funding_rates
            
        except Exception as e:
            logger.error(f"Error collecting funding rates from {exchange_name}: {e}")
            return None
    
    @cached(cache_type='tickers', ttl=30.0)
    async def _collect_exchange_tickers_cached(self, exchange_name: str) -> Optional[Dict[str, Any]]:
        """Кэшированный метод сбора тикеров."""
        return await self._collect_exchange_tickers_optimized(exchange_name)
    
    @cached(cache_type='funding_rates', ttl=300.0)
    async def _collect_exchange_funding_rates_cached(self, exchange_name: str) -> Optional[Dict[str, Any]]:
        """Кэшированный метод сбора funding rates."""
        return await self._collect_exchange_funding_rates_optimized(exchange_name)
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """Получение статистики сбора данных (реализация абстрактного метода)."""
        return {
            'total_requests': self.stats.total_requests,
            'successful_requests': self.stats.successful_requests,
            'failed_requests': self.stats.failed_requests,
            'cached_requests': self.stats.cached_requests,
            'success_rate': self.stats.success_rate,
            'cache_hit_rate': self.stats.cache_hit_rate,
            'average_response_time': self.stats.average_response_time,
            'efficiency_score': self.stats.efficiency_score
        }
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Получение детальных метрик производительности."""
        metrics = {
            'collection_stats': {
                'total_requests': self.stats.total_requests,
                'success_rate': self.stats.success_rate,
                'cache_hit_rate': self.stats.cache_hit_rate,
                'avg_response_time': self.stats.average_response_time,
                'efficiency_score': self.stats.efficiency_score
            }
        }
        
        # Добавляем метрики кэша
        if self.cache_manager:
            cache_stats = self.cache_manager.get_all_stats()
            metrics['cache_stats'] = {
                cache_type: {
                    'hits': stats.hits,
                    'misses': stats.misses,
                    'hit_ratio': stats.hit_ratio,
                    'size': stats.size
                }
                for cache_type, stats in cache_stats.items()
            }
        
        # Добавляем метрики connection pool
        if self.connection_pool_manager:
            pool_stats = self.connection_pool_manager.get_all_stats()
            metrics['connection_pool_stats'] = {
                exchange: {
                    'total_requests': stats.total_requests,
                    'success_rate': stats.success_rate,
                    'avg_response_time': stats.avg_response_time,
                    'requests_per_minute': stats.requests_per_minute
                }
                for exchange, stats in pool_stats.items()
            }
        
        # Добавляем метрики batch processor
        if self.batch_processor_manager:
            batch_stats = self.batch_processor_manager.get_all_stats()
            metrics['batch_processor_stats'] = {
                processor: {
                    'total_batches': stats.total_batches,
                    'success_rate': stats.success_rate,
                    'avg_batch_size': stats.avg_batch_size,
                    'throughput': stats.throughput
                }
                for processor, stats in batch_stats.items()
            }
        
        return metrics
