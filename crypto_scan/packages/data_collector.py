"""
DataCollector - модуль для сбора данных с криптобирж.
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field

from .interfaces import DataCollectorInterface, CollectionResult
from .exchange_manager import ExchangeManager

logger = logging.getLogger(__name__)


@dataclass
class CollectionStats:
    """Статистика сбора данных."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_response_time: float = 0.0
    last_collection_time: float = 0.0
    
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


class DataCollector(DataCollectorInterface):
    """Коллектор данных с криптобирж."""
    
    def __init__(self, exchange_manager: ExchangeManager):
        self.exchange_manager = exchange_manager
        self.ticker_stats = CollectionStats()
        self.funding_stats = CollectionStats()
        self._collection_lock = asyncio.Lock()
        
    async def collect_tickers(self, exchanges: List[str] = None) -> Dict[str, CollectionResult]:
        """Параллельный сбор тикеров с бирж."""
        target_exchanges = exchanges or self.exchange_manager.get_healthy_exchanges()
        
        if not target_exchanges:
            logger.warning("No healthy exchanges available for ticker collection")
            return {}
        
        logger.debug(f"Collecting tickers from {len(target_exchanges)} exchanges")
        
        # Создаем задачи для параллельного сбора
        tasks = []
        for exchange_name in target_exchanges:
            task = asyncio.create_task(
                self._collect_ticker_from_exchange(exchange_name)
            )
            tasks.append((exchange_name, task))
        
        # Выполняем все задачи параллельно
        results = {}
        completed_tasks = await asyncio.gather(
            *[task for _, task in tasks], 
            return_exceptions=True
        )
        
        # Обрабатываем результаты
        for (exchange_name, _), result in zip(tasks, completed_tasks):
            if isinstance(result, CollectionResult):
                results[exchange_name] = result
                self._update_ticker_stats(result)
            elif isinstance(result, Exception):
                error_result = CollectionResult(
                    success=False,
                    data={},
                    exchange=exchange_name,
                    error=str(result),
                    timestamp=time.time()
                )
                results[exchange_name] = error_result
                self._update_ticker_stats(error_result)
                logger.error(f"Error collecting tickers from {exchange_name}: {result}")
        
        successful = sum(1 for r in results.values() if r.success)
        logger.info(f"Ticker collection completed: {successful}/{len(target_exchanges)} successful")
        
        return results
    
    async def collect_funding_rates(self, exchanges: List[str] = None) -> Dict[str, CollectionResult]:
        """Параллельный сбор фандинг рейтов с бирж."""
        target_exchanges = exchanges or self.exchange_manager.get_healthy_exchanges()
        
        if not target_exchanges:
            logger.warning("No healthy exchanges available for funding rate collection")
            return {}
        
        logger.debug(f"Collecting funding rates from {len(target_exchanges)} exchanges")
        
        # Создаем задачи для параллельного сбора
        tasks = []
        for exchange_name in target_exchanges:
            task = asyncio.create_task(
                self._collect_funding_rates_from_exchange(exchange_name)
            )
            tasks.append((exchange_name, task))
        
        # Выполняем все задачи параллельно
        results = {}
        completed_tasks = await asyncio.gather(
            *[task for _, task in tasks], 
            return_exceptions=True
        )
        
        # Обрабатываем результаты
        for (exchange_name, _), result in zip(tasks, completed_tasks):
            if isinstance(result, CollectionResult):
                results[exchange_name] = result
                self._update_funding_stats(result)
            elif isinstance(result, Exception):
                error_result = CollectionResult(
                    success=False,
                    data={},
                    exchange=exchange_name,
                    error=str(result),
                    timestamp=time.time()
                )
                results[exchange_name] = error_result
                self._update_funding_stats(error_result)
                logger.error(f"Error collecting funding rates from {exchange_name}: {result}")
        
        successful = sum(1 for r in results.values() if r.success)
        logger.info(f"Funding rate collection completed: {successful}/{len(target_exchanges)} successful")
        
        return results
    
    async def _collect_ticker_from_exchange(self, exchange_name: str) -> CollectionResult:
        """Сбор тикеров с одной биржи."""
        start_time = time.time()
        
        try:
            exchange = self.exchange_manager.get_exchange(exchange_name)
            if not exchange:
                raise RuntimeError(f"Exchange {exchange_name} not found")
            
            tickers = await exchange.fetch_tickers()
            response_time = time.time() - start_time
            
            result = CollectionResult(
                success=True,
                data=tickers,
                exchange=exchange_name,
                response_time=response_time,
                timestamp=time.time()
            )
            
            logger.debug(
                f"Collected {len(tickers)} tickers from {exchange_name} "
                f"in {response_time:.2f}s"
            )
            
            return result
            
        except Exception as e:
            response_time = time.time() - start_time
            error_msg = f"Failed to collect tickers from {exchange_name}: {e}"
            logger.error(error_msg)
            
            return CollectionResult(
                success=False,
                data={},
                exchange=exchange_name,
                error=str(e),
                response_time=response_time,
                timestamp=time.time()
            )
    
    async def _collect_funding_rates_from_exchange(self, exchange_name: str) -> CollectionResult:
        """Сбор фандинг рейтов с одной биржи."""
        start_time = time.time()
        
        try:
            exchange = self.exchange_manager.get_exchange(exchange_name)
            if not exchange:
                raise RuntimeError(f"Exchange {exchange_name} not found")
            
            funding_rates = await exchange.fetch_funding_rates()
            response_time = time.time() - start_time
            
            result = CollectionResult(
                success=True,
                data=funding_rates,
                exchange=exchange_name,
                response_time=response_time,
                timestamp=time.time()
            )
            
            logger.debug(
                f"Collected funding rates for {len(funding_rates)} symbols from {exchange_name} "
                f"in {response_time:.2f}s"
            )
            
            return result
            
        except Exception as e:
            response_time = time.time() - start_time
            error_msg = f"Failed to collect funding rates from {exchange_name}: {e}"
            logger.error(error_msg)
            
            return CollectionResult(
                success=False,
                data={},
                exchange=exchange_name,
                error=str(e),
                response_time=response_time,
                timestamp=time.time()
            )
    
    def _update_ticker_stats(self, result: CollectionResult) -> None:
        """Обновление статистики тикеров."""
        self.ticker_stats.total_requests += 1
        self.ticker_stats.last_collection_time = result.timestamp
        
        if result.success:
            self.ticker_stats.successful_requests += 1
            self.ticker_stats.total_response_time += result.response_time
        else:
            self.ticker_stats.failed_requests += 1
    
    def _update_funding_stats(self, result: CollectionResult) -> None:
        """Обновление статистики фандинг рейтов."""
        self.funding_stats.total_requests += 1
        self.funding_stats.last_collection_time = result.timestamp
        
        if result.success:
            self.funding_stats.successful_requests += 1
            self.funding_stats.total_response_time += result.response_time
        else:
            self.funding_stats.failed_requests += 1
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """Получение статистики сбора данных."""
        return {
            "tickers": {
                "total_requests": self.ticker_stats.total_requests,
                "successful_requests": self.ticker_stats.successful_requests,
                "failed_requests": self.ticker_stats.failed_requests,
                "success_rate": self.ticker_stats.success_rate,
                "average_response_time": self.ticker_stats.average_response_time,
                "last_collection_time": self.ticker_stats.last_collection_time
            },
            "funding_rates": {
                "total_requests": self.funding_stats.total_requests,
                "successful_requests": self.funding_stats.successful_requests,
                "failed_requests": self.funding_stats.failed_requests,
                "success_rate": self.funding_stats.success_rate,
                "average_response_time": self.funding_stats.average_response_time,
                "last_collection_time": self.funding_stats.last_collection_time
            }
        }
    
    async def collect_all_data(self, exchanges: List[str] = None) -> Dict[str, Any]:
        """Сбор всех типов данных одновременно."""
        logger.info("Starting parallel collection of all data types")
        
        # Запускаем сбор тикеров и фандинг рейтов параллельно
        ticker_task = asyncio.create_task(self.collect_tickers(exchanges))
        funding_task = asyncio.create_task(self.collect_funding_rates(exchanges))
        
        # Ждем завершения обеих задач
        ticker_results, funding_results = await asyncio.gather(
            ticker_task, funding_task, return_exceptions=True
        )
        
        # Обрабатываем результаты
        if isinstance(ticker_results, Exception):
            logger.error(f"Error in ticker collection: {ticker_results}")
            ticker_results = {}
            
        if isinstance(funding_results, Exception):
            logger.error(f"Error in funding rate collection: {funding_results}")
            funding_results = {}
        
        # Формируем общий результат
        combined_data = {
            "tickers": {
                exchange: result.data if result.success else {}
                for exchange, result in ticker_results.items()
            },
            "funding_rates": {
                exchange: result.data if result.success else {}
                for exchange, result in funding_results.items()
            },
            "metadata": {
                "collection_timestamp": time.time(),
                "ticker_exchanges": len(ticker_results),
                "funding_exchanges": len(funding_results),
                "successful_ticker_exchanges": sum(1 for r in ticker_results.values() if r.success),
                "successful_funding_exchanges": sum(1 for r in funding_results.values() if r.success)
            }
        }
        
        logger.info(
            f"Data collection completed: "
            f"tickers from {combined_data['metadata']['successful_ticker_exchanges']} exchanges, "
            f"funding rates from {combined_data['metadata']['successful_funding_exchanges']} exchanges"
        )
        
        return combined_data


class ContinuousDataCollector:
    """Непрерывный сборщик данных."""
    
    def __init__(self, data_collector: DataCollector, ticker_interval: int = 10, funding_interval: int = 20):
        self.data_collector = data_collector
        self.ticker_interval = ticker_interval
        self.funding_interval = funding_interval
        self._shutdown_event = asyncio.Event()
        self._tasks: List[asyncio.Task] = []
        
    async def start(self) -> None:
        """Запуск непрерывного сбора данных."""
        logger.info("Starting continuous data collection")
        
        # Создаем задачи для непрерывного сбора
        ticker_task = asyncio.create_task(self._continuous_ticker_collection())
        funding_task = asyncio.create_task(self._continuous_funding_collection())
        
        self._tasks = [ticker_task, funding_task]
        
        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            logger.info("Continuous data collection cancelled")
        except Exception as e:
            logger.error(f"Error in continuous data collection: {e}")
            raise
    
    async def stop(self) -> None:
        """Остановка непрерывного сбора данных."""
        logger.info("Stopping continuous data collection")
        self._shutdown_event.set()
        
        # Отменяем все задачи
        for task in self._tasks:
            if not task.done():
                task.cancel()
        
        # Ждем завершения задач
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        
        logger.info("Continuous data collection stopped")
    
    async def _continuous_ticker_collection(self) -> None:
        """Непрерывный сбор тикеров."""
        logger.info(f"Starting continuous ticker collection (interval: {self.ticker_interval}s)")
        
        while not self._shutdown_event.is_set():
            start_time = time.time()
            
            try:
                await self.data_collector.collect_tickers()
            except Exception as e:
                logger.error(f"Error in ticker collection cycle: {e}")
            
            # Рассчитываем время ожидания
            elapsed = time.time() - start_time
            sleep_time = max(0, self.ticker_interval - elapsed)
            
            if sleep_time > 0:
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), 
                        timeout=sleep_time
                    )
                    break  # Shutdown event was set
                except asyncio.TimeoutError:
                    continue  # Normal timeout, continue loop
    
    async def _continuous_funding_collection(self) -> None:
        """Непрерывный сбор фандинг рейтов."""
        logger.info(f"Starting continuous funding rate collection (interval: {self.funding_interval}s)")
        
        while not self._shutdown_event.is_set():
            start_time = time.time()
            
            try:
                await self.data_collector.collect_funding_rates()
            except Exception as e:
                logger.error(f"Error in funding rate collection cycle: {e}")
            
            # Рассчитываем время ожидания
            elapsed = time.time() - start_time
            sleep_time = max(0, self.funding_interval - elapsed)
            
            if sleep_time > 0:
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), 
                        timeout=sleep_time
                    )
                    break  # Shutdown event was set
                except asyncio.TimeoutError:
                    continue  # Normal timeout, continue loop
