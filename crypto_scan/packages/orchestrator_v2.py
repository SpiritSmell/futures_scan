"""
CryptoDataOrchestrator v2 - оптимизированный оркестратор с компонентами производительности.
Интегрирует кэширование, connection pooling, batch processing и мониторинг производительности.
"""

import asyncio
import logging
import signal
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from .interfaces import OrchestratorInterface
from .exchange_manager import ExchangeManager
from .data_collector_v2 import OptimizedDataCollector
from .data_sender_v2 import OptimizedDataSender
from .cache_manager import CacheManager
from .connection_pool import ConnectionPoolManager
from .batch_processor import BatchProcessorManager, BatchConfig

logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    """Агрегированные метрики производительности системы."""
    overall_efficiency: float = 0.0
    cache_hit_rate: float = 0.0
    connection_pool_efficiency: float = 0.0
    batch_processing_efficiency: float = 0.0
    data_collection_rate: float = 0.0  # элементов в секунду
    data_sending_rate: float = 0.0     # элементов в секунду
    memory_usage_mb: float = 0.0
    uptime_seconds: float = 0.0


class OptimizedCryptoDataOrchestrator(OrchestratorInterface):
    """
    Оптимизированный оркестратор для координации всех компонентов системы сбора данных.
    Включает компоненты производительности и детальный мониторинг.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._running = False
        self._start_time = 0.0
        
        # Основные компоненты
        self.cache_manager = CacheManager()
        self.connection_pool_manager = ConnectionPoolManager()
        self.batch_processor_manager = BatchProcessorManager()
        
        self.exchange_manager = ExchangeManager(
            connection_pool_manager=self.connection_pool_manager,
            cache_manager=self.cache_manager
        )
        
        self.data_collector = OptimizedDataCollector(
            exchange_manager=self.exchange_manager,
            cache_manager=self.cache_manager,
            connection_pool_manager=self.connection_pool_manager,
            batch_processor_manager=self.batch_processor_manager
        )
        
        # DataSender будет создан после инициализации send_function
        self.data_sender: Optional[OptimizedDataSender] = None
        
        # Задачи и управление
        self._data_collection_task: Optional[asyncio.Task] = None
        self._monitoring_task: Optional[asyncio.Task] = None
        self._performance_metrics = PerformanceMetrics()
        
        # Настройки производительности
        self.ticker_interval = config.get('ticker_interval', 30)  # секунды
        self.funding_interval = config.get('funding_interval', 300)  # секунды
        self.monitoring_interval = config.get('monitoring_interval', 60)  # секунды
        
        # Настройки batch processing
        self.batch_config = BatchConfig(
            max_batch_size=config.get('batch_size', 50),
            max_wait_time=config.get('batch_wait_time', 2.0),
            strategy=config.get('batch_strategy', 'hybrid'),
            compression_enabled=config.get('batch_compression', True)
        )
    
    async def start(self):
        """Запуск оркестратора и всех компонентов."""
        if self._running:
            logger.warning("Orchestrator already running")
            return
        
        logger.info("Starting Optimized Crypto Data Orchestrator...")
        self._start_time = time.time()
        self._running = True
        
        try:
            # Запускаем компоненты производительности
            logger.info("Starting performance components...")
            await self.cache_manager.start()
            await self.connection_pool_manager.start()
            await self.batch_processor_manager.start()
            
            # Инициализируем биржи
            logger.info("Initializing exchanges...")
            exchange_configs = self._create_exchange_configs()
            await self.exchange_manager.initialize_exchanges(exchange_configs)
            
            # Запускаем коллектор данных
            logger.info("Starting data collector...")
            await self.data_collector.start()
            
            # Создаем и запускаем data sender
            logger.info("Starting data sender...")
            send_function = self._create_send_function()
            self.data_sender = OptimizedDataSender(
                send_function=send_function,
                batch_processor_manager=self.batch_processor_manager,
                cache_manager=self.cache_manager,
                batch_config=self.batch_config
            )
            await self.data_sender.start()
            
            # Запускаем основные задачи
            logger.info("Starting main tasks...")
            self._data_collection_task = asyncio.create_task(self._data_collection_loop())
            self._monitoring_task = asyncio.create_task(self._monitoring_loop())
            
            # Настраиваем обработчики сигналов
            self._setup_signal_handlers()
            
            logger.info("Optimized Crypto Data Orchestrator started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start orchestrator: {e}")
            await self.stop()
            raise
    
    async def stop(self):
        """Остановка оркестратора и всех компонентов."""
        if not self._running:
            return
        
        logger.info("Stopping Optimized Crypto Data Orchestrator...")
        self._running = False
        
        # Останавливаем задачи
        if self._data_collection_task:
            self._data_collection_task.cancel()
            try:
                await self._data_collection_task
            except asyncio.CancelledError:
                pass
        
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
        
        # Останавливаем компоненты
        if self.data_sender:
            await self.data_sender.stop()
        
        await self.data_collector.stop()
        await self.batch_processor_manager.stop()
        await self.connection_pool_manager.stop()
        await self.cache_manager.stop()
        
        logger.info("Optimized Crypto Data Orchestrator stopped")
    
    async def _data_collection_loop(self):
        """Основной цикл сбора данных с оптимизированным планированием."""
        logger.info("Starting optimized data collection loop")
        
        last_ticker_time = 0.0
        last_funding_time = 0.0
        
        while self._running:
            try:
                current_time = time.time()
                
                # Планируем сбор тикеров
                if current_time - last_ticker_time >= self.ticker_interval:
                    asyncio.create_task(self._collect_and_send_tickers())
                    last_ticker_time = current_time
                
                # Планируем сбор funding rates
                if current_time - last_funding_time >= self.funding_interval:
                    asyncio.create_task(self._collect_and_send_funding_rates())
                    last_funding_time = current_time
                
                # Короткая пауза для предотвращения busy-waiting
                await asyncio.sleep(1.0)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in data collection loop: {e}")
                await asyncio.sleep(5.0)
        
        logger.info("Data collection loop stopped")
    
    async def _collect_and_send_tickers(self):
        """Сбор и отправка тикеров с оптимизациями."""
        try:
            logger.debug("Collecting tickers...")
            start_time = time.time()
            
            # Собираем тикеры
            tickers = await self.data_collector.collect_tickers()
            
            if tickers and self.data_sender:
                # Отправляем через batch processor
                await self.data_sender.send_data(tickers, "tickers")
                
                collection_time = time.time() - start_time
                logger.info(f"Collected and queued tickers from {len(tickers)} exchanges in {collection_time:.2f}s")
                
                # Обновляем метрики
                self._performance_metrics.data_collection_rate = len(tickers) / collection_time
            
        except Exception as e:
            logger.error(f"Error collecting tickers: {e}")
    
    async def _collect_and_send_funding_rates(self):
        """Сбор и отправка funding rates с оптимизациями."""
        try:
            logger.debug("Collecting funding rates...")
            start_time = time.time()
            
            # Собираем funding rates
            funding_rates = await self.data_collector.collect_funding_rates()
            
            if funding_rates and self.data_sender:
                # Отправляем через batch processor
                await self.data_sender.send_data(funding_rates, "funding_rates")
                
                collection_time = time.time() - start_time
                logger.info(f"Collected and queued funding rates from {len(funding_rates)} exchanges in {collection_time:.2f}s")
                
                # Обновляем метрики
                self._performance_metrics.data_collection_rate = len(funding_rates) / collection_time
            
        except Exception as e:
            logger.error(f"Error collecting funding rates: {e}")
    
    async def _monitoring_loop(self):
        """Цикл мониторинга производительности."""
        logger.info("Starting performance monitoring loop")
        
        while self._running:
            try:
                await asyncio.sleep(self.monitoring_interval)
                
                # Собираем метрики производительности
                await self._update_performance_metrics()
                
                # Логируем метрики
                self._log_performance_metrics()
                
                # Проверяем здоровье системы
                await self._health_check()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(10.0)
        
        logger.info("Performance monitoring loop stopped")
    
    async def _update_performance_metrics(self):
        """Обновление метрик производительности."""
        try:
            # Базовые метрики
            self._performance_metrics.uptime_seconds = time.time() - self._start_time
            
            # Метрики коллектора данных
            collector_metrics = self.data_collector.get_performance_metrics()
            collection_stats = collector_metrics.get('collection_stats', {})
            
            self._performance_metrics.cache_hit_rate = collection_stats.get('cache_hit_rate', 0.0)
            
            # Метрики отправителя данных
            if self.data_sender:
                sender_metrics = self.data_sender.get_performance_metrics()
                sender_stats = sender_metrics.get('sender_stats', {})
                
                self._performance_metrics.batch_processing_efficiency = sender_stats.get('success_rate', 0.0)
                self._performance_metrics.data_sending_rate = 1.0 / max(sender_stats.get('avg_send_time', 1.0), 0.001)
            
            # Метрики connection pool
            if self.connection_pool_manager:
                pool_stats = self.connection_pool_manager.get_all_stats()
                if pool_stats:
                    avg_success_rate = sum(stats.success_rate for stats in pool_stats.values()) / len(pool_stats)
                    self._performance_metrics.connection_pool_efficiency = avg_success_rate * 100
            
            # Общая эффективность
            self._performance_metrics.overall_efficiency = (
                self._performance_metrics.cache_hit_rate * 0.3 +
                self._performance_metrics.connection_pool_efficiency * 0.3 +
                self._performance_metrics.batch_processing_efficiency * 0.4
            )
            
            # Использование памяти (приблизительно)
            import psutil
            process = psutil.Process()
            self._performance_metrics.memory_usage_mb = process.memory_info().rss / 1024 / 1024
            
        except Exception as e:
            logger.error(f"Error updating performance metrics: {e}")
    
    def _log_performance_metrics(self):
        """Логирование метрик производительности."""
        metrics = self._performance_metrics
        
        logger.info(f"Performance Metrics:")
        logger.info(f"  Overall Efficiency: {metrics.overall_efficiency:.1f}%")
        logger.info(f"  Cache Hit Rate: {metrics.cache_hit_rate:.1f}%")
        logger.info(f"  Connection Pool Efficiency: {metrics.connection_pool_efficiency:.1f}%")
        logger.info(f"  Batch Processing Efficiency: {metrics.batch_processing_efficiency:.1f}%")
        logger.info(f"  Data Collection Rate: {metrics.data_collection_rate:.1f} items/s")
        logger.info(f"  Data Sending Rate: {metrics.data_sending_rate:.1f} items/s")
        logger.info(f"  Memory Usage: {metrics.memory_usage_mb:.1f} MB")
        logger.info(f"  Uptime: {metrics.uptime_seconds:.0f}s")
    
    async def _health_check(self):
        """Проверка здоровья системы."""
        try:
            # Проверяем здоровье бирж
            healthy_exchanges = self.exchange_manager.get_healthy_exchanges()
            total_exchanges = len(self.exchange_manager.exchanges)
            
            if len(healthy_exchanges) < total_exchanges * 0.5:  # Менее 50% здоровых
                logger.warning(f"Only {len(healthy_exchanges)}/{total_exchanges} exchanges are healthy")
            
            # Проверяем метрики производительности
            if self._performance_metrics.overall_efficiency < 50.0:
                logger.warning(f"Low overall efficiency: {self._performance_metrics.overall_efficiency:.1f}%")
            
            # Проверяем использование памяти
            if self._performance_metrics.memory_usage_mb > 1000:  # Более 1GB
                logger.warning(f"High memory usage: {self._performance_metrics.memory_usage_mb:.1f} MB")
            
        except Exception as e:
            logger.error(f"Error in health check: {e}")
    
    def _create_exchange_configs(self) -> List:
        """Создание конфигураций бирж из настроек."""
        from .exchange_manager import ExchangeConfig
        
        configs = []
        exchanges_config = self.config.get('exchanges', [])
        api_keys_config = self.config.get('api_keys', {})
        
        # Обрабатываем случай, когда exchanges - это список имен бирж
        if isinstance(exchanges_config, list):
            for exchange_name in exchanges_config:
                # Получаем API ключи для биржи
                exchange_api_keys = api_keys_config.get(exchange_name, {})
                
                config = ExchangeConfig(
                    name=exchange_name,
                    api_key=exchange_api_keys.get('apiKey', ''),
                    secret=exchange_api_keys.get('secret', ''),
                    enabled=True    # По умолчанию включены
                )
                configs.append(config)
                
        # Обрабатываем случай, когда exchanges - это словарь с конфигурациями
        elif isinstance(exchanges_config, dict):
            for exchange_name, exchange_data in exchanges_config.items():
                if isinstance(exchange_data, dict) and exchange_data.get('enabled', True):
                    config = ExchangeConfig(
                        name=exchange_name,
                        api_key=exchange_data.get('api_key', ''),
                        secret=exchange_data.get('secret', ''),
                        enabled=exchange_data.get('enabled', True)
                    )
                    configs.append(config)
        
        return configs
    
    def _create_send_function(self):
        """Создание функции отправки данных в RabbitMQ."""
        from .rabbitmq_producer_2_async import AsyncRabbitMQClient
        
        # Создаем RabbitMQ клиент
        rabbitmq_client = AsyncRabbitMQClient(
            host=self.config.get('host', 'localhost'),
            user=self.config.get('user', 'rmuser'),
            password=self.config.get('password', 'rmpassword'),
            exchange=self.config.get('out_exchange', 'futures_price_collector_out')
        )
        
        async def send_data(data: Dict[str, Any], data_type: str) -> bool:
            try:
                # Подготавливаем данные для отправки
                if isinstance(data, dict) and 'type' in data:
                    # Это уже batch данные
                    send_data_formatted = data
                else:
                    # Форматируем обычные данные
                    send_data_formatted = {
                        'type': data_type,
                        'timestamp': time.time(),
                        'data': data,
                        'source': 'futures_price_collector_v3'
                    }
                
                # Отправляем в RabbitMQ
                success = await rabbitmq_client.send_to_rabbitmq(
                    data=send_data_formatted,
                    fanout=True  # Используем fanout exchange
                )
                
                if success:
                    item_count = len(data) if isinstance(data, dict) else 1
                    logger.debug(f"Successfully sent {data_type} data to RabbitMQ: {item_count} items")
                else:
                    logger.warning(f"Failed to send {data_type} data to RabbitMQ")
                
                return success
                
            except Exception as e:
                logger.error(f"Error sending {data_type} data to RabbitMQ: {e}")
                return False
        
        return send_data
    
    def _setup_signal_handlers(self):
        """Настройка обработчиков сигналов для graceful shutdown."""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            asyncio.create_task(self.stop())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def get_status(self) -> Dict[str, Any]:
        """Получение статуса оркестратора."""
        return {
            'running': self._running,
            'uptime_seconds': self._performance_metrics.uptime_seconds,
            'performance_metrics': {
                'overall_efficiency': self._performance_metrics.overall_efficiency,
                'cache_hit_rate': self._performance_metrics.cache_hit_rate,
                'connection_pool_efficiency': self._performance_metrics.connection_pool_efficiency,
                'batch_processing_efficiency': self._performance_metrics.batch_processing_efficiency,
                'memory_usage_mb': self._performance_metrics.memory_usage_mb
            },
            'exchanges': {
                'total': len(self.exchange_manager.exchanges),
                'healthy': len(self.exchange_manager.get_healthy_exchanges())
            }
        }
    
    def get_system_status(self) -> Dict[str, Any]:
        """Получение статуса системы (реализация абстрактного метода)."""
        return self.get_status()
    
    async def restart_exchange(self, exchange_name: str) -> bool:
        """Перезапуск конкретной биржи (реализация абстрактного метода)."""
        try:
            logger.info(f"Restarting exchange {exchange_name}...")
            
            # Получаем существующую биржу
            exchange = self.exchange_manager.get_exchange(exchange_name)
            if not exchange:
                logger.error(f"Exchange {exchange_name} not found")
                return False
            
            # Останавливаем биржу
            if hasattr(exchange, 'stop'):
                await exchange.stop()
            
            # Удаляем из менеджера
            if exchange_name in self.exchange_manager.exchanges:
                del self.exchange_manager.exchanges[exchange_name]
            
            # Находим конфигурацию биржи
            exchange_config = None
            exchanges_config = self.config.get('exchanges', {})
            
            if isinstance(exchanges_config, list):
                # Если exchanges - это список имен
                if exchange_name in exchanges_config:
                    from .exchange_manager import ExchangeConfig
                    api_keys = self.config.get('api_keys', {}).get(exchange_name, {})
                    exchange_config = ExchangeConfig(
                        name=exchange_name,
                        api_key=api_keys.get('apiKey', ''),
                        secret=api_keys.get('secret', ''),
                        password=api_keys.get('password', ''),
                        enabled=True
                    )
            elif isinstance(exchanges_config, dict):
                # Если exchanges - это словарь с конфигурациями
                exchange_data = exchanges_config.get(exchange_name)
                if exchange_data:
                    from .exchange_manager import ExchangeConfig
                    exchange_config = ExchangeConfig(
                        name=exchange_name,
                        api_key=exchange_data.get('api_key', ''),
                        secret=exchange_data.get('secret', ''),
                        password=exchange_data.get('password', ''),
                        enabled=exchange_data.get('enabled', True)
                    )
            
            if not exchange_config:
                logger.error(f"Configuration for exchange {exchange_name} not found")
                return False
            
            # Переинициализируем биржу
            success = await self.exchange_manager.add_exchange(exchange_config)
            
            if success:
                logger.info(f"Successfully restarted exchange {exchange_name}")
                return True
            else:
                logger.error(f"Failed to restart exchange {exchange_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error restarting exchange {exchange_name}: {e}")
            return False
    
    async def get_detailed_metrics(self) -> Dict[str, Any]:
        """Получение детальных метрик всех компонентов."""
        metrics = {
            'orchestrator': {
                'performance_metrics': self._performance_metrics.__dict__,
                'status': self.get_status()
            }
        }
        
        # Метрики коллектора данных
        if self.data_collector:
            metrics['data_collector'] = self.data_collector.get_performance_metrics()
        
        # Метрики отправителя данных
        if self.data_sender:
            metrics['data_sender'] = self.data_sender.get_performance_metrics()
        
        # Метрики кэша
        if self.cache_manager:
            metrics['cache'] = self.cache_manager.get_all_stats()
        
        # Метрики connection pool
        if self.connection_pool_manager:
            metrics['connection_pools'] = self.connection_pool_manager.get_all_stats()
        
        # Метрики batch processor
        if self.batch_processor_manager:
            metrics['batch_processors'] = self.batch_processor_manager.get_all_stats()
        
        return metrics
