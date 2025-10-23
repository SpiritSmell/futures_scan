#!/usr/bin/env python3
"""
Crypto Futures Price Collector v5 - Enhanced Configuration System
Advanced configuration management with Pydantic validation, multiple formats, and environment profiles.
"""

import asyncio
import logging
import signal
import sys
import os
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

# Добавляем путь к packages для импорта модулей
sys.path.append(str(Path(__file__).parent / "packages"))

# Импорты новой системы конфигурации
from config_manager import ConfigManager, AppConfig, ExchangeConfig as ConfigExchangeConfig

# Импорты компонентов устойчивости
from packages.exchange_manager_v3 import ResilientExchangeManager, ExchangeConfig
from packages.data_collector_v3 import ResilientDataCollector
from packages.data_sender_v2 import OptimizedDataSender
from packages.cache_manager import CacheManager
from packages.connection_pool import ConnectionPoolManager
from packages.batch_processor import BatchProcessorManager, BatchConfig, BatchStrategy
from packages.circuit_breaker import CircuitBreakerConfig
from packages.retry_manager import RetryConfig, RetryStrategy
from packages.health_monitor import HealthCheckConfig

logger = logging.getLogger(__name__)


@dataclass
class SystemMetrics:
    """System performance metrics."""
    uptime: float = 0.0
    memory_usage: float = 0.0
    cpu_usage: float = 0.0
    exchanges_healthy: int = 0
    total_exchanges: int = 0
    data_collection_rate: float = 0.0
    error_rate: float = 0.0


class UltimateResilientOrchestrator:
    """
    Ultimate Resilient Orchestrator v5 with Enhanced Configuration System.
    
    Features:
    - Advanced Pydantic-based configuration with validation
    - Multiple configuration formats (JSON, YAML, TOML)
    - Environment-specific profiles (dev, staging, production)
    - Dynamic configuration reloading
    - Environment variable overrides
    - Configuration rollback capabilities
    """
    
    def __init__(self, config_path: Optional[str] = None, environment: Optional[str] = None):
        self.config_manager = ConfigManager(config_path)
        self.config: Optional[AppConfig] = None
        self.environment = environment
        
        # Компоненты системы
        self.cache_manager = None
        self.connection_pool_manager = None
        self.batch_processor_manager = None
        self.exchange_manager = None
        self.data_collector = None
        self.data_sender = None
        
        # Задачи и состояние
        self._ticker_task = None
        self._funding_task = None
        self._health_task = None
        self._config_reload_task = None
        self.is_running = False
        
        # Метрики
        self.metrics = SystemMetrics()
        
        # Настройка обработчиков сигналов
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Обработчик сигналов для graceful shutdown."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        asyncio.create_task(self.stop())
    
    async def initialize(self):
        """Инициализация системы с новой конфигурацией."""
        try:
            # Загружаем конфигурацию
            self.config = self.config_manager.load_config(environment=self.environment)
            
            # Настраиваем логирование согласно конфигурации
            self._setup_logging()
            
            logger.info(f"=== {self.config.app_name} v{self.config.version} ===")
            logger.info(f"Environment: {self.config.environment.value}")
            logger.info(f"Debug mode: {self.config.debug}")
            
            # Инициализируем компоненты производительности
            await self._initialize_performance_components()
            
            # Инициализируем компоненты сбора данных
            await self._initialize_data_components()
            
            logger.info("System initialization completed successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize system: {e}")
            raise
    
    def _setup_logging(self):
        """Настройка логирования согласно конфигурации."""
        log_config = self.config.logging
        
        # Создаем директорию для логов если нужно
        if log_config.file_enabled:
            log_path = Path(log_config.file_path)
            log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Настраиваем корневой логгер
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, log_config.level.value))
        
        # Очищаем существующие обработчики
        root_logger.handlers.clear()
        
        formatter = logging.Formatter(log_config.format)
        
        # Консольный обработчик
        if log_config.console_enabled:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(getattr(logging, log_config.level.value))
            console_handler.setFormatter(formatter)
            root_logger.addHandler(console_handler)
        
        # Файловый обработчик
        if log_config.file_enabled:
            from logging.handlers import RotatingFileHandler
            file_handler = RotatingFileHandler(
                log_config.file_path,
                maxBytes=log_config.max_file_size * 1024 * 1024,  # MB to bytes
                backupCount=log_config.backup_count
            )
            file_handler.setLevel(getattr(logging, log_config.level.value))
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
        
        logger.info(f"Logging configured: level={log_config.level.value}, "
                   f"console={log_config.console_enabled}, file={log_config.file_enabled}")
    
    async def _initialize_performance_components(self):
        """Инициализация компонентов производительности."""
        # Cache Manager
        self.cache_manager = CacheManager()
        await self.cache_manager.start()
        
        # Connection Pool Manager
        self.connection_pool_manager = ConnectionPoolManager()
        await self.connection_pool_manager.start()
        
        # Batch Processor Manager
        self.batch_processor_manager = BatchProcessorManager()
        await self.batch_processor_manager.start()
        
        logger.info("Performance components initialized")
    
    async def _initialize_data_components(self):
        """Инициализация компонентов сбора и отправки данных."""
        # Создаем конфигурации бирж
        exchange_configs = self._create_exchange_configs()
        
        # Resilient Exchange Manager
        self.exchange_manager = ResilientExchangeManager()
        
        # Инициализируем биржи
        await self.exchange_manager.initialize_exchanges(exchange_configs)
        
        # Resilient Data Collector
        self.data_collector = ResilientDataCollector(
            exchange_configs,
            self.cache_manager,
            self.connection_pool_manager
        )
        await self.data_collector.start()
        
        # Создаем функцию отправки для batch processor
        send_function = self._create_send_function()
        
        # Создаем batch config для OptimizedDataSender
        batch_config = BatchConfig(
            max_batch_size=10,
            max_wait_time=5.0,
            strategy=BatchStrategy.HYBRID,
            max_retries=3
        )
        
        # Optimized Data Sender с RabbitMQ и правильным batch processing
        self.data_sender = OptimizedDataSender(
            send_function,
            self.batch_processor_manager,
            enable_change_detection=self.config.enable_change_detection,
            batch_config=batch_config
        )
        await self.data_sender.start()
        
        logger.info("Data components initialized")
    
    def _create_exchange_configs(self) -> List[ExchangeConfig]:
        """Создание конфигураций бирж из новой системы конфигурации."""
        configs = []
        
        for exchange_name in self.config.exchanges:
            # Получаем конфигурацию биржи
            exchange_config = self.config.get_exchange_config(exchange_name)
            api_config = self.config.get_api_config(exchange_name)
            
            # Создаем конфигурацию для ResilientExchange
            config = ExchangeConfig(
                name=exchange_name,
                api_key=api_config.apiKey,
                secret=api_config.secret,
                enabled=exchange_config.enabled,
                timeout=exchange_config.timeout,
                rate_limit=exchange_config.rate_limit,
                sandbox=exchange_config.sandbox,
                circuit_breaker_config=CircuitBreakerConfig(
                    failure_threshold=exchange_config.circuit_breaker.failure_threshold,
                    recovery_timeout=exchange_config.circuit_breaker.recovery_timeout,
                    success_threshold=exchange_config.circuit_breaker.success_threshold,
                    timeout=exchange_config.circuit_breaker.timeout,
                    max_failure_threshold=exchange_config.circuit_breaker.max_failure_threshold,
                    backoff_multiplier=exchange_config.circuit_breaker.backoff_multiplier,
                    max_recovery_timeout=exchange_config.circuit_breaker.max_recovery_timeout
                ),
                retry_config=RetryConfig(
                    max_attempts=exchange_config.retry.max_attempts,
                    base_delay=exchange_config.retry.base_delay,
                    max_delay=exchange_config.retry.max_delay,
                    strategy=RetryStrategy(exchange_config.retry.strategy),
                    backoff_multiplier=exchange_config.retry.backoff_multiplier,
                    jitter=exchange_config.retry.jitter
                ),
                health_check_config=HealthCheckConfig(
                    check_interval=exchange_config.health_check.check_interval,
                    timeout=exchange_config.health_check.timeout,
                    failure_threshold=exchange_config.health_check.failure_threshold,
                    recovery_threshold=exchange_config.health_check.recovery_threshold,
                    adaptive_scaling=exchange_config.health_check.adaptive_scaling
                )
            )
            configs.append(config)
        
        return configs
    
    def _create_send_function(self):
        """Создание функции отправки данных в RabbitMQ."""
        from packages.rabbitmq_producer_2_async import AsyncRabbitMQClient
        
        rabbitmq_config = self.config.rabbitmq
        rabbitmq_client = AsyncRabbitMQClient(
            host=rabbitmq_config.host,
            user=rabbitmq_config.user,
            password=rabbitmq_config.password,
            exchange=rabbitmq_config.out_exchange
        )
        
        async def send_data(data: Dict[str, Any], data_type: str) -> bool:
            try:
                logger.info(f"🚀 Preparing to send {data_type} data to RabbitMQ exchange: {rabbitmq_config.out_exchange}")
                
                send_data_formatted = {
                    'type': data_type,
                    'timestamp': data.get('timestamp'),
                    'data': data.get('data', data),
                    'source': f'{self.config.app_name} v{self.config.version}',
                    'environment': self.config.environment.value,
                    'collection_stats': data.get('collection_stats', {})
                }
                
                data_size = len(str(send_data_formatted))
                logger.info(f"📦 Formatted data size: {data_size} chars, exchange: {rabbitmq_config.out_exchange}")
                
                success = await rabbitmq_client.send_to_rabbitmq(
                    data=send_data_formatted,
                    fanout=True
                )
                
                if success:
                    logger.info(f"✅ Successfully sent {data_type} data to RabbitMQ exchange: {rabbitmq_config.out_exchange}")
                else:
                    logger.error(f"❌ Failed to send {data_type} data to RabbitMQ exchange: {rabbitmq_config.out_exchange}")
                
                return success
                
            except Exception as e:
                logger.error(f"Error sending {data_type} data to RabbitMQ: {e}")
                return False
        
        return send_data
    
    async def start(self):
        """Запуск системы."""
        if self.is_running:
            logger.warning("System is already running")
            return
        
        try:
            await self.initialize()
            
            # Запускаем основные задачи
            await self._start_main_tasks()
            
            # Запускаем задачу перезагрузки конфигурации
            self._config_reload_task = asyncio.create_task(self._config_reload_loop())
            
            self.is_running = True
            logger.info(f"{self.config.app_name} started successfully with enhanced configuration system")
            logger.info("System is running with maximum resilience and advanced configuration. Press Ctrl+C to stop...")
            
            # Основной цикл мониторинга
            await self._main_monitoring_loop()
            
        except Exception as e:
            logger.error(f"Failed to start system: {e}")
            await self.stop()
            raise
    
    async def _start_main_tasks(self):
        """Запуск основных задач сбора данных."""
        # Задача сбора тикеров
        self._ticker_task = asyncio.create_task(self._ticker_collection_loop())
        
        # Задача сбора фандинга
        self._funding_task = asyncio.create_task(self._funding_collection_loop())
        
        # Задача мониторинга здоровья системы
        self._health_task = asyncio.create_task(self._health_monitoring_loop())
        
        logger.info("Main tasks started")
    
    async def _ticker_collection_loop(self):
        """Цикл сбора тикеров."""
        while self.is_running:
            try:
                interval = self.config.ticker_interval
                
                ticker_data = await self.data_collector.collect_tickers()
                
                # Отладочная информация
                logger.debug(f"Ticker data collected: {bool(ticker_data)}")
                if ticker_data:
                    logger.debug(f"Ticker data keys: {list(ticker_data.keys())}")
                    logger.debug(f"Has 'data' key: {bool(ticker_data.get('data'))}")
                    if ticker_data.get('data'):
                        data_count = len(ticker_data['data']) if isinstance(ticker_data['data'], (list, dict)) else 1
                        logger.debug(f"Data count: {data_count}")
                
                # Отправляем данные
                if ticker_data and ticker_data.get('data'):
                    logger.info(f"🚀 MAIN LOOP: Sending ticker data to RabbitMQ...")
                    logger.info(f"📊 Ticker data keys: {list(ticker_data.keys())}")
                    logger.info(f"📦 Data size: {len(ticker_data.get('data', {}))}")
                    
                    result = await self.data_sender.send_data(ticker_data, 'tickers')
                    
                    if result:
                        logger.info(f"✅ MAIN LOOP: Ticker data sent successfully via data_sender")
                    else:
                        logger.error(f"❌ MAIN LOOP: Failed to send ticker data via data_sender")
                else:
                    logger.warning(f"⚠️ MAIN LOOP: No ticker data to send - ticker_data: {bool(ticker_data)}, has_data: {bool(ticker_data.get('data') if ticker_data else False)}")
                
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"Error in ticker collection loop: {e}")
                await asyncio.sleep(10)  # Пауза при ошибке
    
    async def _funding_collection_loop(self):
        """Цикл сбора фандинга."""
        while self.is_running:
            try:
                interval = self.config.funding_rate_interval
                
                funding_data = await self.data_collector.collect_funding_rates()
                
                # Отладочная информация
                logger.debug(f"Funding data collected: {bool(funding_data)}")
                if funding_data:
                    logger.debug(f"Funding data keys: {list(funding_data.keys())}")
                    logger.debug(f"Has 'data' key: {bool(funding_data.get('data'))}")
                    if funding_data.get('data'):
                        data_count = len(funding_data['data']) if isinstance(funding_data['data'], (list, dict)) else 1
                        logger.debug(f"Funding data count: {data_count}")
                
                # Отправляем данные
                if funding_data and funding_data.get('data'):
                    logger.info(f"🚀 MAIN LOOP: Sending funding data to RabbitMQ...")
                    logger.info(f"📊 Funding data keys: {list(funding_data.keys())}")
                    logger.info(f"📦 Data size: {len(funding_data.get('data', {}))}")
                    
                    result = await self.data_sender.send_data(funding_data, 'funding_rates')
                    
                    if result:
                        logger.info(f"✅ MAIN LOOP: Funding data sent successfully via data_sender")
                    else:
                        logger.error(f"❌ MAIN LOOP: Failed to send funding data via data_sender")
                else:
                    logger.warning(f"⚠️ MAIN LOOP: No funding data to send - funding_data: {bool(funding_data)}, has_data: {bool(funding_data.get('data') if funding_data else False)}")
                
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"Error in funding collection loop: {e}")
                await asyncio.sleep(30)  # Пауза при ошибке
    
    async def _health_monitoring_loop(self):
        """Цикл мониторинга здоровья системы."""
        while self.is_running:
            try:
                interval = self.config.performance.metrics_interval
                
                # Обновляем метрики системы
                await self._update_system_metrics()
                
                # Логируем статус системы
                system_status = self.get_system_status()
                health_percentage = system_status.get('health_percentage', 0)
                exchanges_status = f"{system_status.get('healthy_exchanges', 0)}/{system_status.get('total_exchanges', 0)}"
                performance = system_status.get('performance_score', 0)
                
                logger.info(f"System Health - Exchanges: {exchanges_status}, "
                           f"Performance: {performance:.1f}%, Health: {health_percentage:.1f}%")
                
                # Подробный статус каждые 5 минут
                if interval >= 300 or self.config.debug:
                    await self._log_detailed_status()
                
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"Error in health monitoring loop: {e}")
                await asyncio.sleep(60)
    
    async def _config_reload_loop(self):
        """Цикл проверки и перезагрузки конфигурации."""
        while self.is_running:
            try:
                # Проверяем изменения конфигурации каждые 30 секунд
                if self.config_manager.reload_config():
                    logger.info("Configuration reloaded successfully")
                    self.config = self.config_manager.config
                    
                    # Обновляем настройки логирования
                    self._setup_logging()
                
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"Error in config reload loop: {e}")
                await asyncio.sleep(60)
    
    async def _update_system_metrics(self):
        """Обновление метрик системы."""
        try:
            import psutil
            
            # Системные метрики
            self.metrics.memory_usage = psutil.virtual_memory().percent
            self.metrics.cpu_usage = psutil.cpu_percent()
            
            # Метрики бирж
            if self.exchange_manager:
                exchange_status = self.exchange_manager.get_status()
                self.metrics.exchanges_healthy = exchange_status.get('healthy_count', 0)
                self.metrics.total_exchanges = exchange_status.get('total_count', 0)
            
        except ImportError:
            # psutil не установлен
            pass
        except Exception as e:
            logger.error(f"Error updating system metrics: {e}")
    
    async def _log_detailed_status(self):
        """Подробное логирование статуса системы."""
        try:
            # Статус компонентов
            cache_stats = self.cache_manager.get_stats() if self.cache_manager else {}
            batch_stats = self.batch_processor_manager.get_stats() if self.batch_processor_manager else {}
            sender_stats = self.data_sender.get_send_stats() if self.data_sender else {}
            
            logger.info("=== Detailed System Status ===")
            logger.info(f"Cache: {cache_stats.get('hit_rate', 0):.1f}% hit rate, "
                       f"{cache_stats.get('total_requests', 0)} requests")
            logger.info(f"Batch: {batch_stats.get('efficiency', 0):.1f}% efficiency, "
                       f"{batch_stats.get('processed_batches', 0)} batches")
            logger.info(f"Sender: {sender_stats.get('success_rate', 0):.1f}% success rate, "
                       f"{sender_stats.get('total_sends', 0)} sends")
            
            # Конфигурационная информация
            config_summary = self.config_manager.get_config_summary()
            logger.info(f"Config: {config_summary.get('environment')}, "
                       f"{config_summary.get('exchanges_count')} exchanges, "
                       f"{config_summary.get('snapshots_count')} snapshots")
            
        except Exception as e:
            logger.error(f"Error logging detailed status: {e}")
    
    async def _main_monitoring_loop(self):
        """Главный цикл мониторинга системы."""
        while self.is_running:
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in main monitoring loop: {e}")
                await asyncio.sleep(5)
    
    def get_system_status(self) -> Dict[str, Any]:
        """Получение статуса системы."""
        try:
            # Статус бирж
            exchange_status = self.data_collector.get_exchange_status() if self.data_collector else {}
            
            # Статус отправителя данных
            sender_stats = self.data_sender.get_send_stats() if self.data_sender else {}
            
            # Общие метрики
            healthy_exchanges = exchange_status.get('healthy_count', 0)
            total_exchanges = exchange_status.get('total_count', 1)
            health_percentage = (healthy_exchanges / max(1, total_exchanges)) * 100
            
            return {
                'is_running': self.is_running,
                'environment': self.config.environment.value if self.config else 'unknown',
                'healthy_exchanges': healthy_exchanges,
                'total_exchanges': total_exchanges,
                'health_percentage': health_percentage,
                'performance_score': sender_stats.get('success_rate', 0),
                'memory_usage': self.metrics.memory_usage,
                'cpu_usage': self.metrics.cpu_usage,
                'config_snapshots': len(self.config_manager.snapshots) if self.config_manager else 0
            }
            
        except Exception as e:
            logger.error(f"Error getting system status: {e}")
            return {'is_running': self.is_running, 'error': str(e)}
    
    async def stop(self):
        """Остановка системы."""
        if not self.is_running:
            return
        
        logger.info("Stopping Ultimate Resilient Orchestrator v5...")
        self.is_running = False
        
        try:
            # Останавливаем задачи
            tasks = [self._ticker_task, self._funding_task, self._health_task, self._config_reload_task]
            for task in tasks:
                if task and not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            
            # Останавливаем компоненты
            if self.data_sender:
                await self.data_sender.stop()
            
            if self.data_collector:
                await self.data_collector.stop()
            
            if self.exchange_manager:
                await self.exchange_manager.stop()
            
            if self.batch_processor_manager:
                await self.batch_processor_manager.stop()
            
            if self.connection_pool_manager:
                await self.connection_pool_manager.stop()
            
            if self.cache_manager:
                await self.cache_manager.stop()
            
            logger.info("Ultimate Resilient Orchestrator v5 stopped successfully")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")


async def main():
    """Главная функция."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Crypto Futures Price Collector v5 with Enhanced Configuration')
    parser.add_argument('--config', '-c', help='Configuration file path')
    parser.add_argument('--environment', '-e', help='Environment override (development/staging/production)')
    parser.add_argument('--format', '-f', help='Config format (json/yaml/toml)', default='yaml')
    
    args = parser.parse_args()
    
    # Определяем путь к конфигурации
    config_path = args.config
    if not config_path:
        # Используем конфигурацию по умолчанию в зависимости от формата
        config_dir = Path(__file__).parent / "config"
        config_path = config_dir / f"base_config.{args.format}"
        
        if not config_path.exists():
            # Fallback к YAML если указанный формат не найден
            config_path = config_dir / "base_config.yaml"
    
    # Создаем и запускаем оркестратор
    orchestrator = UltimateResilientOrchestrator(
        config_path=str(config_path),
        environment=args.environment
    )
    
    try:
        await orchestrator.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Unhandled error: {e}")
    finally:
        await orchestrator.stop()


if __name__ == "__main__":
    asyncio.run(main())
