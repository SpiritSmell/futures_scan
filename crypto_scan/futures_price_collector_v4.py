"""
Crypto Futures Price Collector v4 - Максимальная устойчивость системы.
Интегрирует все компоненты Фазы 3: Circuit Breaker, Retry, Health Monitoring.
"""

import asyncio
import logging
import signal
import sys
from typing import Dict, List, Any

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
from packages.app_template import AppTemplate

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('crypto_collector_v4.log')
    ]
)

logger = logging.getLogger(__name__)


class UltimateResilientOrchestrator:
    """
    Финальный оркестратор с максимальной устойчивостью системы.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.is_running = False
        
        # Компоненты производительности
        self.cache_manager = None
        self.connection_pool_manager = None
        self.batch_processor_manager = None
        
        # Компоненты сбора и отправки данных
        self.data_collector = None
        self.data_sender = None
        
        # Задачи
        self._ticker_task = None
        self._funding_task = None
        self._monitoring_task = None
        
        logger.info("Ultimate Resilient Orchestrator initialized")
    
    async def start(self):
        """Запуск оркестратора с максимальной устойчивостью."""
        if self.is_running:
            return
        
        logger.info("Starting Ultimate Resilient Orchestrator...")
        
        try:
            # Инициализируем компоненты производительности
            await self._initialize_performance_components()
            
            # Инициализируем компоненты сбора данных
            await self._initialize_data_components()
            
            # Запускаем основные задачи
            await self._start_main_tasks()
            
            self.is_running = True
            logger.info("Ultimate Resilient Orchestrator started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start orchestrator: {e}")
            await self.stop()
            raise
    
    async def _initialize_performance_components(self):
        """Инициализация компонентов производительности."""
        # Cache Manager
        self.cache_manager = CacheManager()
        await self.cache_manager.start()
        
        # Connection Pool Manager
        self.connection_pool_manager = ConnectionPoolManager()
        
        # Batch Processor Manager
        self.batch_processor_manager = BatchProcessorManager()
        
        logger.info("Performance components initialized")
    
    async def _initialize_data_components(self):
        """Инициализация компонентов сбора и отправки данных."""
        # Создаем конфигурации бирж с компонентами устойчивости
        exchange_configs = self._create_exchange_configs()
        
        # Resilient Data Collector
        self.data_collector = ResilientDataCollector(
            exchange_configs,
            self.cache_manager,
            self.connection_pool_manager
        )
        await self.data_collector.start()
        
        # Optimized Data Sender с RabbitMQ
        self.data_sender = OptimizedDataSender(
            self._create_send_function(),
            self.batch_processor_manager,
            enable_change_detection=self.config.get('enable_change_detection', True)
        )
        await self.data_sender.start()
        
        logger.info("Data components initialized")
    
    def _create_exchange_configs(self) -> List[ExchangeConfig]:
        """Создание конфигураций бирж с компонентами устойчивости."""
        configs = []
        exchanges = self.config.get('exchanges', [])
        api_keys = self.config.get('api_keys', {})
        
        for exchange_name in exchanges:
            # Circuit Breaker конфигурация
            cb_config = CircuitBreakerConfig(
                failure_threshold=5,
                recovery_timeout=60.0,
                success_threshold=3,
                timeout=30.0,
                max_failure_threshold=15,
                backoff_multiplier=1.5,
                max_recovery_timeout=300.0
            )
            
            # Retry конфигурация
            retry_config = RetryConfig(
                max_attempts=3,
                base_delay=1.0,
                max_delay=30.0,
                strategy=RetryStrategy.ADAPTIVE,
                backoff_multiplier=2.0
            )
            
            # Health Check конфигурация
            health_config = HealthCheckConfig(
                check_interval=120.0,
                timeout=30.0,
                failure_threshold=3,
                recovery_threshold=2,
                adaptive_scaling=True
            )
            
            # API ключи
            exchange_api = api_keys.get(exchange_name, {})
            
            config = ExchangeConfig(
                name=exchange_name,
                api_key=exchange_api.get('apiKey', ''),
                secret=exchange_api.get('secret', ''),
                enabled=True,
                circuit_breaker_config=cb_config,
                retry_config=retry_config,
                health_check_config=health_config,
                timeout=30.0
            )
            configs.append(config)
        
        return configs
    
    def _create_send_function(self):
        """Создание функции отправки данных в RabbitMQ."""
        from packages.rabbitmq_producer_2_async import AsyncRabbitMQClient
        
        rabbitmq_client = AsyncRabbitMQClient(
            host=self.config.get('host', 'localhost'),
            user=self.config.get('user', 'rmuser'),
            password=self.config.get('password', 'rmpassword'),
            exchange=self.config.get('out_exchange', 'futures_price_collector_out')
        )
        
        async def send_data(data: Dict[str, Any], data_type: str) -> bool:
            try:
                send_data_formatted = {
                    'type': data_type,
                    'timestamp': data.get('timestamp'),
                    'data': data.get('data', data),
                    'source': 'futures_price_collector_v4',
                    'collection_stats': data.get('collection_stats', {})
                }
                
                success = await rabbitmq_client.send_to_rabbitmq(
                    data=send_data_formatted,
                    fanout=True
                )
                
                if success:
                    logger.debug(f"Successfully sent {data_type} data to RabbitMQ")
                else:
                    logger.warning(f"Failed to send {data_type} data to RabbitMQ")
                
                return success
                
            except Exception as e:
                logger.error(f"Error sending {data_type} data to RabbitMQ: {e}")
                return False
        
        return send_data
    
    async def _start_main_tasks(self):
        """Запуск основных задач сбора данных."""
        # Задача сбора тикеров
        self._ticker_task = asyncio.create_task(self._ticker_collection_loop())
        
        # Задача сбора funding rates
        self._funding_task = asyncio.create_task(self._funding_collection_loop())
        
        # Задача мониторинга
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        
        logger.info("Main tasks started")
    
    async def _ticker_collection_loop(self):
        """Цикл сбора тикеров."""
        interval = self.config.get('ticker_interval', 30)
        
        while self.is_running:
            try:
                # Собираем тикеры
                ticker_data = await self.data_collector.collect_tickers()
                
                # Отправляем данные
                if ticker_data and ticker_data.get('data'):
                    await self.data_sender.send_data(ticker_data, 'tickers')
                
                await asyncio.sleep(interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in ticker collection loop: {e}")
                await asyncio.sleep(5)
    
    async def _funding_collection_loop(self):
        """Цикл сбора funding rates."""
        interval = self.config.get('funding_interval', 300)
        
        while self.is_running:
            try:
                # Собираем funding rates
                funding_data = await self.data_collector.collect_funding_rates()
                
                # Отправляем данные
                if funding_data and funding_data.get('data'):
                    await self.data_sender.send_data(funding_data, 'funding_rates')
                
                await asyncio.sleep(interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in funding collection loop: {e}")
                await asyncio.sleep(10)
    
    async def _monitoring_loop(self):
        """Цикл мониторинга системы."""
        interval = self.config.get('monitoring_interval', 60)
        
        while self.is_running:
            try:
                # Получаем статус системы
                status = self.get_system_status()
                
                # Логируем ключевые метрики
                logger.info(
                    f"System Status - "
                    f"Health: {status['health']['health_percentage']:.1f}%, "
                    f"Exchanges: {status['exchanges']['healthy']}/{status['exchanges']['total']}, "
                    f"Success Rate: {status['performance']['overall_success_rate']:.1f}%"
                )
                
                await asyncio.sleep(interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(30)
    
    def get_system_status(self) -> Dict[str, Any]:
        """Получение общего статуса системы."""
        # Статус сборщика данных
        collector_stats = self.data_collector.get_collection_stats()
        exchange_status = self.data_collector.get_exchange_status()
        
        # Статус отправителя данных
        sender_stats = self.data_sender.get_send_stats()
        
        return {
            'is_running': self.is_running,
            'exchanges': {
                'total': exchange_status['exchanges']['total'],
                'healthy': exchange_status['exchanges']['healthy'],
                'degraded': exchange_status['exchanges']['degraded'],
                'unhealthy': exchange_status['exchanges']['unhealthy']
            },
            'performance': {
                'collection_success_rate': collector_stats['basic_stats']['success_rate'],
                'sending_success_rate': sender_stats['success_rate'],
                'overall_success_rate': (
                    collector_stats['basic_stats']['success_rate'] + 
                    sender_stats['success_rate']
                ) / 2,
                'cache_hit_rate': collector_stats['basic_stats']['cache_hit_rate']
            },
            'health': exchange_status['health_monitor'],
            'circuit_breakers': exchange_status['circuit_breakers'],
            'retry_managers': exchange_status['retry_managers']
        }
    
    async def stop(self):
        """Остановка оркестратора."""
        if not self.is_running:
            return
        
        logger.info("Stopping Ultimate Resilient Orchestrator...")
        self.is_running = False
        
        # Останавливаем задачи
        for task in [self._ticker_task, self._funding_task, self._monitoring_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        # Останавливаем компоненты
        if self.data_collector:
            await self.data_collector.stop()
        
        if self.cache_manager:
            await self.cache_manager.stop()
        
        logger.info("Ultimate Resilient Orchestrator stopped")


async def main():
    """Главная функция с максимальной устойчивостью."""
    logger.info("=" * 60)
    logger.info("Starting Crypto Futures Price Collector v4 (Ultimate Resilience)")
    logger.info("=" * 60)
    
    try:
        # Загружаем конфигурацию
        config_template = AppTemplate([('config=', "<config filename>")])
        config_template.get_arguments_from_env()
        config_template.get_arguments()
        config_template.load_settings_from_file()
        config = config_template.settings
        
        if not config:
            logger.error("Failed to load configuration")
            return 1
        
        # Добавляем настройки устойчивости
        resilience_config = {
            # Интервалы сбора данных
            'ticker_interval': 30,
            'funding_interval': 300,
            'monitoring_interval': 60,
            
            # Настройки batch processing
            'batch_size': 50,
            'batch_wait_time': 2.0,
            'batch_strategy': 'hybrid',
            'batch_compression': True,
            
            # Настройки кэширования
            'ticker_cache_ttl': 30,
            'funding_cache_ttl': 300,
            
            # Настройки connection pooling
            'max_connections_per_exchange': 10,
            'connection_timeout': 30,
            
            # Настройки устойчивости
            'enable_change_detection': True,
            
            # RabbitMQ настройки
            'host': config.get('host', 'localhost'),
            'user': config.get('user', 'rmuser'),
            'password': config.get('password', 'rmpassword'),
            'out_exchange': config.get('out_exchange', 'futures_price_collector_out')
        }
        
        config.update(resilience_config)
        
        logger.info("Configuration loaded with resilience components:")
        logger.info(f"  - Circuit Breakers: Adaptive thresholds")
        logger.info(f"  - Retry Mechanisms: 5 strategies available")
        logger.info(f"  - Health Monitoring: Automatic checks")
        logger.info(f"  - RabbitMQ: {config['host']}")
        
        # Создаем и запускаем оркестратор
        orchestrator = UltimateResilientOrchestrator(config)
        
        # Настройка graceful shutdown
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            asyncio.create_task(orchestrator.stop())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Запускаем систему
        await orchestrator.start()
        
        logger.info("System is running with maximum resilience. Press Ctrl+C to stop...")
        
        # Основной цикл
        while orchestrator.is_running:
            await asyncio.sleep(60)
            
            # Логируем статус каждую минуту
            status = orchestrator.get_system_status()
            logger.info(
                f"System Health - "
                f"Exchanges: {status['exchanges']['healthy']}/{status['exchanges']['total']}, "
                f"Performance: {status['performance']['overall_success_rate']:.1f}%, "
                f"Health: {status['health']['health_percentage']:.1f}%"
            )
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Critical error: {e}")
        return 1
    finally:
        if 'orchestrator' in locals():
            await orchestrator.stop()
    
    logger.info("Crypto Futures Price Collector v4 shutdown complete")
    return 0


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
