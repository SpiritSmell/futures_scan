"""
CryptoDataOrchestrator - главный координатор системы сбора данных.
"""

import asyncio
import logging
import signal
import time
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from .interfaces import OrchestratorInterface
from .exchange_manager import ExchangeManager, ExchangeConfig
from .data_collector import DataCollector
from .data_sender import DataSender, ContinuousDataSender
from .app_template import AppTemplate
from . import processor_template

logger = logging.getLogger(__name__)


@dataclass
class SystemStatus:
    """Статус системы."""
    is_running: bool = False
    start_time: Optional[float] = None
    exchanges_count: int = 0
    healthy_exchanges_count: int = 0
    last_data_collection: Optional[float] = None
    last_data_send: Optional[float] = None
    errors_count: int = 0


class CryptoDataOrchestrator(OrchestratorInterface):
    """Главный координатор системы сбора данных."""
    
    def __init__(self):
        self.exchange_manager = ExchangeManager()
        self.data_collector: Optional[DataCollector] = None
        self.data_sender: Optional[DataSender] = None
        # Убираем continuous_collector - используем только координатор
        self.continuous_sender: Optional[ContinuousDataSender] = None
        
        self.status = SystemStatus()
        self._shutdown_event = asyncio.Event()
        self._tasks: List[asyncio.Task] = []
        self._config: Dict[str, Any] = {}
        
    async def initialize(self, config_path: str) -> None:
        """Инициализация системы с конфигурацией."""
        logger.info("Initializing CryptoDataOrchestrator")
        
        try:
            # Загрузка конфигурации
            await self._load_configuration(config_path)
            
            # Инициализация компонентов
            await self._initialize_exchanges()
            self._initialize_data_components()
            
            logger.info("CryptoDataOrchestrator initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize orchestrator: {e}")
            raise
    
    async def start(self) -> None:
        """Запуск системы."""
        if self.status.is_running:
            logger.warning("System is already running")
            return
        
        logger.info("Starting CryptoDataOrchestrator")
        
        try:
            self.status.is_running = True
            self.status.start_time = time.time()
            
            # Настройка обработчиков сигналов
            self._setup_signal_handlers()
            
            # Запуск основных компонентов
            await self._start_components()
            
            logger.info("CryptoDataOrchestrator started successfully")
            
            # Основной цикл работы
            await self._main_loop()
            
        except asyncio.CancelledError:
            logger.info("System shutdown requested")
        except Exception as e:
            logger.error(f"Error in orchestrator: {e}")
            self.status.errors_count += 1
            raise
        finally:
            await self.stop()
    
    async def stop(self) -> None:
        """Остановка системы."""
        if not self.status.is_running:
            return
        
        logger.info("Stopping CryptoDataOrchestrator")
        self._shutdown_event.set()
        
        try:
            # Остановка компонентов
            await self._stop_components()
            
            # Отмена всех задач
            await self._cancel_tasks()
            
            # Закрытие соединений с биржами
            await self.exchange_manager.close_all()
            
            self.status.is_running = False
            logger.info("CryptoDataOrchestrator stopped successfully")
            
        except Exception as e:
            logger.error(f"Error stopping orchestrator: {e}")
    
    async def restart_exchange(self, exchange_name: str) -> bool:
        """Перезапуск конкретной биржи."""
        logger.info(f"Restarting exchange {exchange_name}")
        
        try:
            success = await self.exchange_manager.restart_exchange(exchange_name)
            if success:
                logger.info(f"Exchange {exchange_name} restarted successfully")
            else:
                logger.error(f"Failed to restart exchange {exchange_name}")
            return success
            
        except Exception as e:
            logger.error(f"Error restarting exchange {exchange_name}: {e}")
            return False
    
    def get_system_status(self) -> Dict[str, Any]:
        """Получение статуса системы."""
        exchange_status = self.exchange_manager.get_exchange_status()
        
        # Подсчет здоровых бирж
        healthy_count = sum(
            1 for info in exchange_status.values() 
            if info.status.value == "healthy"
        )
        
        self.status.exchanges_count = len(exchange_status)
        self.status.healthy_exchanges_count = healthy_count
        
        # Статистика сбора данных
        collection_stats = {}
        if self.data_collector:
            collection_stats = self.data_collector.get_collection_stats()
        
        # Статистика отправки данных
        send_stats = {}
        if self.data_sender:
            send_stats = self.data_sender.get_send_stats()
        
        uptime = time.time() - self.status.start_time if self.status.start_time else 0
        
        return {
            "system": {
                "is_running": self.status.is_running,
                "uptime": uptime,
                "start_time": self.status.start_time,
                "errors_count": self.status.errors_count
            },
            "exchanges": {
                "total": self.status.exchanges_count,
                "healthy": self.status.healthy_exchanges_count,
                "status": {name: info.status.value for name, info in exchange_status.items()}
            },
            "data_collection": collection_stats,
            "data_sending": send_stats
        }
    
    async def _load_configuration(self, config_path: str) -> None:
        """Загрузка конфигурации."""
        logger.info(f"Loading configuration from {config_path}")
        
        try:
            # Используем существующий AppTemplate для совместимости
            at = AppTemplate([('config=', "<config filename>")])
            at.get_arguments_from_env()
            at.parameters['config'] = config_path
            at.load_settings_from_file(config_path)
            
            self._config = at.settings
            logger.info("Configuration loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    async def _initialize_exchanges(self) -> None:
        """Инициализация бирж."""
        logger.info("Initializing exchanges")
        
        exchanges_config = self._config.get("exchanges", [])
        if not exchanges_config:
            raise ValueError("No exchanges configured")
        
        # Создание конфигураций бирж
        exchange_configs = []
        api_keys = self._config.get("api_keys", {})
        
        for exchange_name in exchanges_config:
            exchange_api_keys = api_keys.get(exchange_name, {})
            config = ExchangeConfig(
                name=exchange_name,
                api_key=exchange_api_keys.get('apiKey', ''),
                secret=exchange_api_keys.get('secret', ''),
                enabled=True
            )
            exchange_configs.append(config)
        
        # Параллельная инициализация бирж
        results = await self.exchange_manager.initialize_exchanges(exchange_configs)
        
        successful = sum(1 for success in results.values() if success)
        logger.info(f"Initialized {successful}/{len(exchange_configs)} exchanges")
        
        if successful == 0:
            raise RuntimeError("Failed to initialize any exchanges")
    
    def _initialize_data_components(self) -> None:
        """Инициализация компонентов для работы с данными."""
        logger.info("Initializing data components")
        
        # Инициализация data collector
        self.data_collector = DataCollector(self.exchange_manager)
        
        # Инициализация data dispatcher если настроен
        data_dispatcher = None
        if all(key in self._config for key in ["user", "password", "host", "out_exchange"]):
            data_dispatcher = processor_template.DataDispatcher(
                user=self._config["user"],
                password=self._config["password"],
                host=self._config["host"],
                exchange=self._config["out_exchange"]
            )
        else:
            logger.warning("Data dispatcher not configured")
        
        # Инициализация data sender
        self.data_sender = DataSender(data_dispatcher)
        
        # Инициализация непрерывного отправщика данных
        self.continuous_sender = ContinuousDataSender(self.data_sender)
        
        # Сохраняем интервалы для координатора
        self.ticker_interval = int(self._config.get("polls_delay", 10))
        self.funding_interval = self.ticker_interval * 2  # Фандинг рейты обновляются реже
        
        logger.info("Data components initialized")
    
    def _setup_signal_handlers(self) -> None:
        """Настройка обработчиков сигналов."""
        try:
            loop = asyncio.get_running_loop()
            
            for sig in (signal.SIGTERM, signal.SIGINT):
                try:
                    loop.add_signal_handler(
                        sig, 
                        lambda s=sig: asyncio.create_task(self._signal_handler(s))
                    )
                except (NotImplementedError, RuntimeError):
                    # Windows или не в главном потоке
                    signal.signal(
                        sig, 
                        lambda s, f: asyncio.create_task(self._signal_handler(s))
                    )
                    
        except Exception as e:
            logger.warning(f"Failed to setup signal handlers: {e}")
    
    async def _signal_handler(self, sig: int) -> None:
        """Обработчик сигналов."""
        logger.info(f"Received signal {sig}, initiating shutdown")
        self._shutdown_event.set()
    
    async def _start_components(self) -> None:
        """Запуск основных компонентов."""
        logger.info("Starting system components")
        
        # Запуск непрерывного отправщика данных
        sender_task = asyncio.create_task(self.continuous_sender.start())
        self._tasks.append(sender_task)
        
        # Запуск координатора данных (он заменяет ContinuousDataCollector)
        coordinator_task = asyncio.create_task(self._data_coordinator())
        self._tasks.append(coordinator_task)
        
        logger.info("System components started")
    
    async def _stop_components(self) -> None:
        """Остановка компонентов."""
        logger.info("Stopping system components")
        
        if self.continuous_sender:
            await self.continuous_sender.stop()
        
        logger.info("System components stopped")
    
    async def _cancel_tasks(self) -> None:
        """Отмена всех задач."""
        if not self._tasks:
            return
        
        logger.info(f"Cancelling {len(self._tasks)} tasks")
        
        for task in self._tasks:
            if not task.done():
                task.cancel()
        
        # Ждем завершения задач
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
    
    async def _main_loop(self) -> None:
        """Основной цикл работы."""
        logger.info("Starting main loop")
        
        try:
            # Ждем завершения всех задач или сигнала остановки
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            logger.info("Main loop cancelled")
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            raise
    
    async def _data_coordinator(self) -> None:
        """Координатор данных - собирает и отправляет данные по расписанию."""
        logger.info("Starting data coordinator")
        
        last_ticker_collection = 0
        last_funding_collection = 0
        
        while not self._shutdown_event.is_set():
            try:
                current_time = time.time()
                
                # Собираем тикеры по расписанию
                if current_time - last_ticker_collection >= self.ticker_interval:
                    logger.debug("Collecting tickers...")
                    ticker_results = await self.data_collector.collect_tickers()
                    last_ticker_collection = current_time
                    
                    if ticker_results:
                        ticker_data = {
                            "tickers": {name: result.data for name, result in ticker_results.items() if result.success},
                            "metadata": {
                                "timestamp": time.time(),
                                "type": "tickers",
                                "exchanges": len(ticker_results)
                            }
                        }
                        await self.continuous_sender.queue_data(ticker_data)
                        logger.info(f"Collected tickers from {len(ticker_results)} exchanges")
                
                # Собираем фандинг рейты по расписанию
                if current_time - last_funding_collection >= self.funding_interval:
                    logger.debug("Collecting funding rates...")
                    funding_results = await self.data_collector.collect_funding_rates()
                    last_funding_collection = current_time
                    
                    if funding_results:
                        funding_data = {
                            "futures": {name: result.data for name, result in funding_results.items() if result.success},
                            "metadata": {
                                "timestamp": time.time(),
                                "type": "funding_rates",
                                "exchanges": len(funding_results)
                            }
                        }
                        await self.continuous_sender.queue_data(funding_data)
                        logger.info(f"Collected funding rates from {len(funding_results)} exchanges")
                
                self.status.last_data_collection = time.time()
                
                # Короткая пауза для проверки shutdown event
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error in data coordinator: {e}", exc_info=True)
                self.status.errors_count += 1
                await asyncio.sleep(5)  # Пауза при ошибке
        
        logger.info("Data coordinator stopped")
