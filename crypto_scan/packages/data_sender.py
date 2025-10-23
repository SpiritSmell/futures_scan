"""
DataSender - модуль для отправки данных в очередь сообщений.
"""

import asyncio
import logging
import time
import hashlib
import json
from typing import Dict, Any, Optional
from dataclasses import dataclass, field

from .interfaces import DataSenderInterface

logger = logging.getLogger(__name__)


@dataclass
class SendStats:
    """Статистика отправки данных."""
    total_sends: int = 0
    successful_sends: int = 0
    failed_sends: int = 0
    skipped_sends: int = 0  # Пропущено из-за отсутствия изменений
    last_send_time: float = 0.0
    last_error: Optional[str] = None
    consecutive_errors: int = 0
    
    @property
    def success_rate(self) -> float:
        """Процент успешных отправок."""
        if self.total_sends == 0:
            return 0.0
        return (self.successful_sends / self.total_sends) * 100


class HashManager:
    """Менеджер для отслеживания изменений данных через хеширование."""
    
    def __init__(self):
        self._hashes: Dict[str, str] = {}
        
    def calculate_hash(self, data: Dict[str, Any]) -> str:
        """Вычисление хеша данных."""
        try:
            # Создаем стабильное представление данных
            sorted_data = json.dumps(data, sort_keys=True, default=str)
            return hashlib.md5(sorted_data.encode()).hexdigest()
        except Exception as e:
            logger.error(f"Error calculating hash: {e}")
            return ""
    
    def has_changed(self, key: str, data: Dict[str, Any]) -> bool:
        """Проверка изменения данных."""
        current_hash = self.calculate_hash(data)
        previous_hash = self._hashes.get(key, "")
        
        if current_hash != previous_hash:
            self._hashes[key] = current_hash
            return True
        return False
    
    def get_current_hash(self, key: str) -> str:
        """Получение текущего хеша."""
        return self._hashes.get(key, "")
    
    def reset_hash(self, key: str) -> None:
        """Сброс хеша."""
        self._hashes.pop(key, None)


class DataSender(DataSenderInterface):
    """Отправщик данных в очередь сообщений."""
    
    def __init__(self, data_dispatcher=None, max_consecutive_errors: int = 5):
        self.data_dispatcher = data_dispatcher
        self.max_consecutive_errors = max_consecutive_errors
        self.hash_manager = HashManager()
        self.stats = SendStats()
        self._send_lock = asyncio.Lock()
        
    async def send_data(self, data: Dict[str, Any]) -> bool:
        """Отправка данных."""
        if not self.data_dispatcher:
            logger.warning("No data dispatcher configured, skipping send")
            return False
        
        async with self._send_lock:
            try:
                start_time = time.time()
                
                # Подготавливаем данные для отправки
                prepared_data = self._prepare_data_for_sending(data)
                if not prepared_data:
                    logger.debug("No data to send")
                    return False
                
                # Отправляем данные
                await self.data_dispatcher.set_data(prepared_data)
                
                # Обновляем статистику
                self.stats.total_sends += 1
                self.stats.successful_sends += 1
                self.stats.last_send_time = time.time()
                self.stats.consecutive_errors = 0
                
                send_time = time.time() - start_time
                logger.debug(f"Data sent successfully in {send_time:.3f}s")
                
                return True
                
            except Exception as e:
                self.stats.total_sends += 1
                self.stats.failed_sends += 1
                self.stats.last_error = str(e)
                self.stats.consecutive_errors += 1
                
                logger.error(f"Error sending data: {e}")
                
                # Проверяем лимит последовательных ошибок
                if self.stats.consecutive_errors >= self.max_consecutive_errors:
                    logger.critical(
                        f"Max consecutive errors ({self.max_consecutive_errors}) reached. "
                        "Data sending may be disabled."
                    )
                
                return False
    
    def should_send(self, data: Dict[str, Any]) -> bool:
        """Проверка необходимости отправки данных."""
        if not data:
            return False
        
        # Проверяем изменения в тикерах
        tickers_changed = False
        if "tickers" in data and data["tickers"]:
            tickers_changed = self.hash_manager.has_changed("tickers", data["tickers"])
        
        # Проверяем изменения в фандинг рейтах
        funding_changed = False
        if "funding_rates" in data and data["funding_rates"]:
            funding_changed = self.hash_manager.has_changed("funding_rates", data["funding_rates"])
        
        should_send = tickers_changed or funding_changed
        
        if not should_send:
            self.stats.skipped_sends += 1
            logger.debug("No changes detected, skipping send")
        
        return should_send
    
    def _prepare_data_for_sending(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Подготовка данных для отправки."""
        try:
            # Создаем структуру данных для отправки
            prepared_data = {
                "tickers": data.get("tickers", {}),
                "futures": data.get("funding_rates", {}),  # Совместимость со старым форматом
                "metadata": {
                    "timestamp": int(time.time()),
                    "collection_timestamp": data.get("metadata", {}).get("collection_timestamp", time.time()),
                    "ticker_exchanges": len(data.get("tickers", {})),
                    "funding_exchanges": len(data.get("funding_rates", {})),
                    "total_tickers": sum(
                        len(tickers) for tickers in data.get("tickers", {}).values()
                        if isinstance(tickers, dict)
                    ),
                    "total_funding_rates": sum(
                        len(rates) for rates in data.get("funding_rates", {}).values()
                        if isinstance(rates, dict)
                    )
                }
            }
            
            # Валидация данных
            if not prepared_data["tickers"] and not prepared_data["futures"]:
                logger.warning("No ticker or funding rate data to send")
                return {}
            
            return prepared_data
            
        except Exception as e:
            logger.error(f"Error preparing data for sending: {e}")
            return {}
    
    def get_send_stats(self) -> Dict[str, Any]:
        """Получение статистики отправки."""
        return {
            "total_sends": self.stats.total_sends,
            "successful_sends": self.stats.successful_sends,
            "failed_sends": self.stats.failed_sends,
            "skipped_sends": self.stats.skipped_sends,
            "success_rate": self.stats.success_rate,
            "last_send_time": self.stats.last_send_time,
            "last_error": self.stats.last_error,
            "consecutive_errors": self.stats.consecutive_errors,
            "is_healthy": self.stats.consecutive_errors < self.max_consecutive_errors
        }
    
    def reset_error_count(self) -> None:
        """Сброс счетчика ошибок."""
        self.stats.consecutive_errors = 0
        logger.info("Error count reset")


class ContinuousDataSender:
    """Непрерывный отправщик данных."""
    
    def __init__(self, data_sender: DataSender, check_interval: float = 0.1):
        self.data_sender = data_sender
        self.check_interval = check_interval
        self._shutdown_event = asyncio.Event()
        self._data_queue = asyncio.Queue()
        self._sender_task: Optional[asyncio.Task] = None
        
    async def start(self) -> None:
        """Запуск непрерывной отправки данных."""
        logger.info("Starting continuous data sender")
        
        self._sender_task = asyncio.create_task(self._sender_loop())
        
        try:
            await self._sender_task
        except asyncio.CancelledError:
            logger.info("Continuous data sender cancelled")
        except Exception as e:
            logger.error(f"Error in continuous data sender: {e}")
            raise
    
    async def stop(self) -> None:
        """Остановка непрерывной отправки данных."""
        logger.info("Stopping continuous data sender")
        self._shutdown_event.set()
        
        if self._sender_task and not self._sender_task.done():
            self._sender_task.cancel()
            try:
                await self._sender_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Continuous data sender stopped")
    
    async def queue_data(self, data: Dict[str, Any]) -> None:
        """Добавление данных в очередь для отправки."""
        try:
            await self._data_queue.put(data)
        except Exception as e:
            logger.error(f"Error queuing data: {e}")
    
    async def _sender_loop(self) -> None:
        """Основной цикл отправки данных."""
        logger.info("Data sender loop started")
        
        while not self._shutdown_event.is_set():
            try:
                # Ждем данные из очереди с таймаутом
                try:
                    data = await asyncio.wait_for(
                        self._data_queue.get(),
                        timeout=self.check_interval
                    )
                except asyncio.TimeoutError:
                    continue  # Проверяем shutdown event
                
                # Проверяем необходимость отправки
                if self.data_sender.should_send(data):
                    success = await self.data_sender.send_data(data)
                    if success:
                        logger.debug("Data sent successfully from queue")
                    else:
                        logger.warning("Failed to send data from queue")
                
                # Отмечаем задачу как выполненную
                self._data_queue.task_done()
                
            except Exception as e:
                logger.error(f"Error in sender loop: {e}")
                await asyncio.sleep(1)  # Предотвращаем tight loop при ошибках
        
        logger.info("Data sender loop stopped")


class SmartDataSender(DataSender):
    """Умный отправщик данных с адаптивной логикой."""
    
    def __init__(self, data_dispatcher=None, max_consecutive_errors: int = 5, 
                 adaptive_sending: bool = True):
        super().__init__(data_dispatcher, max_consecutive_errors)
        self.adaptive_sending = adaptive_sending
        self._last_send_times: Dict[str, float] = {}
        self._min_send_interval = 1.0  # Минимальный интервал между отправками (сек)
        
    def should_send(self, data: Dict[str, Any]) -> bool:
        """Умная проверка необходимости отправки данных."""
        if not data:
            return False
        
        current_time = time.time()
        
        # Базовая проверка изменений
        base_should_send = super().should_send(data)
        
        if not self.adaptive_sending:
            return base_should_send
        
        # Адаптивная логика
        if base_should_send:
            # Проверяем минимальный интервал между отправками
            last_send = self._last_send_times.get("last_successful_send", 0)
            if current_time - last_send < self._min_send_interval:
                logger.debug(f"Skipping send due to min interval ({self._min_send_interval}s)")
                return False
            
            # Проверяем состояние системы
            if self.stats.consecutive_errors > 0:
                # При наличии ошибок увеличиваем интервал
                error_backoff = min(2 ** self.stats.consecutive_errors, 60)  # Max 60 секунд
                if current_time - last_send < error_backoff:
                    logger.debug(f"Skipping send due to error backoff ({error_backoff}s)")
                    return False
        
        return base_should_send
    
    async def send_data(self, data: Dict[str, Any]) -> bool:
        """Отправка данных с обновлением временных меток."""
        success = await super().send_data(data)
        
        if success:
            self._last_send_times["last_successful_send"] = time.time()
        
        return success
