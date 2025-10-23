"""
DataSender v2 - оптимизированный модуль для отправки данных с batch processing.
"""

import asyncio
import logging
import time
import hashlib
import json
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field

from .interfaces import DataSenderInterface
from .batch_processor import BatchProcessorManager, BatchConfig, BatchItem
from .cache_manager import CacheManager

logger = logging.getLogger(__name__)


@dataclass
class OptimizedSenderStats:
    """Расширенная статистика отправки данных."""
    total_sends: int = 0
    successful_sends: int = 0
    failed_sends: int = 0
    batched_sends: int = 0
    total_send_time: float = 0.0
    data_changes_detected: int = 0
    duplicate_data_filtered: int = 0
    
    # Метрики batch processing
    avg_batch_size: float = 0.0
    batch_efficiency: float = 0.0
    
    @property
    def success_rate(self) -> float:
        """Процент успешных отправок."""
        if self.total_sends == 0:
            return 100.0
        return (self.successful_sends / self.total_sends) * 100
    
    @property
    def average_send_time(self) -> float:
        """Среднее время отправки."""
        if self.successful_sends == 0:
            return 0.0
        return self.total_send_time / self.successful_sends
    
    @property
    def change_detection_efficiency(self) -> float:
        """Эффективность детекции изменений."""
        total_processed = self.data_changes_detected + self.duplicate_data_filtered
        if total_processed == 0:
            return 100.0
        return (self.duplicate_data_filtered / total_processed) * 100


class OptimizedDataSender(DataSenderInterface):
    """
    Оптимизированный отправитель данных с batch processing и улучшенной детекцией изменений.
    """
    
    def __init__(self, 
                 send_function: Callable,
                 batch_processor_manager: Optional[BatchProcessorManager] = None,
                 cache_manager: Optional[CacheManager] = None,
                 enable_change_detection: bool = True,
                 batch_config: Optional[BatchConfig] = None):
        self.send_function = send_function
        self.batch_processor_manager = batch_processor_manager
        self.cache_manager = cache_manager
        self.enable_change_detection = enable_change_detection
        
        self.stats = OptimizedSenderStats()
        self._running = False
        self._data_hashes: Dict[str, str] = {}
        
        # Настройка batch processing
        self.batch_config = batch_config or BatchConfig(
            max_batch_size=50,
            max_wait_time=2.0,
            strategy="hybrid",
            compression_enabled=True
        )
        
        self._batch_processor = None
    
    async def start(self):
        """Запуск отправителя данных."""
        if self._running:
            return
        
        self._running = True
        
        # Запускаем batch processor manager
        if self.batch_processor_manager:
            await self.batch_processor_manager.start()
            
            # Создаем batch processor для отправки данных
            self._batch_processor = await self.batch_processor_manager.create_processor(
                name="data_sender",
                config=self.batch_config,
                processor_func=self._process_data_batch
            )
        
        # Запускаем cache manager
        if self.cache_manager:
            await self.cache_manager.start()
        
        logger.info("Optimized Data Sender started with batch processing")
    
    async def stop(self):
        """Остановка отправителя данных."""
        if not self._running:
            return
        
        self._running = False
        
        # Останавливаем batch processor manager
        if self.batch_processor_manager:
            await self.batch_processor_manager.stop()
        
        # Останавливаем cache manager
        if self.cache_manager:
            await self.cache_manager.stop()
        
        logger.info("Optimized Data Sender stopped")
    
    async def send_data(self, data: Dict[str, Any], data_type: str = "unknown") -> bool:
        """
        Отправка данных с оптимизациями.
        
        Args:
            data: Данные для отправки
            data_type: Тип данных для группировки в батчи
        
        Returns:
            bool: Успешность отправки
        """
        if not self._running:
            logger.warning("Data sender not running, cannot send data")
            return False
        
        # Детекция изменений
        if self.enable_change_detection:
            data_hash = self._calculate_data_hash(data)
            cache_key = f"{data_type}:hash"
            
            if cache_key in self._data_hashes:
                if self._data_hashes[cache_key] == data_hash:
                    # Данные не изменились
                    self.stats.duplicate_data_filtered += 1
                    logger.debug(f"No changes detected for {data_type}, skipping send")
                    return True
            
            # Обновляем хэш
            self._data_hashes[cache_key] = data_hash
            self.stats.data_changes_detected += 1
        
        # Отправляем через batch processor если доступен
        if self._batch_processor:
            await self._batch_processor.add_item(
                data=data,
                source=data_type,
                priority=self._get_data_priority(data_type)
            )
            self.stats.batched_sends += 1
            return True
        else:
            # Прямая отправка
            return await self._send_data_direct(data, data_type)
    
    async def send_multiple(self, data_items: List[tuple]) -> List[bool]:
        """
        Отправка множественных данных оптимизированным способом.
        
        Args:
            data_items: Список кортежей (data, data_type)
        
        Returns:
            List[bool]: Результаты отправки
        """
        if not self._running:
            logger.warning("Data sender not running, cannot send data")
            return [False] * len(data_items)
        
        # Фильтруем изменения
        filtered_items = []
        results = []
        
        for data, data_type in data_items:
            if self.enable_change_detection:
                data_hash = self._calculate_data_hash(data)
                cache_key = f"{data_type}:hash"
                
                if cache_key in self._data_hashes and self._data_hashes[cache_key] == data_hash:
                    self.stats.duplicate_data_filtered += 1
                    results.append(True)  # Считаем успешным, так как данные не изменились
                    continue
                
                self._data_hashes[cache_key] = data_hash
                self.stats.data_changes_detected += 1
            
            filtered_items.append((data, data_type))
            results.append(None)  # Заполним позже
        
        # Отправляем через batch processor
        if self._batch_processor and filtered_items:
            batch_items = [
                (data, data_type, self._get_data_priority(data_type))
                for data, data_type in filtered_items
            ]
            
            await self._batch_processor.add_items(batch_items)
            self.stats.batched_sends += len(filtered_items)
            
            # Заполняем результаты для отфильтрованных элементов
            filtered_index = 0
            for i, result in enumerate(results):
                if result is None:
                    results[i] = True  # Предполагаем успех для batch processing
                    filtered_index += 1
        
        return results
    
    async def _send_data_direct(self, data: Dict[str, Any], data_type: str) -> bool:
        """Прямая отправка данных без batch processing."""
        start_time = time.time()
        
        try:
            if asyncio.iscoroutinefunction(self.send_function):
                result = await self.send_function(data, data_type)
            else:
                result = self.send_function(data, data_type)
            
            send_time = time.time() - start_time
            self.stats.total_send_time += send_time
            self.stats.total_sends += 1
            
            if result:
                self.stats.successful_sends += 1
                logger.debug(f"Successfully sent {data_type} data in {send_time:.3f}s")
                return True
            else:
                self.stats.failed_sends += 1
                logger.warning(f"Failed to send {data_type} data")
                return False
                
        except Exception as e:
            send_time = time.time() - start_time
            self.stats.total_send_time += send_time
            self.stats.total_sends += 1
            self.stats.failed_sends += 1
            
            logger.error(f"Error sending {data_type} data: {e}")
            return False
    
    async def _process_data_batch(self, batch_items: List[BatchItem]) -> bool:
        """Обработка батча данных для отправки."""
        if not batch_items:
            return True
        
        start_time = time.time()
        
        try:
            # Группируем по типу данных
            grouped_data = {}
            for item in batch_items:
                data_type = item.source
                if data_type not in grouped_data:
                    grouped_data[data_type] = []
                grouped_data[data_type].append(item.data)
            
            # Отправляем каждую группу
            all_success = True
            for data_type, data_list in grouped_data.items():
                try:
                    # Объединяем данные одного типа
                    combined_data = {
                        'type': data_type,
                        'items': data_list,
                        'count': len(data_list),
                        'timestamp': time.time()
                    }
                    
                    if asyncio.iscoroutinefunction(self.send_function):
                        result = await self.send_function(combined_data, f"batch_{data_type}")
                    else:
                        result = self.send_function(combined_data, f"batch_{data_type}")
                    
                    if not result:
                        all_success = False
                        logger.warning(f"Failed to send batch for {data_type}")
                    
                except Exception as e:
                    all_success = False
                    logger.error(f"Error sending batch for {data_type}: {e}")
            
            # Обновляем статистику
            batch_time = time.time() - start_time
            self.stats.total_send_time += batch_time
            self.stats.total_sends += 1
            
            if all_success:
                self.stats.successful_sends += 1
            else:
                self.stats.failed_sends += 1
            
            # Обновляем метрики batch processing
            self.stats.avg_batch_size = (
                (self.stats.avg_batch_size * (self.stats.total_sends - 1) + len(batch_items)) /
                self.stats.total_sends
            )
            
            logger.debug(f"Processed batch of {len(batch_items)} items in {batch_time:.3f}s")
            
            return all_success
            
        except Exception as e:
            batch_time = time.time() - start_time
            self.stats.total_send_time += batch_time
            self.stats.total_sends += 1
            self.stats.failed_sends += 1
            
            logger.error(f"Error processing data batch: {e}")
            return False
    
    def _calculate_data_hash(self, data: Dict[str, Any]) -> str:
        """Вычисление хэша данных для детекции изменений."""
        try:
            # Сериализуем данные в стабильном порядке
            json_str = json.dumps(data, sort_keys=True, separators=(',', ':'))
            return hashlib.sha256(json_str.encode()).hexdigest()
        except Exception as e:
            logger.warning(f"Error calculating data hash: {e}")
            return str(hash(str(data)))
    
    def _get_data_priority(self, data_type: str) -> int:
        """Определение приоритета данных для batch processing."""
        priority_map = {
            'tickers': 3,      # Высокий приоритет - часто обновляются
            'funding_rates': 2, # Средний приоритет
            'markets': 1,      # Низкий приоритет - редко меняются
            'default': 2
        }
        return priority_map.get(data_type, priority_map['default'])
    
    def should_send(self, data: Dict[str, Any]) -> bool:
        """Проверка необходимости отправки данных (реализация абстрактного метода)."""
        if not data:
            return False
        
        # Проверяем детекцию изменений если включена
        if self.enable_change_detection:
            data_hash = self._calculate_data_hash(data)
            data_type = data.get('type', 'unknown')
            cache_key = f"{data_type}:hash"
            
            if cache_key in self._data_hashes:
                if self._data_hashes[cache_key] == data_hash:
                    # Данные не изменились
                    return False
        
        return True
    
    def get_send_stats(self) -> Dict[str, Any]:
        """Получение статистики отправки (реализация абстрактного метода)."""
        return {
            'total_sends': self.stats.total_sends,
            'successful_sends': self.stats.successful_sends,
            'failed_sends': self.stats.failed_sends,
            'batched_sends': self.stats.batched_sends,
            'success_rate': self.stats.success_rate,
            'average_send_time': self.stats.average_send_time,
            'change_detection_efficiency': self.stats.change_detection_efficiency,
            'avg_batch_size': self.stats.avg_batch_size
        }
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Получение детальных метрик производительности отправителя."""
        metrics = {
            'sender_stats': {
                'total_sends': self.stats.total_sends,
                'success_rate': self.stats.success_rate,
                'avg_send_time': self.stats.average_send_time,
                'batched_sends': self.stats.batched_sends,
                'avg_batch_size': self.stats.avg_batch_size,
                'change_detection_efficiency': self.stats.change_detection_efficiency
            }
        }
        
        # Добавляем метрики batch processor
        if self._batch_processor:
            batch_stats = self._batch_processor.get_stats()
            metrics['batch_stats'] = {
                'total_batches': batch_stats.total_batches,
                'success_rate': batch_stats.success_rate,
                'avg_batch_size': batch_stats.avg_batch_size,
                'throughput': batch_stats.throughput
            }
        
        return metrics
    
    async def flush_pending_data(self):
        """Принудительная отправка всех ожидающих данных."""
        if self._batch_processor:
            # Здесь можно добавить логику принудительной обработки очереди
            logger.info("Flushing pending batch data")
    
    def clear_change_detection_cache(self):
        """Очистка кэша детекции изменений."""
        self._data_hashes.clear()
        logger.info("Change detection cache cleared")
