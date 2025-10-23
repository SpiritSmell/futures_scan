"""
Batch Processor для оптимизации отправки данных.
Группирует данные в батчи для более эффективной обработки и отправки.
"""

import asyncio
import time
import json
from typing import Any, Dict, List, Optional, Callable, Tuple
from dataclasses import dataclass, field
from collections import defaultdict, deque
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class BatchStrategy(Enum):
    """Стратегии формирования батчей."""
    SIZE_BASED = "size_based"  # По количеству элементов
    TIME_BASED = "time_based"  # По времени
    HYBRID = "hybrid"  # Комбинированная стратегия


@dataclass
class BatchConfig:
    """Конфигурация батч-процессора."""
    max_batch_size: int = 100
    max_wait_time: float = 5.0  # секунды
    strategy: BatchStrategy = BatchStrategy.HYBRID
    
    # Специфичные настройки
    min_batch_size: int = 1
    compression_enabled: bool = True
    retry_failed_batches: bool = True
    max_retries: int = 3


@dataclass
class BatchItem:
    """Элемент батча с метаданными."""
    data: Any
    timestamp: float
    source: str
    priority: int = 0
    retry_count: int = 0
    
    def __post_init__(self):
        if self.timestamp == 0:
            self.timestamp = time.time()


@dataclass
class BatchStats:
    """Статистика обработки батчей."""
    total_batches: int = 0
    successful_batches: int = 0
    failed_batches: int = 0
    total_items: int = 0
    avg_batch_size: float = 0.0
    avg_processing_time: float = 0.0
    
    # Скользящие окна для метрик
    batch_sizes: deque = field(default_factory=lambda: deque(maxlen=100))
    processing_times: deque = field(default_factory=lambda: deque(maxlen=100))
    
    def add_batch(self, batch_size: int, processing_time: float, success: bool = True):
        """Добавление статистики батча."""
        self.total_batches += 1
        self.total_items += batch_size
        
        if success:
            self.successful_batches += 1
        else:
            self.failed_batches += 1
        
        self.batch_sizes.append(batch_size)
        self.processing_times.append(processing_time)
        
        # Обновляем средние значения
        if self.batch_sizes:
            self.avg_batch_size = sum(self.batch_sizes) / len(self.batch_sizes)
        if self.processing_times:
            self.avg_processing_time = sum(self.processing_times) / len(self.processing_times)
    
    @property
    def success_rate(self) -> float:
        """Коэффициент успешных батчей."""
        if self.total_batches == 0:
            return 1.0
        return self.successful_batches / self.total_batches
    
    @property
    def throughput(self) -> float:
        """Пропускная способность (элементов в секунду)."""
        if not self.processing_times or self.avg_processing_time == 0:
            return 0.0
        return self.avg_batch_size / self.avg_processing_time


class BatchProcessor:
    """
    Процессор для группировки и обработки данных батчами.
    """
    
    def __init__(self, 
                 name: str,
                 config: BatchConfig,
                 processor_func: Callable[[List[BatchItem]], Any]):
        self.name = name
        self.config = config
        self.processor_func = processor_func
        
        self._queue: deque = deque()
        self._processing_task: Optional[asyncio.Task] = None
        self._running = False
        self._stats = BatchStats()
        self._lock = asyncio.Lock()
        
        # Очереди для повторной обработки
        self._retry_queue: deque = deque()
        self._failed_items: List[BatchItem] = []
    
    async def start(self):
        """Запуск процессора."""
        if self._running:
            return
        
        self._running = True
        self._processing_task = asyncio.create_task(self._processing_loop())
        logger.info(f"Batch processor '{self.name}' started")
    
    async def stop(self):
        """Остановка процессора."""
        if not self._running:
            return
        
        self._running = False
        
        # Обрабатываем оставшиеся элементы
        if self._queue:
            await self._process_remaining_items()
        
        if self._processing_task:
            self._processing_task.cancel()
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass
        
        logger.info(f"Batch processor '{self.name}' stopped")
    
    async def add_item(self, data: Any, source: str = "unknown", priority: int = 0):
        """Добавление элемента в очередь."""
        item = BatchItem(
            data=data,
            timestamp=time.time(),
            source=source,
            priority=priority
        )
        
        async with self._lock:
            self._queue.append(item)
    
    async def add_items(self, items: List[Tuple[Any, str, int]]):
        """Добавление множественных элементов."""
        batch_items = [
            BatchItem(data=data, timestamp=time.time(), source=source, priority=priority)
            for data, source, priority in items
        ]
        
        async with self._lock:
            self._queue.extend(batch_items)
    
    def get_stats(self) -> BatchStats:
        """Получение статистики процессора."""
        return self._stats
    
    async def _processing_loop(self):
        """Основной цикл обработки батчей."""
        while self._running:
            try:
                # Проверяем retry очередь
                if self._retry_queue:
                    await self._process_retry_queue()
                
                # Формируем и обрабатываем батч
                batch = await self._form_batch()
                if batch:
                    await self._process_batch(batch)
                else:
                    # Если нет данных, ждем немного
                    await asyncio.sleep(0.1)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in batch processing loop for '{self.name}': {e}")
                await asyncio.sleep(1.0)
    
    async def _form_batch(self) -> Optional[List[BatchItem]]:
        """Формирование батча согласно стратегии."""
        async with self._lock:
            if not self._queue:
                return None
            
            batch = []
            current_time = time.time()
            
            if self.config.strategy == BatchStrategy.SIZE_BASED:
                # Формируем по размеру
                while len(batch) < self.config.max_batch_size and self._queue:
                    batch.append(self._queue.popleft())
            
            elif self.config.strategy == BatchStrategy.TIME_BASED:
                # Формируем по времени
                oldest_item_time = self._queue[0].timestamp if self._queue else current_time
                if current_time - oldest_item_time >= self.config.max_wait_time:
                    while self._queue:
                        batch.append(self._queue.popleft())
            
            elif self.config.strategy == BatchStrategy.HYBRID:
                # Гибридная стратегия
                oldest_item_time = self._queue[0].timestamp if self._queue else current_time
                time_exceeded = current_time - oldest_item_time >= self.config.max_wait_time
                size_reached = len(self._queue) >= self.config.max_batch_size
                
                if time_exceeded or size_reached:
                    max_items = min(self.config.max_batch_size, len(self._queue))
                    for _ in range(max_items):
                        if self._queue:
                            batch.append(self._queue.popleft())
            
            # Проверяем минимальный размер батча
            if len(batch) < self.config.min_batch_size and self._queue:
                # Возвращаем элементы обратно в очередь
                for item in reversed(batch):
                    self._queue.appendleft(item)
                return None
            
            return batch if batch else None
    
    async def _process_batch(self, batch: List[BatchItem]):
        """Обработка батча."""
        if not batch:
            return
        
        start_time = time.time()
        
        try:
            # Сортируем по приоритету (высокий приоритет первым)
            batch.sort(key=lambda x: x.priority, reverse=True)
            
            # Обрабатываем батч
            if asyncio.iscoroutinefunction(self.processor_func):
                result = await self.processor_func(batch)
            else:
                result = self.processor_func(batch)
            
            processing_time = time.time() - start_time
            self._stats.add_batch(len(batch), processing_time, success=True)
            
            logger.debug(f"Successfully processed batch of {len(batch)} items in {processing_time:.3f}s")
            
        except Exception as e:
            processing_time = time.time() - start_time
            self._stats.add_batch(len(batch), processing_time, success=False)
            
            logger.error(f"Failed to process batch of {len(batch)} items: {e}")
            
            # Добавляем в retry очередь если включены повторы
            if self.config.retry_failed_batches:
                await self._add_to_retry_queue(batch)
            else:
                self._failed_items.extend(batch)
    
    async def _add_to_retry_queue(self, batch: List[BatchItem]):
        """Добавление батча в очередь повторной обработки."""
        for item in batch:
            item.retry_count += 1
            if item.retry_count <= self.config.max_retries:
                self._retry_queue.append(item)
            else:
                self._failed_items.append(item)
                logger.warning(f"Item from {item.source} exceeded max retries ({self.config.max_retries})")
    
    async def _process_retry_queue(self):
        """Обработка очереди повторов."""
        if not self._retry_queue:
            return
        
        # Берем элементы из retry очереди
        retry_batch = []
        while len(retry_batch) < self.config.max_batch_size and self._retry_queue:
            retry_batch.append(self._retry_queue.popleft())
        
        if retry_batch:
            logger.info(f"Processing retry batch of {len(retry_batch)} items")
            await self._process_batch(retry_batch)
    
    async def _process_remaining_items(self):
        """Обработка оставшихся элементов при остановке."""
        while self._queue:
            batch = []
            while len(batch) < self.config.max_batch_size and self._queue:
                batch.append(self._queue.popleft())
            
            if batch:
                await self._process_batch(batch)


class BatchProcessorManager:
    """
    Менеджер множественных batch процессоров.
    """
    
    def __init__(self):
        self._processors: Dict[str, BatchProcessor] = {}
        self._running = False
    
    async def start(self):
        """Запуск всех процессоров."""
        if self._running:
            return
        
        self._running = True
        logger.info("Batch Processor Manager started")
    
    async def stop(self):
        """Остановка всех процессоров."""
        if not self._running:
            return
        
        self._running = False
        
        # Останавливаем все процессоры
        for processor in self._processors.values():
            await processor.stop()
        
        logger.info("Batch Processor Manager stopped")
    
    async def create_processor(self, 
                             name: str, 
                             config: BatchConfig,
                             processor_func: Callable) -> BatchProcessor:
        """Создание нового процессора."""
        if name in self._processors:
            raise ValueError(f"Processor '{name}' already exists")
        
        processor = BatchProcessor(name, config, processor_func)
        await processor.start()
        
        self._processors[name] = processor
        logger.info(f"Created batch processor '{name}'")
        
        return processor
    
    def get_processor(self, name: str) -> Optional[BatchProcessor]:
        """Получение процессора по имени."""
        return self._processors.get(name)
    
    def get_stats(self) -> Dict[str, Any]:
        """Получение общей статистики менеджера."""
        all_stats = self.get_all_stats()
        
        total_items = sum(stats.total_items for stats in all_stats.values())
        total_batches = sum(stats.total_batches for stats in all_stats.values())
        total_failed = sum(stats.failed_batches for stats in all_stats.values())
        
        return {
            'processors_count': len(self._processors),
            'total_items': total_items,
            'total_batches': total_batches,
            'total_failed': total_failed,
            'success_rate': (total_batches - total_failed) / max(1, total_batches) * 100,
            'processors': all_stats
        }
    
    def get_all_stats(self) -> Dict[str, BatchStats]:
        """Получение статистики всех процессоров."""
        return {
            name: processor.get_stats()
            for name, processor in self._processors.items()
        }


# Утилиты для работы с батчами
def compress_batch_data(items: List[BatchItem]) -> bytes:
    """Сжатие данных батча."""
    import gzip
    
    data = [item.data for item in items]
    json_data = json.dumps(data, separators=(',', ':')).encode('utf-8')
    
    return gzip.compress(json_data)


def decompress_batch_data(compressed_data: bytes) -> List[Any]:
    """Распаковка сжатых данных батча."""
    import gzip
    
    json_data = gzip.decompress(compressed_data).decode('utf-8')
    return json.loads(json_data)


def split_batch_by_source(items: List[BatchItem]) -> Dict[str, List[BatchItem]]:
    """Разделение батча по источникам."""
    result = defaultdict(list)
    for item in items:
        result[item.source].append(item)
    return dict(result)


def merge_similar_data(items: List[BatchItem]) -> List[BatchItem]:
    """Объединение похожих данных в батче."""
    # Простая реализация - группировка по типу данных
    grouped = defaultdict(list)
    
    for item in items:
        data_type = type(item.data).__name__
        grouped[data_type].append(item)
    
    # Возвращаем объединенные элементы
    merged = []
    for data_type, group in grouped.items():
        if len(group) == 1:
            merged.extend(group)
        else:
            # Создаем объединенный элемент
            combined_data = [item.data for item in group]
            merged_item = BatchItem(
                data=combined_data,
                timestamp=min(item.timestamp for item in group),
                source=f"merged_{data_type}",
                priority=max(item.priority for item in group)
            )
            merged.append(merged_item)
    
    return merged
