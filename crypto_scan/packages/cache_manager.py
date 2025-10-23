"""
Менеджер кэширования для оптимизации производительности системы сбора данных.
Реализует TTL кэш с автоматической очисткой и статистикой.
"""

import asyncio
import time
import weakref
from typing import Any, Dict, Optional, Set, Tuple, Callable
from dataclasses import dataclass, field
from threading import RLock
import logging

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """Запись в кэше с метаданными."""
    value: Any
    created_at: float
    ttl: float
    access_count: int = 0
    last_access: float = field(default_factory=time.time)
    
    @property
    def is_expired(self) -> bool:
        """Проверка истечения TTL."""
        return time.time() - self.created_at > self.ttl
    
    def access(self) -> Any:
        """Обновление статистики доступа и возврат значения."""
        self.access_count += 1
        self.last_access = time.time()
        return self.value


@dataclass
class CacheStats:
    """Статистика работы кэша."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    size: int = 0
    
    @property
    def hit_ratio(self) -> float:
        """Коэффициент попаданий в кэш."""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0


class TTLCache:
    """
    Thread-safe TTL кэш с автоматической очисткой устаревших записей.
    """
    
    def __init__(self, 
                 max_size: int = 1000,
                 default_ttl: float = 300.0,
                 cleanup_interval: float = 60.0):
        """
        Инициализация кэша.
        
        Args:
            max_size: Максимальный размер кэша
            default_ttl: TTL по умолчанию в секундах
            cleanup_interval: Интервал очистки в секундах
        """
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.cleanup_interval = cleanup_interval
        
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = RLock()
        self._stats = CacheStats()
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = False
        
    async def start(self):
        """Запуск фоновой очистки кэша."""
        if self._running:
            return
            
        self._running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info(f"TTL Cache started with max_size={self.max_size}, default_ttl={self.default_ttl}s")
    
    async def stop(self):
        """Остановка фоновой очистки кэша."""
        self._running = False
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        logger.info("TTL Cache stopped")
    
    def get(self, key: str) -> Optional[Any]:
        """Получение значения из кэша."""
        with self._lock:
            entry = self._cache.get(key)
            
            if entry is None:
                self._stats.misses += 1
                return None
            
            if entry.is_expired:
                del self._cache[key]
                self._stats.misses += 1
                self._stats.evictions += 1
                return None
            
            self._stats.hits += 1
            return entry.access()
    
    def set(self, key: str, value: Any, ttl: Optional[float] = None):
        """Сохранение значения в кэш."""
        if ttl is None:
            ttl = self.default_ttl
            
        with self._lock:
            # Проверяем размер кэша и очищаем если нужно
            if len(self._cache) >= self.max_size and key not in self._cache:
                self._evict_oldest()
            
            entry = CacheEntry(
                value=value,
                created_at=time.time(),
                ttl=ttl
            )
            
            self._cache[key] = entry
            self._stats.size = len(self._cache)
    
    def delete(self, key: str) -> bool:
        """Удаление значения из кэша."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                self._stats.size = len(self._cache)
                return True
            return False
    
    def clear(self):
        """Очистка всего кэша."""
        with self._lock:
            self._cache.clear()
            self._stats.size = 0
            self._stats.evictions += len(self._cache)
    
    def get_stats(self) -> CacheStats:
        """Получение статистики кэша."""
        with self._lock:
            self._stats.size = len(self._cache)
            return CacheStats(
                hits=self._stats.hits,
                misses=self._stats.misses,
                evictions=self._stats.evictions,
                size=self._stats.size
            )
    
    def _evict_oldest(self):
        """Удаление самой старой записи."""
        if not self._cache:
            return
            
        oldest_key = min(self._cache.keys(), 
                        key=lambda k: self._cache[k].last_access)
        del self._cache[oldest_key]
        self._stats.evictions += 1
    
    async def _cleanup_loop(self):
        """Фоновая очистка устаревших записей."""
        while self._running:
            try:
                await asyncio.sleep(self.cleanup_interval)
                self._cleanup_expired()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cache cleanup: {e}")
    
    def _cleanup_expired(self):
        """Очистка устаревших записей."""
        with self._lock:
            expired_keys = [
                key for key, entry in self._cache.items()
                if entry.is_expired
            ]
            
            for key in expired_keys:
                del self._cache[key]
                self._stats.evictions += 1
            
            if expired_keys:
                logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")
                self._stats.size = len(self._cache)


class CacheManager:
    """
    Менеджер множественных кэшей для разных типов данных.
    """
    
    def __init__(self):
        self._caches: Dict[str, TTLCache] = {}
        self._running = False
    
    async def start(self):
        """Запуск всех кэшей."""
        if self._running:
            return
            
        self._running = True
        
        # Создаем специализированные кэши
        self._caches['tickers'] = TTLCache(
            max_size=5000,
            default_ttl=30.0,  # Тикеры обновляются часто
            cleanup_interval=30.0
        )
        
        self._caches['funding_rates'] = TTLCache(
            max_size=2000,
            default_ttl=300.0,  # Фандинг обновляется реже
            cleanup_interval=60.0
        )
        
        self._caches['markets'] = TTLCache(
            max_size=1000,
            default_ttl=3600.0,  # Рынки меняются редко
            cleanup_interval=300.0
        )
        
        self._caches['exchange_info'] = TTLCache(
            max_size=100,
            default_ttl=1800.0,  # Информация о биржах
            cleanup_interval=300.0
        )
        
        # Запускаем все кэши
        for cache in self._caches.values():
            await cache.start()
            
        logger.info(f"Cache Manager started with {len(self._caches)} caches")
    
    async def stop(self):
        """Остановка всех кэшей."""
        if not self._running:
            return
            
        self._running = False
        
        for cache in self._caches.values():
            await cache.stop()
            
        logger.info("Cache Manager stopped")
    
    def get_cache(self, cache_type: str) -> Optional[TTLCache]:
        """Получение кэша по типу."""
        return self._caches.get(cache_type)
    
    def get(self, cache_type: str, key: str) -> Optional[Any]:
        """Получение значения из указанного кэша."""
        cache = self._caches.get(cache_type)
        if cache:
            return cache.get(key)
        return None
    
    def set(self, cache_type: str, key: str, value: Any, ttl: Optional[float] = None):
        """Установка значения в указанный кэш."""
        cache = self._caches.get(cache_type)
        if cache:
            cache.set(key, value, ttl)
    
    def get_stats(self) -> Dict[str, Any]:
        """Получение общей статистики кэшей."""
        all_stats = self.get_all_stats()
        
        total_hits = sum(stats.hits for stats in all_stats.values())
        total_misses = sum(stats.misses for stats in all_stats.values())
        total_requests = total_hits + total_misses
        
        return {
            'total_requests': total_requests,
            'total_hits': total_hits,
            'total_misses': total_misses,
            'hit_rate': (total_hits / max(1, total_requests)) * 100,
            'cache_count': len(self._caches),
            'individual_stats': all_stats
        }
    
    def get_all_stats(self) -> Dict[str, CacheStats]:
        """Получение статистики всех кэшей."""
        return {
            cache_type: cache.get_stats()
            for cache_type, cache in self._caches.items()
        }


def cache_key(*args, **kwargs) -> str:
    """Генерация ключа кэша из аргументов."""
    key_parts = []
    
    # Добавляем позиционные аргументы
    for arg in args:
        if hasattr(arg, '__name__'):  # Для функций/методов
            key_parts.append(arg.__name__)
        else:
            key_parts.append(str(arg))
    
    # Добавляем именованные аргументы
    for k, v in sorted(kwargs.items()):
        key_parts.append(f"{k}={v}")
    
    return ":".join(key_parts)


def cached(cache_type: str, ttl: Optional[float] = None):
    """
    Декоратор для кэширования результатов функций.
    
    Args:
        cache_type: Тип кэша для использования
        ttl: TTL для кэшированного значения
    """
    def decorator(func: Callable):
        async def async_wrapper(*args, **kwargs):
            # Получаем менеджер кэша из глобального контекста
            cache_manager = getattr(async_wrapper, '_cache_manager', None)
            if not cache_manager:
                return await func(*args, **kwargs)
            
            cache = cache_manager.get_cache(cache_type)
            if not cache:
                return await func(*args, **kwargs)
            
            # Генерируем ключ кэша
            key = cache_key(func.__name__, *args, **kwargs)
            
            # Пытаемся получить из кэша
            cached_result = cache.get(key)
            if cached_result is not None:
                return cached_result
            
            # Выполняем функцию и кэшируем результат
            result = await func(*args, **kwargs)
            cache.set(key, result, ttl)
            
            return result
        
        def sync_wrapper(*args, **kwargs):
            # Аналогично для синхронных функций
            cache_manager = getattr(sync_wrapper, '_cache_manager', None)
            if not cache_manager:
                return func(*args, **kwargs)
            
            cache = cache_manager.get_cache(cache_type)
            if not cache:
                return func(*args, **kwargs)
            
            key = cache_key(func.__name__, *args, **kwargs)
            
            cached_result = cache.get(key)
            if cached_result is not None:
                return cached_result
            
            result = func(*args, **kwargs)
            cache.set(key, result, ttl)
            
            return result
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator
