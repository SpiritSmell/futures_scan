"""
RetryManager - утилита для управления повторными попытками.
"""

import asyncio
import logging
import time
from typing import Callable, Any, Optional, Type, Union, List
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class BackoffStrategy(Enum):
    """Стратегии backoff."""
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"


@dataclass
class RetryConfig:
    """Конфигурация повторных попыток."""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    backoff_multiplier: float = 2.0
    jitter: bool = True
    exceptions: Union[Type[Exception], List[Type[Exception]]] = Exception


class RetryManager:
    """Менеджер повторных попыток."""
    
    def __init__(self, config: RetryConfig = None):
        self.config = config or RetryConfig()
        
    async def retry_async(self, func: Callable, *args, **kwargs) -> Any:
        """Выполнение асинхронной функции с повторными попытками."""
        last_exception = None
        
        for attempt in range(1, self.config.max_attempts + 1):
            try:
                result = await func(*args, **kwargs)
                if attempt > 1:
                    logger.info(f"Function {func.__name__} succeeded on attempt {attempt}")
                return result
                
            except Exception as e:
                last_exception = e
                
                # Проверяем, нужно ли повторять для этого типа исключения
                if not self._should_retry_exception(e):
                    logger.error(f"Non-retryable exception in {func.__name__}: {e}")
                    raise e
                
                if attempt < self.config.max_attempts:
                    delay = self._calculate_delay(attempt)
                    logger.warning(
                        f"Attempt {attempt}/{self.config.max_attempts} failed for {func.__name__}: {e}. "
                        f"Retrying in {delay:.2f}s"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"All {self.config.max_attempts} attempts failed for {func.__name__}. "
                        f"Last error: {e}"
                    )
        
        # Если все попытки исчерпаны, поднимаем последнее исключение
        raise last_exception
    
    def retry_sync(self, func: Callable, *args, **kwargs) -> Any:
        """Выполнение синхронной функции с повторными попытками."""
        last_exception = None
        
        for attempt in range(1, self.config.max_attempts + 1):
            try:
                result = func(*args, **kwargs)
                if attempt > 1:
                    logger.info(f"Function {func.__name__} succeeded on attempt {attempt}")
                return result
                
            except Exception as e:
                last_exception = e
                
                # Проверяем, нужно ли повторять для этого типа исключения
                if not self._should_retry_exception(e):
                    logger.error(f"Non-retryable exception in {func.__name__}: {e}")
                    raise e
                
                if attempt < self.config.max_attempts:
                    delay = self._calculate_delay(attempt)
                    logger.warning(
                        f"Attempt {attempt}/{self.config.max_attempts} failed for {func.__name__}: {e}. "
                        f"Retrying in {delay:.2f}s"
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        f"All {self.config.max_attempts} attempts failed for {func.__name__}. "
                        f"Last error: {e}"
                    )
        
        # Если все попытки исчерпаны, поднимаем последнее исключение
        raise last_exception
    
    def _should_retry_exception(self, exception: Exception) -> bool:
        """Проверка, нужно ли повторять для данного исключения."""
        if isinstance(self.config.exceptions, list):
            return any(isinstance(exception, exc_type) for exc_type in self.config.exceptions)
        else:
            return isinstance(exception, self.config.exceptions)
    
    def _calculate_delay(self, attempt: int) -> float:
        """Вычисление задержки для попытки."""
        if self.config.backoff_strategy == BackoffStrategy.FIXED:
            delay = self.config.base_delay
        elif self.config.backoff_strategy == BackoffStrategy.LINEAR:
            delay = self.config.base_delay * attempt
        elif self.config.backoff_strategy == BackoffStrategy.EXPONENTIAL:
            delay = self.config.base_delay * (self.config.backoff_multiplier ** (attempt - 1))
        else:
            delay = self.config.base_delay
        
        # Ограничиваем максимальную задержку
        delay = min(delay, self.config.max_delay)
        
        # Добавляем jitter для избежания thundering herd
        if self.config.jitter:
            import random
            jitter_range = delay * 0.1  # 10% jitter
            delay += random.uniform(-jitter_range, jitter_range)
            delay = max(0, delay)  # Убеждаемся, что задержка не отрицательная
        
        return delay


# Декораторы для удобного использования
def retry_async(config: RetryConfig = None):
    """Декоратор для асинхронных функций с повторными попытками."""
    def decorator(func: Callable):
        retry_manager = RetryManager(config)
        
        async def wrapper(*args, **kwargs):
            return await retry_manager.retry_async(func, *args, **kwargs)
        
        return wrapper
    return decorator


def retry_sync(config: RetryConfig = None):
    """Декоратор для синхронных функций с повторными попытками."""
    def decorator(func: Callable):
        retry_manager = RetryManager(config)
        
        def wrapper(*args, **kwargs):
            return retry_manager.retry_sync(func, *args, **kwargs)
        
        return wrapper
    return decorator


# Предустановленные конфигурации
NETWORK_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=1.0,
    max_delay=30.0,
    backoff_strategy=BackoffStrategy.EXPONENTIAL,
    exceptions=[ConnectionError, TimeoutError, OSError]
)

API_RETRY_CONFIG = RetryConfig(
    max_attempts=5,
    base_delay=0.5,
    max_delay=10.0,
    backoff_strategy=BackoffStrategy.EXPONENTIAL,
    exceptions=[ConnectionError, TimeoutError]
)

DATABASE_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=2.0,
    max_delay=60.0,
    backoff_strategy=BackoffStrategy.LINEAR,
    exceptions=[ConnectionError, OSError]
)
