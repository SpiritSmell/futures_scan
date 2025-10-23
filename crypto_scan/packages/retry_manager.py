"""
Адаптивная система retry механизмов с экспоненциальным backoff.
Интегрируется с Circuit Breaker для обеспечения устойчивости системы.
"""

import asyncio
import logging
import time
import random
from typing import Dict, Any, Optional, Callable, Awaitable, Type, Union
from dataclasses import dataclass, field
from enum import Enum
from collections import deque

logger = logging.getLogger(__name__)


class RetryStrategy(Enum):
    """Стратегии повторных попыток."""
    FIXED = "fixed"                    # Фиксированная задержка
    LINEAR = "linear"                  # Линейное увеличение
    EXPONENTIAL = "exponential"        # Экспоненциальное увеличение
    FIBONACCI = "fibonacci"            # Последовательность Фибоначчи
    ADAPTIVE = "adaptive"              # Адаптивная на основе истории


@dataclass
class RetryConfig:
    """Конфигурация retry механизма."""
    max_attempts: int = 3              # Максимальное количество попыток
    base_delay: float = 1.0            # Базовая задержка (секунды)
    max_delay: float = 60.0            # Максимальная задержка
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    backoff_multiplier: float = 2.0    # Множитель для экспоненциального backoff
    jitter: bool = True                # Добавление случайности
    jitter_range: float = 0.1          # Диапазон jitter (0.0-1.0)
    
    # Адаптивные параметры
    success_rate_threshold: float = 0.8  # Порог успешности для адаптации
    adaptation_window: int = 100         # Размер окна для адаптации
    
    # Исключения для retry
    retryable_exceptions: tuple = (
        ConnectionError,
        TimeoutError,
        asyncio.TimeoutError,
    )
    non_retryable_exceptions: tuple = (
        ValueError,
        TypeError,
        KeyError,
    )


@dataclass
class RetryStats:
    """Статистика retry механизма."""
    total_attempts: int = 0
    successful_attempts: int = 0
    failed_attempts: int = 0
    total_retries: int = 0
    max_retries_reached: int = 0
    
    # История для адаптации
    recent_attempts: deque = field(default_factory=lambda: deque(maxlen=100))
    recent_delays: deque = field(default_factory=lambda: deque(maxlen=100))
    
    @property
    def success_rate(self) -> float:
        """Процент успешных попыток."""
        if self.total_attempts == 0:
            return 0.0
        return (self.successful_attempts / self.total_attempts) * 100
    
    @property
    def average_retries(self) -> float:
        """Среднее количество повторов на запрос."""
        if self.successful_attempts == 0:
            return 0.0
        return self.total_retries / self.successful_attempts
    
    @property
    def recent_success_rate(self) -> float:
        """Процент успешных попыток в недавнем окне."""
        if not self.recent_attempts:
            return 0.0
        
        successful = sum(1 for success in self.recent_attempts if success)
        return (successful / len(self.recent_attempts)) * 100


class RetryManager:
    """
    Менеджер retry механизмов с адаптивными алгоритмами.
    """
    
    def __init__(self, name: str, config: RetryConfig = None):
        self.name = name
        self.config = config or RetryConfig()
        self.stats = RetryStats()
        
        # Адаптивные параметры
        self._adaptive_base_delay = self.config.base_delay
        self._adaptive_max_attempts = self.config.max_attempts
        
        logger.info(f"Retry Manager '{name}' initialized with strategy: {self.config.strategy.value}")
    
    async def execute_with_retry(
        self,
        func: Callable[..., Awaitable[Any]],
        *args,
        **kwargs
    ) -> Any:
        """
        Выполнение функции с retry механизмом.
        """
        last_exception = None
        attempt = 0
        start_time = time.time()
        
        while attempt < self._adaptive_max_attempts:
            attempt += 1
            self.stats.total_attempts += 1
            
            try:
                # Выполняем функцию
                result = await func(*args, **kwargs)
                
                # Успешное выполнение
                self.stats.successful_attempts += 1
                self.stats.recent_attempts.append(True)
                
                if attempt > 1:
                    self.stats.total_retries += (attempt - 1)
                    logger.info(f"Retry Manager '{self.name}' succeeded after {attempt} attempts")
                
                # Адаптация параметров при успехе
                await self._adapt_on_success(attempt, time.time() - start_time)
                
                return result
                
            except Exception as e:
                last_exception = e
                
                # Проверяем, можно ли повторить
                if not self._is_retryable_exception(e):
                    logger.warning(f"Retry Manager '{self.name}' encountered non-retryable exception: {e}")
                    break
                
                if attempt >= self._adaptive_max_attempts:
                    self.stats.max_retries_reached += 1
                    break
                
                # Вычисляем задержку
                delay = self._calculate_delay(attempt)
                self.stats.recent_delays.append(delay)
                
                logger.warning(
                    f"Retry Manager '{self.name}' attempt {attempt} failed: {e}. "
                    f"Retrying in {delay:.2f}s..."
                )
                
                # Ждем перед следующей попыткой
                await asyncio.sleep(delay)
        
        # Все попытки исчерпаны
        self.stats.failed_attempts += 1
        self.stats.recent_attempts.append(False)
        
        # Адаптация параметров при неудаче
        await self._adapt_on_failure(attempt, time.time() - start_time)
        
        logger.error(
            f"Retry Manager '{self.name}' failed after {attempt} attempts. "
            f"Last exception: {last_exception}"
        )
        
        raise last_exception
    
    def _is_retryable_exception(self, exception: Exception) -> bool:
        """Проверка, можно ли повторить при данном исключении."""
        # Проверяем non-retryable исключения
        if isinstance(exception, self.config.non_retryable_exceptions):
            return False
        
        # Проверяем retryable исключения
        if isinstance(exception, self.config.retryable_exceptions):
            return True
        
        # По умолчанию считаем retryable для сетевых ошибок
        exception_name = exception.__class__.__name__.lower()
        retryable_keywords = ['network', 'connection', 'timeout', 'unavailable', 'service']
        
        return any(keyword in exception_name for keyword in retryable_keywords)
    
    def _calculate_delay(self, attempt: int) -> float:
        """Вычисление задержки на основе стратегии."""
        if self.config.strategy == RetryStrategy.FIXED:
            delay = self._adaptive_base_delay
        
        elif self.config.strategy == RetryStrategy.LINEAR:
            delay = self._adaptive_base_delay * attempt
        
        elif self.config.strategy == RetryStrategy.EXPONENTIAL:
            delay = self._adaptive_base_delay * (self.config.backoff_multiplier ** (attempt - 1))
        
        elif self.config.strategy == RetryStrategy.FIBONACCI:
            delay = self._adaptive_base_delay * self._fibonacci(attempt)
        
        elif self.config.strategy == RetryStrategy.ADAPTIVE:
            delay = self._calculate_adaptive_delay(attempt)
        
        else:
            delay = self._adaptive_base_delay
        
        # Ограничиваем максимальной задержкой
        delay = min(delay, self.config.max_delay)
        
        # Добавляем jitter для избежания thundering herd
        if self.config.jitter:
            jitter_amount = delay * self.config.jitter_range
            jitter = random.uniform(-jitter_amount, jitter_amount)
            delay = max(0.1, delay + jitter)  # Минимум 0.1 секунды
        
        return delay
    
    def _fibonacci(self, n: int) -> int:
        """Вычисление числа Фибоначчи."""
        if n <= 1:
            return 1
        elif n == 2:
            return 1
        
        a, b = 1, 1
        for _ in range(3, n + 1):
            a, b = b, a + b
        return b
    
    def _calculate_adaptive_delay(self, attempt: int) -> float:
        """Адаптивное вычисление задержки на основе истории."""
        base_delay = self._adaptive_base_delay
        
        # Если недостаточно данных, используем экспоненциальную стратегию
        if len(self.stats.recent_delays) < 10:
            return base_delay * (self.config.backoff_multiplier ** (attempt - 1))
        
        # Анализируем недавние задержки
        recent_avg_delay = sum(self.stats.recent_delays) / len(self.stats.recent_delays)
        recent_success_rate = self.stats.recent_success_rate
        
        # Адаптируем на основе успешности
        if recent_success_rate > 80:
            # Высокая успешность - можем уменьшить задержки
            adaptive_multiplier = 0.8
        elif recent_success_rate > 60:
            # Средняя успешность - оставляем как есть
            adaptive_multiplier = 1.0
        else:
            # Низкая успешность - увеличиваем задержки
            adaptive_multiplier = 1.5
        
        return min(
            recent_avg_delay * adaptive_multiplier * attempt,
            self.config.max_delay
        )
    
    async def _adapt_on_success(self, attempts: int, total_time: float):
        """Адаптация параметров при успешном выполнении."""
        if self.config.strategy != RetryStrategy.ADAPTIVE:
            return
        
        # Если успех был быстрым, можем быть более агрессивными
        if attempts == 1 and total_time < 5.0:
            self._adaptive_base_delay = max(
                0.5,
                self._adaptive_base_delay * 0.9
            )
        
        # Если недавняя успешность высокая, можем уменьшить количество попыток
        if self.stats.recent_success_rate > 90 and len(self.stats.recent_attempts) > 50:
            self._adaptive_max_attempts = max(
                2,
                self._adaptive_max_attempts - 1
            )
    
    async def _adapt_on_failure(self, attempts: int, total_time: float):
        """Адаптация параметров при неудачном выполнении."""
        if self.config.strategy != RetryStrategy.ADAPTIVE:
            return
        
        # Увеличиваем базовую задержку при неудачах
        self._adaptive_base_delay = min(
            self.config.max_delay / 4,
            self._adaptive_base_delay * 1.2
        )
        
        # Если недавняя успешность низкая, увеличиваем количество попыток
        if self.stats.recent_success_rate < 50 and len(self.stats.recent_attempts) > 20:
            self._adaptive_max_attempts = min(
                self.config.max_attempts * 2,
                self._adaptive_max_attempts + 1
            )
    
    def get_status(self) -> Dict[str, Any]:
        """Получение статуса retry механизма."""
        return {
            'name': self.name,
            'config': {
                'strategy': self.config.strategy.value,
                'max_attempts': self.config.max_attempts,
                'adaptive_max_attempts': self._adaptive_max_attempts,
                'base_delay': self.config.base_delay,
                'adaptive_base_delay': self._adaptive_base_delay,
                'max_delay': self.config.max_delay
            },
            'stats': {
                'total_attempts': self.stats.total_attempts,
                'successful_attempts': self.stats.successful_attempts,
                'failed_attempts': self.stats.failed_attempts,
                'success_rate': self.stats.success_rate,
                'recent_success_rate': self.stats.recent_success_rate,
                'total_retries': self.stats.total_retries,
                'average_retries': self.stats.average_retries,
                'max_retries_reached': self.stats.max_retries_reached
            }
        }


class RetryManagerRegistry:
    """
    Реестр retry менеджеров для различных компонентов системы.
    """
    
    def __init__(self):
        self.managers: Dict[str, RetryManager] = {}
        self._default_config = RetryConfig()
        
        logger.info("Retry Manager Registry initialized")
    
    def get_manager(self, name: str, config: RetryConfig = None) -> RetryManager:
        """Получение или создание retry менеджера."""
        if name not in self.managers:
            self.managers[name] = RetryManager(
                name=name,
                config=config or self._default_config
            )
        
        return self.managers[name]
    
    def get_all_status(self) -> Dict[str, Dict[str, Any]]:
        """Получение статуса всех retry менеджеров."""
        return {name: manager.get_status() 
                for name, manager in self.managers.items()}
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Получение сводки по производительности retry механизмов."""
        if not self.managers:
            return {
                'total_managers': 0,
                'overall_success_rate': 100.0,
                'total_retries': 0,
                'average_retries_per_request': 0.0
            }
        
        total_attempts = sum(m.stats.total_attempts for m in self.managers.values())
        successful_attempts = sum(m.stats.successful_attempts for m in self.managers.values())
        total_retries = sum(m.stats.total_retries for m in self.managers.values())
        
        overall_success_rate = 0.0
        if total_attempts > 0:
            overall_success_rate = (successful_attempts / total_attempts) * 100
        
        average_retries = 0.0
        if successful_attempts > 0:
            average_retries = total_retries / successful_attempts
        
        return {
            'total_managers': len(self.managers),
            'overall_success_rate': overall_success_rate,
            'total_attempts': total_attempts,
            'successful_attempts': successful_attempts,
            'total_retries': total_retries,
            'average_retries_per_request': average_retries,
            'managers_by_strategy': self._get_strategy_distribution()
        }
    
    def _get_strategy_distribution(self) -> Dict[str, int]:
        """Получение распределения стратегий retry."""
        distribution = {}
        for manager in self.managers.values():
            strategy = manager.config.strategy.value
            distribution[strategy] = distribution.get(strategy, 0) + 1
        return distribution


# Глобальный реестр retry менеджеров
retry_manager_registry = RetryManagerRegistry()


def with_retry(name: str, config: RetryConfig = None):
    """
    Декоратор для применения retry механизма к функции.
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            manager = retry_manager_registry.get_manager(name, config)
            return await manager.execute_with_retry(func, *args, **kwargs)
        return wrapper
    return decorator
