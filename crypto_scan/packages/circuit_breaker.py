"""
Circuit Breaker Pattern для обеспечения устойчивости системы.
Предотвращает каскадные сбои при работе с проблемными биржами.
"""

import asyncio
import logging
import time
from enum import Enum
from typing import Dict, Any, Optional, Callable, Awaitable
from dataclasses import dataclass, field
from collections import deque

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Состояния Circuit Breaker."""
    CLOSED = "closed"        # Нормальная работа
    OPEN = "open"           # Блокировка запросов
    HALF_OPEN = "half_open" # Тестирование восстановления


@dataclass
class CircuitBreakerConfig:
    """Конфигурация Circuit Breaker."""
    failure_threshold: int = 5          # Количество ошибок для открытия
    recovery_timeout: float = 60.0      # Время до попытки восстановления (сек)
    success_threshold: int = 3          # Успешных запросов для закрытия
    timeout: float = 30.0               # Таймаут запроса (сек)
    
    # Адаптивные параметры
    max_failure_threshold: int = 20     # Максимальный порог ошибок
    backoff_multiplier: float = 1.5     # Множитель для увеличения таймаута
    max_recovery_timeout: float = 300.0 # Максимальное время восстановления


@dataclass
class CircuitBreakerStats:
    """Статистика Circuit Breaker."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    circuit_opens: int = 0
    circuit_closes: int = 0
    current_failures: int = 0
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None
    
    # Скользящее окно для адаптивности
    recent_failures: deque = field(default_factory=lambda: deque(maxlen=100))
    recent_successes: deque = field(default_factory=lambda: deque(maxlen=100))
    
    @property
    def success_rate(self) -> float:
        """Процент успешных запросов."""
        if self.total_requests == 0:
            return 0.0
        return (self.successful_requests / self.total_requests) * 100
    
    @property
    def failure_rate(self) -> float:
        """Процент неудачных запросов."""
        return 100.0 - self.success_rate
    
    @property
    def recent_failure_rate(self) -> float:
        """Процент неудачных запросов в последнем окне."""
        total_recent = len(self.recent_failures) + len(self.recent_successes)
        if total_recent == 0:
            return 0.0
        return (len(self.recent_failures) / total_recent) * 100


class CircuitBreakerError(Exception):
    """Исключение при открытом Circuit Breaker."""
    pass


class CircuitBreaker:
    """
    Circuit Breaker с адаптивными параметрами и детальной статистикой.
    """
    
    def __init__(self, name: str, config: CircuitBreakerConfig = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitState.CLOSED
        self.stats = CircuitBreakerStats()
        self._lock = asyncio.Lock()
        
        # Адаптивные параметры
        self._current_failure_threshold = self.config.failure_threshold
        self._current_recovery_timeout = self.config.recovery_timeout
        self._last_state_change = time.time()
        
        logger.info(f"Circuit Breaker '{name}' initialized with config: {self.config}")
    
    async def call(self, func: Callable[..., Awaitable[Any]], *args, **kwargs) -> Any:
        """
        Выполнение функции через Circuit Breaker.
        """
        async with self._lock:
            # Проверяем состояние и можем ли выполнить запрос
            if not self._can_execute():
                self.stats.total_requests += 1
                raise CircuitBreakerError(
                    f"Circuit breaker '{self.name}' is {self.state.value}, "
                    f"blocking request"
                )
        
        # Выполняем запрос с таймаутом
        self.stats.total_requests += 1
        start_time = time.time()
        
        try:
            # Применяем таймаут
            result = await asyncio.wait_for(
                func(*args, **kwargs),
                timeout=self.config.timeout
            )
            
            # Запрос успешен
            await self._on_success()
            return result
            
        except asyncio.TimeoutError:
            await self._on_failure(f"Timeout after {self.config.timeout}s")
            raise
        except Exception as e:
            await self._on_failure(str(e))
            raise
        finally:
            execution_time = time.time() - start_time
            logger.debug(f"Circuit breaker '{self.name}' request took {execution_time:.2f}s")
    
    def _can_execute(self) -> bool:
        """Проверка возможности выполнения запроса."""
        current_time = time.time()
        
        if self.state == CircuitState.CLOSED:
            return True
        
        elif self.state == CircuitState.OPEN:
            # Проверяем, можно ли перейти в HALF_OPEN
            if current_time - self._last_state_change >= self._current_recovery_timeout:
                self._transition_to_half_open()
                return True
            return False
        
        elif self.state == CircuitState.HALF_OPEN:
            return True
        
        return False
    
    async def _on_success(self):
        """Обработка успешного запроса."""
        async with self._lock:
            self.stats.successful_requests += 1
            self.stats.current_failures = 0
            self.stats.last_success_time = time.time()
            self.stats.recent_successes.append(time.time())
            
            # Если в HALF_OPEN состоянии, проверяем закрытие
            if self.state == CircuitState.HALF_OPEN:
                success_count = sum(1 for t in self.stats.recent_successes 
                                  if time.time() - t < 60)  # Последняя минута
                
                if success_count >= self.config.success_threshold:
                    self._transition_to_closed()
    
    async def _on_failure(self, error_msg: str):
        """Обработка неудачного запроса."""
        async with self._lock:
            self.stats.failed_requests += 1
            self.stats.current_failures += 1
            self.stats.last_failure_time = time.time()
            self.stats.recent_failures.append(time.time())
            
            logger.warning(f"Circuit breaker '{self.name}' failure: {error_msg}")
            
            # Проверяем необходимость открытия
            if (self.state in [CircuitState.CLOSED, CircuitState.HALF_OPEN] and
                self.stats.current_failures >= self._current_failure_threshold):
                self._transition_to_open()
    
    def _transition_to_open(self):
        """Переход в состояние OPEN."""
        self.state = CircuitState.OPEN
        self.stats.circuit_opens += 1
        self._last_state_change = time.time()
        
        # Адаптивное увеличение параметров
        self._adapt_parameters()
        
        logger.warning(
            f"Circuit breaker '{self.name}' opened after {self.stats.current_failures} failures. "
            f"Recovery timeout: {self._current_recovery_timeout}s"
        )
    
    def _transition_to_half_open(self):
        """Переход в состояние HALF_OPEN."""
        self.state = CircuitState.HALF_OPEN
        self._last_state_change = time.time()
        
        logger.info(f"Circuit breaker '{self.name}' transitioned to HALF_OPEN")
    
    def _transition_to_closed(self):
        """Переход в состояние CLOSED."""
        self.state = CircuitState.CLOSED
        self.stats.circuit_closes += 1
        self.stats.current_failures = 0
        self._last_state_change = time.time()
        
        # Сброс адаптивных параметров при успешном восстановлении
        self._reset_adaptive_parameters()
        
        logger.info(f"Circuit breaker '{self.name}' closed successfully")
    
    def _adapt_parameters(self):
        """Адаптивное изменение параметров на основе истории."""
        # Увеличиваем порог ошибок для более толерантной работы
        self._current_failure_threshold = min(
            self._current_failure_threshold + 2,
            self.config.max_failure_threshold
        )
        
        # Увеличиваем время восстановления
        self._current_recovery_timeout = min(
            self._current_recovery_timeout * self.config.backoff_multiplier,
            self.config.max_recovery_timeout
        )
        
        logger.debug(
            f"Circuit breaker '{self.name}' adapted parameters: "
            f"failure_threshold={self._current_failure_threshold}, "
            f"recovery_timeout={self._current_recovery_timeout}"
        )
    
    def _reset_adaptive_parameters(self):
        """Сброс адаптивных параметров к исходным значениям."""
        self._current_failure_threshold = self.config.failure_threshold
        self._current_recovery_timeout = self.config.recovery_timeout
    
    def get_status(self) -> Dict[str, Any]:
        """Получение текущего статуса Circuit Breaker."""
        current_time = time.time()
        
        return {
            'name': self.name,
            'state': self.state.value,
            'stats': {
                'total_requests': self.stats.total_requests,
                'successful_requests': self.stats.successful_requests,
                'failed_requests': self.stats.failed_requests,
                'success_rate': self.stats.success_rate,
                'failure_rate': self.stats.failure_rate,
                'recent_failure_rate': self.stats.recent_failure_rate,
                'current_failures': self.stats.current_failures,
                'circuit_opens': self.stats.circuit_opens,
                'circuit_closes': self.stats.circuit_closes
            },
            'config': {
                'current_failure_threshold': self._current_failure_threshold,
                'current_recovery_timeout': self._current_recovery_timeout,
                'timeout': self.config.timeout
            },
            'timing': {
                'last_failure_time': self.stats.last_failure_time,
                'last_success_time': self.stats.last_success_time,
                'last_state_change': self._last_state_change,
                'time_since_last_change': current_time - self._last_state_change
            }
        }


class CircuitBreakerManager:
    """
    Менеджер для управления множественными Circuit Breaker'ами.
    """
    
    def __init__(self):
        self.breakers: Dict[str, CircuitBreaker] = {}
        self._default_config = CircuitBreakerConfig()
        
        logger.info("Circuit Breaker Manager initialized")
    
    def get_breaker(self, name: str, config: CircuitBreakerConfig = None) -> CircuitBreaker:
        """Получение или создание Circuit Breaker."""
        if name not in self.breakers:
            self.breakers[name] = CircuitBreaker(
                name=name,
                config=config or self._default_config
            )
        
        return self.breakers[name]
    
    def get_all_status(self) -> Dict[str, Dict[str, Any]]:
        """Получение статуса всех Circuit Breaker'ов."""
        return {name: breaker.get_status() 
                for name, breaker in self.breakers.items()}
    
    def get_health_summary(self) -> Dict[str, Any]:
        """Получение сводки по здоровью всех Circuit Breaker'ов."""
        total_breakers = len(self.breakers)
        if total_breakers == 0:
            return {'total': 0, 'healthy': 0, 'unhealthy': 0, 'health_percentage': 100.0}
        
        healthy_count = sum(1 for breaker in self.breakers.values() 
                           if breaker.state == CircuitState.CLOSED)
        
        return {
            'total': total_breakers,
            'healthy': healthy_count,
            'unhealthy': total_breakers - healthy_count,
            'health_percentage': (healthy_count / total_breakers) * 100,
            'states': {
                'closed': sum(1 for b in self.breakers.values() if b.state == CircuitState.CLOSED),
                'open': sum(1 for b in self.breakers.values() if b.state == CircuitState.OPEN),
                'half_open': sum(1 for b in self.breakers.values() if b.state == CircuitState.HALF_OPEN)
            }
        }
    
    async def reset_breaker(self, name: str) -> bool:
        """Принудительный сброс Circuit Breaker."""
        if name in self.breakers:
            breaker = self.breakers[name]
            async with breaker._lock:
                breaker.state = CircuitState.CLOSED
                breaker.stats.current_failures = 0
                breaker._last_state_change = time.time()
                breaker._reset_adaptive_parameters()
            
            logger.info(f"Circuit breaker '{name}' manually reset")
            return True
        
        return False
    
    async def reset_all_breakers(self):
        """Сброс всех Circuit Breaker'ов."""
        for name in self.breakers.keys():
            await self.reset_breaker(name)
        
        logger.info("All circuit breakers reset")


# Глобальный менеджер Circuit Breaker'ов
circuit_breaker_manager = CircuitBreakerManager()


def circuit_breaker(name: str, config: CircuitBreakerConfig = None):
    """
    Декоратор для применения Circuit Breaker к функции.
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            breaker = circuit_breaker_manager.get_breaker(name, config)
            return await breaker.call(func, *args, **kwargs)
        return wrapper
    return decorator
