"""
Система мониторинга здоровья бирж с автоматическими health checks.
Интегрируется с Circuit Breaker и retry механизмами.
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional, List, Callable, Awaitable
from dataclasses import dataclass, field
from enum import Enum
from collections import deque

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Статусы здоровья компонента."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckConfig:
    """Конфигурация health check."""
    check_interval: float = 60.0        # Интервал проверки (секунды)
    timeout: float = 30.0               # Таймаут проверки
    failure_threshold: int = 3          # Количество неудач для UNHEALTHY
    recovery_threshold: int = 2         # Количество успехов для восстановления
    degraded_threshold: int = 1         # Количество неудач для DEGRADED
    
    # Адаптивные параметры
    min_check_interval: float = 30.0    # Минимальный интервал
    max_check_interval: float = 300.0   # Максимальный интервал
    adaptive_scaling: bool = True       # Адаптивное масштабирование интервалов


@dataclass
class HealthMetrics:
    """Метрики здоровья компонента."""
    total_checks: int = 0
    successful_checks: int = 0
    failed_checks: int = 0
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_check_time: Optional[float] = None
    last_success_time: Optional[float] = None
    last_failure_time: Optional[float] = None
    
    # История для анализа трендов
    recent_checks: deque = field(default_factory=lambda: deque(maxlen=100))
    response_times: deque = field(default_factory=lambda: deque(maxlen=50))
    
    @property
    def success_rate(self) -> float:
        """Процент успешных проверок."""
        if self.total_checks == 0:
            return 0.0
        return (self.successful_checks / self.total_checks) * 100
    
    @property
    def recent_success_rate(self) -> float:
        """Процент успешных проверок в недавней истории."""
        if not self.recent_checks:
            return 0.0
        
        successful = sum(1 for success in self.recent_checks if success)
        return (successful / len(self.recent_checks)) * 100
    
    @property
    def average_response_time(self) -> float:
        """Среднее время отклика."""
        if not self.response_times:
            return 0.0
        return sum(self.response_times) / len(self.response_times)
    
    @property
    def uptime_percentage(self) -> float:
        """Процент времени в рабочем состоянии."""
        if not self.last_success_time:
            return 0.0
        
        current_time = time.time()
        if self.last_failure_time and self.last_failure_time > self.last_success_time:
            # Сейчас не работает
            total_time = current_time - (self.last_success_time - 3600)  # За последний час
            downtime = current_time - self.last_failure_time
            return max(0.0, ((total_time - downtime) / total_time) * 100)
        
        return 100.0  # Работает сейчас


class HealthCheck:
    """
    Компонент для выполнения health check с адаптивными параметрами.
    """
    
    def __init__(
        self,
        name: str,
        check_function: Callable[[], Awaitable[bool]],
        config: HealthCheckConfig = None
    ):
        self.name = name
        self.check_function = check_function
        self.config = config or HealthCheckConfig()
        self.metrics = HealthMetrics()
        self.status = HealthStatus.UNKNOWN
        
        # Адаптивные параметры
        self._current_check_interval = self.config.check_interval
        self._is_running = False
        self._check_task: Optional[asyncio.Task] = None
        
        logger.info(f"Health Check '{name}' initialized")
    
    async def start(self):
        """Запуск мониторинга здоровья."""
        if self._is_running:
            return
        
        self._is_running = True
        self._check_task = asyncio.create_task(self._monitoring_loop())
        
        logger.info(f"Health Check '{self.name}' started")
    
    async def stop(self):
        """Остановка мониторинга здоровья."""
        self._is_running = False
        
        if self._check_task:
            self._check_task.cancel()
            try:
                await self._check_task
            except asyncio.CancelledError:
                pass
        
        logger.info(f"Health Check '{self.name}' stopped")
    
    async def _monitoring_loop(self):
        """Основной цикл мониторинга."""
        while self._is_running:
            try:
                await self.perform_check()
                await asyncio.sleep(self._current_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health Check '{self.name}' monitoring loop error: {e}")
                await asyncio.sleep(5)  # Короткая пауза при ошибке
    
    async def perform_check(self) -> bool:
        """Выполнение одной проверки здоровья."""
        start_time = time.time()
        self.metrics.total_checks += 1
        self.metrics.last_check_time = start_time
        
        try:
            # Выполняем проверку с таймаутом
            is_healthy = await asyncio.wait_for(
                self.check_function(),
                timeout=self.config.timeout
            )
            
            # Записываем время отклика
            response_time = time.time() - start_time
            self.metrics.response_times.append(response_time)
            
            if is_healthy:
                await self._on_check_success(response_time)
            else:
                await self._on_check_failure("Health check returned False")
            
            return is_healthy
            
        except asyncio.TimeoutError:
            await self._on_check_failure(f"Timeout after {self.config.timeout}s")
            return False
        except Exception as e:
            await self._on_check_failure(f"Exception: {e}")
            return False
    
    async def _on_check_success(self, response_time: float):
        """Обработка успешной проверки."""
        self.metrics.successful_checks += 1
        self.metrics.consecutive_successes += 1
        self.metrics.consecutive_failures = 0
        self.metrics.last_success_time = time.time()
        self.metrics.recent_checks.append(True)
        
        # Определяем новый статус
        old_status = self.status
        
        if self.metrics.consecutive_successes >= self.config.recovery_threshold:
            self.status = HealthStatus.HEALTHY
        elif self.status == HealthStatus.UNHEALTHY:
            self.status = HealthStatus.DEGRADED
        
        # Адаптируем интервал проверки при успехе
        if self.config.adaptive_scaling and self.status == HealthStatus.HEALTHY:
            self._current_check_interval = min(
                self._current_check_interval * 1.1,
                self.config.max_check_interval
            )
        
        if old_status != self.status:
            logger.info(
                f"Health Check '{self.name}' status changed: {old_status.value} -> {self.status.value}"
            )
        
        logger.debug(
            f"Health Check '{self.name}' success: {response_time:.2f}s, "
            f"consecutive: {self.metrics.consecutive_successes}"
        )
    
    async def _on_check_failure(self, error_msg: str):
        """Обработка неудачной проверки."""
        self.metrics.failed_checks += 1
        self.metrics.consecutive_failures += 1
        self.metrics.consecutive_successes = 0
        self.metrics.last_failure_time = time.time()
        self.metrics.recent_checks.append(False)
        
        # Определяем новый статус
        old_status = self.status
        
        if self.metrics.consecutive_failures >= self.config.failure_threshold:
            self.status = HealthStatus.UNHEALTHY
        elif self.metrics.consecutive_failures >= self.config.degraded_threshold:
            self.status = HealthStatus.DEGRADED
        
        # Адаптируем интервал проверки при неудаче
        if self.config.adaptive_scaling and self.status in [HealthStatus.DEGRADED, HealthStatus.UNHEALTHY]:
            self._current_check_interval = max(
                self._current_check_interval * 0.8,
                self.config.min_check_interval
            )
        
        if old_status != self.status:
            logger.warning(
                f"Health Check '{self.name}' status changed: {old_status.value} -> {self.status.value}"
            )
        
        logger.warning(
            f"Health Check '{self.name}' failure: {error_msg}, "
            f"consecutive: {self.metrics.consecutive_failures}"
        )
    
    def get_status(self) -> Dict[str, Any]:
        """Получение текущего статуса health check."""
        current_time = time.time()
        
        return {
            'name': self.name,
            'status': self.status.value,
            'is_running': self._is_running,
            'config': {
                'check_interval': self.config.check_interval,
                'current_check_interval': self._current_check_interval,
                'timeout': self.config.timeout,
                'failure_threshold': self.config.failure_threshold,
                'recovery_threshold': self.config.recovery_threshold
            },
            'metrics': {
                'total_checks': self.metrics.total_checks,
                'successful_checks': self.metrics.successful_checks,
                'failed_checks': self.metrics.failed_checks,
                'success_rate': self.metrics.success_rate,
                'recent_success_rate': self.metrics.recent_success_rate,
                'consecutive_failures': self.metrics.consecutive_failures,
                'consecutive_successes': self.metrics.consecutive_successes,
                'average_response_time': self.metrics.average_response_time,
                'uptime_percentage': self.metrics.uptime_percentage
            },
            'timing': {
                'last_check_time': self.metrics.last_check_time,
                'last_success_time': self.metrics.last_success_time,
                'last_failure_time': self.metrics.last_failure_time,
                'time_since_last_check': current_time - self.metrics.last_check_time if self.metrics.last_check_time else None
            }
        }


class HealthMonitor:
    """
    Центральный монитор здоровья для управления множественными health checks.
    """
    
    def __init__(self):
        self.health_checks: Dict[str, HealthCheck] = {}
        self._is_running = False
        
        logger.info("Health Monitor initialized")
    
    def add_health_check(
        self,
        name: str,
        check_function: Callable[[], Awaitable[bool]],
        config: HealthCheckConfig = None
    ) -> HealthCheck:
        """Добавление health check."""
        if name in self.health_checks:
            logger.warning(f"Health check '{name}' already exists, replacing")
        
        health_check = HealthCheck(name, check_function, config)
        self.health_checks[name] = health_check
        
        # Запускаем, если монитор уже работает
        if self._is_running:
            asyncio.create_task(health_check.start())
        
        logger.info(f"Health check '{name}' added")
        return health_check
    
    async def start(self):
        """Запуск всех health checks."""
        if self._is_running:
            return
        
        self._is_running = True
        
        # Запускаем все health checks
        start_tasks = [hc.start() for hc in self.health_checks.values()]
        if start_tasks:
            await asyncio.gather(*start_tasks, return_exceptions=True)
        
        logger.info(f"Health Monitor started with {len(self.health_checks)} checks")
    
    async def stop(self):
        """Остановка всех health checks."""
        if not self._is_running:
            return
        
        self._is_running = False
        
        # Останавливаем все health checks
        stop_tasks = [hc.stop() for hc in self.health_checks.values()]
        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)
        
        logger.info("Health Monitor stopped")
    
    def get_health_check(self, name: str) -> Optional[HealthCheck]:
        """Получение health check по имени."""
        return self.health_checks.get(name)
    
    def get_overall_status(self) -> Dict[str, Any]:
        """Получение общего статуса здоровья системы."""
        if not self.health_checks:
            return {
                'overall_status': HealthStatus.UNKNOWN.value,
                'total_checks': 0,
                'healthy_count': 0,
                'degraded_count': 0,
                'unhealthy_count': 0,
                'unknown_count': 0,
                'health_percentage': 0.0
            }
        
        status_counts = {
            HealthStatus.HEALTHY: 0,
            HealthStatus.DEGRADED: 0,
            HealthStatus.UNHEALTHY: 0,
            HealthStatus.UNKNOWN: 0
        }
        
        for hc in self.health_checks.values():
            status_counts[hc.status] += 1
        
        total_checks = len(self.health_checks)
        healthy_count = status_counts[HealthStatus.HEALTHY]
        degraded_count = status_counts[HealthStatus.DEGRADED]
        unhealthy_count = status_counts[HealthStatus.UNHEALTHY]
        unknown_count = status_counts[HealthStatus.UNKNOWN]
        
        # Определяем общий статус
        if unhealthy_count > 0:
            overall_status = HealthStatus.UNHEALTHY
        elif degraded_count > 0:
            overall_status = HealthStatus.DEGRADED
        elif healthy_count > 0:
            overall_status = HealthStatus.HEALTHY
        else:
            overall_status = HealthStatus.UNKNOWN
        
        # Вычисляем процент здоровья
        health_score = (healthy_count * 1.0 + degraded_count * 0.5) / total_checks
        health_percentage = health_score * 100
        
        return {
            'overall_status': overall_status.value,
            'total_checks': total_checks,
            'healthy_count': healthy_count,
            'degraded_count': degraded_count,
            'unhealthy_count': unhealthy_count,
            'unknown_count': unknown_count,
            'health_percentage': health_percentage,
            'is_running': self._is_running
        }
    
    def get_all_status(self) -> Dict[str, Dict[str, Any]]:
        """Получение статуса всех health checks."""
        return {name: hc.get_status() 
                for name, hc in self.health_checks.items()}
    
    def get_unhealthy_components(self) -> List[str]:
        """Получение списка нездоровых компонентов."""
        return [name for name, hc in self.health_checks.items() 
                if hc.status == HealthStatus.UNHEALTHY]
    
    def get_degraded_components(self) -> List[str]:
        """Получение списка деградированных компонентов."""
        return [name for name, hc in self.health_checks.items() 
                if hc.status == HealthStatus.DEGRADED]
    
    async def force_check_all(self) -> Dict[str, bool]:
        """Принудительная проверка всех компонентов."""
        if not self.health_checks:
            return {}
        
        check_tasks = {name: hc.perform_check() 
                      for name, hc in self.health_checks.items()}
        
        results = await asyncio.gather(*check_tasks.values(), return_exceptions=True)
        
        return {name: result if isinstance(result, bool) else False 
                for name, result in zip(check_tasks.keys(), results)}
    
    async def force_check(self, name: str) -> Optional[bool]:
        """Принудительная проверка конкретного компонента."""
        if name not in self.health_checks:
            return None
        
        return await self.health_checks[name].perform_check()


# Глобальный монитор здоровья
health_monitor = HealthMonitor()
