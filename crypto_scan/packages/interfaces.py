"""
Базовые интерфейсы и абстракции для модульной архитектуры crypto data collector.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum


class ExchangeStatus(Enum):
    """Статус биржи."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"
    DISABLED = "disabled"


@dataclass
class ExchangeInfo:
    """Информация о бирже."""
    name: str
    status: ExchangeStatus
    last_success: Optional[float] = None
    last_error: Optional[str] = None
    error_count: int = 0
    symbols: List[str] = None
    
    def __post_init__(self):
        if self.symbols is None:
            self.symbols = []


@dataclass
class CollectionResult:
    """Результат сбора данных."""
    success: bool
    data: Dict[str, Any]
    exchange: str
    error: Optional[str] = None
    response_time: float = 0.0
    timestamp: float = 0.0


class ExchangeInterface(ABC):
    """Интерфейс для работы с биржами."""
    
    @abstractmethod
    async def initialize(self, config: Dict[str, Any]) -> bool:
        """Инициализация биржи."""
        pass
    
    @abstractmethod
    async def fetch_tickers(self) -> Dict[str, Any]:
        """Получение тикеров."""
        pass
    
    @abstractmethod
    async def fetch_funding_rates(self, symbols: List[str] = None) -> Dict[str, Any]:
        """Получение фандинг рейтов."""
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """Закрытие соединения."""
        pass
    
    @abstractmethod
    def get_status(self) -> ExchangeInfo:
        """Получение статуса биржи."""
        pass


class DataCollectorInterface(ABC):
    """Интерфейс для сбора данных."""
    
    @abstractmethod
    async def collect_tickers(self, exchanges: List[str] = None) -> Dict[str, CollectionResult]:
        """Сбор тикеров с бирж."""
        pass
    
    @abstractmethod
    async def collect_funding_rates(self, exchanges: List[str] = None) -> Dict[str, CollectionResult]:
        """Сбор фандинг рейтов с бирж."""
        pass
    
    @abstractmethod
    def get_collection_stats(self) -> Dict[str, Any]:
        """Получение статистики сбора данных."""
        pass


class DataSenderInterface(ABC):
    """Интерфейс для отправки данных."""
    
    @abstractmethod
    async def send_data(self, data: Dict[str, Any]) -> bool:
        """Отправка данных."""
        pass
    
    @abstractmethod
    def should_send(self, data: Dict[str, Any]) -> bool:
        """Проверка необходимости отправки данных."""
        pass
    
    @abstractmethod
    def get_send_stats(self) -> Dict[str, Any]:
        """Получение статистики отправки."""
        pass


class ConfigManagerInterface(ABC):
    """Интерфейс для управления конфигурацией."""
    
    @abstractmethod
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """Загрузка конфигурации."""
        pass
    
    @abstractmethod
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """Валидация конфигурации."""
        pass
    
    @abstractmethod
    def get_exchange_configs(self) -> List[Dict[str, Any]]:
        """Получение конфигураций бирж."""
        pass


class MetricsCollectorInterface(ABC):
    """Интерфейс для сбора метрик."""
    
    @abstractmethod
    def record_request(self, exchange: str, success: bool, response_time: float, error_type: str = None) -> None:
        """Запись метрики запроса."""
        pass
    
    @abstractmethod
    def get_metrics(self) -> Dict[str, Any]:
        """Получение всех метрик."""
        pass
    
    @abstractmethod
    def get_health_status(self) -> Dict[str, bool]:
        """Получение статуса здоровья."""
        pass


class OrchestratorInterface(ABC):
    """Интерфейс для главного координатора."""
    
    @abstractmethod
    async def start(self) -> None:
        """Запуск системы."""
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """Остановка системы."""
        pass
    
    @abstractmethod
    async def restart_exchange(self, exchange_name: str) -> bool:
        """Перезапуск конкретной биржи."""
        pass
    
    @abstractmethod
    def get_system_status(self) -> Dict[str, Any]:
        """Получение статуса системы."""
        pass
