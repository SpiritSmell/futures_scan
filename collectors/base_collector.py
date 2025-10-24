from abc import ABC, abstractmethod
import asyncio
import logging
from typing import Optional, Callable, Any
from models.futures_data import FuturesData


class BaseCollector(ABC):
    def __init__(self, exchange_name: str, api_keys: dict, retry_attempts: int = 3, retry_delays: list = None):
        self.exchange_name = exchange_name
        self.api_keys = api_keys
        self.retry_attempts = retry_attempts
        self.retry_delays = retry_delays or [1, 2, 4]
        self.logger = logging.getLogger(exchange_name)
    
    async def retry_with_backoff(self, func: Callable, *args, **kwargs) -> Any:
        """Выполняет функцию с retry и экспоненциальной задержкой"""
        for attempt in range(self.retry_attempts):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if attempt < self.retry_attempts - 1:
                    delay = self.retry_delays[attempt]
                    self.logger.warning(f"Retry {attempt + 1}/{self.retry_attempts} after {delay}s: {e}")
                    await asyncio.sleep(delay)
                else:
                    self.logger.error(f"Failed after {self.retry_attempts} attempts: {e}")
                    raise
    
    @abstractmethod
    async def collect_futures_data(self, symbol: str) -> Optional[FuturesData]:
        """Собирает данные о фьючерсе"""
        pass
