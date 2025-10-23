from abc import ABC, abstractmethod
import logging
from typing import Optional
from models.futures_data import FuturesData


class BaseCollector(ABC):
    def __init__(self, exchange_name: str, api_keys: dict):
        self.exchange_name = exchange_name
        self.api_keys = api_keys
        self.logger = logging.getLogger(exchange_name)
    
    @abstractmethod
    async def collect_futures_data(self, symbol: str) -> Optional[FuturesData]:
        """Собирает данные о фьючерсе"""
        pass
