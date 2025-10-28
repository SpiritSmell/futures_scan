import asyncio
import logging
from typing import List, Set


class SharedState:
    """Разделяемое состояние между задачами для управления символами"""
    
    def __init__(self, initial_symbols: List[str]):
        self.symbols: Set[str] = set(initial_symbols)
        self.lock = asyncio.Lock()
        self.logger = logging.getLogger("shared_state")
    
    async def get_symbols(self) -> List[str]:
        """Получить текущий список символов"""
        async with self.lock:
            return sorted(list(self.symbols))
    
    async def add_symbol(self, symbol: str) -> bool:
        """Добавить символ. Возвращает True если добавлен, False если уже существует"""
        async with self.lock:
            if symbol not in self.symbols:
                self.symbols.add(symbol)
                self.logger.info(f"Added symbol: {symbol}")
                return True
            self.logger.warning(f"Symbol already exists: {symbol}")
            return False
    
    async def remove_symbol(self, symbol: str) -> bool:
        """Удалить символ. Возвращает True если удален, False если не найден"""
        async with self.lock:
            if symbol in self.symbols:
                self.symbols.remove(symbol)
                self.logger.info(f"Removed symbol: {symbol}")
                return True
            self.logger.warning(f"Symbol not found: {symbol}")
            return False
    
    async def set_symbols(self, symbols: List[str]):
        """Заменить весь список символов"""
        async with self.lock:
            old_symbols = self.symbols.copy()
            self.symbols = set(symbols)
            self.logger.info(f"Symbols updated: {len(old_symbols)} -> {len(self.symbols)}")
