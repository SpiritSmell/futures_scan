"""
Connection Pool Manager для оптимизации сетевых соединений к биржам.
Реализует пулинг соединений, адаптивное rate limiting и мониторинг производительности.
"""

import asyncio
import aiohttp
import time
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from collections import defaultdict, deque
import logging
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


@dataclass
class ConnectionStats:
    """Статистика соединений для биржи."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    avg_response_time: float = 0.0
    last_request_time: float = 0.0
    rate_limit_hits: int = 0
    
    # Скользящее окно для расчета среднего времени ответа
    response_times: deque = field(default_factory=lambda: deque(maxlen=100))
    
    def add_request(self, response_time: float, success: bool = True):
        """Добавление статистики запроса."""
        self.total_requests += 1
        self.last_request_time = time.time()
        
        if success:
            self.successful_requests += 1
            self.response_times.append(response_time)
            if self.response_times:
                self.avg_response_time = sum(self.response_times) / len(self.response_times)
        else:
            self.failed_requests += 1
    
    def add_rate_limit_hit(self):
        """Регистрация попадания в rate limit."""
        self.rate_limit_hits += 1
    
    @property
    def success_rate(self) -> float:
        """Коэффициент успешных запросов."""
        if self.total_requests == 0:
            return 1.0
        return self.successful_requests / self.total_requests
    
    @property
    def requests_per_minute(self) -> float:
        """Количество запросов в минуту."""
        if self.total_requests == 0:
            return 0.0
        
        time_span = time.time() - (self.last_request_time - (self.total_requests * self.avg_response_time))
        if time_span <= 0:
            return 0.0
        
        return (self.total_requests / time_span) * 60


@dataclass
class RateLimitConfig:
    """Конфигурация rate limiting для биржи."""
    requests_per_second: float = 10.0
    burst_size: int = 20
    backoff_factor: float = 1.5
    max_backoff: float = 60.0
    
    # Адаптивные параметры
    adaptive: bool = True
    min_requests_per_second: float = 1.0
    max_requests_per_second: float = 50.0


class AdaptiveRateLimiter:
    """
    Адаптивный rate limiter, который подстраивается под ответы биржи.
    """
    
    def __init__(self, config: RateLimitConfig):
        self.config = config
        self.current_rate = config.requests_per_second
        self.last_request_time = 0.0
        self.request_times = deque(maxlen=config.burst_size)
        self.backoff_until = 0.0
        
    async def acquire(self) -> float:
        """
        Получение разрешения на запрос.
        Возвращает время ожидания в секундах.
        """
        now = time.time()
        
        # Проверяем backoff
        if now < self.backoff_until:
            wait_time = self.backoff_until - now
            await asyncio.sleep(wait_time)
            return wait_time
        
        # Очищаем старые запросы
        cutoff_time = now - 1.0  # Окно в 1 секунду
        while self.request_times and self.request_times[0] < cutoff_time:
            self.request_times.popleft()
        
        # Проверяем rate limit
        if len(self.request_times) >= self.current_rate:
            wait_time = 1.0 / self.current_rate
            await asyncio.sleep(wait_time)
            return wait_time
        
        # Регистрируем запрос
        self.request_times.append(now)
        self.last_request_time = now
        
        return 0.0
    
    def on_success(self, response_time: float):
        """Обработка успешного ответа."""
        if not self.config.adaptive:
            return
        
        # Если ответ быстрый и нет ошибок, можем увеличить rate
        if response_time < 1.0 and self.current_rate < self.config.max_requests_per_second:
            self.current_rate = min(
                self.current_rate * 1.1,
                self.config.max_requests_per_second
            )
    
    def on_rate_limit(self):
        """Обработка rate limit ошибки."""
        # Уменьшаем rate и устанавливаем backoff
        self.current_rate = max(
            self.current_rate / self.config.backoff_factor,
            self.config.min_requests_per_second
        )
        
        backoff_time = min(
            1.0 / self.current_rate * self.config.backoff_factor,
            self.config.max_backoff
        )
        
        self.backoff_until = time.time() + backoff_time
        logger.warning(f"Rate limit hit, backing off for {backoff_time:.2f}s, new rate: {self.current_rate:.2f} req/s")
    
    def on_error(self):
        """Обработка ошибки запроса."""
        if not self.config.adaptive:
            return
        
        # Немного снижаем rate при ошибках
        self.current_rate = max(
            self.current_rate * 0.9,
            self.config.min_requests_per_second
        )


class ConnectionPool:
    """
    Пул соединений для конкретной биржи.
    """
    
    def __init__(self, 
                 exchange_name: str,
                 max_connections: int = 10,
                 rate_limit_config: Optional[RateLimitConfig] = None):
        self.exchange_name = exchange_name
        self.max_connections = max_connections
        self.rate_limiter = AdaptiveRateLimiter(rate_limit_config or RateLimitConfig())
        
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(max_connections)
        self._stats = ConnectionStats()
        self._connector: Optional[aiohttp.TCPConnector] = None
        
    async def start(self):
        """Инициализация пула соединений."""
        if self._session:
            return
        
        # Создаем оптимизированный connector
        self._connector = aiohttp.TCPConnector(
            limit=self.max_connections,
            limit_per_host=self.max_connections,
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        
        # Создаем сессию с оптимизированными настройками
        timeout = aiohttp.ClientTimeout(
            total=30,
            connect=10,
            sock_read=20
        )
        
        self._session = aiohttp.ClientSession(
            connector=self._connector,
            timeout=timeout,
            headers={
                'User-Agent': 'CryptoCollector/2.0',
                'Accept': 'application/json',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive'
            }
        )
        
        logger.info(f"Connection pool started for {self.exchange_name} with {self.max_connections} max connections")
    
    async def stop(self):
        """Закрытие пула соединений."""
        if self._session:
            await self._session.close()
            self._session = None
        
        if self._connector:
            await self._connector.close()
            self._connector = None
        
        logger.info(f"Connection pool stopped for {self.exchange_name}")
    
    @asynccontextmanager
    async def get_session(self):
        """Получение сессии из пула."""
        if not self._session:
            raise RuntimeError(f"Connection pool for {self.exchange_name} not started")
        
        # Ждем разрешения от rate limiter
        await self.rate_limiter.acquire()
        
        # Ждем свободного слота в пуле
        async with self._semaphore:
            yield self._session
    
    async def request(self, method: str, url: str, **kwargs) -> Tuple[Any, float]:
        """
        Выполнение HTTP запроса через пул.
        
        Returns:
            Tuple[response_data, response_time]
        """
        start_time = time.time()
        
        try:
            async with self.get_session() as session:
                async with session.request(method, url, **kwargs) as response:
                    response_time = time.time() - start_time
                    
                    # Проверяем на rate limit
                    if response.status == 429:
                        self.rate_limiter.on_rate_limit()
                        self._stats.add_rate_limit_hit()
                        self._stats.add_request(response_time, success=False)
                        raise aiohttp.ClientResponseError(
                            request_info=response.request_info,
                            history=response.history,
                            status=response.status,
                            message="Rate limit exceeded"
                        )
                    
                    # Проверяем успешность
                    response.raise_for_status()
                    
                    # Получаем данные
                    if response.content_type == 'application/json':
                        data = await response.json()
                    else:
                        data = await response.text()
                    
                    # Обновляем статистику
                    self.rate_limiter.on_success(response_time)
                    self._stats.add_request(response_time, success=True)
                    
                    return data, response_time
                    
        except aiohttp.ClientResponseError as e:
            response_time = time.time() - start_time
            if e.status != 429:  # Rate limit уже обработан выше
                self.rate_limiter.on_error()
            self._stats.add_request(response_time, success=False)
            raise
            
        except Exception as e:
            response_time = time.time() - start_time
            self.rate_limiter.on_error()
            self._stats.add_request(response_time, success=False)
            raise
    
    def get_stats(self) -> ConnectionStats:
        """Получение статистики соединений."""
        return self._stats


class ConnectionPoolManager:
    """
    Менеджер пулов соединений для всех бирж.
    """
    
    def __init__(self):
        self._pools: Dict[str, ConnectionPool] = {}
        self._running = False
        
        # Конфигурации rate limiting для разных бирж
        self._rate_configs = {
            'binance': RateLimitConfig(requests_per_second=20.0, burst_size=50),
            'bybit': RateLimitConfig(requests_per_second=15.0, burst_size=30),
            'okx': RateLimitConfig(requests_per_second=20.0, burst_size=40),
            'htx': RateLimitConfig(requests_per_second=10.0, burst_size=20),
            'default': RateLimitConfig(requests_per_second=10.0, burst_size=20)
        }
    
    async def start(self):
        """Запуск менеджера пулов."""
        if self._running:
            return
        
        self._running = True
        logger.info("Connection Pool Manager started")
    
    async def stop(self):
        """Остановка всех пулов."""
        if not self._running:
            return
        
        self._running = False
        
        # Закрываем все пулы
        for pool in self._pools.values():
            await pool.stop()
        
        self._pools.clear()
        logger.info("Connection Pool Manager stopped")
    
    async def get_pool(self, exchange_name: str, max_connections: int = 10) -> ConnectionPool:
        """Получение или создание пула для биржи."""
        if exchange_name not in self._pools:
            rate_config = self._rate_configs.get(
                exchange_name.lower(), 
                self._rate_configs['default']
            )
            
            pool = ConnectionPool(
                exchange_name=exchange_name,
                max_connections=max_connections,
                rate_limit_config=rate_config
            )
            
            await pool.start()
            self._pools[exchange_name] = pool
            
            logger.info(f"Created connection pool for {exchange_name}")
        
        return self._pools[exchange_name]
    
    def get_all_stats(self) -> Dict[str, ConnectionStats]:
        """Получение статистики всех пулов."""
        return {
            exchange: pool.get_stats()
            for exchange, pool in self._pools.items()
        }
    
    async def health_check(self) -> Dict[str, bool]:
        """Проверка здоровья всех пулов."""
        health_status = {}
        
        for exchange, pool in self._pools.items():
            stats = pool.get_stats()
            # Считаем пул здоровым если success rate > 80% и есть недавние запросы
            is_healthy = (
                stats.success_rate > 0.8 and
                (time.time() - stats.last_request_time) < 300  # 5 минут
            )
            health_status[exchange] = is_healthy
        
        return health_status
