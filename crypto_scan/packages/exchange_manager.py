"""
ExchangeManager - управление подключениями к криптобиржам.
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field

import ccxt.pro as ccxt_async
import ccxt as ccxt_sync
from ccxt.base.exchange import Exchange as CCXTExchange

from .interfaces import ExchangeInterface, ExchangeInfo, ExchangeStatus
from .connection_pool import ConnectionPoolManager
from .cache_manager import CacheManager

logger = logging.getLogger(__name__)

# Константы
DEFAULT_TIMEOUT = 30
DEFAULT_RATE_LIMIT = 1000
MAX_RETRIES = 3
SYSTEM_CA_BUNDLE = '/etc/ssl/certs/ca-certificates.crt'


@dataclass
class ExchangeConfig:
    """Конфигурация биржи."""
    name: str
    api_key: str = ""
    secret: str = ""
    market_type: str = "swap"
    rate_limit: int = DEFAULT_RATE_LIMIT
    timeout: int = DEFAULT_TIMEOUT
    max_retries: int = MAX_RETRIES
    enabled: bool = True
    options: Dict[str, Any] = field(default_factory=dict)


class CcxtExchangeWrapper(ExchangeInterface):
    """Обертка для CCXT биржи."""
    
    def __init__(self, config: ExchangeConfig):
        self.config = config
        self.async_exchange: Optional[CCXTExchange] = None
        self.sync_exchange: Optional[CCXTExchange] = None
        self.info = ExchangeInfo(
            name=config.name,
            status=ExchangeStatus.DISABLED
        )
        self._lock = asyncio.Lock()
        
    async def initialize(self, config: Dict[str, Any] = None) -> bool:
        """Инициализация биржи."""
        try:
            logger.info(f"Initializing exchange {self.config.name}")
            
            # Конфигурация CCXT
            exchange_config = {
                'enableRateLimit': True,
                'rateLimit': self.config.rate_limit,
                'timeout': self.config.timeout * 1000,  # Convert to ms
                'options': {
                    'defaultType': self.config.market_type,
                    'adjustForTimeDifference': True,
                    'recvWindow': 10 * 1000,
                    **self.config.options
                },
                'apiKey': self.config.api_key,
                'secret': self.config.secret,
                'verbose': False,
                'verify': False if self.config.name == 'htx' else True,
            }
            
            # Специальная конфигурация для HTX
            if self.config.name == 'htx':
                exchange_config['sandbox'] = False
                exchange_config['urls'] = {
                    'logo': 'https://user-images.githubusercontent.com/1294454/76137448-22748a80-604e-11ea-8069-6e389271911d.jpg',
                    'api': {
                        'public': 'https://api.hbdm.com',
                        'private': 'https://api.hbdm.com',
                        'v1': 'https://api.hbdm.com/api/v1',
                        'v2': 'https://api.hbdm.com/api/v2',
                        'swap': 'https://api.hbdm.com/swap-api/v1',
                        'future': 'https://api.hbdm.com/api/v1'
                    },
                    'www': 'https://www.hbdm.com',
                    'doc': 'https://huobiapi.github.io/docs/dm/v1/en/',
                    'test': {
                        'public': 'https://api.hbdm.com',
                        'private': 'https://api.hbdm.com'
                    }
                }
                exchange_config['options']['defaultType'] = 'swap'
                # Принудительно переопределяем базовые URL
                exchange_config['hostname'] = 'api.hbdm.com'
            
            # Создание экземпляров
            self.async_exchange = getattr(ccxt_async, self.config.name)(exchange_config)
            self.sync_exchange = getattr(ccxt_sync, self.config.name)(exchange_config)
            
            # Дополнительная настройка для HTX после создания
            if self.config.name == 'htx':
                # Принудительно устанавливаем правильные URL
                for exchange in [self.async_exchange, self.sync_exchange]:
                    exchange.urls['api']['public'] = 'https://api.hbdm.com'
                    exchange.urls['api']['private'] = 'https://api.hbdm.com'
                    exchange.hostname = 'api.hbdm.com'
                    if hasattr(exchange, 'baseURL'):
                        exchange.baseURL = 'https://api.hbdm.com'
            
            # Проверка поддерживаемых методов
            required_methods = ['fetchTicker', 'fetchMarkets', 'fetchFundingRates']
            missing_methods = [m for m in required_methods if not self.async_exchange.has.get(m, False)]
            
            if missing_methods:
                raise ValueError(f"Exchange {self.config.name} missing methods: {missing_methods}")
            
            # Загрузка рынков для проверки соединения
            try:
                markets = self.sync_exchange.load_markets()
            except Exception as load_error:
                # Для HTX пробуем альтернативный подход
                if self.config.name == 'htx':
                    logger.warning(f"Standard market loading failed for HTX, trying alternative approach: {load_error}")
                    try:
                        # Пробуем загрузить только swap рынки
                        self.sync_exchange.options['defaultType'] = 'swap'
                        markets = self.sync_exchange.load_markets()
                    except Exception as alt_error:
                        logger.error(f"Alternative market loading also failed for HTX: {alt_error}")
                        # Если HTX недоступна, пропускаем её но не падаем
                        raise ConnectionError(f"HTX exchange is currently unavailable: {alt_error}")
                else:
                    raise load_error
            
            # Фильтрация активных swap рынков с учетом особенностей бирж
            if self.config.name == 'htx':
                # HTX использует другую структуру для фьючерсов
                swap_markets = [
                    market['symbol'] 
                    for market in markets.values() 
                    if (market.get('type') == 'swap' or market.get('contract', False)) and market.get('active', True)
                ]
            else:
                swap_markets = [
                    market['symbol'] 
                    for market in markets.values() 
                    if market.get('swap', False) and market.get('active', True)
                ]
            
            self.info.symbols = swap_markets
            self.info.status = ExchangeStatus.HEALTHY
            self.info.last_success = time.time()
            
            logger.info(f"Successfully initialized {self.config.name} with {len(swap_markets)} markets")
            return True
            
        except ImportError:
            error_msg = f"Exchange {self.config.name} not supported by CCXT"
            logger.error(error_msg)
            self.info.status = ExchangeStatus.FAILED
            self.info.last_error = error_msg
            return False
            
        except ConnectionError as e:
            error_msg = f"Network connection failed for {self.config.name}: {e}"
            logger.warning(error_msg)
            self.info.status = ExchangeStatus.FAILED
            self.info.last_error = str(e)
            self.info.error_count += 1
            
            # Закрываем соединения при ошибке
            if self.async_exchange:
                try:
                    await self.async_exchange.close()
                except:
                    pass
            
            # Для сетевых ошибок не падаем, а просто помечаем биржу как недоступную
            logger.info(f"Exchange {self.config.name} will be marked as unavailable and skipped")
            return False
            
        except Exception as e:
            error_msg = f"Failed to initialize {self.config.name}: {e}"
            logger.error(error_msg, exc_info=True)
            self.info.status = ExchangeStatus.FAILED
            self.info.last_error = str(e)
            self.info.error_count += 1
            
            # Закрываем соединения при ошибке
            if self.async_exchange:
                try:
                    await self.async_exchange.close()
                except:
                    pass
                    
            return False
    
    async def fetch_tickers(self) -> Dict[str, Any]:
        """Получение тикеров."""
        if not self.async_exchange or self.info.status == ExchangeStatus.FAILED:
            raise RuntimeError(f"Exchange {self.config.name} not initialized or failed")
        
        try:
            async with self._lock:
                start_time = time.time()
                
                # Rate limiting
                if hasattr(self.async_exchange, 'rate_limit'):
                    await asyncio.sleep(self.async_exchange.rate_limit / 1000)
                
                tickers = await self.async_exchange.fetch_tickers()
                
                # Обновляем статистику
                self.info.last_success = time.time()
                if self.info.status == ExchangeStatus.DEGRADED:
                    self.info.status = ExchangeStatus.HEALTHY
                
                logger.debug(f"Fetched {len(tickers)} tickers from {self.config.name} in {time.time() - start_time:.2f}s")
                return tickers
                
        except Exception as e:
            self._handle_error(f"Error fetching tickers: {e}")
            raise
    
    async def fetch_funding_rates(self, symbols: List[str] = None) -> Dict[str, Any]:
        """Получение фандинг рейтов."""
        if not self.async_exchange or self.info.status == ExchangeStatus.FAILED:
            raise RuntimeError(f"Exchange {self.config.name} not initialized or failed")
        
        try:
            symbols_to_fetch = symbols or self.info.symbols
            if not symbols_to_fetch:
                logger.warning(f"No symbols to fetch funding rates for {self.config.name}")
                return {}
            
            async with self._lock:
                start_time = time.time()
                
                # Rate limiting
                if hasattr(self.async_exchange, 'rate_limit'):
                    await asyncio.sleep(self.async_exchange.rate_limit / 1000)
                
                funding_rates = {}
                
                # Обрабатываем символы чанками для избежания rate limits
                chunk_size = 10
                for i in range(0, len(symbols_to_fetch), chunk_size):
                    chunk = symbols_to_fetch[i:i + chunk_size]
                    try:
                        rates = await self.async_exchange.fetch_funding_rates(chunk)
                        funding_rates.update(rates)
                    except Exception as e:
                        logger.warning(f"Error fetching funding rates chunk {i//chunk_size + 1} from {self.config.name}: {e}")
                
                # Обновляем статистику
                self.info.last_success = time.time()
                if self.info.status == ExchangeStatus.DEGRADED:
                    self.info.status = ExchangeStatus.HEALTHY
                
                logger.debug(f"Fetched funding rates for {len(funding_rates)} symbols from {self.config.name} in {time.time() - start_time:.2f}s")
                return funding_rates
                
        except Exception as e:
            self._handle_error(f"Error fetching funding rates: {e}")
            raise
    
    async def close(self) -> None:
        """Закрытие соединения."""
        if self.async_exchange:
            try:
                await self.async_exchange.close()
                logger.debug(f"Closed connection to {self.config.name}")
            except Exception as e:
                logger.warning(f"Error closing {self.config.name}: {e}")
            finally:
                self.async_exchange = None
                self.sync_exchange = None
                self.info.status = ExchangeStatus.DISABLED
    
    def get_status(self) -> ExchangeInfo:
        """Получение статуса биржи."""
        return self.info
    
    def _handle_error(self, error_msg: str) -> None:
        """Обработка ошибки."""
        self.info.error_count += 1
        self.info.last_error = error_msg
        
        # Определяем статус на основе количества ошибок
        if self.info.error_count >= 5:
            self.info.status = ExchangeStatus.FAILED
        elif self.info.error_count >= 2:
            self.info.status = ExchangeStatus.DEGRADED
        
        logger.error(f"{self.config.name}: {error_msg}")


class ExchangeManager:
    """Менеджер для управления множественными биржами."""
    
    def __init__(self, 
                 connection_pool_manager: Optional[ConnectionPoolManager] = None,
                 cache_manager: Optional[CacheManager] = None):
        self.exchanges: Dict[str, CcxtExchangeWrapper] = {}
        self._initialization_lock = asyncio.Lock()
        self.connection_pool_manager = connection_pool_manager
        self.cache_manager = cache_manager
        
    async def add_exchange(self, config: ExchangeConfig) -> bool:
        """Добавление и инициализация биржи."""
        if not config.enabled:
            logger.info(f"Exchange {config.name} is disabled, skipping")
            return False
            
        async with self._initialization_lock:
            if config.name in self.exchanges:
                logger.warning(f"Exchange {config.name} already exists")
                return True
            
            exchange = CcxtExchangeWrapper(config)
            success = await exchange.initialize()
            
            if success:
                self.exchanges[config.name] = exchange
                logger.info(f"Successfully added exchange {config.name}")
            else:
                logger.error(f"Failed to add exchange {config.name}")
                
            return success
    
    async def initialize_exchanges(self, configs: List[ExchangeConfig]) -> Dict[str, bool]:
        """Параллельная инициализация множественных бирж."""
        logger.info(f"Initializing {len(configs)} exchanges")
        
        # Создаем задачи для параллельной инициализации
        tasks = []
        for config in configs:
            task = asyncio.create_task(self.add_exchange(config))
            tasks.append((config.name, task))
        
        # Ждем завершения всех задач
        results = {}
        for name, task in tasks:
            try:
                results[name] = await task
            except Exception as e:
                logger.error(f"Failed to initialize {name}: {e}")
                results[name] = False
        
        successful = sum(1 for success in results.values() if success)
        failed_exchanges = [name for name, success in results.items() if not success]
        
        if failed_exchanges:
            logger.warning(f"Failed to initialize exchanges: {', '.join(failed_exchanges)}")
        
        logger.info(f"Successfully initialized {successful}/{len(configs)} exchanges")
        
        # Продолжаем работу даже если некоторые биржи недоступны
        if successful == 0:
            logger.critical("No exchanges were successfully initialized!")
            raise RuntimeError("Failed to initialize any exchanges")
        elif successful < len(configs):
            logger.warning(f"Only {successful} out of {len(configs)} exchanges initialized. Continuing with available exchanges.")
        
        return results
    
    def get_exchange(self, name: str) -> Optional[CcxtExchangeWrapper]:
        """Получение биржи по имени."""
        return self.exchanges.get(name)
    
    def get_healthy_exchanges(self) -> List[str]:
        """Получение списка здоровых бирж."""
        return [
            name for name, exchange in self.exchanges.items()
            if exchange.get_status().status == ExchangeStatus.HEALTHY
        ]
    
    def get_all_exchanges(self) -> List[str]:
        """Получение списка всех бирж."""
        return list(self.exchanges.keys())
    
    def get_exchange_status(self) -> Dict[str, ExchangeInfo]:
        """Получение статуса всех бирж."""
        return {name: exchange.get_status() for name, exchange in self.exchanges.items()}
    
    async def restart_exchange(self, name: str) -> bool:
        """Перезапуск биржи."""
        exchange = self.exchanges.get(name)
        if not exchange:
            logger.error(f"Exchange {name} not found")
            return False
        
        logger.info(f"Restarting exchange {name}")
        
        # Закрываем текущее соединение
        await exchange.close()
        
        # Переинициализируем
        success = await exchange.initialize()
        
        if success:
            logger.info(f"Successfully restarted {name}")
        else:
            logger.error(f"Failed to restart {name}")
            
        return success
    
    async def close_all(self) -> None:
        """Закрытие всех соединений."""
        logger.info("Closing all exchange connections")
        
        close_tasks = []
        for name, exchange in self.exchanges.items():
            task = asyncio.create_task(exchange.close())
            close_tasks.append((name, task))
        
        # Ждем закрытия всех соединений с таймаутом
        for name, task in close_tasks:
            try:
                await asyncio.wait_for(task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning(f"Timeout closing {name}")
            except Exception as e:
                logger.error(f"Error closing {name}: {e}")
        
        self.exchanges.clear()
        logger.info("All exchanges closed")
