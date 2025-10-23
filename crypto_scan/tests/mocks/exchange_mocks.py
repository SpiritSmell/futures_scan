"""
Mock implementations for cryptocurrency exchanges (CCXT).
Provides realistic mock data for testing without real API calls.
"""

import asyncio
import random
import time
from typing import Dict, List, Any, Optional
from unittest.mock import AsyncMock, Mock


class MockCCXTExchange:
    """Mock CCXT exchange implementation with realistic behavior."""
    
    def __init__(self, exchange_id: str, **kwargs):
        self.id = exchange_id
        self.name = exchange_id.capitalize()
        self.sandbox = kwargs.get('sandbox', False)
        self.timeout = kwargs.get('timeout', 30.0)
        self.rateLimit = kwargs.get('rateLimit', 1000)
        self.enableRateLimit = kwargs.get('enableRateLimit', True)
        
        # Mock capabilities
        self.has = {
            'fetchTickers': True,
            'fetchFundingRates': True,
            'fetchOrderBook': True,
            'fetchOHLCV': True,
            'fetchTrades': True,
            'fetchBalance': False,
            'createOrder': False,
            'cancelOrder': False
        }
        
        # Mock markets data
        self.markets = self._generate_mock_markets()
        self.symbols = list(self.markets.keys())
        
        # State tracking
        self._is_loaded = False
        self._call_count = 0
        self._last_call_time = 0
        self._failure_rate = 0.0  # Configurable failure rate for testing
        
    def _generate_mock_markets(self) -> Dict[str, Dict]:
        """Generate realistic mock market data."""
        base_currencies = ['BTC', 'ETH', 'BNB', 'ADA', 'DOT', 'LINK', 'UNI', 'AAVE', 'SUSHI', 'COMP']
        quote_currencies = ['USDT', 'BUSD', 'USD', 'EUR']
        
        markets = {}
        for base in base_currencies:
            for quote in quote_currencies:
                if base == quote:
                    continue
                    
                symbol = f"{base}/{quote}"
                markets[symbol] = {
                    'id': f"{base.lower()}{quote.lower()}",
                    'symbol': symbol,
                    'base': base,
                    'quote': quote,
                    'active': True,
                    'type': 'spot',
                    'spot': True,
                    'margin': False,
                    'future': False,
                    'option': False,
                    'contract': False,
                    'precision': {
                        'amount': 8,
                        'price': 8
                    },
                    'limits': {
                        'amount': {'min': 0.001, 'max': 1000000},
                        'price': {'min': 0.01, 'max': 1000000},
                        'cost': {'min': 10, 'max': None}
                    },
                    'info': {}
                }
        
        # Add some futures markets for supported exchanges
        if self.id in ['binance', 'bybit', 'bitget']:
            for base in base_currencies[:5]:  # Fewer futures markets
                symbol = f"{base}/USDT"
                markets[symbol] = {
                    'id': f"{base.lower()}usdt",
                    'symbol': symbol,
                    'base': base,
                    'quote': 'USDT',
                    'active': True,
                    'type': 'future',
                    'spot': False,
                    'margin': False,
                    'future': True,
                    'option': False,
                    'contract': True,
                    'precision': {'amount': 8, 'price': 8},
                    'limits': {
                        'amount': {'min': 0.001, 'max': 1000000},
                        'price': {'min': 0.01, 'max': 1000000},
                        'cost': {'min': 10, 'max': None}
                    },
                    'info': {}
                }
        
        return markets
    
    async def load_markets(self, reload: bool = False) -> Dict[str, Dict]:
        """Mock load_markets method."""
        await asyncio.sleep(0.1)  # Simulate network delay
        self._is_loaded = True
        return self.markets
    
    async def fetch_tickers(self, symbols: Optional[List[str]] = None) -> Dict[str, Dict]:
        """Mock fetch_tickers method with realistic data."""
        await self._simulate_call()
        
        if symbols is None:
            symbols = list(self.markets.keys())
        
        tickers = {}
        for symbol in symbols:
            if symbol not in self.markets:
                continue
                
            # Generate realistic ticker data
            base_price = random.uniform(0.1, 50000)
            change_percent = random.uniform(-10, 10)
            
            tickers[symbol] = {
                'symbol': symbol,
                'timestamp': int(time.time() * 1000),
                'datetime': time.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
                'high': base_price * (1 + abs(change_percent) / 100),
                'low': base_price * (1 - abs(change_percent) / 100),
                'bid': base_price * 0.999,
                'bidVolume': random.uniform(100, 10000),
                'ask': base_price * 1.001,
                'askVolume': random.uniform(100, 10000),
                'vwap': base_price,
                'open': base_price * (1 - change_percent / 100),
                'close': base_price,
                'last': base_price,
                'previousClose': base_price * (1 - change_percent / 100),
                'change': base_price * change_percent / 100,
                'percentage': change_percent,
                'average': base_price,
                'baseVolume': random.uniform(1000, 100000),
                'quoteVolume': random.uniform(1000000, 100000000),
                'info': {}
            }
        
        return tickers
    
    async def fetch_funding_rates(self, symbols: Optional[List[str]] = None) -> List[Dict]:
        """Mock fetch_funding_rates method."""
        await self._simulate_call()
        
        if not self.has.get('fetchFundingRates', False):
            raise Exception(f"{self.name} does not support funding rates")
        
        if symbols is None:
            # Only futures markets have funding rates
            symbols = [s for s, m in self.markets.items() if m.get('future', False)]
        
        funding_rates = []
        for symbol in symbols:
            if symbol not in self.markets or not self.markets[symbol].get('future', False):
                continue
                
            funding_rates.append({
                'symbol': symbol,
                'fundingRate': random.uniform(-0.001, 0.001),
                'fundingTimestamp': int(time.time() * 1000),
                'fundingDatetime': time.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
                'nextFundingRate': random.uniform(-0.001, 0.001),
                'nextFundingTimestamp': int(time.time() * 1000) + 8 * 3600 * 1000,
                'nextFundingDatetime': time.strftime('%Y-%m-%dT%H:%M:%S.000Z', 
                                                   time.gmtime(time.time() + 8 * 3600)),
                'info': {}
            })
        
        return funding_rates
    
    async def fetch_order_book(self, symbol: str, limit: Optional[int] = None) -> Dict:
        """Mock fetch_order_book method."""
        await self._simulate_call()
        
        if symbol not in self.markets:
            raise Exception(f"Market {symbol} not found")
        
        # Generate realistic order book
        base_price = random.uniform(0.1, 50000)
        
        bids = []
        asks = []
        
        # Generate bids (buy orders) below market price
        for i in range(limit or 20):
            price = base_price * (1 - (i + 1) * 0.001)
            volume = random.uniform(0.1, 100)
            bids.append([price, volume])
        
        # Generate asks (sell orders) above market price
        for i in range(limit or 20):
            price = base_price * (1 + (i + 1) * 0.001)
            volume = random.uniform(0.1, 100)
            asks.append([price, volume])
        
        return {
            'symbol': symbol,
            'bids': bids,
            'asks': asks,
            'timestamp': int(time.time() * 1000),
            'datetime': time.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            'nonce': None,
            'info': {}
        }
    
    async def close(self):
        """Mock close method."""
        await asyncio.sleep(0.01)  # Simulate cleanup time
        self._is_loaded = False
    
    async def _simulate_call(self):
        """Simulate API call with rate limiting and potential failures."""
        self._call_count += 1
        current_time = time.time()
        
        # Simulate rate limiting
        if self.enableRateLimit and self._last_call_time > 0:
            time_since_last = current_time - self._last_call_time
            min_interval = 1.0 / (self.rateLimit / 60)  # Convert per minute to per second
            
            if time_since_last < min_interval:
                await asyncio.sleep(min_interval - time_since_last)
        
        self._last_call_time = current_time
        
        # Simulate network delay
        await asyncio.sleep(random.uniform(0.05, 0.2))
        
        # Simulate failures based on configured failure rate
        if random.random() < self._failure_rate:
            error_types = [
                "NetworkError: Connection timeout",
                "ExchangeError: Rate limit exceeded",
                "ExchangeError: Invalid symbol",
                "RequestTimeout: Request timed out",
                "ExchangeNotAvailable: Exchange is under maintenance"
            ]
            raise Exception(random.choice(error_types))
    
    def set_failure_rate(self, rate: float):
        """Set the failure rate for testing error scenarios."""
        self._failure_rate = max(0.0, min(1.0, rate))
    
    def get_call_statistics(self) -> Dict[str, Any]:
        """Get call statistics for testing."""
        return {
            'total_calls': self._call_count,
            'failure_rate': self._failure_rate,
            'is_loaded': self._is_loaded,
            'market_count': len(self.markets)
        }


class MockExchangeFactory:
    """Factory for creating mock exchanges."""
    
    SUPPORTED_EXCHANGES = {
        'binance': {
            'name': 'Binance',
            'has_funding_rates': True,
            'market_count': 40,
            'default_rate_limit': 1200
        },
        'bybit': {
            'name': 'Bybit',
            'has_funding_rates': True,
            'market_count': 30,
            'default_rate_limit': 600
        },
        'bitget': {
            'name': 'Bitget',
            'has_funding_rates': True,
            'market_count': 25,
            'default_rate_limit': 600
        },
        'htx': {
            'name': 'HTX',
            'has_funding_rates': True,
            'market_count': 35,
            'default_rate_limit': 100
        },
        'gateio': {
            'name': 'Gate.io',
            'has_funding_rates': True,
            'market_count': 50,
            'default_rate_limit': 900
        }
    }
    
    @classmethod
    def create_exchange(cls, exchange_id: str, **kwargs) -> MockCCXTExchange:
        """Create a mock exchange instance."""
        if exchange_id not in cls.SUPPORTED_EXCHANGES:
            raise ValueError(f"Unsupported exchange: {exchange_id}")
        
        exchange_info = cls.SUPPORTED_EXCHANGES[exchange_id]
        
        # Set default parameters based on exchange
        default_kwargs = {
            'rateLimit': exchange_info['default_rate_limit'],
            'timeout': 30.0,
            'enableRateLimit': True,
            'sandbox': False
        }
        default_kwargs.update(kwargs)
        
        exchange = MockCCXTExchange(exchange_id, **default_kwargs)
        
        # Configure exchange-specific features
        if not exchange_info['has_funding_rates']:
            exchange.has['fetchFundingRates'] = False
        
        return exchange
    
    @classmethod
    def create_all_exchanges(cls, **common_kwargs) -> Dict[str, MockCCXTExchange]:
        """Create all supported mock exchanges."""
        exchanges = {}
        for exchange_id in cls.SUPPORTED_EXCHANGES:
            exchanges[exchange_id] = cls.create_exchange(exchange_id, **common_kwargs)
        return exchanges
    
    @classmethod
    def create_failing_exchange(cls, exchange_id: str, failure_rate: float = 0.5, **kwargs) -> MockCCXTExchange:
        """Create a mock exchange that fails frequently for testing error handling."""
        exchange = cls.create_exchange(exchange_id, **kwargs)
        exchange.set_failure_rate(failure_rate)
        return exchange


# Utility functions for testing
def create_mock_ticker_data(symbol: str, base_price: float = 100.0) -> Dict[str, Any]:
    """Create realistic mock ticker data for a specific symbol."""
    change_percent = random.uniform(-5, 5)
    
    return {
        'symbol': symbol,
        'timestamp': int(time.time() * 1000),
        'datetime': time.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
        'high': base_price * (1 + abs(change_percent) / 100),
        'low': base_price * (1 - abs(change_percent) / 100),
        'bid': base_price * 0.999,
        'bidVolume': random.uniform(100, 10000),
        'ask': base_price * 1.001,
        'askVolume': random.uniform(100, 10000),
        'vwap': base_price,
        'open': base_price * (1 - change_percent / 100),
        'close': base_price,
        'last': base_price,
        'previousClose': base_price * (1 - change_percent / 100),
        'change': base_price * change_percent / 100,
        'percentage': change_percent,
        'average': base_price,
        'baseVolume': random.uniform(1000, 100000),
        'quoteVolume': random.uniform(1000000, 100000000),
        'info': {}
    }


def create_mock_funding_rate_data(symbol: str) -> Dict[str, Any]:
    """Create realistic mock funding rate data for a specific symbol."""
    return {
        'symbol': symbol,
        'fundingRate': random.uniform(-0.001, 0.001),
        'fundingTimestamp': int(time.time() * 1000),
        'fundingDatetime': time.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
        'nextFundingRate': random.uniform(-0.001, 0.001),
        'nextFundingTimestamp': int(time.time() * 1000) + 8 * 3600 * 1000,
        'nextFundingDatetime': time.strftime('%Y-%m-%dT%H:%M:%S.000Z', 
                                           time.gmtime(time.time() + 8 * 3600)),
        'info': {}
    }
