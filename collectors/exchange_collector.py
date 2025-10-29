import ccxt.async_support as ccxt
import time
from typing import Optional
from collectors.base_collector import BaseCollector
from models.futures_data import FuturesData, TickerData, OrderbookData


class ExchangeCollector(BaseCollector):
    def __init__(self, exchange_name: str, api_keys: dict, retry_attempts: int = 3, retry_delays: list = None):
        super().__init__(exchange_name, api_keys, retry_attempts, retry_delays)
        # Создаем экземпляр биржи через ccxt
        exchange_class = getattr(ccxt, exchange_name)
        self.exchange = exchange_class({
            'apiKey': api_keys.get('apiKey'),
            'secret': api_keys.get('secret'),
            'options': {'defaultType': 'swap'},  # swap = perpetual futures
            'enableRateLimit': True
        })
        # Отключаем DEBUG логи от ccxt
        self.exchange.logger.setLevel('WARNING')
    
    async def collect_futures_data(self, symbol: str) -> Optional[FuturesData]:
        try:
            self.logger.debug(f"Fetching data for {symbol}")
            
            # Получаем ticker с retry
            ticker = await self.retry_with_backoff(self.exchange.fetch_ticker, symbol)
            
            # Получаем orderbook с retry
            orderbook = await self.retry_with_backoff(self.exchange.fetch_order_book, symbol)
            
            # Получаем bid/ask из orderbook если нет в ticker
            best_bid = ticker.get('bid') or (orderbook['bids'][0][0] if orderbook['bids'] else 0.0)
            best_ask = ticker.get('ask') or (orderbook['asks'][0][0] if orderbook['asks'] else 0.0)
            
            # Получаем funding rate
            funding_rate = None
            next_funding_time = None
            try:
                funding = await self.exchange.fetch_funding_rate(symbol)
                funding_rate = funding.get('fundingRate')
                next_funding_time = funding.get('fundingTimestamp')
            except Exception as e:
                self.logger.debug(f"Funding rate not available for {symbol}: {e}")
            
            # Получаем mark price
            mark_price = None
            try:
                if hasattr(self.exchange, 'fetch_mark_price'):
                    mark_data = await self.exchange.fetch_mark_price(symbol)
                    mark_price = mark_data.get('markPrice')
            except Exception as e:
                self.logger.debug(f"Mark price not available for {symbol}: {e}")
            
            # Формируем данные
            futures_data = FuturesData(
                exchange=self.exchange_name,
                symbol=symbol,
                timestamp=int(time.time() * 1000),
                ticker=TickerData(
                    bid=best_bid,
                    ask=best_ask,
                    last=ticker.get('last') or ticker.get('close') or 0.0,
                    volume_24h=ticker.get('quoteVolume') or ticker.get('volume')
                ),
                orderbook=OrderbookData(
                    bids=orderbook['bids'],
                    asks=orderbook['asks'],
                    timestamp=orderbook.get('timestamp')
                ),
                funding_rate=funding_rate,
                next_funding_time=next_funding_time,
                mark_price=mark_price
            )
            
            self.logger.info(f"Successfully collected {symbol}")
            return futures_data
            
        except Exception as e:
            self.logger.error(f"Failed to collect {symbol}: {e}")
            return None
    
    async def close(self):
        await self.exchange.close()
