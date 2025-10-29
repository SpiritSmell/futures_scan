from pydantic import BaseModel
from typing import Optional, List, Dict, Any


class TickerData(BaseModel):
    bid: float
    ask: float
    last: float
    volume_24h: Optional[float] = None


class OrderbookData(BaseModel):
    bids: List[List[float]]
    asks: List[List[float]]
    timestamp: Optional[int] = None


class FuturesData(BaseModel):
    exchange: str
    symbol: str
    timestamp: int
    ticker: TickerData
    orderbook: OrderbookData
    funding_rate: Optional[float] = None
    next_funding_time: Optional[int] = None
    mark_price: Optional[float] = None
    extra: Optional[Dict[str, Any]] = None
