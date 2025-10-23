import asyncio
import logging
from utils.config_loader import load_config, load_api_keys
from utils.logger import setup_logging
from collectors.exchange_collector import ExchangeCollector


async def main():
    # Загрузка конфигурации
    config = load_config()
    
    # Настройка логирования
    setup_logging(config.logging.level, config.logging.file)
    logger = logging.getLogger("main")
    
    logger.info("Futures Data Collector started")
    logger.info(f"Exchanges: {config.exchanges}")
    logger.info(f"Symbols: {config.symbols}")
    logger.info(f"Collection interval: {config.collection.interval_seconds}s")
    
    # Загружаем API ключи
    api_keys = load_api_keys(config.api_keys_file)
    
    # Создаем сборщик для Binance
    collector = ExchangeCollector("binance", api_keys.get("binance", {}))
    
    # Собираем данные для первого символа
    symbol = config.symbols[0]
    logger.info(f"Starting collection for {symbol} from Binance")
    
    try:
        while True:
            data = await collector.collect_futures_data(symbol)
            if data:
                logger.info(f"Collected data: bid={data.ticker.bid}, ask={data.ticker.ask}, "
                           f"funding_rate={data.funding_rate}, orderbook_depth={len(data.orderbook.bids)}")
            
            await asyncio.sleep(config.collection.interval_seconds)
    finally:
        await collector.close()


if __name__ == "__main__":
    asyncio.run(main())
