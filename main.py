import asyncio
import logging
from utils.config_loader import load_config, load_api_keys
from utils.logger import setup_logging
from collectors.exchange_collector import ExchangeCollector
from publishers.rabbitmq_publisher import RabbitMQPublisher


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
    
    # Подключаемся к RabbitMQ
    publisher = RabbitMQPublisher(
        host=config.rabbitmq.host,
        port=config.rabbitmq.port,
        user=config.rabbitmq.user,
        password=config.rabbitmq.password,
        exchange_name=config.rabbitmq.exchange
    )
    await publisher.connect()
    
    # Создаем сборщик для Binance
    collector = ExchangeCollector("binance", api_keys.get("binance", {}))
    
    # Собираем данные для первого символа
    symbol = config.symbols[0]
    logger.info(f"Starting collection for {symbol} from Binance")
    
    try:
        while True:
            data = await collector.collect_futures_data(symbol)
            if data:
                # Отправляем в RabbitMQ
                success = await publisher.publish(data)
                if success:
                    logger.info(f"Data sent: bid={data.ticker.bid}, ask={data.ticker.ask}, "
                               f"funding_rate={data.funding_rate}")
            
            await asyncio.sleep(config.collection.interval_seconds)
    finally:
        await collector.close()
        await publisher.close()


if __name__ == "__main__":
    asyncio.run(main())
