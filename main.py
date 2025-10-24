import asyncio
import logging
from typing import List
from utils.config_loader import load_config, load_api_keys
from utils.logger import setup_logging
from collectors.exchange_collector import ExchangeCollector
from publishers.rabbitmq_publisher import RabbitMQPublisher


async def run_exchange_collector(
    exchange_name: str,
    symbols: List[str],
    api_keys: dict,
    publisher: RabbitMQPublisher,
    interval: int
):
    """Запускает сборщик для одной биржи"""
    logger = logging.getLogger(exchange_name)
    collector = ExchangeCollector(exchange_name, api_keys.get(exchange_name, {}))
    
    logger.info(f"Starting collector for {exchange_name}")
    
    try:
        while True:
            for symbol in symbols:
                data = await collector.collect_futures_data(symbol)
                if data:
                    await publisher.publish(data)
            
            await asyncio.sleep(interval)
    finally:
        await collector.close()


async def main():
    # Загрузка конфигурации
    config = load_config()
    
    # Настройка логирования
    setup_logging(config.logging.level, config.logging.file)
    logger = logging.getLogger("main")
    
    # Отключаем DEBUG логи от aio_pika
    logging.getLogger("aio_pika").setLevel(logging.INFO)
    
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
    
    # Создаем задачи для каждой биржи
    tasks = []
    for exchange in config.exchanges:
        task = asyncio.create_task(
            run_exchange_collector(
                exchange,
                config.symbols,
                api_keys,
                publisher,
                config.collection.interval_seconds
            )
        )
        tasks.append(task)
    
    try:
        # Ждем выполнения всех задач
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        # Отменяем все задачи
        for task in tasks:
            task.cancel()
        await publisher.close()


if __name__ == "__main__":
    asyncio.run(main())
