import asyncio
import logging
from typing import List
from utils.config_loader import load_config, load_api_keys
from utils.logger import setup_logging
from utils.statistics import Statistics
from collectors.exchange_collector import ExchangeCollector
from publishers.rabbitmq_publisher import RabbitMQPublisher


async def run_exchange_collector(
    exchange_name: str,
    symbols: List[str],
    api_keys: dict,
    publisher: RabbitMQPublisher,
    interval: int,
    retry_attempts: int,
    retry_delays: List[int],
    stats: Statistics
):
    """Запускает сборщик для одной биржи"""
    logger = logging.getLogger(exchange_name)
    collector = ExchangeCollector(
        exchange_name, 
        api_keys.get(exchange_name, {}),
        retry_attempts,
        retry_delays
    )
    
    logger.info(f"Starting collector for {exchange_name}")
    
    try:
        while True:
            for symbol in symbols:
                data = await collector.collect_futures_data(symbol)
                if data:
                    stats.record_success(exchange_name)
                    success = await publisher.publish(data)
                    if success:
                        stats.record_published()
                    else:
                        stats.record_publish_failed()
                else:
                    stats.record_error(exchange_name)
            
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info(f"Collector for {exchange_name} cancelled")
    except Exception as e:
        logger.error(f"Unexpected error in {exchange_name}: {e}")
    finally:
        try:
            await collector.close()
        except Exception as e:
            logger.debug(f"Error closing {exchange_name}: {e}")


async def print_statistics_periodically(stats: Statistics, interval: int = 60):
    """Выводит статистику каждые N секунд"""
    try:
        while True:
            await asyncio.sleep(interval)
            stats.print_and_reset()
    except asyncio.CancelledError:
        pass


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
    
    # Создаем объект статистики
    stats = Statistics()
    
    # Создаем задачи для каждой биржи
    tasks = []
    for exchange in config.exchanges:
        task = asyncio.create_task(
            run_exchange_collector(
                exchange,
                config.symbols,
                api_keys,
                publisher,
                config.collection.interval_seconds,
                config.collection.retry_attempts,
                config.collection.retry_delays,
                stats
            )
        )
        tasks.append(task)
    
    # Добавляем задачу для вывода статистики
    stats_task = asyncio.create_task(print_statistics_periodically(stats))
    tasks.append(stats_task)
    
    try:
        # Ждем выполнения всех задач
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # Отменяем все задачи
        logger.info("Cancelling all tasks...")
        for task in tasks:
            if not task.done():
                task.cancel()
        
        # Ждем завершения всех задач
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Закрываем соединение с RabbitMQ
        await publisher.close()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
